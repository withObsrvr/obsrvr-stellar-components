package ledgerfixture

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type RecordOptions struct {
	ManifestPath   string
	ObjectStoreURL string
	BatchesPerFile int
}

type chunkWriter struct {
	file   *os.File
	hash   hash.Hash
	writer io.Writer
	path   string
	bytes  int64
	count  int
	start  uint32
	end    uint32
}

// RecordJSONL converts the jsonl-sink output into hashed, length-delimited
// protobuf fixture chunks. Existing files are never overwritten.
func RecordJSONL(input io.Reader, options RecordOptions) (*Manifest, error) {
	if options.ManifestPath == "" {
		return nil, fmt.Errorf("manifest path is required")
	}
	if options.BatchesPerFile <= 0 {
		return nil, fmt.Errorf("batches per file must be positive")
	}
	dir := filepath.Dir(options.ManifestPath)
	if err := os.MkdirAll(dir, 0o755); err != nil && dir != "." {
		return nil, fmt.Errorf("create fixture directory: %w", err)
	}
	if _, err := os.Stat(options.ManifestPath); err == nil {
		return nil, fmt.Errorf("manifest %q already exists", options.ManifestPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("stat manifest %q: %w", options.ManifestPath, err)
	}

	manifest := &Manifest{
		FormatVersion:  FormatVersion,
		MessageType:    MessageType,
		ObjectStoreURL: options.ObjectStoreURL,
	}
	reader := bufio.NewReader(input)
	var chunk *chunkWriter
	defer func() {
		if chunk != nil && chunk.file != nil {
			_ = chunk.file.Close()
		}
	}()
	for lineNumber := 1; ; lineNumber++ {
		line, err := reader.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read JSONL line %d: %w", lineNumber, err)
		}
		line = []byte(strings.TrimSpace(string(line)))
		if len(line) > 0 {
			var batch componentsv1.LedgerBatch
			if unmarshalErr := protojson.Unmarshal(line, &batch); unmarshalErr != nil {
				return nil, fmt.Errorf("decode JSONL line %d: %w", lineNumber, unmarshalErr)
			}
			if validateErr := validateRecordedBatch(manifest, &batch); validateErr != nil {
				return nil, fmt.Errorf("JSONL line %d: %w", lineNumber, validateErr)
			}
			if chunk == nil || chunk.count == options.BatchesPerFile {
				if chunk != nil {
					if closeErr := finishChunk(manifest, chunk); closeErr != nil {
						return nil, closeErr
					}
				}
				chunk, err = openChunk(options.ManifestPath, len(manifest.Files), batch.LedgerSequence)
				if err != nil {
					return nil, err
				}
			}
			written, writeErr := WriteDelimited(chunk.writer, &batch)
			chunk.bytes += written
			if writeErr != nil {
				_ = chunk.file.Close()
				return nil, fmt.Errorf("write fixture ledger %d: %w", batch.LedgerSequence, writeErr)
			}
			chunk.count++
			chunk.end = batch.LedgerSequence
			manifest.BatchCount++
			manifest.LedgerEnd = batch.LedgerSequence
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	if chunk != nil {
		if err := finishChunk(manifest, chunk); err != nil {
			return nil, err
		}
	}
	if manifest.BatchCount == 0 {
		return nil, fmt.Errorf("input contains no LedgerBatch records")
	}
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("validate recorded manifest: %w", err)
	}
	if err := writeManifest(options.ManifestPath, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func validateRecordedBatch(manifest *Manifest, batch *componentsv1.LedgerBatch) error {
	if batch.LedgerSequence == 0 {
		return fmt.Errorf("ledger_sequence must be positive")
	}
	if batch.NetworkPassphrase == "" || batch.SchemaVersion == "" || batch.ExtractionVersion == "" {
		return fmt.Errorf("network_passphrase, schema_version, and extraction_version are required")
	}
	if manifest.BatchCount == 0 {
		manifest.NetworkPassphrase = batch.NetworkPassphrase
		manifest.SchemaVersion = batch.SchemaVersion
		manifest.ExtractionVersion = batch.ExtractionVersion
		manifest.LedgerStart = batch.LedgerSequence
		return nil
	}
	expected := manifest.LedgerEnd + 1
	if batch.LedgerSequence != expected {
		return fmt.Errorf("ledger_sequence is %d, want contiguous ledger %d", batch.LedgerSequence, expected)
	}
	if batch.NetworkPassphrase != manifest.NetworkPassphrase {
		return fmt.Errorf("network_passphrase changed at ledger %d", batch.LedgerSequence)
	}
	if batch.SchemaVersion != manifest.SchemaVersion {
		return fmt.Errorf("schema_version changed from %q to %q", manifest.SchemaVersion, batch.SchemaVersion)
	}
	if batch.ExtractionVersion != manifest.ExtractionVersion {
		return fmt.Errorf("extraction_version changed from %q to %q", manifest.ExtractionVersion, batch.ExtractionVersion)
	}
	return nil
}

func openChunk(manifestPath string, index int, start uint32) (*chunkWriter, error) {
	base := filepath.Base(manifestPath)
	if strings.HasSuffix(base, ".manifest.json") {
		base = strings.TrimSuffix(base, ".manifest.json")
	} else {
		base = strings.TrimSuffix(base, filepath.Ext(base))
	}
	name := fmt.Sprintf("%s-%05d.pb", base, index)
	path := filepath.Join(filepath.Dir(manifestPath), name)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create fixture chunk %q: %w", path, err)
	}
	hasher := sha256.New()
	return &chunkWriter{
		file:   file,
		hash:   hasher,
		writer: io.MultiWriter(file, hasher),
		path:   name,
		start:  start,
	}, nil
}

func finishChunk(manifest *Manifest, chunk *chunkWriter) error {
	if err := chunk.file.Sync(); err != nil {
		_ = chunk.file.Close()
		return fmt.Errorf("sync fixture chunk %q: %w", chunk.path, err)
	}
	if err := chunk.file.Close(); err != nil {
		return fmt.Errorf("close fixture chunk %q: %w", chunk.path, err)
	}
	chunk.file = nil
	manifest.Files = append(manifest.Files, ManifestFile{
		Path:        chunk.path,
		SHA256:      hex.EncodeToString(chunk.hash.Sum(nil)),
		Bytes:       chunk.bytes,
		BatchCount:  chunk.count,
		LedgerStart: chunk.start,
		LedgerEnd:   chunk.end,
	})
	return nil
}

func writeManifest(path string, manifest *Manifest) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create fixture manifest %q: %w", path, err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(manifest); err != nil {
		_ = file.Close()
		return fmt.Errorf("write fixture manifest: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync fixture manifest: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close fixture manifest: %w", err)
	}
	return nil
}

package ledgerfixture

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
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
	"google.golang.org/protobuf/proto"
)

const (
	FormatVersion  = "stellar-ledger-batch-delimited-v1"
	MessageType    = "stellar.components.v1.LedgerBatch"
	MaxMessageSize = 64 * 1024 * 1024
)

// Manifest describes an ordered set of length-delimited LedgerBatch protobufs.
// File paths are relative to the manifest so the corpus can be moved intact.
type Manifest struct {
	FormatVersion     string         `json:"format_version"`
	MessageType       string         `json:"message_type"`
	NetworkPassphrase string         `json:"network_passphrase"`
	LedgerStart       uint32         `json:"ledger_start"`
	LedgerEnd         uint32         `json:"ledger_end"`
	BatchCount        int            `json:"batch_count"`
	SchemaVersion     string         `json:"schema_version"`
	ExtractionVersion string         `json:"extraction_version"`
	ObjectStoreURL    string         `json:"object_store_url,omitempty"`
	Files             []ManifestFile `json:"files"`
}

type ManifestFile struct {
	Path        string `json:"path"`
	SHA256      string `json:"sha256"`
	Bytes       int64  `json:"bytes"`
	BatchCount  int    `json:"batch_count"`
	LedgerStart uint32 `json:"ledger_start"`
	LedgerEnd   uint32 `json:"ledger_end"`
}

// ReadManifest parses and validates fixture metadata without reading payload
// files. Consume payloads with RangeReader for inline verification, or call
// VerifyRange before using another reader.
func ReadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read fixture manifest: %w", err)
	}
	var manifest Manifest
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode fixture manifest: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode fixture manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// LoadManifest verifies the complete corpus. Range workers should use
// ReadManifest followed by VerifyRange to avoid hashing unrelated chunks.
func LoadManifest(path string) (*Manifest, error) {
	manifest, err := ReadManifest(path)
	if err != nil {
		return nil, err
	}
	if err := VerifyRange(path, manifest, manifest.LedgerStart, manifest.LedgerEnd); err != nil {
		return nil, err
	}
	return manifest, nil
}

// VerifyRange hashes only files that overlap the inclusive selected range.
// The manifest itself still guarantees complete, ordered corpus coverage.
func VerifyRange(path string, manifest *Manifest, start, end uint32) error {
	if manifest == nil {
		return fmt.Errorf("fixture manifest is nil")
	}
	if start < manifest.LedgerStart || end > manifest.LedgerEnd || end < start {
		return fmt.Errorf("fixture verification range %d-%d falls outside manifest %d-%d", start, end, manifest.LedgerStart, manifest.LedgerEnd)
	}
	base := filepath.Dir(path)
	verified := 0
	for _, file := range manifest.Files {
		if file.LedgerEnd < start || file.LedgerStart > end {
			continue
		}
		fullPath, err := resolveFile(base, file.Path)
		if err != nil {
			return err
		}
		info, err := os.Lstat(fullPath)
		if err != nil {
			return fmt.Errorf("stat fixture file %q: %w", file.Path, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("fixture file %q is not a regular file", file.Path)
		}
		if info.Size() != file.Bytes {
			return fmt.Errorf("fixture file %q size is %d, manifest requires %d", file.Path, info.Size(), file.Bytes)
		}
		actual, err := hashFile(fullPath)
		if err != nil {
			return fmt.Errorf("hash fixture file %q: %w", file.Path, err)
		}
		if !strings.EqualFold(actual, file.SHA256) {
			return fmt.Errorf("fixture file %q sha256 is %s, manifest requires %s", file.Path, actual, file.SHA256)
		}
		verified++
	}
	if verified == 0 {
		return fmt.Errorf("fixture range %d-%d overlaps no payload files", start, end)
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func (m *Manifest) Validate() error {
	if m.FormatVersion != FormatVersion {
		return fmt.Errorf("fixture format_version is %q, want %q", m.FormatVersion, FormatVersion)
	}
	if m.MessageType != MessageType {
		return fmt.Errorf("fixture message_type is %q, want %q", m.MessageType, MessageType)
	}
	if m.NetworkPassphrase == "" {
		return fmt.Errorf("fixture network_passphrase is required")
	}
	if m.SchemaVersion == "" {
		return fmt.Errorf("fixture schema_version is required")
	}
	if m.ExtractionVersion == "" {
		return fmt.Errorf("fixture extraction_version is required")
	}
	if m.BatchCount <= 0 {
		return fmt.Errorf("fixture batch_count must be positive")
	}
	if len(m.Files) == 0 {
		return fmt.Errorf("fixture files must not be empty")
	}
	if m.LedgerEnd < m.LedgerStart || uint64(m.LedgerEnd)-uint64(m.LedgerStart)+1 != uint64(m.BatchCount) {
		return fmt.Errorf("fixture ledger range %d-%d does not contain batch_count %d", m.LedgerStart, m.LedgerEnd, m.BatchCount)
	}

	expectedLedger := m.LedgerStart
	totalBatches := 0
	seenPaths := make(map[string]struct{}, len(m.Files))
	for i, file := range m.Files {
		if _, err := resolveFile(".", file.Path); err != nil {
			return err
		}
		cleanPath := filepath.Clean(file.Path)
		if _, ok := seenPaths[cleanPath]; ok {
			return fmt.Errorf("fixture file path %q is duplicated", file.Path)
		}
		seenPaths[cleanPath] = struct{}{}
		if _, err := hex.DecodeString(file.SHA256); err != nil || len(file.SHA256) != sha256.Size*2 {
			return fmt.Errorf("fixture file %q has invalid sha256", file.Path)
		}
		if file.Bytes <= 0 {
			return fmt.Errorf("fixture file %q bytes must be positive", file.Path)
		}
		if file.BatchCount <= 0 {
			return fmt.Errorf("fixture file %q batch_count must be positive", file.Path)
		}
		if file.LedgerStart != expectedLedger {
			return fmt.Errorf("fixture file %d starts at ledger %d, want %d", i, file.LedgerStart, expectedLedger)
		}
		if file.LedgerEnd < file.LedgerStart || uint64(file.LedgerEnd)-uint64(file.LedgerStart)+1 != uint64(file.BatchCount) {
			return fmt.Errorf("fixture file %q range %d-%d does not contain batch_count %d", file.Path, file.LedgerStart, file.LedgerEnd, file.BatchCount)
		}
		totalBatches += file.BatchCount
		expectedLedger = file.LedgerEnd + 1
	}
	if totalBatches != m.BatchCount {
		return fmt.Errorf("fixture files contain %d batches, manifest requires %d", totalBatches, m.BatchCount)
	}
	if expectedLedger-1 != m.LedgerEnd {
		return fmt.Errorf("fixture files end at ledger %d, manifest requires %d", expectedLedger-1, m.LedgerEnd)
	}
	return nil
}

func resolveFile(base, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", fmt.Errorf("fixture file path %q must be relative", relative)
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("fixture file path %q escapes the manifest directory", relative)
	}
	return filepath.Join(base, clean), nil
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// Reader streams batches without retaining the fixture corpus in memory.
type Reader struct {
	manifest *Manifest
	base     string
	file     *os.File
	buffer   *bufio.Reader
	fileIdx  int
	fileSeen int
	total    int
}

func NewReader(manifestPath string, manifest *Manifest) *Reader {
	return &Reader{manifest: manifest, base: filepath.Dir(manifestPath)}
}

func (r *Reader) Next() (*componentsv1.LedgerBatch, error) {
	for {
		if r.file == nil {
			if r.fileIdx >= len(r.manifest.Files) {
				if r.total != r.manifest.BatchCount {
					return nil, fmt.Errorf("fixture ended after %d batches, want %d", r.total, r.manifest.BatchCount)
				}
				return nil, io.EOF
			}
			path, err := resolveFile(r.base, r.manifest.Files[r.fileIdx].Path)
			if err != nil {
				return nil, err
			}
			r.file, err = os.Open(path)
			if err != nil {
				return nil, fmt.Errorf("open fixture file %q: %w", r.manifest.Files[r.fileIdx].Path, err)
			}
			r.buffer = bufio.NewReader(r.file)
			r.fileSeen = 0
		}

		batch, err := readDelimited(r.buffer)
		if errors.Is(err, io.EOF) {
			fileSpec := r.manifest.Files[r.fileIdx]
			if r.fileSeen != fileSpec.BatchCount {
				return nil, fmt.Errorf("fixture file %q contains %d batches, want %d", fileSpec.Path, r.fileSeen, fileSpec.BatchCount)
			}
			if err := r.file.Close(); err != nil {
				return nil, fmt.Errorf("close fixture file %q: %w", fileSpec.Path, err)
			}
			r.file = nil
			r.buffer = nil
			r.fileIdx++
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read fixture file %q batch %d: %w", r.manifest.Files[r.fileIdx].Path, r.fileSeen, err)
		}
		expected := r.manifest.LedgerStart + uint32(r.total)
		if batch.LedgerSequence != expected {
			return nil, fmt.Errorf("fixture batch %d has ledger %d, want %d", r.total, batch.LedgerSequence, expected)
		}
		if batch.NetworkPassphrase != r.manifest.NetworkPassphrase {
			return nil, fmt.Errorf("fixture ledger %d network passphrase does not match manifest", batch.LedgerSequence)
		}
		if batch.SchemaVersion != r.manifest.SchemaVersion {
			return nil, fmt.Errorf("fixture ledger %d schema_version is %q, want %q", batch.LedgerSequence, batch.SchemaVersion, r.manifest.SchemaVersion)
		}
		if batch.ExtractionVersion != r.manifest.ExtractionVersion {
			return nil, fmt.Errorf("fixture ledger %d extraction_version is %q, want %q", batch.LedgerSequence, batch.ExtractionVersion, r.manifest.ExtractionVersion)
		}
		r.fileSeen++
		r.total++
		return batch, nil
	}
}

func (r *Reader) Close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	r.buffer = nil
	return err
}

// RangeReader opens only fixture chunks that overlap one assigned shard. It
// may decode a bounded prefix inside the first chunk, but never scans earlier
// chunks or reads beyond the selected end ledger.
type RangeReader struct {
	manifest *Manifest
	base     string
	start    uint32
	end      uint32
	file     *os.File
	buffer   *bufio.Reader
	hasher   hash.Hash
	fileIdx  int
	lastIdx  int
	fileSeen int
	done     bool
}

func NewRangeReader(manifestPath string, manifest *Manifest, start, end uint32) (*RangeReader, error) {
	if manifest == nil {
		return nil, fmt.Errorf("fixture manifest is nil")
	}
	if start < manifest.LedgerStart || end > manifest.LedgerEnd || end < start {
		return nil, fmt.Errorf("fixture reader range %d-%d falls outside manifest %d-%d", start, end, manifest.LedgerStart, manifest.LedgerEnd)
	}
	first, last := -1, -1
	for index, file := range manifest.Files {
		if file.LedgerEnd < start || file.LedgerStart > end {
			continue
		}
		if first == -1 {
			first = index
		}
		last = index
	}
	if first == -1 {
		return nil, fmt.Errorf("fixture reader range %d-%d overlaps no payload files", start, end)
	}
	return &RangeReader{
		manifest: manifest,
		base:     filepath.Dir(manifestPath),
		start:    start,
		end:      end,
		fileIdx:  first,
		lastIdx:  last,
	}, nil
}

func (r *RangeReader) Next() (*componentsv1.LedgerBatch, error) {
	for {
		if r.done {
			return nil, io.EOF
		}
		if r.file == nil {
			if r.fileIdx > r.lastIdx {
				return nil, fmt.Errorf("fixture range ended before ledger %d", r.end)
			}
			fileSpec := r.manifest.Files[r.fileIdx]
			path, err := resolveFile(r.base, fileSpec.Path)
			if err != nil {
				return nil, err
			}
			info, err := os.Lstat(path)
			if err != nil {
				return nil, fmt.Errorf("stat fixture file %q: %w", fileSpec.Path, err)
			}
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("fixture file %q is not a regular file", fileSpec.Path)
			}
			if info.Size() != fileSpec.Bytes {
				return nil, fmt.Errorf("fixture file %q size is %d, manifest requires %d", fileSpec.Path, info.Size(), fileSpec.Bytes)
			}
			r.file, err = os.Open(path)
			if err != nil {
				return nil, fmt.Errorf("open fixture file %q: %w", fileSpec.Path, err)
			}
			r.hasher = sha256.New()
			r.buffer = bufio.NewReader(io.TeeReader(r.file, r.hasher))
			r.fileSeen = 0
		}

		fileSpec := r.manifest.Files[r.fileIdx]
		batch, err := readDelimited(r.buffer)
		if errors.Is(err, io.EOF) {
			if r.fileSeen != fileSpec.BatchCount {
				return nil, fmt.Errorf("fixture file %q contains %d batches, want %d", fileSpec.Path, r.fileSeen, fileSpec.BatchCount)
			}
			if err := r.verifyAndCloseCurrent(false); err != nil {
				return nil, err
			}
			r.fileIdx++
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read fixture file %q batch %d: %w", fileSpec.Path, r.fileSeen, err)
		}
		expected := fileSpec.LedgerStart + uint32(r.fileSeen)
		if batch.LedgerSequence != expected {
			return nil, fmt.Errorf("fixture file %q batch %d has ledger %d, want %d", fileSpec.Path, r.fileSeen, batch.LedgerSequence, expected)
		}
		if batch.NetworkPassphrase != r.manifest.NetworkPassphrase {
			return nil, fmt.Errorf("fixture ledger %d network passphrase does not match manifest", batch.LedgerSequence)
		}
		if batch.SchemaVersion != r.manifest.SchemaVersion {
			return nil, fmt.Errorf("fixture ledger %d schema_version is %q, want %q", batch.LedgerSequence, batch.SchemaVersion, r.manifest.SchemaVersion)
		}
		if batch.ExtractionVersion != r.manifest.ExtractionVersion {
			return nil, fmt.Errorf("fixture ledger %d extraction_version is %q, want %q", batch.LedgerSequence, batch.ExtractionVersion, r.manifest.ExtractionVersion)
		}
		r.fileSeen++
		if batch.LedgerSequence < r.start {
			continue
		}
		if batch.LedgerSequence > r.end {
			return nil, fmt.Errorf("fixture reader passed selected end %d at ledger %d", r.end, batch.LedgerSequence)
		}
		if batch.LedgerSequence == r.end {
			if batch.LedgerSequence == fileSpec.LedgerEnd && r.fileSeen != fileSpec.BatchCount {
				return nil, fmt.Errorf("fixture file %q contains at least %d batches, want %d", fileSpec.Path, r.fileSeen, fileSpec.BatchCount)
			}
			if err := r.verifyAndCloseCurrent(true); err != nil {
				return nil, err
			}
			r.done = true
		}
		return batch, nil
	}
}

// verifyAndCloseCurrent finishes hashing the selected chunk while it is still
// in the filesystem cache from decoding. A partial final chunk is drained as
// raw bytes: the manifest hash authenticates the bytes beyond the requested
// ledger without allocating or decoding unrelated LedgerBatch messages.
func (r *RangeReader) verifyAndCloseCurrent(drain bool) error {
	fileSpec := r.manifest.Files[r.fileIdx]
	if drain {
		if _, err := io.Copy(io.Discard, r.buffer); err != nil {
			_ = r.closeCurrent()
			return fmt.Errorf("finish hashing fixture file %q: %w", fileSpec.Path, err)
		}
	}
	actual := hex.EncodeToString(r.hasher.Sum(nil))
	closeErr := r.closeCurrent()
	if !strings.EqualFold(actual, fileSpec.SHA256) {
		return fmt.Errorf("fixture file %q sha256 is %s, manifest requires %s", fileSpec.Path, actual, fileSpec.SHA256)
	}
	if closeErr != nil {
		return fmt.Errorf("close fixture file %q: %w", fileSpec.Path, closeErr)
	}
	return nil
}

func (r *RangeReader) closeCurrent() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	r.buffer = nil
	r.hasher = nil
	return err
}

func (r *RangeReader) Close() error {
	r.done = true
	return r.closeCurrent()
}

func readDelimited(reader *bufio.Reader) (*componentsv1.LedgerBatch, error) {
	size, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, fmt.Errorf("zero-length protobuf message")
	}
	if size > MaxMessageSize {
		return nil, fmt.Errorf("protobuf message is %d bytes, maximum is %d", size, MaxMessageSize)
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, fmt.Errorf("read %d-byte protobuf message: %w", size, err)
	}
	var batch componentsv1.LedgerBatch
	if err := proto.Unmarshal(data, &batch); err != nil {
		return nil, fmt.Errorf("unmarshal LedgerBatch: %w", err)
	}
	return &batch, nil
}

func WriteDelimited(writer io.Writer, batch *componentsv1.LedgerBatch) (int64, error) {
	data, err := proto.Marshal(batch)
	if err != nil {
		return 0, fmt.Errorf("marshal LedgerBatch: %w", err)
	}
	if len(data) == 0 {
		return 0, fmt.Errorf("refusing to write empty LedgerBatch")
	}
	if len(data) > MaxMessageSize {
		return 0, fmt.Errorf("protobuf message is %d bytes, maximum is %d", len(data), MaxMessageSize)
	}
	var prefix [binary.MaxVarintLen64]byte
	prefixLength := binary.PutUvarint(prefix[:], uint64(len(data)))
	n, err := writer.Write(prefix[:prefixLength])
	written := int64(n)
	if err != nil {
		return written, err
	}
	if n != prefixLength {
		return written, io.ErrShortWrite
	}
	n, err = writer.Write(data)
	written += int64(n)
	if err != nil {
		return written, err
	}
	if n != len(data) {
		return written, io.ErrShortWrite
	}
	return written, nil
}

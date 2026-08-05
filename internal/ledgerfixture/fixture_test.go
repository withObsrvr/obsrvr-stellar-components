package ledgerfixture

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRecordAndLoadJSONLFixture(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "pubnet-100-102.manifest.json")
	var input strings.Builder
	for ledger := uint32(100); ledger <= 102; ledger++ {
		data, err := protojson.Marshal(testBatch(ledger))
		if err != nil {
			t.Fatalf("marshal ledger %d: %v", ledger, err)
		}
		input.Write(data)
		input.WriteByte('\n')
	}

	recorded, err := RecordJSONL(strings.NewReader(input.String()), RecordOptions{
		ManifestPath:   manifestPath,
		ObjectStoreURL: "s3://fixtures/pubnet-100-102/",
		BatchesPerFile: 2,
	})
	if err != nil {
		t.Fatalf("record JSONL: %v", err)
	}
	if recorded.BatchCount != 3 || len(recorded.Files) != 2 {
		t.Fatalf("recorded count/files = %d/%d, want 3/2", recorded.BatchCount, len(recorded.Files))
	}
	if recorded.Files[0].BatchCount != 2 || recorded.Files[1].BatchCount != 1 {
		t.Fatalf("chunk counts = %d/%d, want 2/1", recorded.Files[0].BatchCount, recorded.Files[1].BatchCount)
	}

	loaded, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	reader := NewReader(manifestPath, loaded)
	defer reader.Close()
	for ledger := uint32(100); ledger <= 102; ledger++ {
		batch, err := reader.Next()
		if err != nil {
			t.Fatalf("read ledger %d: %v", ledger, err)
		}
		if batch.LedgerSequence != ledger {
			t.Fatalf("ledger = %d, want %d", batch.LedgerSequence, ledger)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("final read error = %v, want EOF", err)
	}
}

func TestExternalFixtureCorpus(t *testing.T) {
	manifestPath := os.Getenv("LEDGER_FIXTURE_MANIFEST")
	if manifestPath == "" {
		t.Skip("set LEDGER_FIXTURE_MANIFEST to validate an external fixture corpus")
	}
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("load and hash fixture manifest: %v", err)
	}
	reader := NewReader(manifestPath, manifest)
	defer reader.Close()
	for index := 0; index < manifest.BatchCount; index++ {
		expected := manifest.LedgerStart + uint32(index)
		batch, err := reader.Next()
		if err != nil {
			t.Fatalf("read ledger %d: %v", expected, err)
		}
		if batch.LedgerSequence != expected {
			t.Fatalf("ledger = %d, want %d", batch.LedgerSequence, expected)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("final read error = %v, want EOF", err)
	}
}

func TestLoadManifestRejectsCorruptFixture(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "fixture.manifest.json")
	data, err := protojson.Marshal(testBatch(10))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RecordJSONL(strings.NewReader(string(data)), RecordOptions{
		ManifestPath:   manifestPath,
		BatchesPerFile: 1,
	}); err != nil {
		t.Fatalf("record JSONL: %v", err)
	}
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(dir, manifest.Files[0].Path), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("corrupt"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(manifestPath); err == nil || !strings.Contains(err.Error(), "size is") {
		t.Fatalf("LoadManifest error = %v, want size mismatch", err)
	}
}

func TestRangeVerificationAndReaderSkipUnrelatedChunks(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "fixture.manifest.json")
	if _, err := RecordJSONL(strings.NewReader(fixtureJSONL(t, 100, 101, 102, 103)), RecordOptions{
		ManifestPath:   manifestPath,
		BatchesPerFile: 2,
	}); err != nil {
		t.Fatalf("record fixture: %v", err)
	}
	manifest, err := ReadManifest(manifestPath)
	if err != nil {
		t.Fatalf("read manifest metadata: %v", err)
	}
	firstChunk := filepath.Join(dir, manifest.Files[0].Path)
	file, err := os.OpenFile(firstChunk, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("unrelated-corruption"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if err := VerifyRange(manifestPath, manifest, 102, 103); err != nil {
		t.Fatalf("verify selected range: %v", err)
	}
	reader, err := NewRangeReader(manifestPath, manifest, 102, 103)
	if err != nil {
		t.Fatalf("open range reader: %v", err)
	}
	defer reader.Close()
	for ledger := uint32(102); ledger <= 103; ledger++ {
		batch, err := reader.Next()
		if err != nil {
			t.Fatalf("read selected ledger %d: %v", ledger, err)
		}
		if batch.LedgerSequence != ledger {
			t.Fatalf("selected ledger = %d, want %d", batch.LedgerSequence, ledger)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("selected range final error = %v, want EOF", err)
	}
	if _, err := LoadManifest(manifestPath); err == nil {
		t.Fatal("full manifest verification succeeded despite unrelated corrupt chunk")
	}
}

func TestRangeReaderVerifiesSelectedChunkInline(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "fixture.manifest.json")
	if _, err := RecordJSONL(strings.NewReader(fixtureJSONL(t, 100, 101, 102, 103)), RecordOptions{
		ManifestPath:   manifestPath,
		BatchesPerFile: 4,
	}); err != nil {
		t.Fatalf("record fixture: %v", err)
	}
	manifest, err := ReadManifest(manifestPath)
	if err != nil {
		t.Fatalf("read manifest metadata: %v", err)
	}
	chunkPath := filepath.Join(dir, manifest.Files[0].Path)
	chunk, err := os.ReadFile(chunkPath)
	if err != nil {
		t.Fatal(err)
	}
	chunk[len(chunk)-1] ^= 0xff
	if err := os.WriteFile(chunkPath, chunk, 0o640); err != nil {
		t.Fatal(err)
	}

	reader, err := NewRangeReader(manifestPath, manifest, 100, 101)
	if err != nil {
		t.Fatalf("open range reader: %v", err)
	}
	defer reader.Close()
	if _, err := reader.Next(); err != nil {
		t.Fatalf("read first selected ledger: %v", err)
	}
	if _, err := reader.Next(); err == nil || !strings.Contains(err.Error(), "sha256 is") {
		t.Fatalf("read final selected ledger error = %v, want inline hash mismatch", err)
	}
}

func TestManifestRejectsEscapingAndGappedFiles(t *testing.T) {
	manifest := Manifest{
		FormatVersion:     FormatVersion,
		MessageType:       MessageType,
		NetworkPassphrase: "network",
		LedgerStart:       10,
		LedgerEnd:         10,
		BatchCount:        1,
		SchemaVersion:     "schema-v1",
		ExtractionVersion: "extract-v1",
		Files: []ManifestFile{{
			Path:        "../escape.pb",
			SHA256:      strings.Repeat("0", 64),
			Bytes:       1,
			BatchCount:  1,
			LedgerStart: 10,
			LedgerEnd:   10,
		}},
	}
	if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("Validate error = %v, want escaping path error", err)
	}
	manifest.Files[0].Path = "fixture.pb"
	manifest.Files[0].LedgerStart = 11
	manifest.Files[0].LedgerEnd = 11
	if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "starts at ledger") {
		t.Fatalf("Validate error = %v, want file gap error", err)
	}
}

func TestRecordJSONLRejectsNoncontiguousLedgers(t *testing.T) {
	var input strings.Builder
	for _, ledger := range []uint32{10, 12} {
		data, err := protojson.Marshal(testBatch(ledger))
		if err != nil {
			t.Fatal(err)
		}
		input.Write(data)
		input.WriteByte('\n')
	}
	_, err := RecordJSONL(strings.NewReader(input.String()), RecordOptions{
		ManifestPath:   filepath.Join(t.TempDir(), "fixture.manifest.json"),
		BatchesPerFile: 10,
	})
	if err == nil || !strings.Contains(err.Error(), "want contiguous ledger 11") {
		t.Fatalf("RecordJSONL error = %v, want contiguous ledger error", err)
	}
}

func TestRecordJSONLReordersWithinWindow(t *testing.T) {
	var input strings.Builder
	for _, ledger := range []uint32{100, 102, 101, 103} {
		data, err := protojson.Marshal(testBatch(ledger))
		if err != nil {
			t.Fatal(err)
		}
		input.Write(data)
		input.WriteByte('\n')
	}
	manifestPath := filepath.Join(t.TempDir(), "fixture.manifest.json")
	recorded, err := RecordJSONL(strings.NewReader(input.String()), RecordOptions{
		ManifestPath:   manifestPath,
		BatchesPerFile: 2,
		ReorderWindow:  2,
	})
	if err != nil {
		t.Fatalf("RecordJSONL: %v", err)
	}
	reader := NewReader(manifestPath, recorded)
	defer reader.Close()
	for ledger := uint32(100); ledger <= 103; ledger++ {
		batch, err := reader.Next()
		if err != nil {
			t.Fatalf("read ledger %d: %v", ledger, err)
		}
		if batch.LedgerSequence != ledger {
			t.Fatalf("ledger = %d, want %d", batch.LedgerSequence, ledger)
		}
	}
}

func TestRecordJSONLRejectsReorderingBeyondWindow(t *testing.T) {
	var input strings.Builder
	for _, ledger := range []uint32{100, 102, 103, 101} {
		data, err := protojson.Marshal(testBatch(ledger))
		if err != nil {
			t.Fatal(err)
		}
		input.Write(data)
		input.WriteByte('\n')
	}
	_, err := RecordJSONL(strings.NewReader(input.String()), RecordOptions{
		ManifestPath:   filepath.Join(t.TempDir(), "fixture.manifest.json"),
		BatchesPerFile: 10,
		ReorderWindow:  1,
	})
	if err == nil || !strings.Contains(err.Error(), "want contiguous ledger 101") {
		t.Fatalf("RecordJSONL error = %v, want bounded reorder error", err)
	}
}

func TestRecordJSONLRejectsDuplicateLedgers(t *testing.T) {
	var input strings.Builder
	for _, ledger := range []uint32{100, 101, 101} {
		data, err := protojson.Marshal(testBatch(ledger))
		if err != nil {
			t.Fatal(err)
		}
		input.Write(data)
		input.WriteByte('\n')
	}
	_, err := RecordJSONL(strings.NewReader(input.String()), RecordOptions{
		ManifestPath:   filepath.Join(t.TempDir(), "fixture.manifest.json"),
		BatchesPerFile: 10,
		ReorderWindow:  2,
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate ledger_sequence 101") {
		t.Fatalf("RecordJSONL error = %v, want duplicate ledger error", err)
	}
}

func TestRecordJSONLCleansCreatedChunksAfterFailure(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "fixture.manifest.json")
	input := fixtureJSONL(t, 100, 101, 103)
	_, err := RecordJSONL(strings.NewReader(input), RecordOptions{
		ManifestPath:   manifestPath,
		BatchesPerFile: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "want contiguous ledger 102") {
		t.Fatalf("RecordJSONL error = %v, want contiguous ledger error", err)
	}
	chunks, err := filepath.Glob(filepath.Join(dir, "*.pb"))
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Fatalf("failed recording left fixture chunks: %v", chunks)
	}
	if _, err := os.Stat(manifestPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed recording manifest stat error = %v, want not exist", err)
	}

	if _, err := RecordJSONL(strings.NewReader(fixtureJSONL(t, 100, 101, 102)), RecordOptions{
		ManifestPath:   manifestPath,
		BatchesPerFile: 1,
	}); err != nil {
		t.Fatalf("retry recording: %v", err)
	}
}

func fixtureJSONL(t *testing.T, ledgers ...uint32) string {
	t.Helper()
	var input strings.Builder
	for _, ledger := range ledgers {
		data, err := protojson.Marshal(testBatch(ledger))
		if err != nil {
			t.Fatal(err)
		}
		input.Write(data)
		input.WriteByte('\n')
	}
	return input.String()
}

func testBatch(ledger uint32) *componentsv1.LedgerBatch {
	return &componentsv1.LedgerBatch{
		NetworkPassphrase: "Public Global Stellar Network ; September 2015",
		LedgerSequence:    ledger,
		ClosedAtUnix:      int64(ledger),
		SchemaVersion:     "schema-v1",
		ExtractionVersion: "extract-v1",
		Ledgers: []*componentsv1.LedgerRow{{
			Id:             "ledger",
			LedgerSequence: ledger,
		}},
	}
}

package rawledgercache

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

const testNetwork = "Test SDF Network ; September 2015"

// sliceSource replays fixed payloads with the borrowed-buffer contract the
// worker's raw ledger source uses.
func sliceSource(payloads [][]byte) Source {
	index := 0
	return func() ([]byte, error) {
		if index == len(payloads) {
			return nil, io.EOF
		}
		raw := payloads[index]
		index++
		return raw, nil
	}
}

func drain(t *testing.T, source Source) [][]byte {
	t.Helper()
	var collected [][]byte
	for {
		raw, err := source()
		if errors.Is(err, io.EOF) {
			return collected
		}
		if err != nil {
			t.Fatal(err)
		}
		collected = append(collected, append([]byte(nil), raw...))
	}
}

func samplePayloads() [][]byte {
	return [][]byte{
		[]byte("ledger-one"),
		[]byte("ledger-two-longer"),
		[]byte("l3"),
	}
}

func TestWarmReadReturnsPopulatedBytesExactly(t *testing.T) {
	cache, err := New(t.TempDir(), testNetwork, 0)
	if err != nil {
		t.Fatal(err)
	}
	payloads := samplePayloads()
	if cache.Complete(10, 12) {
		t.Fatal("empty cache reported a complete range")
	}

	populate, writeStats := cache.Populate(sliceSource(payloads), 10, 12)
	cold := drain(t, populate)
	if writeStats.LedgersWritten != 3 || writeStats.Mode != ModeCold {
		t.Fatalf("populate stats = %+v", writeStats)
	}
	if !cache.Complete(10, 12) {
		t.Fatal("populated range is not complete")
	}

	reader, readStats, err := cache.Reader(10, 12)
	if err != nil {
		t.Fatal(err)
	}
	warm := drain(t, reader)
	if len(warm) != len(cold) {
		t.Fatalf("warm read %d ledgers, cold read %d", len(warm), len(cold))
	}
	for index := range warm {
		if string(warm[index]) != string(cold[index]) {
			t.Fatalf("ledger %d warm %q != cold %q", index, warm[index], cold[index])
		}
	}
	if readStats.LedgersRead != 3 || readStats.Mode != ModeWarm {
		t.Fatalf("read stats = %+v", readStats)
	}
	if readStats.Bytes != writeStats.Bytes {
		t.Fatalf("warm bytes %d != cold bytes %d", readStats.Bytes, writeStats.Bytes)
	}
}

func TestWarmReadFailsClosedOnDivergentCachedBytes(t *testing.T) {
	root := t.TempDir()
	cache, err := New(root, testNetwork, 0)
	if err != nil {
		t.Fatal(err)
	}
	populate, _ := cache.Populate(sliceSource(samplePayloads()), 10, 12)
	drain(t, populate)

	// Same length, different content: only digest verification can catch it.
	path := cache.ledgerPath(11)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), original...)
	tampered[0] ^= 0xFF
	if err := os.WriteFile(path, tampered, 0o640); err != nil {
		t.Fatal(err)
	}
	if !cache.Complete(10, 12) {
		t.Fatal("same-size tampering changed range completeness; digest check would not be exercised")
	}

	reader, _, err := cache.Reader(10, 12)
	if err != nil {
		t.Fatal(err)
	}
	var readErr error
	for readErr == nil {
		_, readErr = reader()
	}
	if errors.Is(readErr, io.EOF) {
		t.Fatal("tampered cache range read to EOF without a digest failure")
	}
}

func TestWarmReadRejectsTruncatedCacheFile(t *testing.T) {
	cache, err := New(t.TempDir(), testNetwork, 0)
	if err != nil {
		t.Fatal(err)
	}
	populate, _ := cache.Populate(sliceSource(samplePayloads()), 10, 12)
	drain(t, populate)

	if err := os.WriteFile(cache.ledgerPath(10), []byte("short"), 0o640); err != nil {
		t.Fatal(err)
	}
	if cache.Complete(10, 12) {
		t.Fatal("truncated ledger file still reported a complete range")
	}
}

func TestPartialPopulationLeavesRangeCold(t *testing.T) {
	cache, err := New(t.TempDir(), testNetwork, 0)
	if err != nil {
		t.Fatal(err)
	}
	populate, _ := cache.Populate(sliceSource(samplePayloads()[:2]), 10, 12)
	// The source ends early, so the index must not be written.
	for {
		if _, err := populate(); err != nil {
			break
		}
	}
	if cache.Complete(10, 12) {
		t.Fatal("short range was marked complete")
	}
	if _, _, err := cache.Reader(10, 12); err == nil {
		t.Fatal("reader opened an incomplete range")
	}
}

func TestPopulationStopsAtByteCeilingWithoutPublishingRange(t *testing.T) {
	cache, err := New(t.TempDir(), testNetwork, 12)
	if err != nil {
		t.Fatal(err)
	}
	populate, stats := cache.Populate(sliceSource(samplePayloads()), 10, 12)
	collected := drain(t, populate)

	if len(collected) != 3 {
		t.Fatalf("byte ceiling changed what the consumer read: %d ledgers", len(collected))
	}
	if !stats.Truncated {
		t.Fatal("stats did not report truncation")
	}
	if stats.Bytes > 12 {
		t.Fatalf("cached %d bytes, want at most 12", stats.Bytes)
	}
	if cache.Complete(10, 12) {
		t.Fatal("truncated population published a complete range")
	}
}

func TestRangeIndexIsScopedToItsNetworkAndRange(t *testing.T) {
	root := t.TempDir()
	cache, err := New(root, testNetwork, 0)
	if err != nil {
		t.Fatal(err)
	}
	populate, _ := cache.Populate(sliceSource(samplePayloads()), 10, 12)
	drain(t, populate)

	other, err := New(root, "Public Global Stellar Network ; September 2015", 0)
	if err != nil {
		t.Fatal(err)
	}
	if other.Complete(10, 12) {
		t.Fatal("a different network reused another network's cached range")
	}
	if cache.Complete(10, 11) {
		t.Fatal("a narrower range matched a wider cached index")
	}
}

func TestPopulationIsAtomicPerLedgerFile(t *testing.T) {
	root := t.TempDir()
	cache, err := New(root, testNetwork, 0)
	if err != nil {
		t.Fatal(err)
	}
	populate, _ := cache.Populate(sliceSource(samplePayloads()), 10, 12)
	drain(t, populate)

	matches, err := filepath.Glob(filepath.Join(cache.root, "ledgers", "*", "*.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("cache left temporary files behind: %v", matches)
	}
}

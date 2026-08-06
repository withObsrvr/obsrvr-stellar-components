// Package rawledgercache stores canonical LedgerCloseMeta XDR for a fixed
// ledger range on local disk so repeated worker measurements can separate
// object-store acquisition cost from local CPU cost.
//
// The cache is measurement infrastructure, not a publication path. It is only
// active when a run opts in explicitly, it never changes the bytes a consumer
// sees, and every warm read is verified against the payload digest recorded
// when the range was populated. A verification failure fails the run closed
// rather than feeding unverified bytes into artifact production.
package rawledgercache

import (
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
	"time"
)

// FormatVersion is the on-disk range index version.
const FormatVersion = 1

const ledgersPerDirectory = 10_000

// Source yields borrowed canonical LedgerCloseMeta XDR and returns io.EOF at
// the end of its range. It matches the worker's raw ledger source contract:
// the returned slice is valid only until the next call.
type Source func() ([]byte, error)

// Cache owns one local directory of raw ledger XDR for one network.
type Cache struct {
	root        string
	networkHash string
	maxBytes    uint64
}

type rangeIndex struct {
	FormatVersion           int      `json:"format_version"`
	NetworkPassphraseSHA256 string   `json:"network_passphrase_sha256"`
	LedgerStart             uint32   `json:"ledger_start"`
	LedgerEnd               uint32   `json:"ledger_end"`
	LedgerBytes             []uint64 `json:"ledger_bytes"`
	EncodedBytes            uint64   `json:"encoded_bytes"`
	PayloadSHA256           string   `json:"payload_sha256"`
}

// Stats reports how a run interacted with the cache.
type Stats struct {
	Mode           string        `json:"mode"`
	LedgersRead    uint32        `json:"ledgers_read"`
	LedgersWritten uint32        `json:"ledgers_written"`
	Bytes          uint64        `json:"bytes"`
	ReadDuration   time.Duration `json:"-"`
	WriteDuration  time.Duration `json:"-"`
	VerifyDuration time.Duration `json:"-"`
	Truncated      bool          `json:"truncated"`
}

// Cache modes reported in run evidence.
const (
	ModeDisabled = "disabled"
	ModeCold     = "cold"
	ModeWarm     = "warm"
)

// New opens or creates a cache directory for one network passphrase. A
// maxBytes of zero disables the population byte ceiling.
func New(dir, networkPassphrase string, maxBytes uint64) (*Cache, error) {
	if dir == "" {
		return nil, fmt.Errorf("raw ledger cache directory is required")
	}
	if networkPassphrase == "" {
		return nil, fmt.Errorf("raw ledger cache network passphrase is required")
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve raw ledger cache directory: %w", err)
	}
	digest := sha256.Sum256([]byte(networkPassphrase))
	networkHash := hex.EncodeToString(digest[:])
	root := filepath.Join(absolute, networkHash[:16])
	if err := os.MkdirAll(filepath.Join(root, "ledgers"), 0o750); err != nil {
		return nil, fmt.Errorf("create raw ledger cache directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "ranges"), 0o750); err != nil {
		return nil, fmt.Errorf("create raw ledger cache range directory: %w", err)
	}
	return &Cache{root: root, networkHash: networkHash, maxBytes: maxBytes}, nil
}

// Complete reports whether the exact inclusive range has a verified index and
// every backing ledger file at its recorded size.
func (cache *Cache) Complete(start, end uint32) bool {
	index, err := cache.readIndex(start, end)
	if err != nil {
		return false
	}
	for offset, size := range index.LedgerBytes {
		info, err := os.Stat(cache.ledgerPath(start + uint32(offset)))
		if err != nil || uint64(info.Size()) != size {
			return false
		}
	}
	return true
}

// Reader serves a fully cached range from local disk. It reuses one buffer, so
// each returned slice is valid only until the next call, matching the network
// source contract. The reader recomputes the payload digest and fails closed at
// the end of the range if the cached bytes have changed.
func (cache *Cache) Reader(start, end uint32) (Source, *Stats, error) {
	index, err := cache.readIndex(start, end)
	if err != nil {
		return nil, nil, err
	}
	stats := &Stats{Mode: ModeWarm}
	hasher := sha256.New()
	buffer := make([]byte, 0, 1<<20)
	next := start
	return func() ([]byte, error) {
		if next > end {
			verifyStarted := time.Now()
			actual := hex.EncodeToString(hasher.Sum(nil))
			stats.VerifyDuration += time.Since(verifyStarted)
			if actual != index.PayloadSHA256 {
				return nil, fmt.Errorf("cached range %d-%d payload digest %s does not match recorded %s", start, end, actual, index.PayloadSHA256)
			}
			return nil, io.EOF
		}
		readStarted := time.Now()
		path := cache.ledgerPath(next)
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open cached ledger %d: %w", next, err)
		}
		size := index.LedgerBytes[next-start]
		if uint64(cap(buffer)) < size {
			buffer = make([]byte, size)
		}
		buffer = buffer[:size]
		if _, err := io.ReadFull(file, buffer); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("read cached ledger %d: %w", next, err)
		}
		// A cached file longer than its recorded size is a divergent cache,
		// not a readable ledger.
		var overflow [1]byte
		if n, err := file.Read(overflow[:]); n != 0 || !errors.Is(err, io.EOF) {
			_ = file.Close()
			return nil, fmt.Errorf("cached ledger %d exceeds its recorded %d bytes", next, size)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("close cached ledger %d: %w", next, err)
		}
		stats.ReadDuration += time.Since(readStarted)

		verifyStarted := time.Now()
		writeFramed(hasher, buffer)
		stats.VerifyDuration += time.Since(verifyStarted)

		stats.LedgersRead++
		stats.Bytes += size
		next++
		return buffer, nil
	}, stats, nil
}

// Populate wraps a network source, writing each ledger to the cache as it
// passes through. The borrowed bytes handed to the consumer are unchanged. The
// range index is written only after the complete range has been observed, so a
// partial or aborted run leaves the range cold.
func (cache *Cache) Populate(next Source, start, end uint32) (Source, *Stats) {
	stats := &Stats{Mode: ModeCold}
	hasher := sha256.New()
	sizes := make([]uint64, 0, int(end-start)+1)
	sequence := start
	return func() ([]byte, error) {
		raw, err := next()
		if err != nil {
			if errors.Is(err, io.EOF) && sequence == end+1 && !stats.Truncated {
				writeStarted := time.Now()
				indexErr := cache.writeIndex(rangeIndex{
					FormatVersion:           FormatVersion,
					NetworkPassphraseSHA256: cache.networkHash,
					LedgerStart:             start,
					LedgerEnd:               end,
					LedgerBytes:             sizes,
					EncodedBytes:            stats.Bytes,
					PayloadSHA256:           hex.EncodeToString(hasher.Sum(nil)),
				})
				stats.WriteDuration += time.Since(writeStarted)
				if indexErr != nil {
					return nil, indexErr
				}
			}
			return nil, err
		}
		if sequence > end {
			return nil, fmt.Errorf("source produced a ledger past %d", end)
		}
		if cache.maxBytes > 0 && stats.Bytes+uint64(len(raw)) > cache.maxBytes {
			stats.Truncated = true
		}
		if !stats.Truncated {
			writeStarted := time.Now()
			if err := cache.writeLedger(sequence, raw); err != nil {
				return nil, err
			}
			stats.WriteDuration += time.Since(writeStarted)
			writeFramed(hasher, raw)
			sizes = append(sizes, uint64(len(raw)))
			stats.LedgersWritten++
			stats.Bytes += uint64(len(raw))
		}
		sequence++
		return raw, nil
	}, stats
}

func (cache *Cache) ledgerPath(sequence uint32) string {
	bucket := fmt.Sprintf("%010d", (sequence/ledgersPerDirectory)*ledgersPerDirectory)
	return filepath.Join(cache.root, "ledgers", bucket, fmt.Sprintf("%010d.xdr", sequence))
}

func (cache *Cache) indexPath(start, end uint32) string {
	return filepath.Join(cache.root, "ranges", fmt.Sprintf("%010d-%010d.index.json", start, end))
}

func (cache *Cache) writeLedger(sequence uint32, raw []byte) error {
	path := cache.ledgerPath(sequence)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create cache bucket for ledger %d: %w", sequence, err)
	}
	return writeFileAtomic(path, raw)
}

func (cache *Cache) writeIndex(index rangeIndex) error {
	encoded, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return fmt.Errorf("encode cache range index: %w", err)
	}
	return writeFileAtomic(cache.indexPath(index.LedgerStart, index.LedgerEnd), append(encoded, '\n'))
}

func (cache *Cache) readIndex(start, end uint32) (rangeIndex, error) {
	raw, err := os.ReadFile(cache.indexPath(start, end))
	if err != nil {
		return rangeIndex{}, fmt.Errorf("read cache range index: %w", err)
	}
	var index rangeIndex
	if err := json.Unmarshal(raw, &index); err != nil {
		return rangeIndex{}, fmt.Errorf("decode cache range index: %w", err)
	}
	if index.FormatVersion != FormatVersion {
		return rangeIndex{}, fmt.Errorf("cache range index version %d is not %d", index.FormatVersion, FormatVersion)
	}
	if index.NetworkPassphraseSHA256 != cache.networkHash {
		return rangeIndex{}, fmt.Errorf("cache range index belongs to another network")
	}
	if index.LedgerStart != start || index.LedgerEnd != end {
		return rangeIndex{}, fmt.Errorf("cache range index covers %d-%d, want %d-%d", index.LedgerStart, index.LedgerEnd, start, end)
	}
	if uint64(len(index.LedgerBytes)) != uint64(end-start)+1 {
		return rangeIndex{}, fmt.Errorf("cache range index lists %d ledgers, want %d", len(index.LedgerBytes), uint64(end-start)+1)
	}
	return index, nil
}

// writeFramed mirrors the worker's length-prefixed payload framing so a cached
// range digest is directly comparable to a run's source digest.
func writeFramed(hasher hash.Hash, raw []byte) {
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(raw)))
	_, _ = hasher.Write(length[:n])
	_, _ = hasher.Write(raw)
}

func writeFileAtomic(path string, content []byte) (resultErr error) {
	temporary, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create cache temporary file: %w", err)
	}
	name := temporary.Name()
	complete := false
	defer func() {
		if !complete {
			_ = temporary.Close()
			_ = os.Remove(name)
		}
	}()
	if _, err := temporary.Write(content); err != nil {
		return fmt.Errorf("write cache file %s: %w", path, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close cache file %s: %w", path, err)
	}
	if err := os.Chmod(name, 0o640); err != nil {
		return fmt.Errorf("set cache file mode %s: %w", path, err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("publish cache file %s: %w", path, err)
	}
	complete = true
	return nil
}

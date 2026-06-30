package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/normalize"
	"google.golang.org/protobuf/proto"
)

func main() {
	var (
		backendType       = flag.String("backend", envOr("BACKEND_TYPE", "RPC"), "ledger backend: RPC or ARCHIVE")
		rpcURL            = flag.String("rpc-url", envOr("RPC_ENDPOINT", "https://mainnet.sorobanrpc.com"), "Stellar RPC URL")
		archiveStorage    = flag.String("archive-storage-type", envOr("ARCHIVE_STORAGE_TYPE", envOr("STORAGE_TYPE", "GCS")), "archive storage type: GCS or S3")
		archiveBucket     = flag.String("archive-bucket-name", envOr("ARCHIVE_BUCKET_NAME", envOr("BUCKET_NAME", "")), "archive bucket name")
		archivePath       = flag.String("archive-path", envOr("ARCHIVE_PATH", envOr("BUCKET_PATH", "")), "optional archive prefix inside bucket")
		ledgersPerFile    = flag.Uint("ledgers-per-file", envUint("LEDGERS_PER_FILE", 64), "archive ledgers per object")
		filesPerPartition = flag.Uint("files-per-partition", envUint("FILES_PER_PARTITION", 10), "archive files per partition")
		bufferSize        = flag.Uint("buffer-size", envUint("BUFFER_SIZE", 5), "archive read-ahead buffer size")
		numWorkers        = flag.Uint("num-workers", envUint("NUM_WORKERS", 2), "archive worker count")
		ledgerSeq         = flag.Uint("ledger", 0, "ledger sequence to fetch")
		passphrase        = flag.String("network-passphrase", envOr("NETWORK_PASSPHRASE", network.PublicNetworkPassphrase), "Stellar network passphrase")
		timeout           = flag.Duration("timeout", 30*time.Second, "fetch timeout")
	)
	flag.Parse()

	if *ledgerSeq == 0 {
		fmt.Fprintln(os.Stderr, "-ledger is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	backend, err := newLedgerBackend(ctx, smokeConfig{
		backendType:       *backendType,
		rpcURL:            *rpcURL,
		archiveStorage:    *archiveStorage,
		archiveBucket:     *archiveBucket,
		archivePath:       *archivePath,
		ledgersPerFile:    uint32(*ledgersPerFile),
		filesPerPartition: uint32(*filesPerPartition),
		bufferSize:        uint32(*bufferSize),
		numWorkers:        uint32(*numWorkers),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "create ledger backend: %v\n", err)
		os.Exit(1)
	}
	defer backend.Close()

	seq := uint32(*ledgerSeq)
	if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(seq, seq)); err != nil {
		fmt.Fprintf(os.Stderr, "prepare ledger range: %v\n", err)
		os.Exit(1)
	}
	lcm, err := backend.GetLedger(ctx, seq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch ledger %d: %v\n", seq, err)
		os.Exit(1)
	}
	batch, err := normalize.LedgerBatch(lcm, normalize.Options{
		NetworkPassphrase: *passphrase,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "normalize ledger %d: %v\n", seq, err)
		os.Exit(1)
	}
	payload, err := proto.Marshal(batch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal ledger batch: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("event_type=%s\n", contracts.LedgerBatchEventType)
	fmt.Printf("ledger_sequence=%d\n", batch.LedgerSequence)
	fmt.Printf("closed_at_unix=%d\n", batch.ClosedAtUnix)
	fmt.Printf("ledger_rows=%d\n", len(batch.Ledgers))
	fmt.Printf("transaction_rows=%d\n", len(batch.Transactions))
	fmt.Printf("operation_rows=%d\n", len(batch.Operations))
	fmt.Printf("contract_event_rows=%d\n", len(batch.ContractEvents))
	fmt.Printf("contract_invocation_rows=%d\n", len(batch.ContractInvocations))
	fmt.Printf("token_transfer_rows=%d\n", len(batch.TokenTransfers))
	fmt.Printf("account_effect_rows=%d\n", len(batch.AccountEffects))
	fmt.Printf("bronze_rows=%d\n", len(batch.BronzeRows))
	for _, line := range bronzeTableCounts(batch.BronzeRows) {
		fmt.Println(line)
	}
	fmt.Printf("protobuf_payload_bytes=%d\n", len(payload))
}

func bronzeTableCounts(rows []*componentsv1.BronzeRow) []string {
	counts := map[string]int{}
	for _, row := range rows {
		counts[row.TableName]++
	}
	tables := make([]string, 0, len(counts))
	for table := range counts {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	out := make([]string, 0, len(tables))
	for _, table := range tables {
		out = append(out, fmt.Sprintf("bronze_table.%s=%d", table, counts[table]))
	}
	return out
}

type smokeConfig struct {
	backendType       string
	rpcURL            string
	archiveStorage    string
	archiveBucket     string
	archivePath       string
	ledgersPerFile    uint32
	filesPerPartition uint32
	bufferSize        uint32
	numWorkers        uint32
}

func newLedgerBackend(ctx context.Context, cfg smokeConfig) (ledgerbackend.LedgerBackend, error) {
	switch strings.ToUpper(cfg.backendType) {
	case "", "RPC":
		if cfg.rpcURL == "" {
			return nil, fmt.Errorf("RPC backend requires -rpc-url or RPC_ENDPOINT")
		}
		return ledgerbackend.NewRPCLedgerBackend(ledgerbackend.RPCLedgerBackendOptions{
			RPCServerURL: cfg.rpcURL,
			BufferSize:   1,
		}), nil
	case "ARCHIVE":
		return newArchiveBackend(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported -backend %q (want RPC or ARCHIVE)", cfg.backendType)
	}
}

func newArchiveBackend(ctx context.Context, cfg smokeConfig) (ledgerbackend.LedgerBackend, error) {
	storageType := strings.ToUpper(cfg.archiveStorage)
	if storageType == "" || cfg.archiveBucket == "" {
		return nil, fmt.Errorf("ARCHIVE backend requires -archive-storage-type and -archive-bucket-name")
	}
	if storageType != "GCS" && storageType != "S3" {
		return nil, fmt.Errorf("unsupported archive storage type %q (want GCS or S3)", cfg.archiveStorage)
	}
	if cfg.ledgersPerFile == 0 || cfg.filesPerPartition == 0 {
		return nil, fmt.Errorf("ledgers-per-file and files-per-partition must be > 0")
	}
	if cfg.bufferSize == 0 || cfg.numWorkers == 0 {
		return nil, fmt.Errorf("buffer-size and num-workers must be > 0")
	}

	schema := datastore.DataStoreSchema{
		LedgersPerFile:    cfg.ledgersPerFile,
		FilesPerPartition: cfg.filesPerPartition,
	}
	params := map[string]string{
		"destination_bucket_path": archiveBucketPath(cfg.archiveBucket, cfg.archivePath),
	}
	dataStore, err := datastore.NewDataStore(ctx, datastore.DataStoreConfig{
		Type:   storageType,
		Schema: schema,
		Params: params,
	})
	if err != nil {
		return nil, fmt.Errorf("create %s datastore %q: %w", storageType, params["destination_bucket_path"], err)
	}

	backend, err := ledgerbackend.NewBufferedStorageBackend(ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: cfg.bufferSize,
		NumWorkers: cfg.numWorkers,
		RetryLimit: 3,
		RetryWait:  time.Second,
	}, dataStore, schema)
	if err != nil {
		dataStore.Close()
		return nil, fmt.Errorf("create buffered storage backend: %w", err)
	}
	return backend, nil
}

func archiveBucketPath(bucket, prefix string) string {
	bucket = strings.Trim(bucket, "/")
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return bucket
	}
	return bucket + "/" + prefix
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envUint(key string, fallback uint) uint {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	var parsed uint
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}

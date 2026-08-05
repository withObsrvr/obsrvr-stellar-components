// Package ledgerstream constructs raw Stellar ledger streams from runtime
// configuration. Consumers receive the SDK's LedgerStream directly so range
// lifecycle, cancellation, and borrowed-XDR semantics have one owner.
package ledgerstream

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/datastore"
)

// Type identifies the upstream ledger source.
type Type string

const (
	TypeRPC         Type = "RPC"
	TypeArchive     Type = "ARCHIVE"
	TypeCaptiveCore Type = "CAPTIVE_CORE"

	defaultLedgersPerFile    = uint32(1)
	defaultFilesPerPartition = uint32(64000)
)

// Config is the stream selection and connection configuration.
type Config struct {
	Type Type

	RPCServerURL  string
	RPCAuthHeader string

	ArchiveStorageType string
	ArchiveBucketName  string
	ArchivePath        string
	AWSRegion          string
	S3EndpointURL      string
	LedgersPerFile     uint32
	FilesPerPartition  uint32
	BufferSize         uint32
	NumWorkers         uint32

	NetworkPassphrase      string
	HistoryArchiveURLs     []string
	StellarCoreBinaryPath  string
	StellarCoreConfigPath  string
	CaptiveCoreStoragePath string
}

// ConfigFromEnv reads stream configuration from the environment.
//
//	BACKEND_TYPE          RPC | ARCHIVE | CAPTIVE_CORE (default RPC)
//	RPC_ENDPOINT          Stellar RPC URL (required for RPC)
//	RPC_AUTH_HEADER       optional Authorization header value
//	ARCHIVE_STORAGE_TYPE  GCS | S3 (required for ARCHIVE; STORAGE_TYPE fallback)
//	ARCHIVE_BUCKET_NAME   bucket name (required for ARCHIVE; BUCKET_NAME fallback)
//	ARCHIVE_PATH          optional prefix inside the bucket (BUCKET_PATH fallback)
//	LEDGERS_PER_FILE      archive schema ledgers per file (default 1)
//	FILES_PER_PARTITION   archive schema files per partition (default 64000)
//	BUFFER_SIZE           archive read-ahead buffer size (default 5)
//	NUM_WORKERS           archive worker count (default 2)
//	AWS_REGION            S3 region (default us-east-1)
//	S3_ENDPOINT_URL       optional S3-compatible endpoint URL
//	NETWORK_PASSPHRASE    network passphrase (default pubnet)
//	HISTORY_ARCHIVE_URLS  comma-separated archive URLs for CAPTIVE_CORE
//	STELLAR_CORE_BINARY_PATH  stellar-core binary path for CAPTIVE_CORE
//	STELLAR_CORE_CONFIG_PATH  optional base stellar-core config file for CAPTIVE_CORE
//	CAPTIVE_CORE_STORAGE_PATH optional captive-core bucket storage base path
func ConfigFromEnv() Config {
	t := Type(strings.ToUpper(os.Getenv("BACKEND_TYPE")))
	if t == "" {
		if storageType := strings.ToUpper(os.Getenv("STORAGE_TYPE")); storageType == "GCS" || storageType == "S3" {
			t = TypeArchive
		} else {
			t = TypeRPC
		}
	}
	return Config{
		Type:                   t,
		RPCServerURL:           os.Getenv("RPC_ENDPOINT"),
		RPCAuthHeader:          os.Getenv("RPC_AUTH_HEADER"),
		ArchiveStorageType:     envOr("ARCHIVE_STORAGE_TYPE", os.Getenv("STORAGE_TYPE")),
		ArchiveBucketName:      envOr("ARCHIVE_BUCKET_NAME", os.Getenv("BUCKET_NAME")),
		ArchivePath:            envOr("ARCHIVE_PATH", os.Getenv("BUCKET_PATH")),
		AWSRegion:              envOr("AWS_REGION", "us-east-1"),
		S3EndpointURL:          os.Getenv("S3_ENDPOINT_URL"),
		LedgersPerFile:         envUint32("LEDGERS_PER_FILE", defaultLedgersPerFile),
		FilesPerPartition:      envUint32("FILES_PER_PARTITION", defaultFilesPerPartition),
		BufferSize:             envUint32("BUFFER_SIZE", 5),
		NumWorkers:             envUint32("NUM_WORKERS", 2),
		NetworkPassphrase:      envOr("NETWORK_PASSPHRASE", network.PublicNetworkPassphrase),
		HistoryArchiveURLs:     csvList(os.Getenv("HISTORY_ARCHIVE_URLS")),
		StellarCoreBinaryPath:  os.Getenv("STELLAR_CORE_BINARY_PATH"),
		StellarCoreConfigPath:  os.Getenv("STELLAR_CORE_CONFIG_PATH"),
		CaptiveCoreStoragePath: os.Getenv("CAPTIVE_CORE_STORAGE_PATH"),
	}
}

// New validates cfg and returns a reusable stream. Each RawLedgers invocation
// owns the underlying backend and tears it down on completion, cancellation,
// error, or an early break.
func New(cfg Config) (ledgerbackend.LedgerStream, error) {
	switch cfg.Type {
	case TypeRPC:
		return newRPC(cfg)
	case TypeArchive:
		return newArchive(cfg)
	case TypeCaptiveCore:
		return newCaptiveCore(cfg)
	default:
		return nil, fmt.Errorf("unknown BACKEND_TYPE %q (want RPC, ARCHIVE, or CAPTIVE_CORE)", cfg.Type)
	}
}

func newRPC(cfg Config) (ledgerbackend.LedgerStream, error) {
	if cfg.RPCServerURL == "" {
		return nil, fmt.Errorf("RPC backend requires RPC_ENDPOINT")
	}
	opts := ledgerbackend.RPCLedgerBackendOptions{
		RPCServerURL: cfg.RPCServerURL,
		BufferSize:   10,
	}
	if cfg.RPCAuthHeader != "" {
		opts.HttpClient = &http.Client{
			Transport: &authTransport{base: http.DefaultTransport, authHeader: cfg.RPCAuthHeader},
		}
	}
	return ledgerbackend.NewRPCStream(opts, nil), nil
}

func newArchive(cfg Config) (ledgerbackend.LedgerStream, error) {
	storageType := strings.ToUpper(cfg.ArchiveStorageType)
	if storageType == "" || cfg.ArchiveBucketName == "" {
		return nil, fmt.Errorf("ARCHIVE backend requires ARCHIVE_STORAGE_TYPE (or STORAGE_TYPE) and ARCHIVE_BUCKET_NAME (or BUCKET_NAME)")
	}
	if cfg.LedgersPerFile == 0 {
		return nil, fmt.Errorf("LEDGERS_PER_FILE must be > 0")
	}
	if cfg.FilesPerPartition == 0 {
		return nil, fmt.Errorf("FILES_PER_PARTITION must be > 0")
	}
	if cfg.BufferSize == 0 {
		return nil, fmt.Errorf("BUFFER_SIZE must be > 0")
	}
	if cfg.NumWorkers == 0 {
		return nil, fmt.Errorf("NUM_WORKERS must be > 0")
	}

	params := map[string]string{
		"destination_bucket_path": archiveBucketPath(cfg.ArchiveBucketName, cfg.ArchivePath),
	}
	switch storageType {
	case "GCS":
	case "S3":
		params["region"] = cfg.AWSRegion
		if cfg.S3EndpointURL != "" {
			params["endpoint_url"] = cfg.S3EndpointURL
		}
	default:
		return nil, fmt.Errorf("unsupported ARCHIVE_STORAGE_TYPE %q (want GCS or S3)", cfg.ArchiveStorageType)
	}

	dsConfig := datastore.DataStoreConfig{
		Type: storageType,
		Schema: datastore.DataStoreSchema{
			LedgersPerFile:    cfg.LedgersPerFile,
			FilesPerPartition: cfg.FilesPerPartition,
		},
		Params:            params,
		NetworkPassphrase: cfg.NetworkPassphrase,
	}
	streamConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: cfg.BufferSize,
		NumWorkers: cfg.NumWorkers,
		RetryLimit: 3,
		RetryWait:  time.Second,
	}
	return ledgerbackend.NewBufferedStorageStream(streamConfig, dsConfig, nil), nil
}

func newCaptiveCore(cfg Config) (ledgerbackend.LedgerStream, error) {
	if cfg.StellarCoreBinaryPath == "" {
		return nil, fmt.Errorf("CAPTIVE_CORE backend requires STELLAR_CORE_BINARY_PATH")
	}
	if cfg.NetworkPassphrase == "" {
		return nil, fmt.Errorf("CAPTIVE_CORE backend requires NETWORK_PASSPHRASE")
	}
	if len(cfg.HistoryArchiveURLs) == 0 {
		return nil, fmt.Errorf("CAPTIVE_CORE backend requires HISTORY_ARCHIVE_URLS")
	}

	tomlParams := ledgerbackend.CaptiveCoreTomlParams{
		NetworkPassphrase:  cfg.NetworkPassphrase,
		HistoryArchiveURLs: cfg.HistoryArchiveURLs,
		CoreBinaryPath:     cfg.StellarCoreBinaryPath,
	}
	var (
		captiveCoreToml *ledgerbackend.CaptiveCoreToml
		err             error
	)
	if cfg.StellarCoreConfigPath != "" {
		captiveCoreToml, err = ledgerbackend.NewCaptiveCoreTomlFromFile(cfg.StellarCoreConfigPath, tomlParams)
	} else {
		captiveCoreToml, err = ledgerbackend.NewCaptiveCoreToml(tomlParams)
	}
	if err != nil {
		return nil, fmt.Errorf("create captive core toml: %w", err)
	}

	return ledgerbackend.NewCaptiveCoreStream(ledgerbackend.CaptiveCoreConfig{
		BinaryPath:          cfg.StellarCoreBinaryPath,
		NetworkPassphrase:   cfg.NetworkPassphrase,
		HistoryArchiveURLs:  cfg.HistoryArchiveURLs,
		CheckpointFrequency: 64,
		Toml:                captiveCoreToml,
		StoragePath:         cfg.CaptiveCoreStoragePath,
	}, nil), nil
}

func archiveBucketPath(bucket, prefix string) string {
	bucket = strings.Trim(bucket, "/")
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return bucket
	}
	return bucket + "/" + prefix
}

func csvList(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envUint32(key string, fallback uint32) uint32 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	number, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return fallback
	}
	return uint32(number)
}

type authTransport struct {
	base       http.RoundTripper
	authHeader string
}

func (t *authTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header.Set("Authorization", t.authHeader)
	return t.base.RoundTrip(clone)
}

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

type replayConfig struct {
	Fixtures           string
	Endpoint           string
	Token              string
	Profile            string
	Cadence            time.Duration
	Jitter             time.Duration
	Duration           time.Duration
	MaxLatency         time.Duration
	AckTimeout         time.Duration
	Offset             int
	Count              int
	Burst              int
	Seed               int64
	SummaryPath        string
	ResultsPath        string
	MetricsURL         string
	RequireCheckpoints int
	CheckpointWait     time.Duration
	MicrobatchLedgers  int
	MicrobatchMaxBytes int64
	MicrobatchMaxRows  int64
}

type profileDefaults struct {
	cadence            time.Duration
	jitter             time.Duration
	duration           time.Duration
	maxLatency         time.Duration
	burst              int
	requireCheckpoints int
	microbatchLedgers  int
	microbatchMaxBytes int64
	microbatchMaxRows  int64
}

var profiles = map[string]profileDefaults{
	"live": {
		cadence:    5 * time.Second,
		jitter:     250 * time.Millisecond,
		duration:   time.Hour,
		maxLatency: 400 * time.Millisecond,
	},
	"future": {
		cadence:    2 * time.Second,
		jitter:     100 * time.Millisecond,
		duration:   time.Hour,
		maxLatency: 400 * time.Millisecond,
	},
	"catch-up": {
		cadence:    5 * time.Second,
		jitter:     250 * time.Millisecond,
		duration:   time.Hour,
		maxLatency: 400 * time.Millisecond,
		burst:      100,
	},
	"checkpoint": {
		cadence:            5 * time.Second,
		jitter:             250 * time.Millisecond,
		duration:           time.Hour,
		maxLatency:         400 * time.Millisecond,
		requireCheckpoints: 3,
	},
	"maintenance": {
		cadence:    5 * time.Second,
		jitter:     250 * time.Millisecond,
		duration:   time.Hour,
		maxLatency: 400 * time.Millisecond,
	},
	"backfill": {
		cadence:            0,
		jitter:             0,
		duration:           0,
		maxLatency:         0,
		microbatchLedgers:  25,
		microbatchMaxBytes: 256 * 1024 * 1024,
		microbatchMaxRows:  500_000,
	},
}

func parseConfig(args []string, output io.Writer) (replayConfig, error) {
	config := replayConfig{}
	flags := flag.NewFlagSet("ingest-replay", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&config.Fixtures, "fixtures", "", "fixture manifest path (required)")
	flags.StringVar(&config.Endpoint, "endpoint", os.Getenv("INGEST_ENDPOINT"), "BronzeIngestService host:port (default INGEST_ENDPOINT)")
	flags.StringVar(&config.Token, "token", os.Getenv("QUACK_TOKEN"), "ingest token (default QUACK_TOKEN; prefer the environment to avoid process-list exposure)")
	flags.StringVar(&config.Profile, "profile", "live", "schedule profile: live, future, catch-up, checkpoint, maintenance, backfill, or custom")
	flags.DurationVar(&config.Cadence, "cadence", -1, "nominal interval between ledger arrivals")
	flags.DurationVar(&config.Jitter, "jitter", -1, "deterministic +/- arrival jitter")
	flags.DurationVar(&config.Duration, "duration", -1, "maximum scheduled run duration; 0 consumes the requested corpus")
	flags.DurationVar(&config.MaxLatency, "max-latency", -1, "maximum scheduled-arrival-to-ack latency; 0 disables the latency gate")
	flags.DurationVar(&config.AckTimeout, "ack-timeout", 30*time.Second, "timeout for an individual ledger acknowledgement")
	flags.IntVar(&config.Offset, "offset", 0, "number of fixture ledgers to skip before replay")
	flags.IntVar(&config.Count, "count", 0, "maximum number of ledgers to replay; 0 uses duration/corpus")
	flags.IntVar(&config.Burst, "burst", -1, "number of initial saturated ledgers; their latency is reported but exempt from the live SLO")
	flags.Int64Var(&config.Seed, "seed", 1, "deterministic jitter seed")
	flags.StringVar(&config.SummaryPath, "summary", "-", "JSON summary output path, or - for stdout")
	flags.StringVar(&config.ResultsPath, "results", "", "optional per-ledger JSONL result path")
	flags.StringVar(&config.MetricsURL, "metrics-url", "", "quack-ducklake-server /metrics URL")
	flags.IntVar(&config.RequireCheckpoints, "require-checkpoints", -1, "minimum new successful idle checkpoints")
	flags.DurationVar(&config.CheckpointWait, "checkpoint-wait", 30*time.Second, "time to wait after replay for required idle checkpoints")
	flags.IntVar(&config.MicrobatchLedgers, "microbatch-ledgers", -1, "maximum contiguous ledgers per backfill transaction")
	flags.Int64Var(&config.MicrobatchMaxBytes, "microbatch-max-encoded-bytes", -1, "maximum protobuf payload bytes per backfill transaction")
	flags.Int64Var(&config.MicrobatchMaxRows, "microbatch-max-bronze-rows", -1, "maximum Bronze rows per backfill transaction")
	if err := flags.Parse(args); err != nil {
		return replayConfig{}, err
	}
	if flags.NArg() != 0 {
		return replayConfig{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if err := applyProfile(&config); err != nil {
		return replayConfig{}, err
	}
	if err := validateReplayConfig(config); err != nil {
		return replayConfig{}, err
	}
	return config, nil
}

func applyProfile(config *replayConfig) error {
	defaults, known := profiles[config.Profile]
	if !known && config.Profile != "custom" {
		return fmt.Errorf("unknown --profile %q", config.Profile)
	}
	if config.Profile == "custom" {
		defaults = profileDefaults{jitter: 0, duration: 0, maxLatency: 400 * time.Millisecond}
		if config.Cadence < 0 {
			return fmt.Errorf("--cadence is required for --profile=custom")
		}
	}
	if config.Cadence < 0 {
		config.Cadence = defaults.cadence
	}
	if config.Jitter < 0 {
		config.Jitter = defaults.jitter
	}
	if config.Duration < 0 {
		config.Duration = defaults.duration
	}
	if config.MaxLatency < 0 {
		config.MaxLatency = defaults.maxLatency
	}
	if config.Burst < 0 {
		config.Burst = defaults.burst
	}
	if config.RequireCheckpoints < 0 {
		config.RequireCheckpoints = defaults.requireCheckpoints
	}
	if config.MicrobatchLedgers < 0 {
		config.MicrobatchLedgers = defaults.microbatchLedgers
		if config.MicrobatchLedgers == 0 {
			config.MicrobatchLedgers = 1
		}
	}
	if config.MicrobatchMaxBytes < 0 {
		config.MicrobatchMaxBytes = defaults.microbatchMaxBytes
	}
	if config.MicrobatchMaxRows < 0 {
		config.MicrobatchMaxRows = defaults.microbatchMaxRows
	}
	return nil
}

func validateReplayConfig(config replayConfig) error {
	if config.Fixtures == "" {
		return fmt.Errorf("--fixtures is required")
	}
	if config.Endpoint == "" {
		return fmt.Errorf("--endpoint or INGEST_ENDPOINT is required")
	}
	if config.Token == "" {
		return fmt.Errorf("--token or QUACK_TOKEN is required")
	}
	if config.Cadence < 0 || config.Jitter < 0 || config.Duration < 0 || config.MaxLatency < 0 {
		return fmt.Errorf("cadence, jitter, duration, and max latency must be non-negative")
	}
	if config.Jitter > config.Cadence {
		return fmt.Errorf("--jitter must not exceed --cadence")
	}
	if config.AckTimeout <= 0 {
		return fmt.Errorf("--ack-timeout must be positive")
	}
	if config.Offset < 0 || config.Count < 0 || config.Burst < 0 || config.RequireCheckpoints < 0 {
		return fmt.Errorf("offset, count, burst, and required checkpoints must be non-negative")
	}
	if config.MicrobatchLedgers <= 0 {
		return fmt.Errorf("--microbatch-ledgers must be positive")
	}
	if config.Profile == "backfill" && (config.MicrobatchMaxBytes <= 0 || config.MicrobatchMaxRows <= 0) {
		return fmt.Errorf("backfill micro-batch byte and row limits must be positive")
	}
	if config.MicrobatchLedgers > 1 && config.Profile != "backfill" && config.Profile != "custom" {
		return fmt.Errorf("--microbatch-ledgers above 1 requires --profile=backfill or custom")
	}
	if config.MicrobatchLedgers > 1 && (config.Cadence != 0 || config.Jitter != 0 || config.MaxLatency != 0) {
		return fmt.Errorf("micro-batch replay requires zero cadence, jitter, and max latency")
	}
	if config.CheckpointWait < 0 {
		return fmt.Errorf("--checkpoint-wait must be non-negative")
	}
	if config.RequireCheckpoints > 0 && config.MetricsURL == "" {
		return fmt.Errorf("--metrics-url is required when checkpoints must be observed")
	}
	if config.SummaryPath == "" {
		return fmt.Errorf("--summary must not be empty")
	}
	return nil
}

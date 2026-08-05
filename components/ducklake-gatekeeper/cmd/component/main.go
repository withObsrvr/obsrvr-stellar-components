// Command ducklake-gatekeeper verifies a snapshot-pinned proposal and atomically
// promotes it into a published DuckLake schema through the catalog-owning Quack
// server.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/withObsrvr/obsrvr-stellar-components/internal/gatekeeper"
)

type config struct {
	ManifestPath string
	ReportPath   string
	QuackURI     string
	QuackToken   string
	RemoteDB     string
	DisableSSL   bool
	Timeout      time.Duration
}

type report struct {
	Status  string                      `json:"status"`
	Receipt gatekeeper.PromotionReceipt `json:"receipt"`
	Error   string                      `json:"error,omitempty"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		log.Printf("ducklake-gatekeeper failed: %v", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	cfg, err := parseConfig(args, io.Discard)
	if err != nil {
		return err
	}
	proposal, hash, err := gatekeeper.LoadProposal(cfg.ManifestPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	remote, err := gatekeeper.OpenQuackRemote(ctx, gatekeeper.QuackConfig{
		URI:        cfg.QuackURI,
		Token:      cfg.QuackToken,
		RemoteDB:   cfg.RemoteDB,
		DisableSSL: cfg.DisableSSL,
	})
	if err != nil {
		return err
	}
	defer remote.Close()

	receipt, runErr := (gatekeeper.Runner{Remote: remote}).Run(ctx, proposal, hash)
	result := report{Status: "promoted", Receipt: receipt}
	if runErr != nil {
		result.Status = "error"
		result.Error = runErr.Error()
		var rejected gatekeeper.RejectedError
		if errors.As(runErr, &rejected) {
			result.Status = "rejected"
		}
	}
	if err := writeReport(cfg.ReportPath, stdout, result); err != nil {
		return errors.Join(runErr, err)
	}
	return runErr
}

func parseConfig(args []string, output io.Writer) (config, error) {
	flags := flag.NewFlagSet("ducklake-gatekeeper", flag.ContinueOnError)
	flags.SetOutput(output)
	var cfg config
	flags.StringVar(&cfg.ManifestPath, "manifest", "", "proposal manifest path")
	flags.StringVar(&cfg.ReportPath, "report", "-", "JSON report path, or - for stdout")
	flags.DurationVar(&cfg.Timeout, "timeout", 10*time.Minute, "overall gate and promotion timeout")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if strings.TrimSpace(cfg.ManifestPath) == "" {
		return config{}, fmt.Errorf("--manifest is required")
	}
	if cfg.Timeout <= 0 {
		return config{}, fmt.Errorf("--timeout must be greater than zero")
	}
	cfg.QuackURI = getenv("QUACK_URI", "quack:127.0.0.1:9494")
	cfg.QuackToken = strings.TrimSpace(os.Getenv("QUACK_TOKEN"))
	cfg.RemoteDB = getenv("QUACK_REMOTE_DB", "remote_lake")
	disableSSL, err := getenvBool("QUACK_DISABLE_SSL", true)
	if err != nil {
		return config{}, err
	}
	cfg.DisableSSL = disableSSL
	if cfg.QuackToken == "" {
		return config{}, fmt.Errorf("QUACK_TOKEN is required")
	}
	return cfg, nil
}

func writeReport(path string, stdout io.Writer, result report) error {
	var writer io.Writer = stdout
	closeWriter := func() error { return nil }
	if path != "" && path != "-" {
		file, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create report: %w", err)
		}
		writer = file
		closeWriter = file.Close
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		_ = closeWriter()
		return fmt.Errorf("write report: %w", err)
	}
	if err := closeWriter(); err != nil {
		return fmt.Errorf("close report: %w", err)
	}
	return nil
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", key, err)
	}
	return value, nil
}

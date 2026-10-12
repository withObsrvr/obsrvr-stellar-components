package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/withObsrvr/flowctl-sdk/pkg/stellar"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/normalize"
	"google.golang.org/protobuf/proto"
)

func main() {
	registry := prometheus.NewRegistry()
	received := prometheus.NewGauge(prometheus.GaugeOpts{Name: "obsrvr_stellar_processor_last_received_ledger", Help: "Last successfully decoded raw ledger passed to the processor callback."})
	transformed := prometheus.NewGauge(prometheus.GaugeOpts{Name: "obsrvr_stellar_processor_last_transformed_ledger", Help: "Last ledger successfully normalized by the processor callback (not yet delivered)."})
	failures := prometheus.NewCounter(prometheus.CounterOpts{Name: "obsrvr_stellar_processor_transform_errors_total", Help: "Ledger normalization failures."})
	registry.MustRegister(received, transformed, failures)
	// The SDK owns HEALTH_PORT; use a distinct port for processor metrics.
	if err := startProcessorMetricsServer(os.Getenv("PROCESSOR_METRICS_ADDR"), registry); err != nil {
		log.Fatalf("start processor metrics endpoint: %v", err)
	}
	stellar.Run(stellar.ProcessorConfig{
		ProcessorName: "Stellar Ledger Processor",
		OutputType:    contracts.LedgerBatchEventType,
		ComponentID:   getenv("COMPONENT_ID", contracts.DefaultComponentID),
		ProcessLedger: func(passphrase string, ledger xdr.LedgerCloseMeta) (proto.Message, error) {
			received.Set(float64(ledger.LedgerSequence()))
			batch, err := normalize.LedgerBatch(ledger, normalize.Options{
				NetworkPassphrase: passphrase,
			})
			if err != nil {
				failures.Inc()
				return nil, err
			}
			transformed.Set(float64(batch.LedgerSequence))
			log.Printf("emitting ledger batch ledger=%d txs=%d ops=%d", batch.LedgerSequence, len(batch.Transactions), len(batch.Operations))
			return batch, nil
		},
	})
}

// startProcessorMetricsServer binds the metrics listener synchronously so a
// misconfigured or occupied port is a startup failure rather than a process
// that runs without the scrape target its deployment checks expect.
func startProcessorMetricsServer(addr string, gatherer prometheus.Gatherer) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil
	}
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	server := &http.Server{Handler: mux}
	go func() {
		log.Printf("processor metrics endpoint listening on %s", addr)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("processor metrics endpoint failed: %v", err)
		}
	}()
	return nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

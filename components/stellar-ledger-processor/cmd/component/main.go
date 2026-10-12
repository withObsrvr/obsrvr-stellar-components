package main

import (
	"log"
	"net/http"
	"os"

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
	if addr := os.Getenv("PROCESSOR_METRICS_ADDR"); addr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
		go func() {
			if err := http.ListenAndServe(addr, mux); err != nil {
				log.Printf("processor metrics server: %v", err)
			}
		}()
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

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

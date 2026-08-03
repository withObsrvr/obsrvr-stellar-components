package main

import (
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var ingestLatencyBuckets = []float64{
	0.010, 0.025, 0.050, 0.075, 0.100,
	0.150, 0.200, 0.250, 0.300, 0.350,
	0.400, 0.500, 0.750, 1, 2, 5, 10,
}

const ingestLatencyBudget = 400 * time.Millisecond

type serverMetrics struct {
	ingestPhase           *prometheus.HistogramVec
	ingestBatches         *prometheus.CounterVec
	ingestRetries         prometheus.Counter
	ingestOverBudget      prometheus.Counter
	ingestInflight        prometheus.Gauge
	ingestLastLedger      prometheus.Gauge
	checkpointDuration    *prometheus.HistogramVec
	checkpointTotal       *prometheus.CounterVec
	checkpointDeferred    *prometheus.CounterVec
	checkpointLastSuccess prometheus.Gauge
}

func newServerMetrics(registerer prometheus.Registerer, catalogPath string) *serverMetrics {
	m := &serverMetrics{
		ingestPhase: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "obsrvr_ducklake_ingest_phase_seconds",
			Help:    "Server-side ledger ingest latency by bounded phase.",
			Buckets: ingestLatencyBuckets,
		}, []string{"phase"}),
		ingestBatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obsrvr_ducklake_ingest_batches_total",
			Help: "Ledger ingest batches by final result and replay path.",
		}, []string{"result", "replayed"}),
		ingestRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "obsrvr_ducklake_ingest_retries_total",
			Help: "Ledger ingest retries attempted after a failed or uncertain commit.",
		}),
		ingestOverBudget: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "obsrvr_ducklake_ingest_over_budget_total",
			Help: "Successfully acknowledged ledgers whose server receive-to-ack latency exceeded 400ms.",
		}),
		ingestInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "obsrvr_ducklake_ingest_inflight",
			Help: "Ledger batches currently being processed by the ingest service.",
		}),
		ingestLastLedger: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "obsrvr_ducklake_ingest_last_ledger",
			Help: "Latest successfully acknowledged ledger sequence.",
		}),
		checkpointDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "obsrvr_ducklake_checkpoint_duration_seconds",
			Help:    "DuckDB catalog checkpoint duration by trigger and result.",
			Buckets: ingestLatencyBuckets,
		}, []string{"trigger", "result"}),
		checkpointTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obsrvr_ducklake_checkpoint_total",
			Help: "DuckDB catalog checkpoints by trigger and result.",
		}, []string{"trigger", "result"}),
		checkpointDeferred: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obsrvr_ducklake_checkpoint_deferred_total",
			Help: "DuckDB catalog checkpoint deferrals by bounded reason.",
		}, []string{"reason"}),
		checkpointLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "obsrvr_ducklake_checkpoint_last_success_timestamp_seconds",
			Help: "Unix timestamp of the most recent successful explicit checkpoint.",
		}),
	}

	registerer.MustRegister(
		m.ingestPhase,
		m.ingestBatches,
		m.ingestRetries,
		m.ingestOverBudget,
		m.ingestInflight,
		m.ingestLastLedger,
		m.checkpointDuration,
		m.checkpointTotal,
		m.checkpointDeferred,
		m.checkpointLastSuccess,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "obsrvr_ducklake_catalog_wal_bytes",
			Help: "Current DuckDB catalog WAL size in bytes.",
		}, func() float64 { return fileSizeBytes(catalogPath + ".wal") }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "obsrvr_ducklake_catalog_file_bytes",
			Help: "Current DuckDB catalog file size in bytes.",
		}, func() float64 { return fileSizeBytes(catalogPath) }),
	)

	for _, phase := range []string{"decode", "staging", "preface", "transfer", "commit", "cleanup", "total"} {
		m.ingestPhase.WithLabelValues(phase)
	}
	for _, reason := range []string{"ingest_active", "retry_backoff"} {
		m.checkpointDeferred.WithLabelValues(reason)
	}
	for _, result := range []string{"success", "error"} {
		for _, replayed := range []string{"false", "true"} {
			m.ingestBatches.WithLabelValues(result, replayed)
		}
		for _, trigger := range []string{"idle", "manual", "hard_limit"} {
			m.checkpointDuration.WithLabelValues(trigger, result)
			m.checkpointTotal.WithLabelValues(trigger, result)
		}
	}
	return m
}

func (m *serverMetrics) recordBatch(result string, replayed bool) {
	if m == nil {
		return
	}
	m.ingestBatches.WithLabelValues(result, strconv.FormatBool(replayed)).Inc()
}

func fileSizeBytes(path string) float64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return float64(info.Size())
}

package main

import "github.com/prometheus/client_golang/prometheus"

var ingestRPCLatencyBuckets = []float64{
	0.010, 0.025, 0.050, 0.075, 0.100,
	0.150, 0.200, 0.250, 0.300, 0.350,
	0.400, 0.500, 0.750, 1, 2, 5, 10,
}

type sinkMetrics struct {
	ingestRPCRoundTrip prometheus.Histogram
	ingestRetries      prometheus.Counter
}

func newSinkMetrics(registerer prometheus.Registerer) *sinkMetrics {
	m := &sinkMetrics{
		ingestRPCRoundTrip: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "obsrvr_ducklake_ingest_rpc_round_trip_seconds",
			Help:    "DuckLake sink send-to-ack latency for successfully acknowledged ingest RPC batches.",
			Buckets: ingestRPCLatencyBuckets,
		}),
		ingestRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "obsrvr_ducklake_ingest_retries_total",
			Help: "DuckLake sink write retries attempted after a failed send or uncertain acknowledgement.",
		}),
	}
	registerer.MustRegister(m.ingestRPCRoundTrip, m.ingestRetries)
	return m
}

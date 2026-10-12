package main

import (
	"net"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestStartProcessorMetricsServerFailsOnBusyAddress(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	if err := startProcessorMetricsServer(busy.Addr().String(), prometheus.NewRegistry()); err == nil {
		t.Fatal("occupied metrics port accepted; startup must fail instead of running without a scrape target")
	}
}

func TestStartProcessorMetricsServerSkipsEmptyAddress(t *testing.T) {
	if err := startProcessorMetricsServer("  ", prometheus.NewRegistry()); err != nil {
		t.Fatalf("unset PROCESSOR_METRICS_ADDR must stay optional: %v", err)
	}
}

func TestStartProcessorMetricsServerServesBarePort(t *testing.T) {
	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "obsrvr_stellar_processor_test_gauge", Help: "Test gauge."})
	registry.MustRegister(gauge)
	gauge.Set(7)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	if err := startProcessorMetricsServer(port, registry); err != nil {
		t.Fatalf("bare port %s rejected: %v", port, err)
	}
	resp, err := http.Get("http://127.0.0.1:" + port + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics status = %d", resp.StatusCode)
	}
}

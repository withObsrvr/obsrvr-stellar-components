package main

import (
	"net"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"google.golang.org/grpc"
)

type ackMismatchServer struct {
	componentsv1.UnimplementedBronzeIngestServiceServer
	attempts atomic.Int32
}

func (s *ackMismatchServer) IngestLedgerBatches(stream componentsv1.BronzeIngestService_IngestLedgerBatchesServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		attempt := s.attempts.Add(1)
		ledger := req.Batch.LedgerSequence
		if attempt == 1 {
			ledger--
		} // sent, but no matching acknowledgement
		if err := stream.Send(&componentsv1.IngestLedgerBatchAck{LedgerSequence: ledger}); err != nil {
			return err
		}
	}
}

func metricValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family.Metric[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("missing metric %s", name)
	return 0
}

func TestIngestRPCDoesNotClaimAcknowledgementOnMismatch(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	fake := &ackMismatchServer{}
	componentsv1.RegisterBronzeIngestServiceServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	sink, err := newIngestRPCSink(DuckLakeConfig{IngestEndpoint: listener.Addr().String(), QuackToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	registry := prometheus.NewRegistry()
	sink.metrics = newSinkMetrics(registry)
	batch := &componentsv1.LedgerBatch{LedgerSequence: 101, NetworkPassphrase: "testnet"}
	if err := sink.WriteBatch(batch); err == nil {
		t.Fatal("mismatched ack accepted")
	}
	if sent := metricValue(t, registry, "obsrvr_ducklake_sink_last_sent_ledger"); sent != 101 {
		t.Fatalf("sent = %v", sent)
	}
	if ack := metricValue(t, registry, "obsrvr_ducklake_sink_last_acknowledged_ledger"); ack != 0 {
		t.Fatalf("ack after mismatch = %v", ack)
	}
	if err := sink.WriteBatch(batch); err != nil {
		t.Fatalf("replay after stream reset: %v", err)
	}
	if ack := metricValue(t, registry, "obsrvr_ducklake_sink_last_acknowledged_ledger"); ack != 101 {
		t.Fatalf("ack after replay = %v", ack)
	}
	if got := fake.attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d", got)
	}
}

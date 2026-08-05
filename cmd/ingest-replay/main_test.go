package main

import (
	"bytes"
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"testing"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/ledgerfixture"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRunWritesFailureSummaryWhenLatencyBudgetIsBreached(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "fixture.manifest.json")
	batchJSON, err := protojson.Marshal(&componentsv1.LedgerBatch{
		NetworkPassphrase: "test network",
		LedgerSequence:    100,
		SchemaVersion:     "schema-v1",
		ExtractionVersion: "extract-v1",
	})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	if _, err := ledgerfixture.RecordJSONL(bytes.NewReader(batchJSON), ledgerfixture.RecordOptions{
		ManifestPath:   manifestPath,
		BatchesPerFile: 1,
	}); err != nil {
		t.Fatalf("record fixture: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	componentsv1.RegisterBronzeIngestServiceServer(server, &echoIngestService{token: "fixture-token"})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	var output bytes.Buffer
	err = run([]string{
		"--fixtures=" + manifestPath,
		"--endpoint=" + listener.Addr().String(),
		"--token=fixture-token",
		"--profile=custom",
		"--cadence=0",
		"--count=1",
		"--max-latency=1ns",
		"--summary=-",
	}, &output)
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("run error = %v, want latency breach", err)
	}
	var summary replaySummary
	if err := json.Unmarshal(output.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v\n%s", err, output.String())
	}
	if summary.Success || summary.OverBudget != 1 || summary.Acknowledged != 1 {
		t.Fatalf("failure summary = %+v", summary)
	}
}

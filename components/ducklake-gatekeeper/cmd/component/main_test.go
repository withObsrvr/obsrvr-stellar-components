package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/withObsrvr/obsrvr-stellar-components/internal/gatekeeper"
)

func TestParseConfigRequiresManifestAndToken(t *testing.T) {
	t.Setenv("QUACK_TOKEN", "")
	if _, err := parseConfig(nil, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "--manifest") {
		t.Fatalf("missing manifest error = %v", err)
	}
	if _, err := parseConfig([]string{"--manifest", "proposal.yaml"}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "QUACK_TOKEN") {
		t.Fatalf("missing token error = %v", err)
	}
}

func TestParseConfigReadsTransportEnvironment(t *testing.T) {
	t.Setenv("QUACK_TOKEN", "secret")
	t.Setenv("QUACK_URI", "quack:lake:9494")
	t.Setenv("QUACK_REMOTE_DB", "primary")
	t.Setenv("QUACK_DISABLE_SSL", "false")
	cfg, err := parseConfig([]string{"--manifest", "proposal.yaml", "--timeout", "1m"}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.QuackURI != "quack:lake:9494" || cfg.RemoteDB != "primary" || cfg.DisableSSL || cfg.Timeout.String() != "1m0s" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestWriteReportIncludesGateFailure(t *testing.T) {
	var output bytes.Buffer
	err := writeReport("-", &output, report{
		Status: "rejected",
		Receipt: gatekeeper.PromotionReceipt{Gates: []gatekeeper.GateResult{{
			Name: "reproducibility", Passed: false, Details: "candidate builds differ",
		}}},
		Error: "proposal rejected",
	})
	if err != nil {
		t.Fatalf("write report: %v", err)
	}
	for _, want := range []string{`"status": "rejected"`, `"name": "reproducibility"`, `"passed": false`} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("report missing %q:\n%s", want, output.String())
		}
	}
}

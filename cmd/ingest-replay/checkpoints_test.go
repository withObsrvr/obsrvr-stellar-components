package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSuccessfulIdleCheckpointsIgnoresOtherSeries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(writer, `obsrvr_ducklake_checkpoint_total{result="success",trigger="idle"} 4`)
		fmt.Fprintln(writer, `obsrvr_ducklake_checkpoint_total{result="error",trigger="idle"} 2`)
		fmt.Fprintln(writer, `obsrvr_ducklake_checkpoint_total{result="success",trigger="manual"} 3`)
	}))
	defer server.Close()
	count, err := successfulIdleCheckpoints(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("read checkpoints: %v", err)
	}
	if count != 4 {
		t.Fatalf("checkpoint count = %v, want 4", count)
	}
}

func TestWaitForIdleCheckpointsUsesBaseline(t *testing.T) {
	count := 10
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		count++
		fmt.Fprintf(writer, `obsrvr_ducklake_checkpoint_total{trigger="idle",result="success"} %d`, count)
	}))
	defer server.Close()
	observed, err := waitForIdleCheckpoints(context.Background(), server.URL, 10, 2, time.Second)
	if err != nil {
		t.Fatalf("wait checkpoints: %v", err)
	}
	if observed != 2 {
		t.Fatalf("observed = %d, want 2", observed)
	}
}

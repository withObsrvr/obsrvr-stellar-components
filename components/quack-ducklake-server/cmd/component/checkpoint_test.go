package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type checkpointExecFunc func(context.Context, string, ...any) (sql.Result, error)

func (f checkpointExecFunc) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return f(ctx, query, args...)
}

func TestCheckpointHoldsWriterCoordinatorAndRecordsSuccess(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newServerMetrics(registry, t.TempDir()+"/catalog.ducklake")
	coordinator := &writerCoordinator{}
	started := make(chan string, 1)
	release := make(chan struct{})
	controller := &checkpointController{
		executor: checkpointExecFunc(func(_ context.Context, query string, _ ...any) (sql.Result, error) {
			started <- query
			<-release
			return nil, nil
		}),
		coordinator:        coordinator,
		metadataAttachName: "__ducklake_metadata_stellar_lake",
		timeout:            time.Second,
		metrics:            metrics,
	}

	done := make(chan error, 1)
	go func() {
		_, err := controller.checkpoint(context.Background(), "manual")
		done <- err
	}()
	if query := <-started; query != "CHECKPOINT __ducklake_metadata_stellar_lake" {
		t.Fatalf("checkpoint query = %q", query)
	}
	if coordinator.TryLock() {
		coordinator.Unlock()
		t.Fatal("writer coordinator was available while checkpoint was running")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	body := scrapeMetrics(t, registry)
	for _, want := range []string{
		`obsrvr_ducklake_checkpoint_duration_seconds_count{result="success",trigger="manual"} 1`,
		`obsrvr_ducklake_checkpoint_total{result="success",trigger="manual"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "obsrvr_ducklake_checkpoint_last_success_timestamp_seconds 0\n") {
		t.Fatalf("last checkpoint success timestamp was not updated:\n%s", body)
	}
}

func TestCheckpointDefersWhenIngestOwnsCoordinator(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newServerMetrics(registry, t.TempDir()+"/catalog.ducklake")
	coordinator := &writerCoordinator{}
	coordinator.Lock()
	defer coordinator.Unlock()
	controller := &checkpointController{
		executor: checkpointExecFunc(func(context.Context, string, ...any) (sql.Result, error) {
			t.Fatal("checkpoint executed while writer coordinator was held")
			return nil, nil
		}),
		coordinator:        coordinator,
		metadataAttachName: "__ducklake_metadata_stellar_lake",
		timeout:            time.Second,
		metrics:            metrics,
	}

	if _, err := controller.checkpoint(context.Background(), "manual"); !errors.Is(err, errCheckpointBusy) {
		t.Fatalf("checkpoint error = %v, want errCheckpointBusy", err)
	}
	body := scrapeMetrics(t, registry)
	if !strings.Contains(body, `obsrvr_ducklake_checkpoint_deferred_total{reason="ingest_active"} 1`) {
		t.Fatalf("deferred metric missing:\n%s", body)
	}
}

func TestCheckpointFailureStatePersistsUntilSuccess(t *testing.T) {
	attempt := 0
	controller := &checkpointController{
		executor: checkpointExecFunc(func(context.Context, string, ...any) (sql.Result, error) {
			attempt++
			if attempt == 1 {
				return nil, errors.New("disk failure")
			}
			return nil, nil
		}),
		coordinator:        &writerCoordinator{},
		metadataAttachName: "__ducklake_metadata_stellar_lake",
		timeout:            time.Second,
	}
	if _, err := controller.checkpoint(context.Background(), "manual"); err == nil {
		t.Fatal("failed checkpoint returned nil error")
	}
	failed := controller.State()
	if controller.Healthy() || failed.LastResult != "error" || failed.LastError == "" || failed.ConsecutiveFailures != 1 {
		t.Fatalf("failed checkpoint state = %+v, healthy=%t", failed, controller.Healthy())
	}
	if _, err := controller.checkpoint(context.Background(), "manual"); err != nil {
		t.Fatalf("recovery checkpoint: %v", err)
	}
	recovered := controller.State()
	if !controller.Healthy() || recovered.LastResult != "success" || recovered.LastError != "" || recovered.ConsecutiveFailures != 0 {
		t.Fatalf("recovered checkpoint state = %+v, healthy=%t", recovered, controller.Healthy())
	}
}

func TestHealthDegradesAfterCheckpointFailure(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	controller := &checkpointController{state: checkpointState{LastResult: "error", LastError: "disk failure", ConsecutiveFailures: 1}}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	newServerHTTPHandler(db, "memory", "", nil, controller, "admin-secret").ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("health status = %d, want 503 after checkpoint failure", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "disk failure") {
		t.Fatalf("health response leaked checkpoint error: %q", recorder.Body.String())
	}
}

func TestManualCheckpointHTTPSuccess(t *testing.T) {
	controller := &checkpointController{
		executor: checkpointExecFunc(func(context.Context, string, ...any) (sql.Result, error) {
			return nil, nil
		}),
		coordinator:        &writerCoordinator{},
		metadataAttachName: "__ducklake_metadata_stellar_lake",
		timeout:            time.Second,
	}
	request := httptest.NewRequest(http.MethodPost, "/admin/checkpoint", nil)
	request.Header.Set("Authorization", "Bearer admin-secret")
	recorder := httptest.NewRecorder()
	controller.manualHTTPHandler("admin-secret").ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("checkpoint status = %d, want 200", recorder.Code)
	}
	var response struct {
		Result          string  `json:"result"`
		DurationSeconds float64 `json:"duration_seconds"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode checkpoint response: %v", err)
	}
	if response.Result != "success" || response.DurationSeconds < 0 {
		t.Fatalf("checkpoint response = %+v", response)
	}
}

func TestManualCheckpointHTTPRequiresAuthAndReportsErrors(t *testing.T) {
	controller := &checkpointController{
		executor: checkpointExecFunc(func(context.Context, string, ...any) (sql.Result, error) {
			return nil, errors.New("sensitive storage path")
		}),
		coordinator:        &writerCoordinator{},
		metadataAttachName: "__ducklake_metadata_stellar_lake",
		timeout:            time.Second,
	}
	handler := controller.manualHTTPHandler("admin-secret")

	request := httptest.NewRequest(http.MethodGet, "/admin/checkpoint", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", recorder.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/admin/checkpoint", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated POST status = %d, want 401", recorder.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/admin/checkpoint", nil)
	request.Header.Set("Authorization", "Bearer admin-secret")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("failed checkpoint status = %d, want 500", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "sensitive storage path") {
		t.Fatalf("handler leaked internal checkpoint error: %q", recorder.Body.String())
	}
}

func TestValidateConfigRequiresManualCheckpointToken(t *testing.T) {
	cfg := validTestConfig()
	cfg.CheckpointEnabled = true
	if err := validateConfig(cfg); err == nil || !strings.Contains(err.Error(), "CHECKPOINT_ADMIN_TOKEN") {
		t.Fatalf("validateConfig error = %v, want checkpoint token requirement", err)
	}
	cfg.CheckpointAdminToken = "admin-secret"
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("validateConfig with checkpoint token: %v", err)
	}
}

func scrapeMetrics(t *testing.T, registry *prometheus.Registry) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	newServerHTTPHandler(nil, "stellar_lake", "", registry, nil, "").ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", recorder.Code)
	}
	return recorder.Body.String()
}

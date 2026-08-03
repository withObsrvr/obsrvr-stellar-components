package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

var errCheckpointBusy = errors.New("writer coordinator is busy")

type writerCoordinator struct {
	mu sync.Mutex
}

func (c *writerCoordinator) Lock() {
	c.mu.Lock()
}

func (c *writerCoordinator) Unlock() {
	c.mu.Unlock()
}

func (c *writerCoordinator) TryLock() bool {
	return c.mu.TryLock()
}

type checkpointExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type checkpointController struct {
	executor           checkpointExecutor
	coordinator        *writerCoordinator
	metadataAttachName string
	timeout            time.Duration
	metrics            *serverMetrics
	close              func() error

	stateMu sync.RWMutex
	state   checkpointState
}

type checkpointState struct {
	LastStart           time.Time
	LastEnd             time.Time
	LastResult          string
	LastError           string
	ConsecutiveFailures int
}

type checkpointRetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

var manualCheckpointRetryPolicy = checkpointRetryPolicy{
	MaxAttempts:    3,
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     time.Second,
}

func newCheckpointController(ctx context.Context, db *sql.DB, attachName, metadataAttachName string, timeout time.Duration, coordinator *writerCoordinator, metrics *serverMetrics) (*checkpointController, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open checkpoint connection: %w", err)
	}
	attachName = sanitizeIdentifier(attachName)
	metadataAttachName = sanitizeIdentifier(metadataAttachName)
	if _, err := conn.ExecContext(ctx, "USE "+attachName); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("point checkpoint session at %s: %w", attachName, err)
	}
	return &checkpointController{
		executor:           conn,
		coordinator:        coordinator,
		metadataAttachName: metadataAttachName,
		timeout:            timeout,
		metrics:            metrics,
		close:              conn.Close,
	}, nil
}

func (c *checkpointController) Close() error {
	if c == nil || c.close == nil {
		return nil
	}
	return c.close()
}

func (c *checkpointController) State() checkpointState {
	if c == nil {
		return checkpointState{}
	}
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state
}

func (c *checkpointController) Healthy() bool {
	return c.State().LastResult != "error"
}

func (c *checkpointController) checkpoint(ctx context.Context, trigger string) (time.Duration, error) {
	if !c.coordinator.TryLock() {
		if c.metrics != nil {
			c.metrics.checkpointDeferred.WithLabelValues("ingest_active").Inc()
		}
		return 0, errCheckpointBusy
	}
	defer c.coordinator.Unlock()
	if c.metrics != nil {
		c.metrics.checkpointInflight.Inc()
		defer c.metrics.checkpointInflight.Dec()
	}

	start := time.Now()
	c.stateMu.Lock()
	c.state.LastStart = start
	c.state.LastResult = "running"
	c.stateMu.Unlock()
	// DuckLake overrides CHECKPOINT on the logical attachment with data-file
	// maintenance (including physical cleanup). Its file-backed metadata WAL
	// belongs to the hidden DuckDB attachment, which is the intended target.
	_, err := c.executor.ExecContext(ctx, "CHECKPOINT "+c.metadataAttachName)
	duration := time.Since(start)
	result := "success"
	if err != nil {
		result = "error"
	}
	end := time.Now()
	c.stateMu.Lock()
	c.state.LastEnd = end
	c.state.LastResult = result
	if err != nil {
		c.state.LastError = err.Error()
		c.state.ConsecutiveFailures++
	} else {
		c.state.LastError = ""
		c.state.ConsecutiveFailures = 0
	}
	c.stateMu.Unlock()
	if c.metrics != nil {
		c.metrics.checkpointDuration.WithLabelValues(trigger, result).Observe(duration.Seconds())
		c.metrics.checkpointTotal.WithLabelValues(trigger, result).Inc()
		if err == nil {
			c.metrics.checkpointLastSuccess.Set(float64(end.Unix()))
		}
	}
	if err != nil {
		log.Printf("checkpoint trigger=%s result=error duration=%s: %v", trigger, duration.Round(time.Millisecond), err)
		return duration, fmt.Errorf("checkpoint %s: %w", c.metadataAttachName, err)
	}
	log.Printf("checkpoint trigger=%s result=success duration=%s", trigger, duration.Round(time.Millisecond))
	return duration, nil
}

func (c *checkpointController) checkpointWithRetry(ctx context.Context, trigger string, policy checkpointRetryPolicy) (time.Duration, error) {
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	if policy.InitialBackoff < 0 {
		policy.InitialBackoff = 0
	}
	if policy.MaxBackoff < policy.InitialBackoff {
		policy.MaxBackoff = policy.InitialBackoff
	}

	var totalDuration time.Duration
	var lastErr error
	backoff := policy.InitialBackoff
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		duration, err := c.checkpoint(ctx, trigger)
		totalDuration += duration
		if err == nil {
			return totalDuration, nil
		}
		lastErr = err
		if attempt == policy.MaxAttempts {
			break
		}
		if c.metrics != nil {
			c.metrics.checkpointDeferred.WithLabelValues("retry_backoff").Inc()
		}
		if backoff > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return totalDuration, fmt.Errorf("checkpoint retry canceled: %w", ctx.Err())
			case <-timer.C:
			}
		}
		if backoff < policy.MaxBackoff {
			backoff *= 2
			if backoff > policy.MaxBackoff {
				backoff = policy.MaxBackoff
			}
		}
	}
	return totalDuration, lastErr
}

func (c *checkpointController) manualHTTPHandler(adminToken string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if provided == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(adminToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		checkpointCtx, cancel := context.WithTimeout(r.Context(), c.timeout)
		defer cancel()
		duration, err := c.checkpointWithRetry(checkpointCtx, "manual", manualCheckpointRetryPolicy)
		if errors.Is(err, errCheckpointBusy) {
			http.Error(w, "checkpoint deferred: ingest active", http.StatusConflict)
			return
		}
		if err != nil {
			http.Error(w, "checkpoint failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"result":"success","duration_seconds":%.6f}`+"\n", duration.Seconds())
	}
}

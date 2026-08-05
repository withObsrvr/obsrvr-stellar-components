package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const checkpointMetricName = "obsrvr_ducklake_checkpoint_total"

func successfulIdleCheckpoints(ctx context.Context, metricsURL string) (float64, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return 0, fmt.Errorf("build metrics request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, fmt.Errorf("scrape checkpoint metrics: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4*1024))
		return 0, fmt.Errorf("scrape checkpoint metrics: HTTP %d", response.StatusCode)
	}

	var total float64
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, checkpointMetricName+"{") {
			continue
		}
		closing := strings.IndexByte(line, '}')
		if closing < 0 {
			continue
		}
		labels := line[len(checkpointMetricName)+1 : closing]
		if !hasPrometheusLabel(labels, "trigger", "idle") || !hasPrometheusLabel(labels, "result", "success") {
			continue
		}
		value := strings.TrimSpace(line[closing+1:])
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %s sample %q: %w", checkpointMetricName, value, err)
		}
		total += parsed
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("read checkpoint metrics: %w", err)
	}
	return total, nil
}

func hasPrometheusLabel(labels, name, value string) bool {
	want := name + "=\"" + value + "\""
	for _, label := range strings.Split(labels, ",") {
		if strings.TrimSpace(label) == want {
			return true
		}
	}
	return false
}

func waitForIdleCheckpoints(ctx context.Context, metricsURL string, baseline float64, required int, timeout time.Duration) (int, error) {
	if required == 0 {
		return 0, nil
	}
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastObserved int
	for {
		current, err := successfulIdleCheckpoints(waitContext, metricsURL)
		if err == nil {
			lastObserved = int(current - baseline)
			if lastObserved >= required {
				return lastObserved, nil
			}
		}
		select {
		case <-waitContext.Done():
			return lastObserved, fmt.Errorf("observed %d new successful idle checkpoints, require %d: %w", lastObserved, required, waitContext.Err())
		case <-ticker.C:
		}
	}
}

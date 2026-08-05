package main

import (
	"math"
	"sort"
	"time"
)

type latencySummary struct {
	Count    int     `json:"count"`
	MinMS    float64 `json:"min_ms"`
	MedianMS float64 `json:"median_ms"`
	P95MS    float64 `json:"p95_ms"`
	P99MS    float64 `json:"p99_ms"`
	MaxMS    float64 `json:"max_ms"`
	MeanMS   float64 `json:"mean_ms"`
}

func summarizeDurations(values []time.Duration) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum time.Duration
	for _, value := range sorted {
		sum += value
	}
	return latencySummary{
		Count:    len(sorted),
		MinMS:    milliseconds(sorted[0]),
		MedianMS: milliseconds(percentile(sorted, 0.50)),
		P95MS:    milliseconds(percentile(sorted, 0.95)),
		P99MS:    milliseconds(percentile(sorted, 0.99)),
		MaxMS:    milliseconds(sorted[len(sorted)-1]),
		MeanMS:   milliseconds(sum / time.Duration(len(sorted))),
	}
}

func percentile(sorted []time.Duration, quantile float64) time.Duration {
	index := int(math.Ceil(quantile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func milliseconds(value time.Duration) float64 {
	return math.Round(float64(value)/float64(time.Millisecond)*1000) / 1000
}

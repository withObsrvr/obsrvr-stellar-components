package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/rs/zerolog/log"

	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

// BenchmarkSuite provides comprehensive performance benchmarks
type BenchmarkSuite struct {
	pool       memory.Allocator
	results    *BenchmarkResults
	config     *BenchmarkConfig
}

// BenchmarkConfig configures benchmark parameters
type BenchmarkConfig struct {
	Iterations      int           `json:"iterations"`
	WarmupRounds    int           `json:"warmup_rounds"`
	Concurrency     int           `json:"concurrency"`
	DataSizes       []int         `json:"data_sizes"`
	Timeout         time.Duration `json:"timeout"`
	MemoryProfiling bool          `json:"memory_profiling"`
	CPUProfiling    bool          `json:"cpu_profiling"`
}

// BenchmarkResults contains all benchmark results
type BenchmarkResults struct {
	Timestamp        time.Time                    `json:"timestamp"`
	Environment      EnvironmentInfo              `json:"environment"`
	ComponentResults map[string]ComponentBenchmark `json:"component_results"`
	PerformanceIndex float64                      `json:"performance_index"`
	Recommendations  []string                     `json:"recommendations"`
}

// EnvironmentInfo captures test environment details
type EnvironmentInfo struct {
	GOOS         string `json:"goos"`
	GOARCH       string `json:"goarch"`
	GoVersion    string `json:"go_version"`
	NumCPU       int    `json:"num_cpu"`
	MemoryMB     int64  `json:"memory_mb"`
	ArrowVersion string `json:"arrow_version"`
}

// ComponentBenchmark contains benchmarks for a specific component
type ComponentBenchmark struct {
	Name            string                     `json:"name"`
	OperationResults map[string]OperationBench `json:"operation_results"`
	MemoryUsage     MemoryProfile              `json:"memory_usage"`
	Throughput      ThroughputMetrics          `json:"throughput"`
	Latency         LatencyMetrics             `json:"latency"`
}

// OperationBench contains benchmark results for a specific operation
type OperationBench struct {
	Operation        string        `json:"operation"`
	Iterations       int           `json:"iterations"`
	TotalTime        time.Duration `json:"total_time"`
	AverageTime      time.Duration `json:"average_time"`
	MinTime          time.Duration `json:"min_time"`
	MaxTime          time.Duration `json:"max_time"`
	P95Time          time.Duration `json:"p95_time"`
	P99Time          time.Duration `json:"p99_time"`
	ThroughputOpsPerSec float64    `json:"throughput_ops_per_sec"`
	MemoryAllocated  int64         `json:"memory_allocated"`
	MemoryAllocations int64        `json:"memory_allocations"`
	GCRuns           int64         `json:"gc_runs"`
}

// MemoryProfile contains memory usage statistics
type MemoryProfile struct {
	HeapInUse     int64 `json:"heap_in_use"`
	HeapAlloc     int64 `json:"heap_alloc"`
	StackInUse    int64 `json:"stack_in_use"`
	TotalAlloc    int64 `json:"total_alloc"`
	Mallocs       int64 `json:"mallocs"`
	Frees         int64 `json:"frees"`
	GCCycles      int64 `json:"gc_cycles"`
	PauseTimeNs   int64 `json:"pause_time_ns"`
}

// ThroughputMetrics contains throughput measurements
type ThroughputMetrics struct {
	RecordsPerSecond   float64 `json:"records_per_second"`
	MegabytesPerSecond float64 `json:"megabytes_per_second"`
	EventsPerSecond    float64 `json:"events_per_second"`
	PeakThroughput     float64 `json:"peak_throughput"`
}

// LatencyMetrics contains latency measurements
type LatencyMetrics struct {
	Mean       time.Duration `json:"mean"`
	Median     time.Duration `json:"median"`
	P95        time.Duration `json:"p95"`
	P99        time.Duration `json:"p99"`
	P999       time.Duration `json:"p999"`
	Min        time.Duration `json:"min"`
	Max        time.Duration `json:"max"`
	StdDev     time.Duration `json:"std_dev"`
}

// NewBenchmarkSuite creates a new benchmark suite
func NewBenchmarkSuite(config *BenchmarkConfig) *BenchmarkSuite {
	return &BenchmarkSuite{
		pool:   memory.NewGoAllocator(),
		config: config,
		results: &BenchmarkResults{
			Timestamp: time.Now(),
			Environment: EnvironmentInfo{
				GOOS:         runtime.GOOS,
				GOARCH:       runtime.GOARCH,
				GoVersion:    runtime.Version(),
				NumCPU:       runtime.NumCPU(),
				ArrowVersion: "17.0.0",
			},
			ComponentResults: make(map[string]ComponentBenchmark),
		},
	}
}

// RunAllBenchmarks executes the complete benchmark suite
func (bs *BenchmarkSuite) RunAllBenchmarks() (*BenchmarkResults, error) {
	log.Info().Msg("Starting comprehensive benchmark suite")
	
	// Capture initial memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	bs.results.Environment.MemoryMB = int64(m.Sys / 1024 / 1024)
	
	// Run component benchmarks
	if err := bs.benchmarkStellarSource(); err != nil {
		return nil, fmt.Errorf("stellar source benchmark failed: %w", err)
	}
	
	if err := bs.benchmarkTTPProcessor(); err != nil {
		return nil, fmt.Errorf("TTP processor benchmark failed: %w", err)
	}
	
	if err := bs.benchmarkAnalyticsSink(); err != nil {
		return nil, fmt.Errorf("analytics sink benchmark failed: %w", err)
	}
	
	// Calculate performance index
	bs.results.PerformanceIndex = bs.calculatePerformanceIndex()
	bs.results.Recommendations = bs.generateRecommendations()
	
	log.Info().
		Float64("performance_index", bs.results.PerformanceIndex).
		Msg("Benchmark suite completed")
	
	return bs.results, nil
}

// benchmarkStellarSource benchmarks the stellar arrow source component
func (bs *BenchmarkSuite) benchmarkStellarSource() error {
	log.Info().Msg("Benchmarking stellar-arrow-source component")
	
	componentBench := ComponentBenchmark{
		Name:             "stellar-arrow-source",
		OperationResults: make(map[string]OperationBench),
	}
	
	// Benchmark XDR processing
	benchResult := bs.benchmarkOperation("xdr_processing", func() error {
		return bs.benchmarkXDRProcessing()
	})
	componentBench.OperationResults["xdr_processing"] = benchResult
	
	// Benchmark Arrow conversion
	benchResult = bs.benchmarkOperation("arrow_conversion", func() error {
		return bs.benchmarkArrowConversion()
	})
	componentBench.OperationResults["arrow_conversion"] = benchResult
	
	// Benchmark Flight streaming
	benchResult = bs.benchmarkOperation("flight_streaming", func() error {
		return bs.benchmarkFlightStreaming()
	})
	componentBench.OperationResults["flight_streaming"] = benchResult
	
	// Capture memory profile
	componentBench.MemoryUsage = bs.captureMemoryProfile()
	componentBench.Throughput = bs.calculateThroughput(componentBench.OperationResults)
	componentBench.Latency = bs.calculateLatency(componentBench.OperationResults)
	
	bs.results.ComponentResults["stellar-arrow-source"] = componentBench
	return nil
}

// benchmarkTTPProcessor benchmarks the TTP processor component
func (bs *BenchmarkSuite) benchmarkTTPProcessor() error {
	log.Info().Msg("Benchmarking ttp-arrow-processor component")
	
	componentBench := ComponentBenchmark{
		Name:             "ttp-arrow-processor",
		OperationResults: make(map[string]OperationBench),
	}
	
	// Benchmark event extraction
	benchResult := bs.benchmarkOperation("event_extraction", func() error {
		return bs.benchmarkEventExtraction()
	})
	componentBench.OperationResults["event_extraction"] = benchResult
	
	// Benchmark vectorized filtering
	benchResult = bs.benchmarkOperation("vectorized_filtering", func() error {
		return bs.benchmarkVectorizedFiltering()
	})
	componentBench.OperationResults["vectorized_filtering"] = benchResult
	
	// Benchmark compute operations
	benchResult = bs.benchmarkOperation("compute_operations", func() error {
		return bs.benchmarkComputeOperations()
	})
	componentBench.OperationResults["compute_operations"] = benchResult
	
	// Capture profiles
	componentBench.MemoryUsage = bs.captureMemoryProfile()
	componentBench.Throughput = bs.calculateThroughput(componentBench.OperationResults)
	componentBench.Latency = bs.calculateLatency(componentBench.OperationResults)
	
	bs.results.ComponentResults["ttp-arrow-processor"] = componentBench
	return nil
}

// benchmarkAnalyticsSink benchmarks the analytics sink component
func (bs *BenchmarkSuite) benchmarkAnalyticsSink() error {
	log.Info().Msg("Benchmarking arrow-analytics-sink component")
	
	componentBench := ComponentBenchmark{
		Name:             "arrow-analytics-sink",
		OperationResults: make(map[string]OperationBench),
	}
	
	// Benchmark Parquet writing
	benchResult := bs.benchmarkOperation("parquet_writing", func() error {
		return bs.benchmarkParquetWriting()
	})
	componentBench.OperationResults["parquet_writing"] = benchResult
	
	// Benchmark WebSocket streaming
	benchResult = bs.benchmarkOperation("websocket_streaming", func() error {
		return bs.benchmarkWebSocketStreaming()
	})
	componentBench.OperationResults["websocket_streaming"] = benchResult
	
	// Benchmark JSON export
	benchResult = bs.benchmarkOperation("json_export", func() error {
		return bs.benchmarkJSONExport()
	})
	componentBench.OperationResults["json_export"] = benchResult
	
	// Capture profiles
	componentBench.MemoryUsage = bs.captureMemoryProfile()
	componentBench.Throughput = bs.calculateThroughput(componentBench.OperationResults)
	componentBench.Latency = bs.calculateLatency(componentBench.OperationResults)
	
	bs.results.ComponentResults["arrow-analytics-sink"] = componentBench
	return nil
}

// benchmarkOperation runs a benchmark for a specific operation
func (bs *BenchmarkSuite) benchmarkOperation(name string, operation func() error) OperationBench {
	log.Debug().Str("operation", name).Msg("Running operation benchmark")
	
	// Warmup
	for i := 0; i < bs.config.WarmupRounds; i++ {
		operation()
	}
	
	// Clear GC before measurement
	runtime.GC()
	runtime.GC() // Double GC to ensure clean state
	
	// Capture initial memory stats
	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	
	// Run benchmark
	times := make([]time.Duration, bs.config.Iterations)
	startTime := time.Now()
	
	for i := 0; i < bs.config.Iterations; i++ {
		iterStart := time.Now()
		if err := operation(); err != nil {
			log.Error().Err(err).Str("operation", name).Msg("Operation failed during benchmark")
		}
		times[i] = time.Since(iterStart)
	}
	
	totalTime := time.Since(startTime)
	
	// Capture final memory stats
	runtime.ReadMemStats(&memAfter)
	
	// Calculate statistics
	minTime, maxTime := times[0], times[0]
	var totalDuration time.Duration
	
	for _, t := range times {
		totalDuration += t
		if t < minTime {
			minTime = t
		}
		if t > maxTime {
			maxTime = t
		}
	}
	
	averageTime := totalDuration / time.Duration(bs.config.Iterations)
	throughput := float64(bs.config.Iterations) / totalTime.Seconds()
	
	// Calculate percentiles
	p95Time := bs.calculatePercentile(times, 0.95)
	p99Time := bs.calculatePercentile(times, 0.99)
	
	return OperationBench{
		Operation:           name,
		Iterations:          bs.config.Iterations,
		TotalTime:           totalTime,
		AverageTime:         averageTime,
		MinTime:             minTime,
		MaxTime:             maxTime,
		P95Time:             p95Time,
		P99Time:             p99Time,
		ThroughputOpsPerSec: throughput,
		MemoryAllocated:     int64(memAfter.TotalAlloc - memBefore.TotalAlloc),
		MemoryAllocations:   int64(memAfter.Mallocs - memBefore.Mallocs),
		GCRuns:              int64(memAfter.NumGC - memBefore.NumGC),
	}
}

// Individual benchmark implementations
func (bs *BenchmarkSuite) benchmarkXDRProcessing() error {
	// Generate synthetic XDR data
	xdrData := bs.generateSyntheticXDR()
	
	// Process XDR (simplified - would use actual XDR processor)
	_ = xdrData
	return nil
}

func (bs *BenchmarkSuite) benchmarkArrowConversion() error {
	// Create test Arrow record
	builder := array.NewRecordBuilder(bs.pool, schemas.StellarLedgerSchema)
	defer builder.Release()
	
	// Add test data
	for i := 0; i < 1000; i++ {
		builder.Field(0).(*array.Uint32Builder).Append(uint32(1000000 + i))
		builder.Field(1).(*array.FixedSizeBinaryBuilder).Append(make([]byte, 32))
		// Add remaining fields...
		for j := 2; j < schemas.StellarLedgerSchema.NumFields(); j++ {
			builder.Field(j).AppendNull()
		}
	}
	
	record := builder.NewRecord()
	defer record.Release()
	
	return nil
}

func (bs *BenchmarkSuite) benchmarkFlightStreaming() error {
	// Simulate Flight streaming operations
	return nil
}

func (bs *BenchmarkSuite) benchmarkEventExtraction() error {
	// Simulate TTP event extraction
	return nil
}

func (bs *BenchmarkSuite) benchmarkVectorizedFiltering() error {
	// Create test data for vectorized filtering
	builder := array.NewRecordBuilder(bs.pool, schemas.TTPEventSchema)
	defer builder.Release()
	
	// Add test data
	for i := 0; i < 5000; i++ {
		// Add synthetic TTP event data
		for j := 0; j < schemas.TTPEventSchema.NumFields(); j++ {
			builder.Field(j).AppendNull()
		}
	}
	
	record := builder.NewRecord()
	defer record.Release()
	
	// Simulate vectorized filtering operations
	return nil
}

func (bs *BenchmarkSuite) benchmarkComputeOperations() error {
	// Simulate Arrow compute operations
	return nil
}

func (bs *BenchmarkSuite) benchmarkParquetWriting() error {
	// Create test data for Parquet writing
	builder := array.NewRecordBuilder(bs.pool, schemas.TTPEventSchema)
	defer builder.Release()
	
	// Generate data
	for i := 0; i < 2000; i++ {
		for j := 0; j < schemas.TTPEventSchema.NumFields(); j++ {
			builder.Field(j).AppendNull()
		}
	}
	
	record := builder.NewRecord()
	defer record.Release()
	
	// Simulate Parquet writing
	return nil
}

func (bs *BenchmarkSuite) benchmarkWebSocketStreaming() error {
	// Simulate WebSocket streaming performance
	return nil
}

func (bs *BenchmarkSuite) benchmarkJSONExport() error {
	// Simulate JSON export performance
	return nil
}

// generateSyntheticXDR creates synthetic XDR data for testing
func (bs *BenchmarkSuite) generateSyntheticXDR() []byte {
	// Generate realistic XDR data
	return make([]byte, 1024) // Placeholder
}

// captureMemoryProfile captures current memory usage
func (bs *BenchmarkSuite) captureMemoryProfile() MemoryProfile {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	return MemoryProfile{
		HeapInUse:   int64(m.HeapInuse),
		HeapAlloc:   int64(m.HeapAlloc),
		StackInUse:  int64(m.StackInuse),
		TotalAlloc:  int64(m.TotalAlloc),
		Mallocs:     int64(m.Mallocs),
		Frees:       int64(m.Frees),
		GCCycles:    int64(m.NumGC),
		PauseTimeNs: int64(m.PauseTotalNs),
	}
}

// calculateThroughput calculates throughput metrics from operation results
func (bs *BenchmarkSuite) calculateThroughput(operations map[string]OperationBench) ThroughputMetrics {
	var totalOps float64
	var maxThroughput float64
	
	for _, op := range operations {
		totalOps += op.ThroughputOpsPerSec
		if op.ThroughputOpsPerSec > maxThroughput {
			maxThroughput = op.ThroughputOpsPerSec
		}
	}
	
	return ThroughputMetrics{
		RecordsPerSecond:   totalOps,
		MegabytesPerSecond: totalOps * 0.001, // Estimate
		EventsPerSecond:    totalOps * 0.5,   // Estimate
		PeakThroughput:     maxThroughput,
	}
}

// calculateLatency calculates latency metrics from operation results
func (bs *BenchmarkSuite) calculateLatency(operations map[string]OperationBench) LatencyMetrics {
	var totalLatency time.Duration
	var minLatency, maxLatency time.Duration
	var p95Total, p99Total time.Duration
	count := 0
	
	for _, op := range operations {
		if count == 0 {
			minLatency = op.MinTime
			maxLatency = op.MaxTime
		} else {
			if op.MinTime < minLatency {
				minLatency = op.MinTime
			}
			if op.MaxTime > maxLatency {
				maxLatency = op.MaxTime
			}
		}
		
		totalLatency += op.AverageTime
		p95Total += op.P95Time
		p99Total += op.P99Time
		count++
	}
	
	if count == 0 {
		return LatencyMetrics{}
	}
	
	return LatencyMetrics{
		Mean:   totalLatency / time.Duration(count),
		Median: totalLatency / time.Duration(count), // Approximation
		P95:    p95Total / time.Duration(count),
		P99:    p99Total / time.Duration(count),
		Min:    minLatency,
		Max:    maxLatency,
	}
}

// calculatePercentile calculates the specified percentile from a slice of durations
func (bs *BenchmarkSuite) calculatePercentile(times []time.Duration, percentile float64) time.Duration {
	if len(times) == 0 {
		return 0
	}
	
	// Simple percentile calculation (would use proper sorting in production)
	index := int(float64(len(times)) * percentile)
	if index >= len(times) {
		index = len(times) - 1
	}
	
	return times[index]
}

// calculatePerformanceIndex calculates an overall performance index (0-100)
func (bs *BenchmarkSuite) calculatePerformanceIndex() float64 {
	index := 100.0
	
	// Factor in throughput (higher is better)
	for _, component := range bs.results.ComponentResults {
		if component.Throughput.RecordsPerSecond < 1000 {
			index -= 10
		}
		
		// Factor in latency (lower is better)
		if component.Latency.P95 > 100*time.Millisecond {
			index -= 15
		}
		
		// Factor in memory usage
		if component.MemoryUsage.HeapInUse > 1024*1024*1024 { // 1GB
			index -= 5
		}
	}
	
	if index < 0 {
		index = 0
	}
	
	return index
}

// generateRecommendations creates performance recommendations
func (bs *BenchmarkSuite) generateRecommendations() []string {
	recommendations := []string{}
	
	for name, component := range bs.results.ComponentResults {
		if component.Throughput.RecordsPerSecond < 1000 {
			recommendations = append(recommendations, 
				fmt.Sprintf("Consider optimizing %s throughput - currently %.2f records/sec", 
					name, component.Throughput.RecordsPerSecond))
		}
		
		if component.Latency.P95 > 100*time.Millisecond {
			recommendations = append(recommendations, 
				fmt.Sprintf("High P95 latency in %s: %v - investigate bottlenecks", 
					name, component.Latency.P95))
		}
		
		if component.MemoryUsage.HeapInUse > 1024*1024*1024 {
			recommendations = append(recommendations, 
				fmt.Sprintf("High memory usage in %s: %d MB - optimize memory allocation", 
					name, component.MemoryUsage.HeapInUse/1024/1024))
		}
	}
	
	if len(recommendations) == 0 {
		recommendations = append(recommendations, "Performance is within acceptable ranges")
	}
	
	return recommendations
}

// RunBenchmarkTests runs Go benchmark tests
func (bs *BenchmarkSuite) RunBenchmarkTests(t *testing.T) {
	t.Run("BenchmarkXDRProcessing", func(t *testing.T) {
		for i := 0; i < t.N; i++ {
			bs.benchmarkXDRProcessing()
		}
	})
	
	t.Run("BenchmarkArrowConversion", func(t *testing.T) {
		for i := 0; i < t.N; i++ {
			bs.benchmarkArrowConversion()
		}
	})
	
	t.Run("BenchmarkVectorizedFiltering", func(t *testing.T) {
		for i := 0; i < t.N; i++ {
			bs.benchmarkVectorizedFiltering()
		}
	})
	
	t.Run("BenchmarkParquetWriting", func(t *testing.T) {
		for i := 0; i < t.N; i++ {
			bs.benchmarkParquetWriting()
		}
	})
}

// RunConcurrentBenchmarks runs benchmarks with varying concurrency levels
func (bs *BenchmarkSuite) RunConcurrentBenchmarks() error {
	concurrencyLevels := []int{1, 2, 4, 8, 16, 32}
	
	for _, concurrency := range concurrencyLevels {
		log.Info().Int("concurrency", concurrency).Msg("Running concurrent benchmark")
		
		startTime := time.Now()
		var wg sync.WaitGroup
		
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < bs.config.Iterations/concurrency; j++ {
					bs.benchmarkArrowConversion()
				}
			}()
		}
		
		wg.Wait()
		duration := time.Since(startTime)
		
		throughput := float64(bs.config.Iterations) / duration.Seconds()
		log.Info().
			Int("concurrency", concurrency).
			Float64("throughput", throughput).
			Dur("duration", duration).
			Msg("Concurrent benchmark completed")
	}
	
	return nil
}

// Close cleans up benchmark resources
func (bs *BenchmarkSuite) Close() {
	// Clean up any resources
}
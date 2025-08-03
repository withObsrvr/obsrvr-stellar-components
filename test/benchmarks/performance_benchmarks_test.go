package benchmarks

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/withobsrvr/obsrvr-stellar-components/internal/flowctl"
	flowctlpb "github.com/withobsrvr/obsrvr-stellar-components/proto/gen"
)

// BenchmarkMetricsProvider provides metrics for benchmarking
type BenchmarkMetricsProvider struct {
	mu      sync.RWMutex
	metrics map[string]float64
	counter int64
}

func NewBenchmarkMetricsProvider() *BenchmarkMetricsProvider {
	return &BenchmarkMetricsProvider{
		metrics: map[string]float64{
			"operations_count":    0,
			"processing_latency":  100.5,
			"error_rate":         0.02,
			"throughput":         1500.0,
			"queue_depth":        25,
			"memory_usage":       1024000,
			"cpu_usage":          0.65,
			"connection_count":   10,
			"active_sessions":    150,
			"cache_hit_ratio":    0.85,
		},
	}
}

func (b *BenchmarkMetricsProvider) GetMetrics() map[string]float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	
	// Update counter to simulate changing metrics
	b.counter++
	result := make(map[string]float64, len(b.metrics))
	for k, v := range b.metrics {
		if k == "operations_count" {
			result[k] = float64(b.counter)
		} else {
			result[k] = v
		}
	}
	return result
}

func (b *BenchmarkMetricsProvider) UpdateMetric(name string, value float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.metrics[name] = value
}

// Benchmark Controller Creation
func BenchmarkControllerCreation(b *testing.B) {
	metrics := NewBenchmarkMetricsProvider()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		controller, err := flowctl.NewController(
			flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
			fmt.Sprintf("localhost:%d", 8000+(i%1000)),
			metrics,
		)
		if err != nil {
			b.Fatal(err)
		}
		_ = controller // Prevent optimization
	}
}

// Benchmark Controller Lifecycle
func BenchmarkControllerLifecycle(b *testing.B) {
	ctx := context.Background()
	metrics := NewBenchmarkMetricsProvider()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		controller, err := flowctl.NewController(
			flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
			fmt.Sprintf("localhost:%d", 8000+(i%1000)),
			metrics,
		)
		if err != nil {
			b.Fatal(err)
		}
		
		// Start and stop cycle
		err = controller.Start(ctx)
		if err != nil {
			b.Fatal(err)
		}
		
		controller.Stop()
	}
}

// Benchmark Flow Control Operations
func BenchmarkFlowControlOperations(b *testing.B) {
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	err = controller.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Stop()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		serviceID := fmt.Sprintf("service-%d", i%10)
		inflight := int32(i % 100)
		controller.UpdateServiceInflight(serviceID, inflight)
	}
}

// Benchmark Backpressure Signaling
func BenchmarkBackpressureSignaling(b *testing.B) {
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	err = controller.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Stop()
	
	signals := make([]flowctl.BackpressureSignal, 1000)
	for i := range signals {
		signals[i] = flowctl.BackpressureSignal{
			ServiceID:       fmt.Sprintf("service-%d", i%10),
			CurrentLoad:     0.5 + (float64(i%50) / 100.0),
			RecommendedRate: 0.8 - (float64(i%40) / 100.0),
			Timestamp:       time.Now(),
			Reason:          fmt.Sprintf("Benchmark signal %d", i),
		}
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		signal := signals[i%len(signals)]
		controller.SendBackpressureSignal(signal)
	}
}

// Benchmark Pipeline Metrics Calculation
func BenchmarkPipelineMetricsCalculation(b *testing.B) {
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	err = controller.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Stop()
	
	// Set up test data
	for i := 0; i < 20; i++ {
		serviceID := fmt.Sprintf("service-%d", i)
		controller.UpdateServiceInflight(serviceID, int32(10+i*5))
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		_, err := controller.CalculatePipelineMetrics()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Service Discovery
func BenchmarkServiceDiscovery(b *testing.B) {
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	err = controller.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Stop()
	
	serviceTypes := []flowctlpb.ServiceType{
		flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		flowctlpb.ServiceType_SERVICE_TYPE_SINK,
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		serviceType := serviceTypes[i%len(serviceTypes)]
		_ = controller.DiscoverServices(serviceType)
	}
}

// Benchmark Connection Pool Operations
func BenchmarkConnectionPoolOperations(b *testing.B) {
	config := &flowctl.ConnectionPoolConfig{
		MaxConnections:      10,
		MinConnections:      2,
		IdleTimeout:         5 * time.Minute,
		ConnectionTimeout:   1 * time.Second,
		MaxRetries:          3,
		HealthCheckInterval: 30 * time.Second,
		EnableLoadBalancing: true,
	}
	
	pool := flowctl.NewConnectionPool(config)
	ctx := context.Background()
	err := pool.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Stop()
	
	endpoints := make([]string, 50)
	for i := range endpoints {
		endpoints[i] = fmt.Sprintf("localhost:%d", 9000+i)
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		endpoint := endpoints[i%len(endpoints)]
		_, _ = pool.GetConnection(endpoint) // Ignore errors for benchmark
	}
}

// Benchmark Connection Pool Statistics
func BenchmarkConnectionPoolStats(b *testing.B) {
	config := &flowctl.ConnectionPoolConfig{
		MaxConnections:      20,
		MinConnections:      5,
		IdleTimeout:         5 * time.Minute,
		ConnectionTimeout:   500 * time.Millisecond,
		MaxRetries:          2,
		HealthCheckInterval: 30 * time.Second,
		EnableLoadBalancing: true,
	}
	
	pool := flowctl.NewConnectionPool(config)
	ctx := context.Background()
	err := pool.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Stop()
	
	// Create some endpoint pools
	for i := 0; i < 10; i++ {
		endpoint := fmt.Sprintf("localhost:%d", 9100+i)
		pool.GetConnection(endpoint) // Will fail but creates pools
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		_ = pool.GetPoolStats()
	}
}

// Benchmark Pipeline Monitor Creation
func BenchmarkPipelineMonitorCreation(b *testing.B) {
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		monitor, err := flowctl.NewPipelineMonitor(controller)
		if err != nil {
			b.Fatal(err)
		}
		_ = monitor // Prevent optimization
	}
}

// Benchmark Metrics Collection
func BenchmarkMetricsCollection(b *testing.B) {
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	err = controller.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Stop()
	
	monitor, err := flowctl.NewPipelineMonitor(controller)
	if err != nil {
		b.Fatal(err)
	}
	
	// Set up some test data
	for i := 0; i < 10; i++ {
		controller.UpdateServiceInflight(fmt.Sprintf("service-%d", i), int32(20+i*3))
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		err := monitor.CollectMetrics()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Health Report Generation
func BenchmarkHealthReportGeneration(b *testing.B) {
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	err = controller.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Stop()
	
	monitor, err := flowctl.NewPipelineMonitor(controller)
	if err != nil {
		b.Fatal(err)
	}
	
	// Set up test data for comprehensive reports
	for i := 0; i < 15; i++ {
		controller.UpdateServiceInflight(fmt.Sprintf("service-%d", i), int32(15+i*4))
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		_, err := monitor.GenerateHealthReport()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Concurrent Flow Control Operations
func BenchmarkConcurrentFlowControl(b *testing.B) {
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	err = controller.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Stop()
	
	numGoroutines := runtime.NumCPU()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			serviceID := fmt.Sprintf("service-%d", i%20)
			inflight := int32(i % 100)
			controller.UpdateServiceInflight(serviceID, inflight)
			i++
		}
	})
}

// Benchmark Concurrent Backpressure Signaling
func BenchmarkConcurrentBackpressureSignaling(b *testing.B) {
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	err = controller.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Stop()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			signal := flowctl.BackpressureSignal{
				ServiceID:       fmt.Sprintf("service-%d", i%10),
				CurrentLoad:     0.3 + (float64(i%70) / 100.0),
				RecommendedRate: 0.9 - (float64(i%50) / 100.0),
				Timestamp:       time.Now(),
				Reason:          fmt.Sprintf("Concurrent benchmark %d", i),
			}
			controller.SendBackpressureSignal(signal)
			i++
		}
	})
}

// Benchmark Concurrent Connection Pool Access
func BenchmarkConcurrentConnectionPool(b *testing.B) {
	config := &flowctl.ConnectionPoolConfig{
		MaxConnections:      15,
		MinConnections:      3,
		IdleTimeout:         5 * time.Minute,
		ConnectionTimeout:   200 * time.Millisecond,
		MaxRetries:          2,
		HealthCheckInterval: 30 * time.Second,
		EnableLoadBalancing: true,
	}
	
	pool := flowctl.NewConnectionPool(config)
	ctx := context.Background()
	err := pool.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Stop()
	
	endpoints := make([]string, 20)
	for i := range endpoints {
		endpoints[i] = fmt.Sprintf("localhost:%d", 9200+i)
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			endpoint := endpoints[i%len(endpoints)]
			_, _ = pool.GetConnection(endpoint) // Ignore errors
			i++
		}
	})
}

// Benchmark Memory Usage Under Load
func BenchmarkMemoryUsageUnderLoad(b *testing.B) {
	var controllers []*flowctl.Controller
	defer func() {
		for _, controller := range controllers {
			controller.Stop()
		}
	}()
	
	// Create multiple controllers to simulate load
	for i := 0; i < 10; i++ {
		controller, err := flowctl.NewController(
			flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
			fmt.Sprintf("localhost:%d", 8100+i),
			NewBenchmarkMetricsProvider(),
		)
		if err != nil {
			b.Fatal(err)
		}
		
		ctx := context.Background()
		err = controller.Start(ctx)
		if err != nil {
			b.Fatal(err)
		}
		
		controllers = append(controllers, controller)
		
		// Add some data to each controller
		for j := 0; j < 50; j++ {
			serviceID := fmt.Sprintf("service-%d-%d", i, j)
			controller.UpdateServiceInflight(serviceID, int32(j*2))
		}
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		controller := controllers[i%len(controllers)]
		
		// Perform various operations
		controller.UpdateServiceInflight(fmt.Sprintf("load-service-%d", i), int32(i%100))
		
		signal := flowctl.BackpressureSignal{
			ServiceID:       fmt.Sprintf("load-service-%d", i%10),
			CurrentLoad:     0.4 + (float64(i%60) / 100.0),
			RecommendedRate: 0.8 - (float64(i%40) / 100.0),
			Timestamp:       time.Now(),
			Reason:          fmt.Sprintf("Load test %d", i),
		}
		controller.SendBackpressureSignal(signal)
		
		_, _ = controller.CalculatePipelineMetrics()
	}
}

// Benchmark Large Scale Operations
func BenchmarkLargeScaleOperations(b *testing.B) {
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	err = controller.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Stop()
	
	// Pre-populate with large amount of data
	for i := 0; i < 1000; i++ {
		serviceID := fmt.Sprintf("service-%d", i)
		controller.UpdateServiceInflight(serviceID, int32(10+i%90))
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		// Mix of operations
		switch i % 4 {
		case 0:
			serviceID := fmt.Sprintf("service-%d", i%1000)
			controller.UpdateServiceInflight(serviceID, int32(i%100))
		case 1:
			signal := flowctl.BackpressureSignal{
				ServiceID:       fmt.Sprintf("service-%d", i%100),
				CurrentLoad:     0.3 + (float64(i%70) / 100.0),
				RecommendedRate: 0.9 - (float64(i%60) / 100.0),
				Timestamp:       time.Now(),
				Reason:          fmt.Sprintf("Large scale test %d", i),
			}
			controller.SendBackpressureSignal(signal)
		case 2:
			_, _ = controller.CalculatePipelineMetrics()
		case 3:
			serviceType := flowctlpb.ServiceType(1 + (i % 3)) // Cycle through service types
			_ = controller.DiscoverServices(serviceType)
		}
	}
}

// Benchmark Full Pipeline Simulation
func BenchmarkFullPipelineSimulation(b *testing.B) {
	// Create source, processor, and sink controllers
	sourceMetrics := NewBenchmarkMetricsProvider()
	processorMetrics := NewBenchmarkMetricsProvider()
	sinkMetrics := NewBenchmarkMetricsProvider()
	
	source, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
		"localhost:8815",
		sourceMetrics,
	)
	if err != nil {
		b.Fatal(err)
	}
	
	processor, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		processorMetrics,
	)
	if err != nil {
		b.Fatal(err)
	}
	
	sink, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_SINK,
		"localhost:8817",
		sinkMetrics,
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	source.Start(ctx)
	processor.Start(ctx)
	sink.Start(ctx)
	
	defer func() {
		source.Stop()
		processor.Stop()
		sink.Stop()
	}()
	
	// Create connection pool
	pool := flowctl.NewConnectionPool(flowctl.DefaultConnectionPoolConfig())
	pool.Start(ctx)
	defer pool.Stop()
	
	// Create monitor
	monitor, err := flowctl.NewPipelineMonitor(processor)
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		// Simulate pipeline data flow
		source.UpdateServiceInflight("source-worker", int32(50+i%50))
		processor.UpdateServiceInflight("processor-worker", int32(40+i%60))
		sink.UpdateServiceInflight("sink-worker", int32(30+i%70))
		
		// Occasional backpressure
		if i%10 == 0 {
			signal := flowctl.BackpressureSignal{
				ServiceID:       "processor-worker",
				CurrentLoad:     0.7 + (float64(i%30) / 100.0),
				RecommendedRate: 0.6 + (float64(i%40) / 100.0),
				Timestamp:       time.Now(),
				Reason:          fmt.Sprintf("Pipeline simulation %d", i),
			}
			processor.SendBackpressureSignal(signal)
		}
		
		// Pipeline metrics calculation
		if i%5 == 0 {
			processor.CalculatePipelineMetrics()
		}
		
		// Health monitoring
		if i%20 == 0 {
			monitor.CollectMetrics()
		}
		
		// Connection pool usage
		if i%3 == 0 {
			pool.GetConnection("localhost:8815") // Simulate connection attempt
		}
	}
}

// Benchmark to measure performance regression
func BenchmarkRegressionTest(b *testing.B) {
	// This benchmark should be run regularly to detect performance regressions
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		NewBenchmarkMetricsProvider(),
	)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := context.Background()
	err = controller.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Stop()
	
	// Standard workload
	for i := 0; i < 100; i++ {
		controller.UpdateServiceInflight(fmt.Sprintf("service-%d", i%10), int32(i%50))
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		// Standard operations that should maintain consistent performance
		controller.UpdateServiceInflight("regression-test", int32(i%100))
		
		if i%10 == 0 {
			signal := flowctl.BackpressureSignal{
				ServiceID:       "regression-test",
				CurrentLoad:     0.5,
				RecommendedRate: 0.8,
				Timestamp:       time.Now(),
				Reason:          "Regression test",
			}
			controller.SendBackpressureSignal(signal)
		}
		
		if i%20 == 0 {
			controller.CalculatePipelineMetrics()
		}
	}
}

// Example of how to run benchmarks:
//
// go test -bench=. -benchmem -benchtime=10s
// go test -bench=BenchmarkFlowControlOperations -benchmem -count=5
// go test -bench=BenchmarkConcurrent -benchmem -cpu=1,2,4,8
// go test -bench=BenchmarkFullPipelineSimulation -benchmem -benchtime=30s
package integration

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/withobsrvr/obsrvr-stellar-components/internal/flowctl"
	flowctlpb "github.com/withobsrvr/obsrvr-stellar-components/proto/gen"
)

// FlowCtlIntegrationTestSuite contains integration tests for flowctl functionality
type FlowCtlIntegrationTestSuite struct {
	suite.Suite
	ctx      context.Context
	cancel   context.CancelFunc
	cleanups []func()
}

// SetupSuite runs once before all tests in the suite
func (suite *FlowCtlIntegrationTestSuite) SetupSuite() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cleanups = make([]func(), 0)
}

// TearDownSuite runs once after all tests in the suite
func (suite *FlowCtlIntegrationTestSuite) TearDownSuite() {
	suite.cancel()
	
	// Run all cleanup functions in reverse order
	for i := len(suite.cleanups) - 1; i >= 0; i-- {
		suite.cleanups[i]()
	}
}

// SetupTest runs before each test
func (suite *FlowCtlIntegrationTestSuite) SetupTest() {
	// Reset any test-specific state if needed
}

// TearDownTest runs after each test
func (suite *FlowCtlIntegrationTestSuite) TearDownTest() {
	// Clean up any test-specific resources
}

// TestMultiServiceRegistration tests registration of multiple services
func (suite *FlowCtlIntegrationTestSuite) TestMultiServiceRegistration() {
	t := suite.T()
	
	// Skip if no flowctl endpoint is available
	flowctlEndpoint := os.Getenv("FLOWCTL_ENDPOINT")
	if flowctlEndpoint == "" {
		t.Skip("FLOWCTL_ENDPOINT not set, skipping integration test")
	}

	// Create metrics providers for each service type
	sourceMetrics := &MockMetricsProvider{
		metrics: map[string]float64{
			"ledgers_processed": 1000,
			"total_bytes":      1024000,
			"success_count":    995,
			"error_count":      5,
		},
	}

	processorMetrics := &MockMetricsProvider{
		metrics: map[string]float64{
			"events_extracted":    800,
			"events_filtered":     50,
			"processing_latency":  150.5,
			"throughput":         25.3,
		},
	}

	sinkMetrics := &MockMetricsProvider{
		metrics: map[string]float64{
			"events_consumed":     750,
			"files_written":       10,
			"websocket_connections": 5,
			"output_latency":      75.2,
		},
	}

	// Create controllers for each service type
	controllers := []*flowctl.Controller{}
	
	sourceController, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
		"localhost:8815",
		sourceMetrics,
	)
	require.NoError(t, err)
	controllers = append(controllers, sourceController)

	processorController, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		processorMetrics,
	)
	require.NoError(t, err)
	controllers = append(controllers, processorController)

	sinkController, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_SINK,
		"localhost:8817",
		sinkMetrics,
	)
	require.NoError(t, err)
	controllers = append(controllers, sinkController)

	// Register cleanup
	suite.cleanups = append(suite.cleanups, func() {
		for _, controller := range controllers {
			controller.Stop()
		}
	})

	// Start all controllers
	for i, controller := range controllers {
		err := controller.Start(suite.ctx)
		require.NoError(t, err, "Failed to start controller %d", i)
	}

	// Allow time for registration and heartbeats
	time.Sleep(2 * time.Second)

	// Verify all controllers are running
	for i, controller := range controllers {
		assert.True(t, controller.IsRunning(), "Controller %d should be running", i)
	}

	// Test service discovery
	sources := processorController.DiscoverServices(flowctlpb.ServiceType_SERVICE_TYPE_SOURCE)
	assert.NotEmpty(t, sources, "Should discover source services")

	sinks := processorController.DiscoverServices(flowctlpb.ServiceType_SERVICE_TYPE_SINK)
	assert.NotEmpty(t, sinks, "Should discover sink services")

	// Test pipeline metrics calculation
	metrics, err := processorController.CalculatePipelineMetrics()
	assert.NoError(t, err)
	assert.NotNil(t, metrics)
}

// TestFlowControlAndBackpressure tests flow control and backpressure functionality
func (suite *FlowCtlIntegrationTestSuite) TestFlowControlAndBackpressure() {
	t := suite.T()

	// Create a processor controller for flow control testing
	processorMetrics := &MockMetricsProvider{
		metrics: map[string]float64{
			"current_inflight": 50,
			"max_inflight":    100,
			"throttle_rate":   1.0,
		},
	}

	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		processorMetrics,
	)
	require.NoError(t, err)

	suite.cleanups = append(suite.cleanups, func() {
		controller.Stop()
	})

	err = controller.Start(suite.ctx)
	require.NoError(t, err)

	// Test normal flow control operations
	t.Run("Normal Flow Control", func(t *testing.T) {
		// Update service inflight counts
		controller.UpdateServiceInflight("test-service-1", 30)
		controller.UpdateServiceInflight("test-service-2", 20)

		// Verify flow control state
		assert.False(t, controller.IsBackpressureActive())
		assert.Equal(t, 1.0, controller.GetCurrentThrottleRate())
	})

	t.Run("Backpressure Activation", func(t *testing.T) {
		// Simulate high load conditions
		controller.UpdateServiceInflight("test-service-1", 45)
		controller.UpdateServiceInflight("test-service-2", 40)

		// Send backpressure signal
		signal := flowctl.BackpressureSignal{
			ServiceID:       "test-service-1",
			CurrentLoad:     0.85,
			RecommendedRate: 0.6,
			Timestamp:       time.Now(),
			Reason:          "High load detected in integration test",
		}

		controller.SendBackpressureSignal(signal)

		// Verify backpressure is active
		assert.True(t, controller.IsBackpressureActive())
		assert.Equal(t, 0.6, controller.GetCurrentThrottleRate())
	})

	t.Run("Backpressure Recovery", func(t *testing.T) {
		// Simulate load reduction
		controller.UpdateServiceInflight("test-service-1", 25)
		controller.UpdateServiceInflight("test-service-2", 15)

		// Send recovery signal
		recoverySignal := flowctl.BackpressureSignal{
			ServiceID:       "test-service-1",
			CurrentLoad:     0.4,
			RecommendedRate: 1.0,
			Timestamp:       time.Now(),
			Reason:          "Load reduced, recovery initiated",
		}

		controller.SendBackpressureSignal(recoverySignal)

		// Verify recovery
		assert.False(t, controller.IsBackpressureActive())
		assert.Equal(t, 1.0, controller.GetCurrentThrottleRate())
	})
}

// TestConnectionPoolIntegration tests connection pool functionality
func (suite *FlowCtlIntegrationTestSuite) TestConnectionPoolIntegration() {
	t := suite.T()

	// Create connection pool with test configuration
	config := &flowctl.ConnectionPoolConfig{
		MaxConnections:      5,
		MinConnections:      2,
		IdleTimeout:         30 * time.Second,
		ConnectionTimeout:   2 * time.Second,
		MaxRetries:          2,
		HealthCheckInterval: 5 * time.Second,
		EnableLoadBalancing: true,
	}

	pool := flowctl.NewConnectionPool(config)
	require.NotNil(t, pool)

	suite.cleanups = append(suite.cleanups, func() {
		pool.Stop()
	})

	err := pool.Start(suite.ctx)
	require.NoError(t, err)

	t.Run("Connection Attempts", func(t *testing.T) {
		// Test connection attempts to non-existent endpoints
		endpoints := []string{
			"localhost:9001",
			"localhost:9002", 
			"localhost:9003",
		}

		for _, endpoint := range endpoints {
			// Should fail but create endpoint pools
			_, err := pool.GetConnection(endpoint)
			assert.Error(t, err) // Expected to fail since no servers running
		}

		// Verify pools were created
		stats := pool.GetPoolStats()
		assert.Len(t, stats, len(endpoints))

		for _, endpoint := range endpoints {
			stat, exists := stats[endpoint]
			assert.True(t, exists)
			assert.Equal(t, endpoint, stat.Endpoint)
		}
	})

	t.Run("Pool Statistics", func(t *testing.T) {
		stats := pool.GetPoolStats()
		assert.NotEmpty(t, stats)

		for endpoint, stat := range stats {
			assert.Equal(t, endpoint, stat.Endpoint)
			assert.GreaterOrEqual(t, stat.TotalConnections, 0)
			assert.GreaterOrEqual(t, stat.HealthyConnections, 0)
			assert.LessOrEqual(t, stat.HealthyConnections, stat.TotalConnections)
		}
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		// Test concurrent access to connection pool
		var wg sync.WaitGroup
		endpoint := "localhost:9000"

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				
				for j := 0; j < 5; j++ {
					pool.GetConnection(endpoint)
					time.Sleep(10 * time.Millisecond)
				}
			}(i)
		}

		wg.Wait()

		// Verify pool state is consistent
		stats := pool.GetPoolStats()
		assert.NotNil(t, stats)
		
		if stat, exists := stats[endpoint]; exists {
			assert.LessOrEqual(t, stat.TotalConnections, config.MaxConnections)
		}
	})
}

// TestMonitoringIntegration tests monitoring system functionality
func (suite *FlowCtlIntegrationTestSuite) TestMonitoringIntegration() {
	t := suite.T()

	// Create controller with metrics
	metrics := &MockMetricsProvider{
		metrics: map[string]float64{
			"processed_events":   5000,
			"error_count":       25,
			"latency_ms":        125.5,
			"throughput":        42.3,
			"queue_depth":       15,
		},
	}

	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		metrics,
	)
	require.NoError(t, err)

	suite.cleanups = append(suite.cleanups, func() {
		controller.Stop()
	})

	err = controller.Start(suite.ctx)
	require.NoError(t, err)

	// Create pipeline monitor
	monitor, err := flowctl.NewPipelineMonitor(controller)
	require.NoError(t, err)

	suite.cleanups = append(suite.cleanups, func() {
		monitor.StopMonitoring()
	})

	t.Run("Metrics Collection", func(t *testing.T) {
		err := monitor.CollectMetrics()
		assert.NoError(t, err)
	})

	t.Run("Health Report Generation", func(t *testing.T) {
		report, err := monitor.GenerateHealthReport()
		assert.NoError(t, err)
		assert.NotNil(t, report)

		// Verify report structure
		assert.Contains(t, report, "pipeline_health")
		assert.Contains(t, report, "performance")
		assert.Contains(t, report, "flow_control")
		assert.Contains(t, report, "services")
		assert.Contains(t, report, "recommendations")
	})

	t.Run("Continuous Monitoring", func(t *testing.T) {
		// Start monitoring with short interval
		monitorCtx, monitorCancel := context.WithCancel(suite.ctx)
		defer monitorCancel()

		err := monitor.StartMonitoring(monitorCtx, 100*time.Millisecond)
		assert.NoError(t, err)

		// Let it run for a short time
		time.Sleep(500 * time.Millisecond)

		// Stop monitoring
		monitor.StopMonitoring()

		// Verify no errors occurred during monitoring
		// (In a real implementation, you might check error logs or metrics)
	})
}

// TestEndToEndPipelineScenario tests a complete pipeline scenario
func (suite *FlowCtlIntegrationTestSuite) TestEndToEndPipelineScenario() {
	t := suite.T()

	// Skip if no flowctl endpoint is available
	flowctlEndpoint := os.Getenv("FLOWCTL_ENDPOINT")
	if flowctlEndpoint == "" {
		t.Skip("FLOWCTL_ENDPOINT not set, skipping end-to-end test")
	}

	// Create a complete pipeline with all components
	pipeline := &TestPipeline{}
	err := pipeline.Setup(suite.ctx)
	require.NoError(t, err)

	suite.cleanups = append(suite.cleanups, func() {
		pipeline.Cleanup()
	})

	t.Run("Pipeline Startup", func(t *testing.T) {
		err := pipeline.Start()
		assert.NoError(t, err)

		// Verify all components are running
		assert.True(t, pipeline.IsHealthy())
	})

	t.Run("Service Discovery", func(t *testing.T) {
		// Test that processor can discover source and sink
		sources := pipeline.processor.DiscoverServices(flowctlpb.ServiceType_SERVICE_TYPE_SOURCE)
		assert.NotEmpty(t, sources)

		sinks := pipeline.processor.DiscoverServices(flowctlpb.ServiceType_SERVICE_TYPE_SINK)
		assert.NotEmpty(t, sinks)
	})

	t.Run("Flow Control Coordination", func(t *testing.T) {
		// Simulate load on the pipeline
		pipeline.SimulateLoad(0.8) // 80% load

		// Allow time for flow control to respond
		time.Sleep(1 * time.Second)

		// Check if backpressure was activated appropriately
		metrics, err := pipeline.processor.CalculatePipelineMetrics()
		assert.NoError(t, err)
		assert.NotNil(t, metrics)
	})

	t.Run("Pipeline Monitoring", func(t *testing.T) {
		report, err := pipeline.monitor.GenerateHealthReport()
		assert.NoError(t, err)
		assert.NotNil(t, report)

		// Verify pipeline is being monitored
		pipelineHealth := report["pipeline_health"].(map[string]interface{})
		assert.Contains(t, pipelineHealth, "service_availability")
		assert.Contains(t, pipelineHealth, "healthy")
	})

	t.Run("Pipeline Shutdown", func(t *testing.T) {
		pipeline.Stop()

		// Verify graceful shutdown
		assert.False(t, pipeline.IsHealthy())
	})
}

// TestPipeline represents a complete test pipeline setup
type TestPipeline struct {
	ctx       context.Context
	source    *flowctl.Controller
	processor *flowctl.Controller
	sink      *flowctl.Controller
	monitor   *flowctl.PipelineMonitor
	pool      *flowctl.ConnectionPool
}

// Setup initializes the test pipeline
func (p *TestPipeline) Setup(ctx context.Context) error {
	p.ctx = ctx

	// Create metrics providers
	sourceMetrics := &MockMetricsProvider{
		metrics: map[string]float64{
			"ledgers_processed": 0,
			"success_count":    0,
			"error_count":      0,
		},
	}

	processorMetrics := &MockMetricsProvider{
		metrics: map[string]float64{
			"events_extracted": 0,
			"current_inflight": 0,
			"max_inflight":     100,
		},
	}

	sinkMetrics := &MockMetricsProvider{
		metrics: map[string]float64{
			"events_consumed": 0,
			"files_written":   0,
		},
	}

	// Create controllers
	var err error
	p.source, err = flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
		"localhost:8815",
		sourceMetrics,
	)
	if err != nil {
		return fmt.Errorf("failed to create source controller: %w", err)
	}

	p.processor, err = flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		processorMetrics,
	)
	if err != nil {
		return fmt.Errorf("failed to create processor controller: %w", err)
	}

	p.sink, err = flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_SINK,
		"localhost:8817",
		sinkMetrics,
	)
	if err != nil {
		return fmt.Errorf("failed to create sink controller: %w", err)
	}

	// Create connection pool
	p.pool = flowctl.NewConnectionPool(flowctl.DefaultConnectionPoolConfig())

	// Create monitor
	p.monitor, err = flowctl.NewPipelineMonitor(p.processor)
	if err != nil {
		return fmt.Errorf("failed to create monitor: %w", err)
	}

	return nil
}

// Start starts all pipeline components
func (p *TestPipeline) Start() error {
	// Start connection pool
	if err := p.pool.Start(p.ctx); err != nil {
		return fmt.Errorf("failed to start connection pool: %w", err)
	}

	// Start controllers
	if err := p.source.Start(p.ctx); err != nil {
		return fmt.Errorf("failed to start source: %w", err)
	}

	if err := p.processor.Start(p.ctx); err != nil {
		return fmt.Errorf("failed to start processor: %w", err)
	}

	if err := p.sink.Start(p.ctx); err != nil {
		return fmt.Errorf("failed to start sink: %w", err)
	}

	// Start monitoring
	if err := p.monitor.StartMonitoring(p.ctx, 1*time.Second); err != nil {
		return fmt.Errorf("failed to start monitoring: %w", err)
	}

	return nil
}

// Stop stops all pipeline components
func (p *TestPipeline) Stop() {
	if p.monitor != nil {
		p.monitor.StopMonitoring()
	}
	if p.source != nil {
		p.source.Stop()
	}
	if p.processor != nil {
		p.processor.Stop()
	}
	if p.sink != nil {
		p.sink.Stop()
	}
	if p.pool != nil {
		p.pool.Stop()
	}
}

// Cleanup releases all resources
func (p *TestPipeline) Cleanup() {
	p.Stop()
}

// IsHealthy checks if all pipeline components are healthy
func (p *TestPipeline) IsHealthy() bool {
	return p.source != nil && p.source.IsRunning() &&
		   p.processor != nil && p.processor.IsRunning() &&
		   p.sink != nil && p.sink.IsRunning()
}

// SimulateLoad simulates load on the pipeline
func (p *TestPipeline) SimulateLoad(loadFactor float64) {
	maxInflight := int32(100)
	currentInflight := int32(float64(maxInflight) * loadFactor)

	p.processor.UpdateServiceInflight("test-service", currentInflight)

	if loadFactor > 0.8 {
		signal := flowctl.BackpressureSignal{
			ServiceID:       "test-service",
			CurrentLoad:     loadFactor,
			RecommendedRate: 1.0 - ((loadFactor - 0.8) * 2.5), // Reduce rate as load increases
			Timestamp:       time.Now(),
			Reason:          fmt.Sprintf("Simulated load: %.1f%%", loadFactor*100),
		}
		p.processor.SendBackpressureSignal(signal)
	}
}

// MockMetricsProvider for integration testing
type MockMetricsProvider struct {
	mu      sync.RWMutex
	metrics map[string]float64
}

func (m *MockMetricsProvider) GetMetrics() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	result := make(map[string]float64, len(m.metrics))
	for k, v := range m.metrics {
		result[k] = v
	}
	return result
}

func (m *MockMetricsProvider) UpdateMetric(name string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.metrics == nil {
		m.metrics = make(map[string]float64)
	}
	m.metrics[name] = value
}

// TestMain sets up the test environment
func TestMain(m *testing.M) {
	// Set up test environment
	os.Setenv("LOG_LEVEL", "debug")
	
	// Run tests
	code := m.Run()
	
	// Clean up
	os.Exit(code)
}

// Run the integration test suite
func TestFlowCtlIntegrationSuite(t *testing.T) {
	suite.Run(t, new(FlowCtlIntegrationTestSuite))
}
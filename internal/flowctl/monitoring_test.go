package flowctl

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	flowctlpb "github.com/withobsrvr/obsrvr-stellar-components/proto/gen"
)

// MockController for testing monitoring
type MockController struct {
	mock.Mock
}

func (m *MockController) GetServiceLoadMetrics() map[string]float64 {
	args := m.Called()
	return args.Get(0).(map[string]float64)
}

func (m *MockController) IsBackpressureActive() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockController) GetCurrentThrottleRate() float64 {
	args := m.Called()
	return args.Float64(0)
}

func (m *MockController) CalculatePipelineMetrics() (*PipelineMetrics, error) {
	args := m.Called()
	return args.Get(0).(*PipelineMetrics), args.Error(1)
}

func (m *MockController) GetPipelineTopology() (*PipelineTopology, error) {
	args := m.Called()
	return args.Get(0).(*PipelineTopology), args.Error(1)
}

func (m *MockController) GetConnectionPoolStats() map[string]PoolStats {
	args := m.Called()
	return args.Get(0).(map[string]PoolStats)
}

// Test PipelineMonitor Creation
func TestNewPipelineMonitor(t *testing.T) {
	mockController := &MockController{}
	
	monitor, err := NewPipelineMonitor(mockController)
	
	assert.NoError(t, err)
	assert.NotNil(t, monitor)
	assert.Equal(t, mockController, monitor.controller)
	assert.NotNil(t, monitor.metricsRegistry)
	
	// Verify all metrics are created
	assert.NotNil(t, monitor.pipelineThroughput)
	assert.NotNil(t, monitor.pipelineLatency)
	assert.NotNil(t, monitor.serviceLoadMetrics)
	assert.NotNil(t, monitor.backpressureActive)
	assert.NotNil(t, monitor.throttleRate)
	assert.NotNil(t, monitor.serviceConnections)
	assert.NotNil(t, monitor.discoveryLatency)
	assert.NotNil(t, monitor.reconnectionEvents)
}

// Test Metrics Collection
func TestPipelineMonitor_CollectMetrics(t *testing.T) {
	mockController := &MockController{}
	monitor, err := NewPipelineMonitor(mockController)
	require.NoError(t, err)

	// Mock controller responses
	mockController.On("GetServiceLoadMetrics").Return(map[string]float64{
		"stellar-source":     0.65,
		"ttp-processor":      0.78,
		"analytics-sink":     0.42,
	})
	
	mockController.On("IsBackpressureActive").Return(true)
	mockController.On("GetCurrentThrottleRate").Return(0.75)
	
	mockController.On("CalculatePipelineMetrics").Return(&PipelineMetrics{
		TotalThroughput: 1250.5,
		EndToEndLatency: 350 * time.Millisecond,
		ServiceThroughput: map[string]float64{
			"stellar-source":  500.0,
			"ttp-processor":   400.5,
			"analytics-sink":  350.0,
		},
		ServiceLatency: map[string]time.Duration{
			"stellar-source":  100 * time.Millisecond,
			"ttp-processor":   150 * time.Millisecond,
			"analytics-sink":  100 * time.Millisecond,
		},
		QueueDepths: map[string]int{
			"stellar-source":  10,
			"ttp-processor":   25,
			"analytics-sink":  5,
		},
		ErrorRates: map[string]float64{
			"stellar-source":  0.02,
			"ttp-processor":   0.01,
			"analytics-sink":  0.005,
		},
		LastUpdated: time.Now(),
	}, nil)

	mockController.On("GetConnectionPoolStats").Return(map[string]PoolStats{
		"localhost:8815": {
			Endpoint:           "localhost:8815",
			TotalConnections:   8,
			HealthyConnections: 7,
			IdleConnections:    2,
			AverageUseCount:    25.5,
			OldestConnection:   5 * time.Minute,
			TotalUseCount:     204,
		},
		"localhost:8816": {
			Endpoint:           "localhost:8816",
			TotalConnections:   6,
			HealthyConnections: 6,
			IdleConnections:    1,
			AverageUseCount:    18.3,
			OldestConnection:   3 * time.Minute,
			TotalUseCount:     110,
		},
	})

	t.Run("Collect All Metrics", func(t *testing.T) {
		err := monitor.CollectMetrics()
		assert.NoError(t, err)

		// Verify pipeline-level metrics
		assert.Equal(t, float64(1), testutil.ToFloat64(monitor.backpressureActive))
		assert.Equal(t, 0.75, testutil.ToFloat64(monitor.throttleRate))
		assert.Equal(t, 1250.5, testutil.ToFloat64(monitor.pipelineThroughput))
		assert.Equal(t, 0.35, testutil.ToFloat64(monitor.pipelineLatency)) // 350ms -> 0.35s

		// Verify service-level metrics exist (detailed values tested in individual service tests)
		serviceLoadGauge, err := monitor.serviceLoadMetrics.GetMetricWithLabelValues("stellar-source")
		assert.NoError(t, err)
		assert.Equal(t, 0.65, testutil.ToFloat64(serviceLoadGauge))

		serviceLoadGauge, err = monitor.serviceLoadMetrics.GetMetricWithLabelValues("ttp-processor")
		assert.NoError(t, err)
		assert.Equal(t, 0.78, testutil.ToFloat64(serviceLoadGauge))

		mockController.AssertExpectations(t)
	})

	t.Run("Collect Metrics with Controller Error", func(t *testing.T) {
		// Reset mocks
		mockController.ExpectedCalls = nil
		mockController.On("GetServiceLoadMetrics").Return(map[string]float64{})
		mockController.On("IsBackpressureActive").Return(false)
		mockController.On("GetCurrentThrottleRate").Return(1.0)
		mockController.On("CalculatePipelineMetrics").Return((*PipelineMetrics)(nil), assert.AnError)
		mockController.On("GetConnectionPoolStats").Return(map[string]PoolStats{})

		err := monitor.CollectMetrics()
		assert.Error(t, err)

		mockController.AssertExpectations(t)
	})
}

// Test Health Report Generation
func TestPipelineMonitor_GenerateHealthReport(t *testing.T) {
	mockController := &MockController{}
	monitor, err := NewPipelineMonitor(mockController)
	require.NoError(t, err)

	tests := []struct {
		name                string
		serviceLoadMetrics  map[string]float64
		backpressureActive  bool
		throttleRate        float64
		pipelineMetrics     *PipelineMetrics
		expectedHealthy     bool
		expectedRecommendations int
	}{
		{
			name: "Healthy pipeline",
			serviceLoadMetrics: map[string]float64{
				"stellar-source":   0.45,
				"ttp-processor":    0.60,
				"analytics-sink":   0.35,
			},
			backpressureActive: false,
			throttleRate:       1.0,
			pipelineMetrics: &PipelineMetrics{
				TotalThroughput: 800.0,
				EndToEndLatency: 200 * time.Millisecond,
				ErrorRates: map[string]float64{
					"stellar-source":  0.01,
					"ttp-processor":   0.015,
					"analytics-sink":  0.005,
				},
			},
			expectedHealthy:         true,
			expectedRecommendations: 0,
		},
		{
			name: "High load with backpressure",
			serviceLoadMetrics: map[string]float64{
				"stellar-source":   0.85,
				"ttp-processor":    0.92,
				"analytics-sink":   0.78,
			},
			backpressureActive: true,
			throttleRate:       0.6,
			pipelineMetrics: &PipelineMetrics{
				TotalThroughput: 1200.0,
				EndToEndLatency: 800 * time.Millisecond,
				ErrorRates: map[string]float64{
					"stellar-source":  0.03,
					"ttp-processor":   0.08,
					"analytics-sink":  0.02,
				},
			},
			expectedHealthy:         false,
			expectedRecommendations: 3, // High load, backpressure, elevated error rates
		},
		{
			name: "High latency issue",
			serviceLoadMetrics: map[string]float64{
				"stellar-source":   0.55,
				"ttp-processor":    0.65,
				"analytics-sink":   0.45,
			},
			backpressureActive: false,
			throttleRate:       1.0,
			pipelineMetrics: &PipelineMetrics{
				TotalThroughput: 600.0,
				EndToEndLatency: 8 * time.Second, // High latency
				ErrorRates: map[string]float64{
					"stellar-source":  0.01,
					"ttp-processor":   0.02,
					"analytics-sink":  0.01,
				},
			},
			expectedHealthy:         false,
			expectedRecommendations: 1, // High latency recommendation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset mocks
			mockController.ExpectedCalls = nil

			mockController.On("GetServiceLoadMetrics").Return(tt.serviceLoadMetrics)
			mockController.On("IsBackpressureActive").Return(tt.backpressureActive)
			mockController.On("GetCurrentThrottleRate").Return(tt.throttleRate)
			mockController.On("CalculatePipelineMetrics").Return(tt.pipelineMetrics, nil)

			// Generate topology for service health
			topology := &PipelineTopology{
				Services: map[string]*flowctlpb.ServiceStatus{
					"stellar-source": {
						ServiceId:   "stellar-source-123",
						ServiceType: flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
						Status:      "healthy",
					},
					"ttp-processor": {
						ServiceId:   "ttp-processor-456",
						ServiceType: flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
						Status:      "healthy",
					},
					"analytics-sink": {
						ServiceId:   "analytics-sink-789",
						ServiceType: flowctlpb.ServiceType_SERVICE_TYPE_SINK,
						Status:      "healthy",
					},
				},
			}
			mockController.On("GetPipelineTopology").Return(topology, nil)

			report, err := monitor.GenerateHealthReport()
			assert.NoError(t, err)
			assert.NotNil(t, report)

			// Verify report structure
			assert.Contains(t, report, "pipeline_health")
			assert.Contains(t, report, "performance")
			assert.Contains(t, report, "flow_control")
			assert.Contains(t, report, "services")
			assert.Contains(t, report, "recommendations")

			// Verify overall health assessment
			pipelineHealth := report["pipeline_health"].(map[string]interface{})
			assert.Equal(t, tt.expectedHealthy, pipelineHealth["healthy"])

			// Verify recommendations count
			recommendations := report["recommendations"].([]string)
			assert.Len(t, recommendations, tt.expectedRecommendations)

			mockController.AssertExpectations(t)
		})
	}
}

// Test Metrics Registration
func TestPipelineMonitor_MetricsRegistration(t *testing.T) {
	mockController := &MockController{}
	monitor, err := NewPipelineMonitor(mockController)
	require.NoError(t, err)

	// Test that all metrics are properly registered
	metricFamilies, err := monitor.metricsRegistry.Gather()
	assert.NoError(t, err)

	expectedMetrics := []string{
		"flowctl_pipeline_throughput_total",
		"flowctl_pipeline_latency_seconds",
		"flowctl_service_load_ratio",
		"flowctl_backpressure_active",
		"flowctl_throttle_rate",
		"flowctl_service_connections",
		"flowctl_service_discovery_duration_seconds",
		"flowctl_reconnection_events_total",
		"flowctl_pool_total_connections",
		"flowctl_pool_healthy_connections",
		"flowctl_pool_idle_connections",
		"flowctl_service_throughput",
		"flowctl_service_queue_depth",
	}

	registeredMetrics := make(map[string]bool)
	for _, mf := range metricFamilies {
		registeredMetrics[*mf.Name] = true
	}

	for _, expectedMetric := range expectedMetrics {
		assert.True(t, registeredMetrics[expectedMetric], 
			"Expected metric %s to be registered", expectedMetric)
	}
}

// Test Connection Pool Metrics
func TestPipelineMonitor_ConnectionPoolMetrics(t *testing.T) {
	mockController := &MockController{}
	monitor, err := NewPipelineMonitor(mockController)
	require.NoError(t, err)

	poolStats := map[string]PoolStats{
		"localhost:8815": {
			Endpoint:           "localhost:8815",
			TotalConnections:   10,
			HealthyConnections: 8,
			IdleConnections:    3,
			AverageUseCount:    45.2,
			OldestConnection:   10 * time.Minute,
			TotalUseCount:     452,
		},
		"localhost:8816": {
			Endpoint:           "localhost:8816",
			TotalConnections:   6,
			HealthyConnections: 6,
			IdleConnections:    1,
			AverageUseCount:    22.8,
			OldestConnection:   5 * time.Minute,
			TotalUseCount:     137,
		},
	}

	mockController.On("GetConnectionPoolStats").Return(poolStats)

	err = monitor.updateConnectionPoolMetrics()
	assert.NoError(t, err)

	// Verify pool metrics are updated correctly
	totalConnGauge, err := monitor.poolTotalConnections.GetMetricWithLabelValues("localhost:8815")
	assert.NoError(t, err)
	assert.Equal(t, float64(10), testutil.ToFloat64(totalConnGauge))

	healthyConnGauge, err := monitor.poolHealthyConnections.GetMetricWithLabelValues("localhost:8815")
	assert.NoError(t, err)
	assert.Equal(t, float64(8), testutil.ToFloat64(healthyConnGauge))

	idleConnGauge, err := monitor.poolIdleConnections.GetMetricWithLabelValues("localhost:8815")
	assert.NoError(t, err)
	assert.Equal(t, float64(3), testutil.ToFloat64(idleConnGauge))

	mockController.AssertExpectations(t)
}

// Test Service Discovery Metrics
func TestPipelineMonitor_ServiceDiscoveryMetrics(t *testing.T) {
	mockController := &MockController{}
	monitor, err := NewPipelineMonitor(mockController)
	require.NoError(t, err)

	t.Run("Record Discovery Latency", func(t *testing.T) {
		// Simulate service discovery operation
		startTime := time.Now()
		time.Sleep(10 * time.Millisecond) // Simulate some latency
		duration := time.Since(startTime)

		monitor.recordServiceDiscovery("stellar-source", duration)

		// Verify histogram recorded the observation
		histogram := monitor.discoveryLatency
		assert.NotNil(t, histogram)

		// Get the metric to verify it recorded the observation
		metric := &prometheus.HistogramVec{}
		metric = histogram
		histogramMetric, err := metric.GetMetricWithLabelValues("stellar-source")
		assert.NoError(t, err)
		assert.NotNil(t, histogramMetric)
	})

	t.Run("Record Reconnection Event", func(t *testing.T) {
		// Record reconnection events
		monitor.recordReconnectionEvent("localhost:8815", "connection_lost")
		monitor.recordReconnectionEvent("localhost:8815", "connection_lost")
		monitor.recordReconnectionEvent("localhost:8816", "health_check_failed")

		// Verify counters are incremented
		counter, err := monitor.reconnectionEvents.GetMetricWithLabelValues("localhost:8815", "connection_lost")
		assert.NoError(t, err)
		assert.Equal(t, float64(2), testutil.ToFloat64(counter))

		counter, err = monitor.reconnectionEvents.GetMetricWithLabelValues("localhost:8816", "health_check_failed")
		assert.NoError(t, err)
		assert.Equal(t, float64(1), testutil.ToFloat64(counter))
	})
}

// Test Monitor Lifecycle
func TestPipelineMonitor_Lifecycle(t *testing.T) {
	mockController := &MockController{}
	monitor, err := NewPipelineMonitor(mockController)
	require.NoError(t, err)

	// Mock all controller methods for continuous monitoring
	mockController.On("GetServiceLoadMetrics").Return(map[string]float64{
		"test-service": 0.5,
	}).Maybe()
	mockController.On("IsBackpressureActive").Return(false).Maybe()
	mockController.On("GetCurrentThrottleRate").Return(1.0).Maybe()
	mockController.On("CalculatePipelineMetrics").Return(&PipelineMetrics{
		TotalThroughput: 100.0,
		EndToEndLatency: 100 * time.Millisecond,
		ErrorRates:      map[string]float64{},
	}, nil).Maybe()
	mockController.On("GetConnectionPoolStats").Return(map[string]PoolStats{}).Maybe()

	t.Run("Start and Stop Monitoring", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start monitoring with very short interval for testing
		err := monitor.StartMonitoring(ctx, 50*time.Millisecond)
		assert.NoError(t, err)

		// Let it run for a short time
		time.Sleep(150 * time.Millisecond)

		// Stop monitoring
		monitor.StopMonitoring()

		// Verify it can be restarted
		err = monitor.StartMonitoring(ctx, 50*time.Millisecond)
		assert.NoError(t, err)

		monitor.StopMonitoring()
	})

	t.Run("Monitor Context Cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		err := monitor.StartMonitoring(ctx, 50*time.Millisecond)
		assert.NoError(t, err)

		// Cancel context
		cancel()

		// Give time for cleanup
		time.Sleep(100 * time.Millisecond)

		// Monitor should have stopped gracefully
		assert.False(t, monitor.isRunning)
	})
}

// Test Error Conditions
func TestPipelineMonitor_ErrorHandling(t *testing.T) {
	mockController := &MockController{}
	monitor, err := NewPipelineMonitor(mockController)
	require.NoError(t, err)

	t.Run("Controller Error Handling", func(t *testing.T) {
		// Mock controller to return errors
		mockController.On("GetServiceLoadMetrics").Return(map[string]float64{})
		mockController.On("IsBackpressureActive").Return(false)
		mockController.On("GetCurrentThrottleRate").Return(1.0)
		mockController.On("CalculatePipelineMetrics").Return((*PipelineMetrics)(nil), assert.AnError)
		mockController.On("GetConnectionPoolStats").Return(map[string]PoolStats{})

		err := monitor.CollectMetrics()
		assert.Error(t, err)

		mockController.AssertExpectations(t)
	})

	t.Run("Partial Failure Handling", func(t *testing.T) {
		// Reset mocks
		mockController.ExpectedCalls = nil

		// Some methods succeed, some fail
		mockController.On("GetServiceLoadMetrics").Return(map[string]float64{
			"test-service": 0.6,
		})
		mockController.On("IsBackpressureActive").Return(true)
		mockController.On("GetCurrentThrottleRate").Return(0.8)
		mockController.On("CalculatePipelineMetrics").Return(&PipelineMetrics{
			TotalThroughput: 500.0,
			EndToEndLatency: 200 * time.Millisecond,
			ErrorRates:      map[string]float64{},
		}, nil)
		mockController.On("GetConnectionPoolStats").Return(map[string]PoolStats{})

		err := monitor.CollectMetrics()
		assert.NoError(t, err)

		// Verify that successful metrics were still collected
		assert.Equal(t, float64(1), testutil.ToFloat64(monitor.backpressureActive))
		assert.Equal(t, 0.8, testutil.ToFloat64(monitor.throttleRate))

		mockController.AssertExpectations(t)
	})
}

// Benchmark Tests
func BenchmarkPipelineMonitor_CollectMetrics(b *testing.B) {
	mockController := &MockController{}
	monitor, _ := NewPipelineMonitor(mockController)

	// Set up mock responses
	mockController.On("GetServiceLoadMetrics").Return(map[string]float64{
		"service1": 0.5,
		"service2": 0.6,
		"service3": 0.7,
	}).Maybe()
	mockController.On("IsBackpressureActive").Return(false).Maybe()
	mockController.On("GetCurrentThrottleRate").Return(1.0).Maybe()
	mockController.On("CalculatePipelineMetrics").Return(&PipelineMetrics{
		TotalThroughput: 1000.0,
		EndToEndLatency: 100 * time.Millisecond,
		ErrorRates:      map[string]float64{},
	}, nil).Maybe()
	mockController.On("GetConnectionPoolStats").Return(map[string]PoolStats{}).Maybe()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		monitor.CollectMetrics()
	}
}

func BenchmarkPipelineMonitor_GenerateHealthReport(b *testing.B) {
	mockController := &MockController{}
	monitor, _ := NewPipelineMonitor(mockController)

	// Set up mock responses
	mockController.On("GetServiceLoadMetrics").Return(map[string]float64{
		"service1": 0.45,
		"service2": 0.60,
		"service3": 0.35,
	}).Maybe()
	mockController.On("IsBackpressureActive").Return(false).Maybe()
	mockController.On("GetCurrentThrottleRate").Return(1.0).Maybe()
	mockController.On("CalculatePipelineMetrics").Return(&PipelineMetrics{
		TotalThroughput: 800.0,
		EndToEndLatency: 200 * time.Millisecond,
		ErrorRates: map[string]float64{
			"service1": 0.01,
			"service2": 0.015,
			"service3": 0.005,
		},
	}, nil).Maybe()
	mockController.On("GetPipelineTopology").Return(&PipelineTopology{
		Services: map[string]*flowctlpb.ServiceStatus{
			"service1": {Status: "healthy"},
			"service2": {Status: "healthy"},
			"service3": {Status: "healthy"},
		},
	}, nil).Maybe()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		monitor.GenerateHealthReport()
	}
}
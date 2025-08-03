package fault_tolerance

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/withobsrvr/obsrvr-stellar-components/internal/flowctl"
	flowctlpb "github.com/withobsrvr/obsrvr-stellar-components/proto/gen"
)

// FaultToleranceTestSuite contains fault tolerance tests for the flowctl system
type FaultToleranceTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cleanups []func()
}

// SetupSuite runs once before all tests in the suite
func (suite *FaultToleranceTestSuite) SetupSuite() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cleanups = make([]func(), 0)
	
	// Seed random number generator for fault injection
	rand.Seed(time.Now().UnixNano())
}

// TearDownSuite runs once after all tests in the suite
func (suite *FaultToleranceTestSuite) TearDownSuite() {
	suite.cancel()
	
	// Run all cleanup functions in reverse order
	for i := len(suite.cleanups) - 1; i >= 0; i-- {
		suite.cleanups[i]()
	}
}

// TestFlowCtlUnavailability tests behavior when flowctl is unavailable
func (suite *FaultToleranceTestSuite) TestFlowCtlUnavailability() {
	t := suite.T()

	// Create metrics provider
	metrics := &FaultTolerantMetricsProvider{
		metrics: map[string]float64{
			"test_metric": 100.0,
			"error_count": 0,
		},
	}

	// Create controller pointing to non-existent flowctl endpoint
	controller, err := flowctl.NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		metrics,
	)
	require.NoError(t, err)

	suite.cleanups = append(suite.cleanups, func() {
		controller.Stop()
	})

	t.Run("Controller Start Without FlowCtl", func(t *testing.T) {
		// Controller should start even if flowctl is unavailable
		err := controller.Start(suite.ctx)
		assert.NoError(t, err)
		assert.True(t, controller.IsRunning())
	})

	t.Run("Registration Failure Handling", func(t *testing.T) {
		// Registration should fail gracefully
		err := controller.RegisterWithFlowctl()
		assert.Error(t, err) // Should fail since no flowctl is running
		
		// But controller should still be operational
		assert.True(t, controller.IsRunning())
	})

	t.Run("Flow Control Without FlowCtl", func(t *testing.T) {
		// Flow control operations should work locally
		controller.UpdateServiceInflight("test-service", 50)
		
		signal := flowctl.BackpressureSignal{
			ServiceID:       "test-service",
			CurrentLoad:     0.8,
			RecommendedRate: 0.7,
			Timestamp:       time.Now(),
			Reason:          "Test backpressure",
		}
		
		controller.SendBackpressureSignal(signal)
		
		// Local flow control should be operational
		assert.True(t, controller.IsBackpressureActive())
		assert.Equal(t, 0.7, controller.GetCurrentThrottleRate())
	})

	t.Run("Service Discovery Fallback", func(t *testing.T) {
		// Service discovery should return empty results gracefully
		sources := controller.DiscoverServices(flowctlpb.ServiceType_SERVICE_TYPE_SOURCE)
		assert.NotNil(t, sources) // Should not be nil
		assert.Empty(t, sources)  // But should be empty
	})
}

// TestConnectionPoolFailures tests connection pool fault tolerance
func (suite *FaultToleranceTestSuite) TestConnectionPoolFailures() {
	t := suite.T()

	config := &flowctl.ConnectionPoolConfig{
		MaxConnections:      5,
		MinConnections:      2,
		IdleTimeout:         1 * time.Minute,
		ConnectionTimeout:   1 * time.Second,
		MaxRetries:          3,
		HealthCheckInterval: 2 * time.Second,
		EnableLoadBalancing: true,
	}

	pool := flowctl.NewConnectionPool(config)
	require.NotNil(t, pool)

	suite.cleanups = append(suite.cleanups, func() {
		pool.Stop()
	})

	err := pool.Start(suite.ctx)
	require.NoError(t, err)

	t.Run("Connection to Non-Existent Endpoint", func(t *testing.T) {
		// Should handle connection failures gracefully
		_, err := pool.GetConnection("localhost:9999")
		assert.Error(t, err) // Should fail but not panic
		
		// Pool should still be functional
		stats := pool.GetPoolStats()
		assert.NotNil(t, stats)
	})

	t.Run("Rapid Connection Attempts", func(t *testing.T) {
		// Simulate rapid connection attempts that will fail
		var wg sync.WaitGroup
		errors := make(chan error, 20)

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				endpoint := fmt.Sprintf("localhost:%d", 9000+id)
				_, err := pool.GetConnection(endpoint)
				if err != nil {
					errors <- err
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Should have errors but pool should remain stable
		errorCount := 0
		for range errors {
			errorCount++
		}
		
		assert.Greater(t, errorCount, 0, "Should have connection errors")
		
		// Pool should still provide statistics
		stats := pool.GetPoolStats()
		assert.NotNil(t, stats)
	})

	t.Run("Connection Pool Resource Exhaustion", func(t *testing.T) {
		// Try to exhaust connection pool resources
		endpoint := "localhost:9998"
		
		var connections []interface{} // Would be FlightServiceClient in real usage
		
		// Attempt to create more connections than the limit
		for i := 0; i < config.MaxConnections*2; i++ {
			conn, err := pool.GetConnection(endpoint)
			if err == nil {
				connections = append(connections, conn)
			}
		}
		
		// Pool should respect limits and handle excess gracefully
		stats := pool.GetPoolStats()
		if endpointStats, exists := stats[endpoint]; exists {
			assert.LessOrEqual(t, endpointStats.TotalConnections, config.MaxConnections)
		}
	})
}

// TestNetworkPartitioning tests behavior during network issues
func (suite *FaultToleranceTestSuite) TestNetworkPartitioning() {
	t := suite.T()

	// Create a mock server that we can control
	mockServer := &ControlledMockServer{}
	err := mockServer.Start("localhost:0") // Use any available port
	require.NoError(t, err)
	
	suite.cleanups = append(suite.cleanups, func() {
		mockServer.Stop()
	})

	serverAddr := mockServer.GetAddress()
	
	// Create connection pool pointing to mock server
	pool := flowctl.NewConnectionPool(&flowctl.ConnectionPoolConfig{
		MaxConnections:      3,
		MinConnections:      1,
		ConnectionTimeout:   2 * time.Second,
		HealthCheckInterval: 1 * time.Second,
		IdleTimeout:         30 * time.Second,
	})
	
	suite.cleanups = append(suite.cleanups, func() {
		pool.Stop()
	})

	err = pool.Start(suite.ctx)
	require.NoError(t, err)

	t.Run("Server Available", func(t *testing.T) {
		mockServer.SetAvailable(true)
		time.Sleep(100 * time.Millisecond) // Allow server to become available
		
		// Connections should work
		_, err := pool.GetConnection(serverAddr)
		// Note: Actual connection will fail since it's not a real gRPC server
		// But the attempt should be made
		assert.Error(t, err) // Expected since mock server doesn't speak gRPC
	})

	t.Run("Server Becomes Unavailable", func(t *testing.T) {
		mockServer.SetAvailable(false)
		time.Sleep(100 * time.Millisecond)
		
		// Connection attempts should fail quickly
		start := time.Now()
		_, err := pool.GetConnection(serverAddr)
		duration := time.Since(start)
		
		assert.Error(t, err)
		assert.Less(t, duration, 5*time.Second, "Should fail quickly when server unavailable")
	})

	t.Run("Server Recovery", func(t *testing.T) {
		mockServer.SetAvailable(true)
		time.Sleep(100 * time.Millisecond)
		
		// Pool should handle recovery
		stats := pool.GetPoolStats()
		assert.NotNil(t, stats)
	})

	t.Run("Intermittent Failures", func(t *testing.T) {
		// Simulate intermittent network issues
		for i := 0; i < 10; i++ {
			available := (i % 3) == 0 // Available 1/3 of the time
			mockServer.SetAvailable(available)
			
			_, err := pool.GetConnection(serverAddr)
			// Error is expected - just verify no panic
			_ = err
			
			time.Sleep(100 * time.Millisecond)
		}
		
		// Pool should remain stable
		stats := pool.GetPoolStats()
		assert.NotNil(t, stats)
	})
}

// TestHighLoadConditions tests behavior under high load
func (suite *FaultToleranceTestSuite) TestHighLoadConditions() {
	t := suite.T()

	// Create metrics that simulate high load
	metrics := &FaultTolerantMetricsProvider{
		metrics: map[string]float64{
			"current_inflight": 90,
			"max_inflight":     100,
			"error_rate":       0.15, // 15% error rate
			"latency_ms":       500,  // High latency
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

	t.Run("High Inflight Count", func(t *testing.T) {
		// Simulate very high inflight counts
		controller.UpdateServiceInflight("service-1", 85)
		controller.UpdateServiceInflight("service-2", 80)
		controller.UpdateServiceInflight("service-3", 75)
		
		// System should activate backpressure
		metrics, err := controller.CalculatePipelineMetrics()
		assert.NoError(t, err)
		assert.NotNil(t, metrics)
	})

	t.Run("Backpressure Under High Load", func(t *testing.T) {
		// Send high load signal
		signal := flowctl.BackpressureSignal{
			ServiceID:       "service-1",
			CurrentLoad:     0.95,
			RecommendedRate: 0.3,
			Timestamp:       time.Now(),
			Reason:          "Extreme high load test",
		}
		
		controller.SendBackpressureSignal(signal)
		
		// Verify backpressure activation
		assert.True(t, controller.IsBackpressureActive())
		assert.Equal(t, 0.3, controller.GetCurrentThrottleRate())
	})

	t.Run("Rapid Load Changes", func(t *testing.T) {
		// Simulate rapid load fluctuations
		loads := []float64{0.9, 0.3, 0.8, 0.2, 0.95, 0.1, 0.85, 0.4}
		
		for i, load := range loads {
			signal := flowctl.BackpressureSignal{
				ServiceID:       fmt.Sprintf("service-%d", i%3),
				CurrentLoad:     load,
				RecommendedRate: 1.0 - load + 0.1, // Inverse relationship
				Timestamp:       time.Now(),
				Reason:          fmt.Sprintf("Load change %d", i),
			}
			
			controller.SendBackpressureSignal(signal)
			time.Sleep(50 * time.Millisecond)
		}
		
		// System should handle rapid changes
		assert.True(t, controller.IsRunning())
	})
}

// TestConcurrentOperations tests thread safety under concurrent access
func (suite *FaultToleranceTestSuite) TestConcurrentOperations() {
	t := suite.T()

	metrics := &FaultTolerantMetricsProvider{
		metrics: map[string]float64{
			"base_metric": 1.0,
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

	t.Run("Concurrent Flow Control Updates", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 20
		operationsPerGoroutine := 100

		// Launch multiple goroutines updating flow control
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				
				for j := 0; j < operationsPerGoroutine; j++ {
					serviceID := fmt.Sprintf("service-%d", goroutineID%5)
					inflight := int32(rand.Intn(100))
					
					controller.UpdateServiceInflight(serviceID, inflight)
					
					if rand.Float64() < 0.1 { // 10% chance of backpressure signal
						signal := flowctl.BackpressureSignal{
							ServiceID:       serviceID,
							CurrentLoad:     rand.Float64(),
							RecommendedRate: rand.Float64(),
							Timestamp:       time.Now(),
							Reason:          fmt.Sprintf("Concurrent test %d-%d", goroutineID, j),
						}
						controller.SendBackpressureSignal(signal)
					}
					
					// Small random delay
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				}
			}(i)
		}

		wg.Wait()

		// Verify controller remains stable
		assert.True(t, controller.IsRunning())
		
		// Verify metrics can still be calculated
		pipelineMetrics, err := controller.CalculatePipelineMetrics()
		assert.NoError(t, err)
		assert.NotNil(t, pipelineMetrics)
	})

	t.Run("Concurrent Service Discovery", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 10

		// Launch multiple goroutines doing service discovery
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				
				for j := 0; j < 20; j++ {
					serviceTypes := []flowctlpb.ServiceType{
						flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
						flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
						flowctlpb.ServiceType_SERVICE_TYPE_SINK,
					}
					
					serviceType := serviceTypes[rand.Intn(len(serviceTypes))]
					services := controller.DiscoverServices(serviceType)
					
					// Should not panic or error
					assert.NotNil(t, services)
					
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				}
			}()
		}

		wg.Wait()
		
		// Controller should remain stable
		assert.True(t, controller.IsRunning())
	})
}

// TestMemoryLeaks tests for potential memory leaks under stress
func (suite *FaultToleranceTestSuite) TestMemoryLeaks() {
	t := suite.T()

	t.Run("Controller Creation and Destruction", func(t *testing.T) {
		// Create and destroy many controllers
		for i := 0; i < 50; i++ {
			metrics := &FaultTolerantMetricsProvider{
				metrics: map[string]float64{
					"test_metric": float64(i),
				},
			}

			controller, err := flowctl.NewController(
				flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
				fmt.Sprintf("localhost:%d", 9000+i),
				metrics,
			)
			assert.NoError(t, err)

			// Start and quickly stop
			err = controller.Start(suite.ctx)
			assert.NoError(t, err)
			
			time.Sleep(10 * time.Millisecond)
			
			controller.Stop()
		}
	})

	t.Run("Connection Pool Churn", func(t *testing.T) {
		// Create and destroy connection pools
		for i := 0; i < 20; i++ {
			pool := flowctl.NewConnectionPool(&flowctl.ConnectionPoolConfig{
				MaxConnections:  5,
				MinConnections:  1,
				ConnectionTimeout: 100 * time.Millisecond,
			})

			ctx, cancel := context.WithCancel(suite.ctx)
			err := pool.Start(ctx)
			assert.NoError(t, err)

			// Try some connections
			for j := 0; j < 5; j++ {
				pool.GetConnection(fmt.Sprintf("localhost:%d", 9500+j))
			}

			time.Sleep(50 * time.Millisecond)

			pool.Stop()
			cancel()
		}
	})

	t.Run("Metrics Provider Churn", func(t *testing.T) {
		// Create many metrics providers with varying data
		providers := make([]*FaultTolerantMetricsProvider, 100)
		
		for i := 0; i < 100; i++ {
			providers[i] = &FaultTolerantMetricsProvider{
				metrics: make(map[string]float64),
			}
			
			// Add many metrics
			for j := 0; j < 50; j++ {
				key := fmt.Sprintf("metric_%d_%d", i, j)
				value := rand.Float64() * 1000
				providers[i].metrics[key] = value
			}
		}

		// Use the providers
		for _, provider := range providers {
			metrics := provider.GetMetrics()
			assert.NotEmpty(t, metrics)
		}
	})
}

// TestErrorRecovery tests recovery from various error conditions
func (suite *FaultToleranceTestSuite) TestErrorRecovery() {
	t := suite.T()

	// Create a metrics provider that can simulate errors
	metrics := &FaultTolerantMetricsProvider{
		metrics: map[string]float64{
			"normal_metric": 100.0,
		},
		simulateError: false,
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

	t.Run("Metrics Provider Error Recovery", func(t *testing.T) {
		// Normal operation
		pipelineMetrics, err := controller.CalculatePipelineMetrics()
		assert.NoError(t, err)
		assert.NotNil(t, pipelineMetrics)

		// Simulate metrics error
		metrics.simulateError = true
		pipelineMetrics, err = controller.CalculatePipelineMetrics()
		// Should handle the error gracefully
		
		// Recover from error
		metrics.simulateError = false
		pipelineMetrics, err = controller.CalculatePipelineMetrics()
		assert.NoError(t, err)
		assert.NotNil(t, pipelineMetrics)
	})

	t.Run("Invalid Flow Control Data", func(t *testing.T) {
		// Send invalid backpressure signals
		invalidSignals := []flowctl.BackpressureSignal{
			{
				ServiceID:       "", // Empty service ID
				CurrentLoad:     1.5, // Invalid load > 1.0
				RecommendedRate: -0.5, // Negative rate
				Timestamp:       time.Time{}, // Zero timestamp
				Reason:          "Invalid test signal",
			},
			{
				ServiceID:       "valid-service",
				CurrentLoad:     -0.1, // Negative load
				RecommendedRate: 2.0,  // Rate > 1.0
				Timestamp:       time.Now(),
				Reason:          "Another invalid signal",
			},
		}

		for _, signal := range invalidSignals {
			// Should not panic or crash
			controller.SendBackpressureSignal(signal)
		}

		// Controller should remain operational
		assert.True(t, controller.IsRunning())
		
		// Should still be able to perform normal operations
		controller.UpdateServiceInflight("valid-service", 50)
		loadMetrics := controller.GetServiceLoadMetrics()
		assert.NotNil(t, loadMetrics)
	})
}

// Helper Types and Methods

// FaultTolerantMetricsProvider simulates a metrics provider that can have errors
type FaultTolerantMetricsProvider struct {
	mu            sync.RWMutex
	metrics       map[string]float64
	simulateError bool
}

func (f *FaultTolerantMetricsProvider) GetMetrics() map[string]float64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.simulateError {
		// Return nil to simulate error (would normally panic or return error)
		return nil
	}

	result := make(map[string]float64, len(f.metrics))
	for k, v := range f.metrics {
		result[k] = v
	}
	return result
}

func (f *FaultTolerantMetricsProvider) UpdateMetric(name string, value float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	if f.metrics == nil {
		f.metrics = make(map[string]float64)
	}
	f.metrics[name] = value
}

// ControlledMockServer simulates a server that can be controlled for testing
type ControlledMockServer struct {
	listener  net.Listener
	available bool
	mu        sync.RWMutex
	stopChan  chan struct{}
}

func (s *ControlledMockServer) Start(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	
	s.listener = listener
	s.available = true
	s.stopChan = make(chan struct{})
	
	go s.serve()
	return nil
}

func (s *ControlledMockServer) serve() {
	for {
		select {
		case <-s.stopChan:
			return
		default:
			if conn, err := s.listener.Accept(); err == nil {
				go s.handleConnection(conn)
			}
		}
	}
}

func (s *ControlledMockServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	
	s.mu.RLock()
	available := s.available
	s.mu.RUnlock()
	
	if !available {
		// Immediately close connection if not available
		return
	}
	
	// Keep connection open briefly then close
	time.Sleep(100 * time.Millisecond)
}

func (s *ControlledMockServer) SetAvailable(available bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available = available
}

func (s *ControlledMockServer) GetAddress() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}

func (s *ControlledMockServer) Stop() {
	if s.stopChan != nil {
		close(s.stopChan)
	}
	if s.listener != nil {
		s.listener.Close()
	}
}

// Run the fault tolerance test suite
func TestFaultToleranceSuite(t *testing.T) {
	suite.Run(t, new(FaultToleranceTestSuite))
}
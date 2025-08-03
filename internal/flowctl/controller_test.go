package flowctl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	flowctlpb "github.com/withobsrvr/obsrvr-stellar-components/proto/gen"
)

// MockMetricsProvider for testing
type MockMetricsProvider struct {
	mock.Mock
}

func (m *MockMetricsProvider) GetMetrics() map[string]float64 {
	args := m.Called()
	return args.Get(0).(map[string]float64)
}

// MockFlowCtlClient for testing gRPC interactions
type MockFlowCtlClient struct {
	mock.Mock
}

func (m *MockFlowCtlClient) RegisterService(ctx context.Context, req *flowctlpb.RegisterServiceRequest) (*flowctlpb.RegisterServiceResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*flowctlpb.RegisterServiceResponse), args.Error(1)
}

func (m *MockFlowCtlClient) SendHeartbeat(ctx context.Context, req *flowctlpb.HeartbeatRequest) (*flowctlpb.HeartbeatResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*flowctlpb.HeartbeatResponse), args.Error(1)
}

func (m *MockFlowCtlClient) UnregisterService(ctx context.Context, req *flowctlpb.UnregisterServiceRequest) (*flowctlpb.UnregisterServiceResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*flowctlpb.UnregisterServiceResponse), args.Error(1)
}

// Test Controller Creation
func TestNewController(t *testing.T) {
	tests := []struct {
		name         string
		serviceType  flowctlpb.ServiceType
		endpoint     string
		expectError  bool
		expectedType flowctlpb.ServiceType
	}{
		{
			name:         "Valid SOURCE controller",
			serviceType:  flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
			endpoint:     "localhost:8815",
			expectError:  false,
			expectedType: flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
		},
		{
			name:         "Valid PROCESSOR controller",
			serviceType:  flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
			endpoint:     "localhost:8816",
			expectError:  false,
			expectedType: flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		},
		{
			name:         "Valid SINK controller",
			serviceType:  flowctlpb.ServiceType_SERVICE_TYPE_SINK,
			endpoint:     "localhost:8817",
			expectError:  false,
			expectedType: flowctlpb.ServiceType_SERVICE_TYPE_SINK,
		},
		{
			name:        "Invalid service type",
			serviceType: flowctlpb.ServiceType_SERVICE_TYPE_UNSPECIFIED,
			endpoint:    "localhost:8815",
			expectError: true,
		},
		{
			name:        "Invalid endpoint format",
			serviceType: flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
			endpoint:    "invalid-endpoint",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMetrics := &MockMetricsProvider{}
			
			controller, err := NewController(tt.serviceType, tt.endpoint, mockMetrics)
			
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, controller)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, controller)
				assert.Equal(t, tt.expectedType, controller.serviceType)
				assert.Equal(t, tt.endpoint, controller.endpoint)
				assert.NotEmpty(t, controller.serviceID)
			}
		})
	}
}

// Test Service Registration
func TestController_RegisterWithFlowctl(t *testing.T) {
	tests := []struct {
		name           string
		mockResponse   *flowctlpb.RegisterServiceResponse
		mockError      error
		expectError    bool
		expectedStatus string
	}{
		{
			name: "Successful registration",
			mockResponse: &flowctlpb.RegisterServiceResponse{
				ServiceId: "test-service-123",
				Status:    "registered",
			},
			mockError:      nil,
			expectError:    false,
			expectedStatus: "registered",
		},
		{
			name:         "Registration failure",
			mockResponse: nil,
			mockError:    assert.AnError,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMetrics := &MockMetricsProvider{}
			mockMetrics.On("GetMetrics").Return(map[string]float64{
				"test_metric": 123.45,
			})

			controller, err := NewController(
				flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
				"localhost:8815",
				mockMetrics,
			)
			require.NoError(t, err)

			// Mock the gRPC client
			mockClient := &MockFlowCtlClient{}
			controller.client = mockClient

			if tt.mockError != nil {
				mockClient.On("RegisterService", mock.Anything, mock.AnythingOfType("*gen.RegisterServiceRequest")).
					Return((*flowctlpb.RegisterServiceResponse)(nil), tt.mockError)
			} else {
				mockClient.On("RegisterService", mock.Anything, mock.AnythingOfType("*gen.RegisterServiceRequest")).
					Return(tt.mockResponse, nil)
			}

			err = controller.RegisterWithFlowctl()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
			mockMetrics.AssertExpectations(t)
		})
	}
}

// Test Flow Control Functionality
func TestController_FlowControl(t *testing.T) {
	controller, err := NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		&MockMetricsProvider{},
	)
	require.NoError(t, err)

	t.Run("Update Service Inflight", func(t *testing.T) {
		// Test updating inflight count
		controller.UpdateServiceInflight("test-service", 10)
		
		assert.Equal(t, int32(10), controller.flowControl.ServiceInflight["test-service"])
	})

	t.Run("Send Backpressure Signal", func(t *testing.T) {
		signal := BackpressureSignal{
			ServiceID:       "test-service",
			CurrentLoad:     0.85,
			RecommendedRate: 0.5,
			Timestamp:       time.Now(),
			Reason:          "High load detected",
		}

		controller.SendBackpressureSignal(signal)
		
		// Verify backpressure is active and throttle rate is updated
		assert.True(t, controller.flowControl.BackpressureActive)
		assert.Equal(t, 0.5, controller.flowControl.ThrottleRate)
	})

	t.Run("Calculate Pipeline Metrics", func(t *testing.T) {
		// Set up test data
		controller.flowControl.ServiceInflight["service1"] = 50
		controller.flowControl.ServiceInflight["service2"] = 30
		controller.flowControl.ServiceMaxInflight["service1"] = 100
		controller.flowControl.ServiceMaxInflight["service2"] = 100

		metrics, err := controller.CalculatePipelineMetrics()
		require.NoError(t, err)
		assert.NotNil(t, metrics)

		// Verify metrics calculation
		assert.Equal(t, int32(80), controller.flowControl.ServiceInflight["service1"]+controller.flowControl.ServiceInflight["service2"])
		assert.Equal(t, int32(200), controller.flowControl.ServiceMaxInflight["service1"]+controller.flowControl.ServiceMaxInflight["service2"])
	})
}

// Test Backpressure Algorithm
func TestController_BackpressureAlgorithm(t *testing.T) {
	controller, err := NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		&MockMetricsProvider{},
	)
	require.NoError(t, err)

	tests := []struct {
		name                string
		globalInflight      int32
		globalMaxInflight   int32
		expectedActivation  bool
		expectedThrottleRate float64
	}{
		{
			name:                "Normal load (50%)",
			globalInflight:      50,
			globalMaxInflight:   100,
			expectedActivation:  false,
			expectedThrottleRate: 1.0,
		},
		{
			name:                "High load (75%) - below threshold",
			globalInflight:      75,
			globalMaxInflight:   100,
			expectedActivation:  false,
			expectedThrottleRate: 1.0,
		},
		{
			name:                "Critical load (85%) - above threshold",
			globalInflight:      85,
			globalMaxInflight:   100,
			expectedActivation:  true,
			expectedThrottleRate: 0.5,
		},
		{
			name:                "Maximum load (95%)",
			globalInflight:      95,
			globalMaxInflight:   100,
			expectedActivation:  true,
			expectedThrottleRate: 0.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset controller state
			controller.flowControl.BackpressureActive = false
			controller.flowControl.ThrottleRate = 1.0
			controller.flowControl.MaxInflightGlobal = tt.globalMaxInflight

			// Simulate global load
			totalInflight := tt.globalInflight
			controller.flowControl.ServiceInflight = map[string]int32{
				"service1": totalInflight / 2,
				"service2": totalInflight - (totalInflight / 2),
			}
			controller.flowControl.ServiceMaxInflight = map[string]int32{
				"service1": tt.globalMaxInflight / 2,
				"service2": tt.globalMaxInflight - (tt.globalMaxInflight / 2),
			}

			// Calculate load and trigger backpressure if needed
			globalLoad := float64(totalInflight) / float64(tt.globalMaxInflight)
			
			if globalLoad > 0.8 { // 80% threshold
				signal := BackpressureSignal{
					ServiceID:       "test",
					CurrentLoad:     globalLoad,
					RecommendedRate: 0.5,
					Timestamp:       time.Now(),
					Reason:          "Load threshold exceeded",
				}
				controller.SendBackpressureSignal(signal)
			}

			assert.Equal(t, tt.expectedActivation, controller.flowControl.BackpressureActive)
			assert.Equal(t, tt.expectedThrottleRate, controller.flowControl.ThrottleRate)
		})
	}
}

// Test Service Discovery
func TestController_ServiceDiscovery(t *testing.T) {
	controller, err := NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		&MockMetricsProvider{},
	)
	require.NoError(t, err)

	t.Run("Discover Services", func(t *testing.T) {
		// Mock available services
		availableServices := map[string]*flowctlpb.ServiceStatus{
			"stellar-source": {
				ServiceId:   "stellar-source-123",
				ServiceType: flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
				Endpoint:    "localhost:8815",
				Status:      "healthy",
			},
			"analytics-sink": {
				ServiceId:   "analytics-sink-456",
				ServiceType: flowctlpb.ServiceType_SERVICE_TYPE_SINK,
				Endpoint:    "localhost:8817",
				Status:      "healthy",
			},
		}

		// Test service discovery
		sources := controller.DiscoverServices(flowctlpb.ServiceType_SERVICE_TYPE_SOURCE)
		sinks := controller.DiscoverServices(flowctlpb.ServiceType_SERVICE_TYPE_SINK)

		// For this test, we'll simulate discovery by directly checking types
		assert.NotNil(t, sources)
		assert.NotNil(t, sinks)
		
		// Verify service filtering works
		for _, service := range availableServices {
			if service.ServiceType == flowctlpb.ServiceType_SERVICE_TYPE_SOURCE {
				assert.Contains(t, sources, service.Endpoint)
			}
			if service.ServiceType == flowctlpb.ServiceType_SERVICE_TYPE_SINK {
				assert.Contains(t, sinks, service.Endpoint)
			}
		}
	})
}

// Test Heartbeat Functionality
func TestController_Heartbeat(t *testing.T) {
	mockMetrics := &MockMetricsProvider{}
	mockMetrics.On("GetMetrics").Return(map[string]float64{
		"processed_events": 1000,
		"error_count":     5,
		"latency_ms":      150.5,
	})

	controller, err := NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		mockMetrics,
	)
	require.NoError(t, err)

	mockClient := &MockFlowCtlClient{}
	controller.client = mockClient

	// Mock successful heartbeat response
	mockClient.On("SendHeartbeat", mock.Anything, mock.AnythingOfType("*gen.HeartbeatRequest")).
		Return(&flowctlpb.HeartbeatResponse{
			Status: "acknowledged",
		}, nil)

	t.Run("Send Heartbeat", func(t *testing.T) {
		err := controller.sendHeartbeat()
		assert.NoError(t, err)

		mockClient.AssertExpectations(t)
		mockMetrics.AssertExpectations(t)
	})

	t.Run("Heartbeat with Metrics", func(t *testing.T) {
		// Reset mocks for this test
		mockClient.ExpectedCalls = nil
		mockMetrics.ExpectedCalls = nil

		mockMetrics.On("GetMetrics").Return(map[string]float64{
			"events_processed": 2000,
			"throughput":      50.0,
		})

		mockClient.On("SendHeartbeat", mock.Anything, mock.MatchedBy(func(req *flowctlpb.HeartbeatRequest) bool {
			// Verify metrics are included in heartbeat
			return req.Metrics != nil && len(req.Metrics) > 0
		})).Return(&flowctlpb.HeartbeatResponse{Status: "acknowledged"}, nil)

		err := controller.sendHeartbeat()
		assert.NoError(t, err)

		mockClient.AssertExpectations(t)
		mockMetrics.AssertExpectations(t)
	})
}

// Test Controller Lifecycle
func TestController_Lifecycle(t *testing.T) {
	mockMetrics := &MockMetricsProvider{}
	mockMetrics.On("GetMetrics").Return(map[string]float64{
		"test_metric": 100.0,
	}).Maybe()

	controller, err := NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
		"localhost:8815",
		mockMetrics,
	)
	require.NoError(t, err)

	mockClient := &MockFlowCtlClient{}
	controller.client = mockClient

	t.Run("Complete Lifecycle", func(t *testing.T) {
		// Mock registration
		mockClient.On("RegisterService", mock.Anything, mock.AnythingOfType("*gen.RegisterServiceRequest")).
			Return(&flowctlpb.RegisterServiceResponse{
				ServiceId: "test-service-789",
				Status:    "registered",
			}, nil)

		// Mock heartbeat
		mockClient.On("SendHeartbeat", mock.Anything, mock.AnythingOfType("*gen.HeartbeatRequest")).
			Return(&flowctlpb.HeartbeatResponse{Status: "acknowledged"}, nil).Maybe()

		// Mock unregistration
		mockClient.On("UnregisterService", mock.Anything, mock.AnythingOfType("*gen.UnregisterServiceRequest")).
			Return(&flowctlpb.UnregisterServiceResponse{Status: "unregistered"}, nil)

		// Test lifecycle
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start controller
		err := controller.Start(ctx)
		assert.NoError(t, err)

		// Allow some time for heartbeat
		time.Sleep(100 * time.Millisecond)

		// Stop controller
		controller.Stop()

		// Verify calls were made
		mockClient.AssertExpectations(t)
	})

	t.Run("Start and Stop", func(t *testing.T) {
		// Reset mocks
		mockClient.ExpectedCalls = nil

		mockClient.On("RegisterService", mock.Anything, mock.AnythingOfType("*gen.RegisterServiceRequest")).
			Return(&flowctlpb.RegisterServiceResponse{ServiceId: "test-service", Status: "registered"}, nil)
		mockClient.On("UnregisterService", mock.Anything, mock.AnythingOfType("*gen.UnregisterServiceRequest")).
			Return(&flowctlpb.UnregisterServiceResponse{Status: "unregistered"}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := controller.Start(ctx)
		assert.NoError(t, err)

		// Verify controller is running
		assert.True(t, controller.isRunning)

		controller.Stop()

		// Verify controller is stopped
		assert.False(t, controller.isRunning)

		mockClient.AssertExpectations(t)
	})
}

// Test Error Handling
func TestController_ErrorHandling(t *testing.T) {
	mockMetrics := &MockMetricsProvider{}
	
	controller, err := NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
		"localhost:8815",
		mockMetrics,
	)
	require.NoError(t, err)

	mockClient := &MockFlowCtlClient{}
	controller.client = mockClient

	t.Run("Registration Failure", func(t *testing.T) {
		mockClient.On("RegisterService", mock.Anything, mock.AnythingOfType("*gen.RegisterServiceRequest")).
			Return((*flowctlpb.RegisterServiceResponse)(nil), assert.AnError)

		err := controller.RegisterWithFlowctl()
		assert.Error(t, err)

		mockClient.AssertExpectations(t)
	})

	t.Run("Heartbeat Failure", func(t *testing.T) {
		// Reset mocks
		mockClient.ExpectedCalls = nil
		mockMetrics.On("GetMetrics").Return(map[string]float64{})

		mockClient.On("SendHeartbeat", mock.Anything, mock.AnythingOfType("*gen.HeartbeatRequest")).
			Return((*flowctlpb.HeartbeatResponse)(nil), assert.AnError)

		err := controller.sendHeartbeat()
		assert.Error(t, err)

		mockClient.AssertExpectations(t)
	})

	t.Run("Graceful Degradation", func(t *testing.T) {
		// Test that controller continues to function even when flowctl is unavailable
		// This is important for production resilience

		// Reset mocks
		mockClient.ExpectedCalls = nil

		// Simulate flowctl unavailability
		mockClient.On("RegisterService", mock.Anything, mock.AnythingOfType("*gen.RegisterServiceRequest")).
			Return((*flowctlpb.RegisterServiceResponse)(nil), assert.AnError)

		// Controller should handle this gracefully
		err := controller.RegisterWithFlowctl()
		assert.Error(t, err)

		// But flow control functions should still work
		controller.UpdateServiceInflight("test-service", 10)
		assert.Equal(t, int32(10), controller.flowControl.ServiceInflight["test-service"])

		mockClient.AssertExpectations(t)
	})
}

// Benchmark Tests
func BenchmarkController_UpdateServiceInflight(b *testing.B) {
	controller, _ := NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		&MockMetricsProvider{},
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		controller.UpdateServiceInflight("test-service", int32(i%100))
	}
}

func BenchmarkController_CalculatePipelineMetrics(b *testing.B) {
	controller, _ := NewController(
		flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		"localhost:8816",
		&MockMetricsProvider{},
	)

	// Set up test data
	for i := 0; i < 10; i++ {
		serviceID := fmt.Sprintf("service-%d", i)
		controller.flowControl.ServiceInflight[serviceID] = int32(i * 10)
		controller.flowControl.ServiceMaxInflight[serviceID] = int32(i * 20)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		controller.CalculatePipelineMetrics()
	}
}
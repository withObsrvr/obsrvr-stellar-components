package flowctl

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	flowctlpb "github.com/withobsrvr/obsrvr-stellar-components/proto/gen"
)

// MetricsProvider interface defines how components provide metrics to flowctl
type MetricsProvider interface {
	GetMetrics() map[string]float64
}

// PipelineTopology represents the complete pipeline structure
type PipelineTopology struct {
	Services    map[string]*flowctlpb.ServiceStatus // ServiceID -> ServiceStatus
	Connections map[string][]string                // ServiceID -> [DownstreamServiceIDs]
}

// ConnectionInfo represents connection details for Arrow Flight
type ConnectionInfo struct {
	ServiceID      string
	ServiceType    flowctlpb.ServiceType
	Endpoint       string
	HealthEndpoint string
	Metadata       map[string]string
	LastSeen       time.Time
}

// PipelineHealth aggregates health status across the pipeline
type PipelineHealth struct {
	OverallStatus string                        // healthy, degraded, unhealthy
	ServiceHealth map[string]*ServiceHealth     // ServiceID -> Health
	PipelineStart time.Time
	LastUpdate    time.Time
}

// ServiceHealth represents health status for a single service
type ServiceHealth struct {
	ServiceID      string
	Status         string    // healthy, unhealthy, unknown
	LastHeartbeat  time.Time
	Metrics        map[string]float64
	ErrorCount     int
	ResponseTime   time.Duration
}

// PipelineFlowControl manages flow control across the pipeline
type PipelineFlowControl struct {
	MaxInflightGlobal    int32                    // Global pipeline inflight limit
	ServiceInflight      map[string]int32         // Per-service current inflight
	ServiceMaxInflight   map[string]int32         // Per-service max inflight
	BackpressureActive   bool                     // Whether backpressure is active
	ThrottleRate         float64                  // Current throttle rate (0.0-1.0)
	LastFlowControlUpdate time.Time
}

// BackpressureSignal represents a backpressure notification
type BackpressureSignal struct {
	ServiceID       string
	CurrentLoad     float64  // 0.0-1.0
	RecommendedRate float64  // 0.0-1.0
	Timestamp       time.Time
	Reason          string
}

// PipelineMetrics aggregates metrics across the entire pipeline
type PipelineMetrics struct {
	TotalThroughput    float64                 // Events/sec across pipeline
	EndToEndLatency    time.Duration           // Source to sink latency
	ServiceThroughput  map[string]float64      // Per-service throughput
	ServiceLatency     map[string]time.Duration // Per-service latency
	QueueDepths        map[string]int          // Per-service queue depths
	ErrorRates         map[string]float64      // Per-service error rates
	LastUpdated        time.Time
}

// Controller manages flowctl integration for obsrvr-stellar-components
type Controller struct {
	// Connection management
	conn     *grpc.ClientConn
	client   flowctlpb.ControlPlaneClient
	endpoint string

	// Service registration
	serviceInfo *flowctlpb.ServiceInfo
	serviceID   string

	// Heartbeat management
	metricsProvider   MetricsProvider
	heartbeatInterval time.Duration
	stopHeartbeat     chan struct{}
	heartbeatWg       sync.WaitGroup

	// Flow control management
	flowControl       *PipelineFlowControl
	backpressureChan  chan BackpressureSignal
	flowControlMu     sync.RWMutex

	// State management
	mu      sync.RWMutex
	running bool
}

// NewController creates a new flowctl controller
func NewController(endpoint string, metricsProvider MetricsProvider) *Controller {
	return &Controller{
		endpoint:          endpoint,
		metricsProvider:   metricsProvider,
		heartbeatInterval: 10 * time.Second,
		stopHeartbeat:     make(chan struct{}),
		backpressureChan:  make(chan BackpressureSignal, 100),
		flowControl: &PipelineFlowControl{
			MaxInflightGlobal:     10000, // Default global limit
			ServiceInflight:       make(map[string]int32),
			ServiceMaxInflight:    make(map[string]int32),
			BackpressureActive:    false,
			ThrottleRate:          1.0, // No throttling initially
			LastFlowControlUpdate: time.Now(),
		},
	}
}

// SetServiceInfo configures the service information for registration
func (c *Controller) SetServiceInfo(info *flowctlpb.ServiceInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.serviceInfo = info
}

// SetHeartbeatInterval configures the heartbeat frequency
func (c *Controller) SetHeartbeatInterval(interval time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.heartbeatInterval = interval
}

// Start initializes the connection to flowctl and registers the service
func (c *Controller) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("controller is already running")
	}

	// Establish gRPC connection to flowctl
	if err := c.connect(); err != nil {
		return fmt.Errorf("failed to connect to flowctl: %w", err)
	}

	// Register service with flowctl
	if err := c.register(); err != nil {
		// Use graceful degradation - create simulated service ID
		c.serviceID = fmt.Sprintf("sim-%s-%s", 
			c.serviceInfo.ServiceType.String(),
			time.Now().Format("20060102150405"))
		fmt.Printf("Warning: Failed to register with flowctl at %s. Using simulated ID: %s. Error: %v\n", 
			c.endpoint, c.serviceID, err)
	}

	// Start heartbeat routine
	c.running = true
	c.heartbeatWg.Add(1)
	go c.heartbeatLoop()

	return nil
}

// Stop gracefully shuts down the flowctl integration
func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return
	}

	// Signal heartbeat routine to stop
	close(c.stopHeartbeat)
	c.running = false

	// Wait for heartbeat routine to finish
	c.heartbeatWg.Wait()

	// Close gRPC connection
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.client = nil
	}
}

// GetServiceID returns the registered service ID
func (c *Controller) GetServiceID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.serviceID
}

// IsConnected returns true if connected to flowctl
func (c *Controller) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.client != nil
}

// connect establishes gRPC connection to flowctl control plane
func (c *Controller) connect() error {
	conn, err := grpc.Dial(c.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial flowctl at %s: %w", c.endpoint, err)
	}

	c.conn = conn
	c.client = flowctlpb.NewControlPlaneClient(conn)
	return nil
}

// register performs service registration with flowctl
func (c *Controller) register() error {
	if c.serviceInfo == nil {
		return fmt.Errorf("service info not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ack, err := c.client.Register(ctx, c.serviceInfo)
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}

	c.serviceID = ack.ServiceId
	fmt.Printf("Successfully registered with flowctl. Service ID: %s, Topics: %v\n", 
		c.serviceID, ack.TopicNames)
	
	return nil
}

// heartbeatLoop sends periodic heartbeats to flowctl
func (c *Controller) heartbeatLoop() {
	defer c.heartbeatWg.Done()

	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopHeartbeat:
			return
		case <-ticker.C:
			c.sendHeartbeat()
		}
	}
}

// sendHeartbeat sends a single heartbeat to flowctl
func (c *Controller) sendHeartbeat() {
	// Skip heartbeat if not connected or no service ID
	if c.client == nil || c.serviceID == "" {
		return
	}

	// Collect metrics
	var metrics map[string]float64
	if c.metricsProvider != nil {
		metrics = c.metricsProvider.GetMetrics()
	}
	if metrics == nil {
		metrics = make(map[string]float64)
	}

	// Create heartbeat message
	heartbeat := &flowctlpb.ServiceHeartbeat{
		ServiceId: c.serviceID,
		Timestamp: timestamppb.Now(),
		Metrics:   metrics,
	}

	// Send heartbeat with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.client.Heartbeat(ctx, heartbeat)
	if err != nil {
		fmt.Printf("Failed to send heartbeat to flowctl: %v\n", err)
		return
	}

	// Optional: log successful heartbeats in debug mode
	// fmt.Printf("Sent heartbeat to flowctl. Service ID: %s, Metrics: %d\n", c.serviceID, len(metrics))
}

// GetServiceStatus queries the service status from flowctl
func (c *Controller) GetServiceStatus() (*flowctlpb.ServiceStatus, error) {
	if c.client == nil {
		return nil, fmt.Errorf("not connected to flowctl")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.client.GetServiceStatus(ctx, c.serviceInfo)
}

// ListServices returns all services registered with flowctl
func (c *Controller) ListServices() (*flowctlpb.ServiceList, error) {
	if c.client == nil {
		return nil, fmt.Errorf("not connected to flowctl")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.client.ListServices(ctx, &empty.Empty{})
}

// DiscoverServices finds services by type
// Note: ServiceStatus doesn't include event type info, so we filter by type only
func (c *Controller) DiscoverServices(serviceType flowctlpb.ServiceType, eventType string) ([]*flowctlpb.ServiceStatus, error) {
	services, err := c.ListServices()
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}

	var matches []*flowctlpb.ServiceStatus
	for _, service := range services.Services {
		// Filter by service type
		if service.ServiceType != serviceType {
			continue
		}

		// TODO: Event type filtering would require ServiceInfo, not ServiceStatus
		// For now, we assume services of the correct type provide the events we need
		matches = append(matches, service)
	}

	return matches, nil
}

// DiscoverUpstreamServices finds services that produce events this service consumes
// Note: Since ServiceStatus doesn't have event type info, we discover by service type
func (c *Controller) DiscoverUpstreamServices(inputEventType string) ([]*flowctlpb.ServiceStatus, error) {
	if c.serviceInfo == nil {
		return nil, fmt.Errorf("service info not configured")
	}

	services, err := c.ListServices()
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}

	var upstreams []*flowctlpb.ServiceStatus
	for _, service := range services.Services {
		// Skip self
		if service.ServiceId == c.serviceID {
			continue
		}

		// For stellar ledger events, look for SOURCE services
		if inputEventType == "stellar_ledger_service.StellarLedger" {
			if service.ServiceType == flowctlpb.ServiceType_SERVICE_TYPE_SOURCE {
				upstreams = append(upstreams, service)
			}
		}
		// For TTP events, look for PROCESSOR services  
		if inputEventType == "ttp_event_service.TTPEvent" {
			if service.ServiceType == flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR {
				upstreams = append(upstreams, service)
			}
		}
	}

	return upstreams, nil
}

// DiscoverDownstreamServices finds services that consume events this service produces
// Note: Since ServiceStatus doesn't have event type info, we discover by service type
func (c *Controller) DiscoverDownstreamServices(outputEventType string) ([]*flowctlpb.ServiceStatus, error) {
	if c.serviceInfo == nil {
		return nil, fmt.Errorf("service info not configured")
	}

	services, err := c.ListServices()
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}

	var downstreams []*flowctlpb.ServiceStatus
	for _, service := range services.Services {
		// Skip self
		if service.ServiceId == c.serviceID {
			continue
		}

		// For stellar ledger events, look for PROCESSOR services
		if outputEventType == "stellar_ledger_service.StellarLedger" {
			if service.ServiceType == flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR {
				downstreams = append(downstreams, service)
			}
		}
		// For TTP events, look for SINK services
		if outputEventType == "ttp_event_service.TTPEvent" {
			if service.ServiceType == flowctlpb.ServiceType_SERVICE_TYPE_SINK {
				downstreams = append(downstreams, service)
			}
		}
	}

	return downstreams, nil
}

// GetPipelineTopology returns the complete pipeline topology
func (c *Controller) GetPipelineTopology() (*PipelineTopology, error) {
	services, err := c.ListServices()
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}

	topology := &PipelineTopology{
		Services:    make(map[string]*flowctlpb.ServiceStatus),
		Connections: make(map[string][]string),
	}

	// Index services by ID
	for _, service := range services.Services {
		topology.Services[service.ServiceId] = service
	}

	// Build simplified connection graph based on service types
	// Since ServiceStatus doesn't have event type info, we use type-based connections
	for _, service := range services.Services {
		var connections []string
		
		// Connect services based on typical pipeline flow
		switch service.ServiceType {
		case flowctlpb.ServiceType_SERVICE_TYPE_SOURCE:
			// Sources connect to processors
			for _, downstream := range services.Services {
				if downstream.ServiceType == flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR {
					connections = append(connections, downstream.ServiceId)
				}
			}
		case flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR:
			// Processors connect to sinks
			for _, downstream := range services.Services {
				if downstream.ServiceType == flowctlpb.ServiceType_SERVICE_TYPE_SINK {
					connections = append(connections, downstream.ServiceId)
				}
			}
		}
		
		topology.Connections[service.ServiceId] = connections
	}

	return topology, nil
}

// Helper function to find services by type (since ServiceStatus doesn't have event types)
func (c *Controller) findServicesByType(services []*flowctlpb.ServiceStatus, serviceType flowctlpb.ServiceType) []*flowctlpb.ServiceStatus {
	var matches []*flowctlpb.ServiceStatus
	for _, service := range services {
		if service.ServiceType == serviceType {
			matches = append(matches, service)
		}
	}
	return matches
}

// ExtractConnectionInfo extracts Arrow Flight connection details from service status
// Note: ServiceStatus doesn't have metadata, so we use defaults based on service type
func (c *Controller) ExtractConnectionInfo(service *flowctlpb.ServiceStatus) *ConnectionInfo {
	// Construct endpoint from service type defaults (since ServiceStatus has no metadata)
	endpoint := ""
	switch service.ServiceType {
	case flowctlpb.ServiceType_SERVICE_TYPE_SOURCE:
		endpoint = "localhost:8815" // stellar-arrow-source default
	case flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR:
		endpoint = "localhost:8816" // ttp-arrow-processor default
	case flowctlpb.ServiceType_SERVICE_TYPE_SINK:
		endpoint = "localhost:8817" // arrow-analytics-sink default
	}

	// Construct health endpoint (since ServiceStatus doesn't have this either)
	healthEndpoint := "http://localhost:8088/health" // Standard health port

	return &ConnectionInfo{
		ServiceID:      service.ServiceId,
		ServiceType:    service.ServiceType,
		Endpoint:       endpoint,
		HealthEndpoint: healthEndpoint,
		Metadata:       make(map[string]string), // Empty since ServiceStatus has no metadata
		LastSeen:       time.Now(),
	}
}

// GetPipelineHealth aggregates health status across all services
func (c *Controller) GetPipelineHealth() (*PipelineHealth, error) {
	services, err := c.ListServices()
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}

	health := &PipelineHealth{
		ServiceHealth: make(map[string]*ServiceHealth),
		PipelineStart: time.Now(), // This should be tracked properly
		LastUpdate:    time.Now(),
	}

	healthyCount := 0
	totalCount := len(services.Services)

	for _, service := range services.Services {
		serviceHealth := &ServiceHealth{
			ServiceID: service.ServiceId,
			Status:    "unknown",
			Metrics:   make(map[string]float64),
		}

		// Use built-in health status from ServiceStatus
		if service.IsHealthy {
			serviceHealth.Status = "healthy"
			healthyCount++
		} else {
			serviceHealth.Status = "unhealthy"
		}
		// Copy metrics if available
		if service.Metrics != nil {
			serviceHealth.Metrics = service.Metrics
		}

		health.ServiceHealth[service.ServiceId] = serviceHealth
	}

	// Determine overall pipeline health
	if totalCount == 0 {
		health.OverallStatus = "unknown"
	} else if healthyCount == totalCount {
		health.OverallStatus = "healthy"
	} else if healthyCount > 0 {
		health.OverallStatus = "degraded"
	} else {
		health.OverallStatus = "unhealthy"
	}

	return health, nil
}

// checkServiceHealth performs a health check on a service endpoint
func (c *Controller) checkServiceHealth(healthEndpoint string) (status string, responseTime time.Duration) {
	start := time.Now()
	defer func() {
		responseTime = time.Since(start)
	}()

	// Simple HTTP GET to health endpoint
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(healthEndpoint)
	if err != nil {
		return "unhealthy", responseTime
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		return "healthy", responseTime
	}
	return "unhealthy", responseTime
}

// WatchServices sets up a watch for service changes (polling-based)
func (c *Controller) WatchServices(ctx context.Context, callback func([]*flowctlpb.ServiceStatus)) error {
	ticker := time.NewTicker(30 * time.Second) // Poll every 30 seconds
	defer ticker.Stop()

	var lastServices []*flowctlpb.ServiceStatus

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			services, err := c.ListServices()
			if err != nil {
				fmt.Printf("Failed to list services during watch: %v\n", err)
				continue
			}

			// Check if services have changed
			if c.servicesChanged(lastServices, services.Services) {
				lastServices = services.Services
				callback(services.Services)
			}
		}
	}
}

// Helper function to check if service list has changed
func (c *Controller) servicesChanged(old, new []*flowctlpb.ServiceStatus) bool {
	if len(old) != len(new) {
		return true
	}

	// Create maps for comparison
	oldMap := make(map[string]*flowctlpb.ServiceStatus)
	for _, svc := range old {
		oldMap[svc.ServiceId] = svc
	}

	for _, svc := range new {
		if oldSvc, exists := oldMap[svc.ServiceId]; !exists {
			return true // New service
		} else {
			// Check if service details changed (ServiceStatus fields only)
			if oldSvc.ServiceType != svc.ServiceType ||
				oldSvc.IsHealthy != svc.IsHealthy {
				return true
			}
		}
	}

	return false
}

// =========================================================================
// Advanced Pipeline Flow Control and Backpressure Management
// =========================================================================

// GetPipelineFlowControl returns current flow control status
func (c *Controller) GetPipelineFlowControl() *PipelineFlowControl {
	c.flowControlMu.RLock()
	defer c.flowControlMu.RUnlock()
	
	// Return a copy to prevent external modification
	return &PipelineFlowControl{
		MaxInflightGlobal:     c.flowControl.MaxInflightGlobal,
		ServiceInflight:       copyInt32Map(c.flowControl.ServiceInflight),
		ServiceMaxInflight:    copyInt32Map(c.flowControl.ServiceMaxInflight),
		BackpressureActive:    c.flowControl.BackpressureActive,
		ThrottleRate:          c.flowControl.ThrottleRate,
		LastFlowControlUpdate: c.flowControl.LastFlowControlUpdate,
	}
}

// UpdateServiceInflight updates the current inflight count for a service
func (c *Controller) UpdateServiceInflight(serviceID string, currentInflight int32) {
	c.flowControlMu.Lock()
	defer c.flowControlMu.Unlock()
	
	c.flowControl.ServiceInflight[serviceID] = currentInflight
	c.flowControl.LastFlowControlUpdate = time.Now()
	
	// Check if we need to activate backpressure
	c.evaluateBackpressure()
}

// SetServiceMaxInflight sets the maximum inflight limit for a service
func (c *Controller) SetServiceMaxInflight(serviceID string, maxInflight int32) {
	c.flowControlMu.Lock()
	defer c.flowControlMu.Unlock()
	
	c.flowControl.ServiceMaxInflight[serviceID] = maxInflight
	c.flowControl.LastFlowControlUpdate = time.Now()
}

// SendBackpressureSignal sends a backpressure signal to the pipeline
func (c *Controller) SendBackpressureSignal(signal BackpressureSignal) {
	select {
	case c.backpressureChan <- signal:
		log.Debug().
			Str("service_id", signal.ServiceID).
			Float64("current_load", signal.CurrentLoad).
			Float64("recommended_rate", signal.RecommendedRate).
			Str("reason", signal.Reason).
			Msg("Sent backpressure signal")
	default:
		log.Warn().
			Str("service_id", signal.ServiceID).
			Msg("Backpressure channel full, dropping signal")
	}
}

// GetBackpressureSignals returns a channel for receiving backpressure signals
func (c *Controller) GetBackpressureSignals() <-chan BackpressureSignal {
	return c.backpressureChan
}

// CalculatePipelineMetrics aggregates metrics across all services
func (c *Controller) CalculatePipelineMetrics() (*PipelineMetrics, error) {
	services, err := c.ListServices()
	if err != nil {
		return nil, fmt.Errorf("failed to list services for metrics: %w", err)
	}

	metrics := &PipelineMetrics{
		ServiceThroughput: make(map[string]float64),
		ServiceLatency:    make(map[string]time.Duration),
		QueueDepths:       make(map[string]int),
		ErrorRates:        make(map[string]float64),
		LastUpdated:       time.Now(),
	}

	var totalThroughput float64
	var maxLatency time.Duration

	for _, service := range services.Services {
		if service.Metrics != nil {
			// Extract throughput metrics
			if throughput, exists := service.Metrics["events_per_second"]; exists {
				metrics.ServiceThroughput[service.ServiceId] = throughput
				totalThroughput += throughput
			}

			// Extract latency metrics
			if latencyMs, exists := service.Metrics["processing_latency_ms"]; exists {
				latency := time.Duration(latencyMs) * time.Millisecond
				metrics.ServiceLatency[service.ServiceId] = latency
				if latency > maxLatency {
					maxLatency = latency
				}
			}

			// Extract queue depth metrics
			if queueDepth, exists := service.Metrics["queue_depth"]; exists {
				metrics.QueueDepths[service.ServiceId] = int(queueDepth)
			}

			// Calculate error rate
			if errorsTotal, exists := service.Metrics["errors_total"]; exists {
				if eventsTotal, exists := service.Metrics["events_processed_total"]; exists && eventsTotal > 0 {
					errorRate := errorsTotal / eventsTotal
					metrics.ErrorRates[service.ServiceId] = errorRate
				}
			}
		}
	}

	metrics.TotalThroughput = totalThroughput
	metrics.EndToEndLatency = maxLatency // Simplified - could be more sophisticated

	return metrics, nil
}

// evaluateBackpressure checks if backpressure should be activated (internal method)
func (c *Controller) evaluateBackpressure() {
	var totalInflight int32
	var totalMaxInflight int32

	// Calculate current load across all services
	for serviceID, current := range c.flowControl.ServiceInflight {
		totalInflight += current
		if max, exists := c.flowControl.ServiceMaxInflight[serviceID]; exists {
			totalMaxInflight += max
		}
	}

	// Calculate global load percentage
	var globalLoad float64
	if totalMaxInflight > 0 {
		globalLoad = float64(totalInflight) / float64(totalMaxInflight)
	}

	// Activate backpressure if load exceeds threshold
	const backpressureThreshold = 0.8 // 80% capacity
	const recoveryThreshold = 0.6     // 60% capacity

	if globalLoad > backpressureThreshold && !c.flowControl.BackpressureActive {
		c.flowControl.BackpressureActive = true
		c.flowControl.ThrottleRate = 0.5 // Reduce to 50% rate
		
		log.Warn().
			Float64("global_load", globalLoad).
			Float64("new_throttle_rate", c.flowControl.ThrottleRate).
			Msg("Activating pipeline backpressure")

		// Send backpressure signal
		signal := BackpressureSignal{
			ServiceID:       c.serviceID,
			CurrentLoad:     globalLoad,
			RecommendedRate: c.flowControl.ThrottleRate,
			Timestamp:       time.Now(),
			Reason:          "Global pipeline capacity exceeded",
		}
		go c.SendBackpressureSignal(signal)

	} else if globalLoad < recoveryThreshold && c.flowControl.BackpressureActive {
		c.flowControl.BackpressureActive = false
		c.flowControl.ThrottleRate = 1.0 // Return to full rate
		
		log.Info().
			Float64("global_load", globalLoad).
			Float64("new_throttle_rate", c.flowControl.ThrottleRate).
			Msg("Deactivating pipeline backpressure")

		// Send recovery signal
		signal := BackpressureSignal{
			ServiceID:       c.serviceID,
			CurrentLoad:     globalLoad,
			RecommendedRate: c.flowControl.ThrottleRate,
			Timestamp:       time.Now(),
			Reason:          "Pipeline capacity recovered",
		}
		go c.SendBackpressureSignal(signal)
	}
}

// GetCurrentThrottleRate returns the current recommended processing rate (0.0-1.0)
func (c *Controller) GetCurrentThrottleRate() float64 {
	c.flowControlMu.RLock()
	defer c.flowControlMu.RUnlock()
	return c.flowControl.ThrottleRate
}

// IsBackpressureActive returns whether backpressure is currently active
func (c *Controller) IsBackpressureActive() bool {
	c.flowControlMu.RLock()
	defer c.flowControlMu.RUnlock()
	return c.flowControl.BackpressureActive
}

// SetGlobalInflightLimit sets the global pipeline inflight limit
func (c *Controller) SetGlobalInflightLimit(limit int32) {
	c.flowControlMu.Lock()
	defer c.flowControlMu.Unlock()
	
	c.flowControl.MaxInflightGlobal = limit
	c.flowControl.LastFlowControlUpdate = time.Now()
	
	log.Info().
		Int32("new_limit", limit).
		Msg("Updated global pipeline inflight limit")
}

// GetServiceLoadMetrics returns load metrics for all services
func (c *Controller) GetServiceLoadMetrics() map[string]float64 {
	c.flowControlMu.RLock()
	defer c.flowControlMu.RUnlock()
	
	loadMetrics := make(map[string]float64)
	
	for serviceID, current := range c.flowControl.ServiceInflight {
		if max, exists := c.flowControl.ServiceMaxInflight[serviceID]; exists && max > 0 {
			load := float64(current) / float64(max)
			loadMetrics[serviceID] = load
		}
	}
	
	return loadMetrics
}

// Helper function to copy int32 maps
func copyInt32Map(src map[string]int32) map[string]int32 {
	dst := make(map[string]int32)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
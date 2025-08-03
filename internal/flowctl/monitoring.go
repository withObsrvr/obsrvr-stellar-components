package flowctl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

// ========================================================================
// Enhanced Monitoring and Metrics Integration for Pipeline Coordination
// ========================================================================

// PipelineMonitor provides enhanced monitoring capabilities for the pipeline
type PipelineMonitor struct {
	controller       *Controller
	metricsRegistry  *prometheus.Registry
	
	// Prometheus metrics
	pipelineThroughput     prometheus.Gauge
	pipelineLatency        prometheus.Gauge
	serviceLoadMetrics     *prometheus.GaugeVec
	backpressureActive     prometheus.Gauge
	throttleRate           prometheus.Gauge
	serviceConnections     *prometheus.GaugeVec
	discoveryLatency       prometheus.Histogram
	reconnectionEvents     *prometheus.CounterVec
	
	// Monitoring state
	mu                     sync.RWMutex
	isRunning              bool
	monitoringInterval     time.Duration
	stopChan               chan struct{}
	wg                     sync.WaitGroup
}

// MetricsSnapshot represents a point-in-time view of pipeline metrics
type MetricsSnapshot struct {
	Timestamp              time.Time
	PipelineMetrics        *PipelineMetrics
	FlowControl            *PipelineFlowControl
	ServiceHealth          map[string]*ServiceHealth
	ActiveConnections      map[string]ConnectionStatus
	RecentBackpressureEvents []BackpressureSignal
}

// ConnectionStatus represents the status of a service connection
type ConnectionStatus struct {
	ServiceID       string
	Endpoint        string
	IsConnected     bool
	LastConnected   time.Time
	ReconnectCount  int
	LatencyMs       float64
}

// AlertRule defines conditions for triggering alerts
type AlertRule struct {
	Name           string
	Description    string
	MetricName     string
	Threshold      float64
	Comparison     string // "gt", "lt", "eq"
	Duration       time.Duration
	Severity       string // "info", "warning", "critical"
	Enabled        bool
}

// Alert represents a triggered alert condition
type Alert struct {
	Rule           *AlertRule
	Timestamp      time.Time
	CurrentValue   float64
	ServiceID      string
	Message        string
	Acknowledged   bool
}

// NewPipelineMonitor creates a new pipeline monitoring instance
func NewPipelineMonitor(controller *Controller) *PipelineMonitor {
	registry := prometheus.NewRegistry()
	
	monitor := &PipelineMonitor{
		controller:         controller,
		metricsRegistry:    registry,
		monitoringInterval: 5 * time.Second,
		stopChan:           make(chan struct{}),
	}
	
	monitor.initializeMetrics()
	return monitor
}

// initializeMetrics sets up Prometheus metrics
func (m *PipelineMonitor) initializeMetrics() {
	// Pipeline-level metrics
	m.pipelineThroughput = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flowctl_pipeline_throughput_total",
		Help: "Total pipeline throughput in events per second",
	})
	
	m.pipelineLatency = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flowctl_pipeline_latency_seconds",
		Help: "End-to-end pipeline latency in seconds",
	})
	
	// Service-level metrics
	m.serviceLoadMetrics = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "flowctl_service_load_ratio",
		Help: "Service load as ratio of current/max inflight",
	}, []string{"service_id", "service_type"})
	
	// Flow control metrics
	m.backpressureActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flowctl_backpressure_active",
		Help: "Whether backpressure is currently active (1=active, 0=inactive)",
	})
	
	m.throttleRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flowctl_throttle_rate",
		Help: "Current throttle rate (0.0-1.0)",
	})
	
	// Connection metrics
	m.serviceConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "flowctl_service_connections",
		Help: "Service connection status (1=connected, 0=disconnected)",
	}, []string{"service_id", "endpoint"})
	
	// Discovery metrics
	m.discoveryLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "flowctl_service_discovery_duration_seconds",
		Help: "Time taken for service discovery operations",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s
	})
	
	// Reconnection metrics
	m.reconnectionEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "flowctl_reconnection_events_total",
		Help: "Total number of service reconnection events",
	}, []string{"service_id", "reason"})
	
	// Register all metrics
	m.metricsRegistry.MustRegister(
		m.pipelineThroughput,
		m.pipelineLatency,
		m.serviceLoadMetrics,
		m.backpressureActive,
		m.throttleRate,
		m.serviceConnections,
		m.discoveryLatency,
		m.reconnectionEvents,
	)
}

// Start begins the monitoring process
func (m *PipelineMonitor) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.isRunning {
		return fmt.Errorf("pipeline monitor is already running")
	}
	
	m.isRunning = true
	
	// Start monitoring goroutine
	m.wg.Add(1)
	go m.monitoringLoop(ctx)
	
	// Start backpressure signal monitoring
	m.wg.Add(1)
	go m.backpressureMonitoringLoop(ctx)
	
	log.Info().
		Dur("interval", m.monitoringInterval).
		Msg("Started pipeline monitoring")
	
	return nil
}

// Stop gracefully stops the monitoring process
func (m *PipelineMonitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.isRunning {
		return
	}
	
	m.isRunning = false
	close(m.stopChan)
	m.wg.Wait()
	
	log.Info().Msg("Stopped pipeline monitoring")
}

// monitoringLoop is the main monitoring loop
func (m *PipelineMonitor) monitoringLoop(ctx context.Context) {
	defer m.wg.Done()
	
	ticker := time.NewTicker(m.monitoringInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopChan:
			return
		case <-ticker.C:
			m.collectAndUpdateMetrics()
		}
	}
}

// backpressureMonitoringLoop monitors backpressure signals
func (m *PipelineMonitor) backpressureMonitoringLoop(ctx context.Context) {
	defer m.wg.Done()
	
	backpressureSignals := m.controller.GetBackpressureSignals()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopChan:
			return
		case signal := <-backpressureSignals:
			m.handleBackpressureSignal(signal)
		}
	}
}

// collectAndUpdateMetrics collects metrics from the controller and updates Prometheus metrics
func (m *PipelineMonitor) collectAndUpdateMetrics() {
	start := time.Now()
	defer func() {
		m.discoveryLatency.Observe(time.Since(start).Seconds())
	}()
	
	// Update pipeline metrics
	if pipelineMetrics, err := m.controller.CalculatePipelineMetrics(); err == nil {
		m.pipelineThroughput.Set(pipelineMetrics.TotalThroughput)
		m.pipelineLatency.Set(pipelineMetrics.EndToEndLatency.Seconds())
	} else {
		log.Warn().Err(err).Msg("Failed to collect pipeline metrics")
	}
	
	// Update flow control metrics
	flowControl := m.controller.GetPipelineFlowControl()
	if flowControl.BackpressureActive {
		m.backpressureActive.Set(1)
	} else {
		m.backpressureActive.Set(0)
	}
	m.throttleRate.Set(flowControl.ThrottleRate)
	
	// Update service load metrics
	loadMetrics := m.controller.GetServiceLoadMetrics()
	for serviceID, load := range loadMetrics {
		serviceType := m.getServiceType(serviceID)
		m.serviceLoadMetrics.WithLabelValues(serviceID, serviceType).Set(load)
	}
	
	// Update service connection status
	m.updateConnectionMetrics()
}

// handleBackpressureSignal processes backpressure signals and updates metrics
func (m *PipelineMonitor) handleBackpressureSignal(signal BackpressureSignal) {
	log.Info().
		Str("service_id", signal.ServiceID).
		Float64("current_load", signal.CurrentLoad).
		Float64("recommended_rate", signal.RecommendedRate).
		Str("reason", signal.Reason).
		Msg("Received backpressure signal")
		
	// You could trigger alerts or additional actions here
	m.checkAlertConditions(signal)
}

// updateConnectionMetrics updates service connection status metrics
func (m *PipelineMonitor) updateConnectionMetrics() {
	// This would be enhanced to track actual connection status
	// For now, we use service discovery to determine if services are available
	services, err := m.controller.ListServices()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to list services for connection metrics")
		return
	}
	
	for _, service := range services.Services {
		connectionInfo := m.controller.ExtractConnectionInfo(service)
		
		// Set connection status based on service health
		var connected float64
		if service.IsHealthy {
			connected = 1
		} else {
			connected = 0
		}
		
		m.serviceConnections.WithLabelValues(
			service.ServiceId,
			connectionInfo.Endpoint,
		).Set(connected)
	}
}

// getServiceType returns a string representation of the service type
func (m *PipelineMonitor) getServiceType(serviceID string) string {
	// This could be enhanced to query actual service info
	// For now, we use naming conventions or defaults
	if services, err := m.controller.ListServices(); err == nil {
		for _, service := range services.Services {
			if service.ServiceId == serviceID {
				return service.ServiceType.String()
			}
		}
	}
	return "unknown"
}

// checkAlertConditions evaluates alert conditions based on metrics
func (m *PipelineMonitor) checkAlertConditions(signal BackpressureSignal) {
	// Example alert conditions
	if signal.CurrentLoad > 0.9 {
		log.Warn().
			Str("service_id", signal.ServiceID).
			Float64("load", signal.CurrentLoad).
			Msg("ALERT: Service load critically high")
	}
	
	if signal.RecommendedRate < 0.5 {
		log.Warn().
			Str("service_id", signal.ServiceID).
			Float64("throttle_rate", signal.RecommendedRate).
			Msg("ALERT: Severe throttling active")
	}
}

// GetMetricsSnapshot returns a comprehensive snapshot of current metrics
func (m *PipelineMonitor) GetMetricsSnapshot() (*MetricsSnapshot, error) {
	snapshot := &MetricsSnapshot{
		Timestamp: time.Now(),
	}
	
	// Collect pipeline metrics
	if pipelineMetrics, err := m.controller.CalculatePipelineMetrics(); err == nil {
		snapshot.PipelineMetrics = pipelineMetrics
	} else {
		return nil, fmt.Errorf("failed to collect pipeline metrics: %w", err)
	}
	
	// Collect flow control status
	snapshot.FlowControl = m.controller.GetPipelineFlowControl()
	
	// Collect service health
	if pipelineHealth, err := m.controller.GetPipelineHealth(); err == nil {
		snapshot.ServiceHealth = pipelineHealth.ServiceHealth
	} else {
		return nil, fmt.Errorf("failed to collect service health: %w", err)
	}
	
	// Collect connection status
	snapshot.ActiveConnections = m.getConnectionStatus()
	
	return snapshot, nil
}

// getConnectionStatus returns current connection status for all services
func (m *PipelineMonitor) getConnectionStatus() map[string]ConnectionStatus {
	connections := make(map[string]ConnectionStatus)
	
	services, err := m.controller.ListServices()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get connection status")
		return connections
	}
	
	for _, service := range services.Services {
		connectionInfo := m.controller.ExtractConnectionInfo(service)
		
		connections[service.ServiceId] = ConnectionStatus{
			ServiceID:     service.ServiceId,
			Endpoint:      connectionInfo.Endpoint,
			IsConnected:   service.IsHealthy,
			LastConnected: time.Now(), // This would be tracked more precisely
			ReconnectCount: 0,         // This would be tracked per service
			LatencyMs:     0,          // This would be measured
		}
	}
	
	return connections
}

// GetPrometheusRegistry returns the Prometheus registry for integration with HTTP handlers
func (m *PipelineMonitor) GetPrometheusRegistry() *prometheus.Registry {
	return m.metricsRegistry
}

// SetMonitoringInterval configures the monitoring collection interval
func (m *PipelineMonitor) SetMonitoringInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.monitoringInterval = interval
}

// RecordReconnectionEvent records a service reconnection event
func (m *PipelineMonitor) RecordReconnectionEvent(serviceID, reason string) {
	m.reconnectionEvents.WithLabelValues(serviceID, reason).Inc()
	
	log.Info().
		Str("service_id", serviceID).
		Str("reason", reason).
		Msg("Recorded service reconnection event")
}

// GetServiceLoadSummary returns a summary of service load across the pipeline
func (m *PipelineMonitor) GetServiceLoadSummary() map[string]interface{} {
	loadMetrics := m.controller.GetServiceLoadMetrics()
	flowControl := m.controller.GetPipelineFlowControl()
	
	summary := map[string]interface{}{
		"backpressure_active": flowControl.BackpressureActive,
		"throttle_rate":       flowControl.ThrottleRate,
		"service_loads":       loadMetrics,
		"last_updated":        flowControl.LastFlowControlUpdate,
	}
	
	return summary
}

// GenerateHealthReport generates a comprehensive health report
func (m *PipelineMonitor) GenerateHealthReport() (map[string]interface{}, error) {
	snapshot, err := m.GetMetricsSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to generate health report: %w", err)
	}
	
	report := map[string]interface{}{
		"timestamp":         snapshot.Timestamp,
		"pipeline_health":   m.assessPipelineHealth(snapshot),
		"performance":       m.assessPerformance(snapshot),
		"flow_control":      snapshot.FlowControl,
		"service_summary":   m.generateServiceSummary(snapshot),
		"recommendations":   m.generateRecommendations(snapshot),
	}
	
	return report, nil
}

// assessPipelineHealth provides an overall health assessment
func (m *PipelineMonitor) assessPipelineHealth(snapshot *MetricsSnapshot) map[string]interface{} {
	healthyServices := 0
	totalServices := len(snapshot.ServiceHealth)
	
	for _, health := range snapshot.ServiceHealth {
		if health.Status == "healthy" {
			healthyServices++
		}
	}
	
	var overallStatus string
	if healthyServices == totalServices {
		overallStatus = "healthy"
	} else if healthyServices > 0 {
		overallStatus = "degraded"
	} else {
		overallStatus = "unhealthy"
	}
	
	return map[string]interface{}{
		"overall_status":   overallStatus,
		"healthy_services": healthyServices,
		"total_services":   totalServices,
		"health_ratio":     float64(healthyServices) / float64(totalServices),
	}
}

// assessPerformance analyzes pipeline performance metrics
func (m *PipelineMonitor) assessPerformance(snapshot *MetricsSnapshot) map[string]interface{} {
	metrics := snapshot.PipelineMetrics
	
	return map[string]interface{}{
		"total_throughput":    metrics.TotalThroughput,
		"end_to_end_latency":  metrics.EndToEndLatency.Seconds(),
		"service_throughput":  metrics.ServiceThroughput,
		"service_latency":     metrics.ServiceLatency,
		"queue_depths":        metrics.QueueDepths,
		"error_rates":         metrics.ErrorRates,
	}
}

// generateServiceSummary creates a summary of all service statuses
func (m *PipelineMonitor) generateServiceSummary(snapshot *MetricsSnapshot) []map[string]interface{} {
	var services []map[string]interface{}
	
	for serviceID, health := range snapshot.ServiceHealth {
		serviceInfo := map[string]interface{}{
			"service_id":     serviceID,
			"status":         health.Status,
			"last_heartbeat": health.LastHeartbeat,
			"error_count":    health.ErrorCount,
			"response_time":  health.ResponseTime.Seconds(),
		}
		
		// Add connection info if available
		if conn, exists := snapshot.ActiveConnections[serviceID]; exists {
			serviceInfo["endpoint"] = conn.Endpoint
			serviceInfo["is_connected"] = conn.IsConnected
			serviceInfo["reconnect_count"] = conn.ReconnectCount
		}
		
		services = append(services, serviceInfo)
	}
	
	return services
}

// generateRecommendations provides actionable recommendations based on current metrics
func (m *PipelineMonitor) generateRecommendations(snapshot *MetricsSnapshot) []string {
	var recommendations []string
	
	// Check for backpressure
	if snapshot.FlowControl.BackpressureActive {
		recommendations = append(recommendations, 
			"Pipeline backpressure is active. Consider scaling up downstream services.")
	}
	
	// Check service loads
	loadMetrics := m.controller.GetServiceLoadMetrics()
	for serviceID, load := range loadMetrics {
		if load > 0.8 {
			recommendations = append(recommendations, 
				fmt.Sprintf("Service %s is at %.1f%% capacity. Consider scaling or optimization.", 
					serviceID, load*100))
		}
	}
	
	// Check error rates
	if snapshot.PipelineMetrics != nil {
		for serviceID, errorRate := range snapshot.PipelineMetrics.ErrorRates {
			if errorRate > 0.05 { // 5% error rate threshold
				recommendations = append(recommendations, 
					fmt.Sprintf("Service %s has elevated error rate (%.2f%%). Investigate logs.", 
						serviceID, errorRate*100))
			}
		}
	}
	
	// Check latency
	if snapshot.PipelineMetrics != nil && snapshot.PipelineMetrics.EndToEndLatency > 5*time.Second {
		recommendations = append(recommendations, 
			"End-to-end latency is high. Check for bottlenecks in the pipeline.")
	}
	
	if len(recommendations) == 0 {
		recommendations = append(recommendations, "Pipeline is operating within normal parameters.")
	}
	
	return recommendations
}
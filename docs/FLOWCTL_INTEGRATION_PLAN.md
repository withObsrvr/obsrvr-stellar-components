# FlowCtl Integration Implementation Plan
## obsrvr-stellar-components

**Document Version**: 1.0  
**Date**: August 3, 2025  
**Status**: Planning Phase  

## Executive Summary

This document outlines the implementation plan to integrate all obsrvr-stellar-components with flowctl control plane, enabling centralized service discovery, health monitoring, and metrics collection. The integration follows the proven patterns from ttp-processor-demo and maintains backward compatibility.

## Background

Currently, obsrvr-stellar-components operate as standalone Apache Arrow Flight services. To enable integration with flowctl's control plane (similar to ttp-processor-demo), each component needs to implement:

- **Registration** with flowctl control plane
- **Periodic heartbeat** with health and metrics
- **Service metadata** publication
- **Graceful degradation** when flowctl is unavailable

## Architecture Overview

### Current State (Standalone)
```
stellar-arrow-source (:8816) ← Arrow Flight Client
ttp-arrow-processor (:8817) ← Arrow Flight Client  
arrow-analytics-sink (:8818) ← Arrow Flight Client
```

### Target State (FlowCtl Integrated)
```
                    flowctl Control Plane (:8080)
                            ↑
                    ┌───────┼───────┐
                    │       │       │
            Register│   Heartbeat   │ Metrics
                    │       │       │
        ┌───────────▼───────▼───────▼───────────┐
        │                                       │
stellar-arrow-source  ttp-arrow-processor  arrow-analytics-sink
    (:8816)              (:8817)              (:8818)
        │                   │                   │
    Arrow Flight        Arrow Flight        Arrow Flight
     + flowctl           + flowctl           + flowctl
```

## Components Analysis

### 1. stellar-arrow-source
- **Service Type**: `SOURCE`
- **Output Event Types**: `["stellar_ledger_service.StellarLedger"]`
- **Current Interfaces**: Arrow Flight (:8816), Health (:8088)
- **Data Sources**: Stellar RPC, Data Lake (GCS/S3), Archive
- **Integration Complexity**: **Medium** (metrics collection needed)

### 2. ttp-arrow-processor  
- **Service Type**: `PROCESSOR`
- **Input Event Types**: `["stellar_ledger_service.StellarLedger"]`
- **Output Event Types**: `["ttp_event_service.TTPEvent"]`
- **Current Interfaces**: Arrow Flight (:8817), Health (:8088)
- **Processing**: TTP event extraction from Stellar ledgers
- **Integration Complexity**: **Medium** (dual input/output metrics)

### 3. arrow-analytics-sink
- **Service Type**: `SINK` 
- **Input Event Types**: `["ttp_event_service.TTPEvent"]`
- **Output Event Types**: `[]` (terminal sink)
- **Current Interfaces**: Arrow Flight (:8818), WebSocket (:8080), REST (:8081), Health (:8088)
- **Output Formats**: Parquet, JSON, CSV, WebSocket, API
- **Integration Complexity**: **High** (multiple output interfaces)

## Implementation Plan

### Phase 1: Shared Infrastructure (1-2 days)

#### 1.1 Add FlowCtl Dependencies
**File**: All `go.mod` files
```go
require (
    github.com/withobsrvr/flowctl/proto v1.0.0
    google.golang.org/grpc v1.50.0
    google.golang.org/protobuf v1.28.0
)
```

#### 1.2 Create Shared FlowCtl Controller
**File**: `internal/flowctl/controller.go` (new shared package)
```go
package flowctl

type Controller struct {
    conn              *grpc.ClientConn
    client            flowctlpb.ControlPlaneClient  
    serviceID         string
    serviceType       flowctlpb.ServiceType
    heartbeatInterval time.Duration
    stopHeartbeat     chan struct{}
    endpoint          string
    healthEndpoint    string
    metricsProvider   MetricsProvider
}

type MetricsProvider interface {
    GetMetrics() map[string]float64
}

func NewController(serviceType flowctlpb.ServiceType, endpoint string, metricsProvider MetricsProvider) *Controller
func (c *Controller) RegisterWithFlowctl() error
func (c *Controller) Start() error
func (c *Controller) Stop()
```

#### 1.3 Define Event Type Constants
**File**: `internal/flowctl/events.go`
```go
package flowctl

const (
    // Input event types
    StellarLedgerEventType = "stellar_ledger_service.StellarLedger"
    TTPEventType          = "ttp_event_service.TTPEvent"
    
    // Service metadata keys
    NetworkMetadataKey     = "network"
    StorageTypeMetadataKey = "storage_type"
    OutputFormatsKey       = "output_formats"
)
```

#### 1.4 Environment Variable Configuration
**File**: `internal/config/flowctl.go`
```go
package config

type FlowCtlConfig struct {
    Enabled           bool          `mapstructure:"flowctl_enabled"`
    Endpoint          string        `mapstructure:"flowctl_endpoint"`
    HeartbeatInterval time.Duration `mapstructure:"flowctl_heartbeat_interval"`
}

func LoadFlowCtlConfig() *FlowCtlConfig {
    return &FlowCtlConfig{
        Enabled:           getEnvBool("ENABLE_FLOWCTL", false),
        Endpoint:          getEnv("FLOWCTL_ENDPOINT", "localhost:8080"),
        HeartbeatInterval: getEnvDuration("FLOWCTL_HEARTBEAT_INTERVAL", 10*time.Second),
    }
}
```

### Phase 2: stellar-arrow-source Integration (1-2 days)

#### 2.1 Metrics Collection Interface
**File**: `components/stellar-arrow-source/src/metrics.go`
```go
type StellarSourceMetrics struct {
    LedgersProcessed    uint64
    TotalBytes          uint64
    LastSequence        uint32
    ProcessingLatency   time.Duration
    SuccessCount        uint64
    ErrorCount          uint64
    mu                  sync.RWMutex
}

func (m *StellarSourceMetrics) GetMetrics() map[string]float64 {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    return map[string]float64{
        "ledgers_processed":     float64(m.LedgersProcessed),
        "total_bytes":           float64(m.TotalBytes),
        "last_sequence":         float64(m.LastSequence),
        "processing_latency_ms": float64(m.ProcessingLatency / time.Millisecond),
        "success_count":         float64(m.SuccessCount),
        "error_count":           float64(m.ErrorCount),
    }
}
```

#### 2.2 FlowCtl Integration
**File**: `components/stellar-arrow-source/src/main.go` (modifications)
```go
import "github.com/withobsrvr/obsrvr-stellar-components/internal/flowctl"

func main() {
    // ... existing config loading ...
    
    // Initialize metrics
    metrics := &StellarSourceMetrics{}
    
    // Initialize flowctl (optional)
    var flowctlController *flowctl.Controller
    if config.FlowCtl.Enabled {
        flowctlController = flowctl.NewController(
            flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
            fmt.Sprintf("localhost:%d", config.FlightPort),
            metrics,
        )
        flowctlController.SetServiceInfo(&flowctlpb.ServiceInfo{
            OutputEventTypes: []string{flowctl.StellarLedgerEventType},
            HealthEndpoint:   fmt.Sprintf("http://localhost:%d/health", config.HealthPort),
            MaxInflight:      uint32(config.BufferSize),
            Metadata: map[string]string{
                flowctl.NetworkMetadataKey:     config.NetworkPassphrase,
                flowctl.StorageTypeMetadataKey: config.SourceType,
                "backend_type":                 config.BackendType,
            },
        })
        
        if err := flowctlController.Start(); err != nil {
            log.Warn().Err(err).Msg("Failed to connect to flowctl, continuing without integration")
        }
        defer flowctlController.Stop()
    }
    
    // ... existing service initialization ...
    // Pass metrics to service for updates
    service.SetMetrics(metrics)
}
```

#### 2.3 Service Metrics Updates
**File**: `components/stellar-arrow-source/src/service.go` (modifications)
```go
type StellarSourceService struct {
    // ... existing fields ...
    metrics *StellarSourceMetrics
}

func (s *StellarSourceService) SetMetrics(metrics *StellarSourceMetrics) {
    s.metrics = metrics
}

// Update metrics in processing methods
func (s *StellarSourceService) processLedger(ledger *LedgerData) error {
    start := time.Now()
    
    // ... existing processing ...
    
    // Update metrics
    if s.metrics != nil {
        s.metrics.mu.Lock()
        s.metrics.LedgersProcessed++
        s.metrics.TotalBytes += uint64(len(ledger.Data))
        s.metrics.LastSequence = ledger.Sequence
        s.metrics.ProcessingLatency = time.Since(start)
        s.metrics.SuccessCount++
        s.metrics.mu.Unlock()
    }
    
    return nil
}
```

### Phase 3: ttp-arrow-processor Integration (1-2 days)

#### 3.1 Processor Metrics Interface
**File**: `components/ttp-arrow-processor/src/metrics.go`
```go
type TTPProcessorMetrics struct {
    LedgersConsumed     uint64
    EventsExtracted     uint64
    EventsFiltered      uint64
    BatchesGenerated    uint64
    ProcessingLatency   time.Duration
    LastLedgerSequence  uint32
    SuccessCount        uint64
    ErrorCount          uint64
    mu                  sync.RWMutex
}
```

#### 3.2 FlowCtl Integration with Dual Input/Output
**File**: `components/ttp-arrow-processor/src/main.go` (modifications)
```go
flowctlController.SetServiceInfo(&flowctlpb.ServiceInfo{
    InputEventTypes:  []string{flowctl.StellarLedgerEventType},
    OutputEventTypes: []string{flowctl.TTPEventType},
    HealthEndpoint:   fmt.Sprintf("http://localhost:%d/health", config.HealthPort),
    MaxInflight:      uint32(config.BufferSize),
    Metadata: map[string]string{
        flowctl.NetworkMetadataKey: config.NetworkPassphrase,
        "processor_type":           "ttp_event_extraction",
        "event_types":              strings.Join(config.EventTypes, ","),
    },
})
```

### Phase 4: arrow-analytics-sink Integration (2-3 days)

#### 4.1 Analytics Sink Metrics Interface
**File**: `components/arrow-analytics-sink/src/metrics.go`
```go
type AnalyticsSinkMetrics struct {
    EventsConsumed         uint64
    EventsWritten          uint64
    FilesWritten           uint64
    WebSocketConnections   int32
    APIRequests            uint64
    OutputLatency          time.Duration
    SuccessCount           uint64
    ErrorCount             uint64
    OutputFormatMetrics    map[string]uint64 // parquet, json, csv, etc.
    mu                     sync.RWMutex
}
```

#### 4.2 Complex Service Registration (Multiple Outputs)
**File**: `components/arrow-analytics-sink/src/main.go` (modifications)
```go
// Build output formats metadata
outputFormats := strings.Join(config.OutputFormats, ",")
endpoints := []string{
    fmt.Sprintf("http://localhost:%d/health", config.HealthPort),
}
if contains(config.OutputFormats, "websocket") {
    endpoints = append(endpoints, fmt.Sprintf("ws://localhost:%d%s", config.WebSocketPort, config.WebSocketPath))
}
if contains(config.OutputFormats, "api") {
    endpoints = append(endpoints, fmt.Sprintf("http://localhost:%d/api", config.APIPort))
}

flowctlController.SetServiceInfo(&flowctlpb.ServiceInfo{
    InputEventTypes: []string{flowctl.TTPEventType},
    OutputEventTypes: []string{}, // Terminal sink
    HealthEndpoint:  fmt.Sprintf("http://localhost:%d/health", config.HealthPort),
    MaxInflight:     uint32(config.BufferSize),
    Metadata: map[string]string{
        flowctl.OutputFormatsKey:   outputFormats,
        "storage_paths":            buildStoragePathsMetadata(config),
        "websocket_enabled":        strconv.FormatBool(contains(config.OutputFormats, "websocket")),
        "api_enabled":              strconv.FormatBool(contains(config.OutputFormats, "api")),
        "endpoints":                strings.Join(endpoints, ","),
    },
})
```

### Phase 5: Configuration & Environment Variables (1 day)

#### 5.1 Update Component Configuration Files
**Files**: All `component.yaml` files
```yaml
# Add flowctl environment variables
environment:
  - name: ENABLE_FLOWCTL
    description: "Enable flowctl integration"
    default: "false"
  - name: FLOWCTL_ENDPOINT  
    description: "FlowCtl control plane endpoint"
    default: "localhost:8080"
  - name: FLOWCTL_HEARTBEAT_INTERVAL
    description: "Heartbeat interval"
    default: "10s"
```

#### 5.2 Update Run Scripts
**Files**: All run scripts
```bash
# FlowCtl Configuration (optional)
export ENABLE_FLOWCTL=${ENABLE_FLOWCTL:-false}
export FLOWCTL_ENDPOINT=${FLOWCTL_ENDPOINT:-localhost:8080}
export FLOWCTL_HEARTBEAT_INTERVAL=${FLOWCTL_HEARTBEAT_INTERVAL:-10s}

echo "FlowCtl Integration: $ENABLE_FLOWCTL"
if [ "$ENABLE_FLOWCTL" = "true" ]; then
    echo "FlowCtl Endpoint: $FLOWCTL_ENDPOINT"
fi
```

### Phase 6: Testing & Validation (2-3 days)

#### 6.1 Unit Tests
**Files**: `*_test.go` files for each component
- Test flowctl controller initialization
- Test metrics collection and reporting
- Test graceful degradation when flowctl unavailable
- Test registration and heartbeat logic

#### 6.2 Integration Testing
1. **Standalone Mode**: Verify components work without flowctl
2. **FlowCtl Integration**: Test registration, heartbeat, metrics
3. **Pipeline Testing**: Full data flow with flowctl monitoring
4. **Fault Tolerance**: flowctl unavailable scenarios

#### 6.3 End-to-End Testing Scenario
```bash
# Terminal 1: Start flowctl
flowctl start

# Terminal 2: Start stellar-arrow-source with flowctl
cd components/stellar-arrow-source/src
export ENABLE_FLOWCTL=true
./run.sh

# Terminal 3: Start ttp-arrow-processor with flowctl  
cd components/ttp-arrow-processor/src
export ENABLE_FLOWCTL=true
./run.sh

# Terminal 4: Start arrow-analytics-sink with flowctl
cd components/arrow-analytics-sink/src  
export ENABLE_FLOWCTL=true
./run.sh

# Terminal 5: Verify registration
curl http://localhost:8080/api/services  # Should show 3 registered services
```

## Environment Variables Reference

### Core FlowCtl Variables
| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ENABLE_FLOWCTL` | Enable flowctl integration | `false` | No |
| `FLOWCTL_ENDPOINT` | FlowCtl control plane address | `localhost:8080` | No |
| `FLOWCTL_HEARTBEAT_INTERVAL` | Heartbeat frequency | `10s` | No |

### Component-Specific Variables (Unchanged)
All existing component environment variables remain the same. FlowCtl integration is purely additive.

## Migration Strategy

### Backward Compatibility
- **Default Behavior**: FlowCtl integration disabled by default (`ENABLE_FLOWCTL=false`)
- **Existing Interfaces**: All Arrow Flight, WebSocket, and REST APIs unchanged
- **Configuration**: Existing config files continue to work
- **Deployment**: Can deploy components individually or together

### Rollout Options

#### Option 1: Gradual Rollout
1. Deploy components with `ENABLE_FLOWCTL=false` (current behavior)
2. Start flowctl control plane
3. Enable flowctl integration component by component
4. Monitor registration and metrics

#### Option 2: Coordinated Deployment  
1. Deploy all components with flowctl integration enabled
2. Start flowctl control plane
3. Verify all services register successfully

### Risk Mitigation
- **Graceful Degradation**: Components continue operating if flowctl unavailable
- **Feature Flags**: Integration can be disabled via environment variable
- **Health Checks**: Existing health endpoints remain functional
- **Monitoring**: Existing metrics endpoints remain operational

## Success Criteria

### Functional Requirements
- [ ] All three components register with flowctl control plane
- [ ] Periodic heartbeats sent with accurate metrics
- [ ] Service metadata correctly published
- [ ] Graceful handling of flowctl unavailability
- [ ] Existing Arrow Flight interfaces remain functional

### Performance Requirements  
- [ ] <5ms overhead for flowctl registration
- [ ] <1ms overhead for heartbeat transmission
- [ ] No impact on existing Arrow Flight throughput
- [ ] Metrics collection <0.1% CPU overhead

### Operational Requirements
- [ ] Zero-downtime deployment possible
- [ ] Backward compatibility maintained
- [ ] Clear monitoring and alerting for flowctl integration
- [ ] Documentation updated for operators

## Dependencies

### External Dependencies
- `github.com/withobsrvr/flowctl/proto` - FlowCtl protobuf definitions
- `google.golang.org/grpc` - gRPC client library
- `google.golang.org/protobuf` - Protocol buffer utilities

### Infrastructure Dependencies
- FlowCtl control plane running on specified endpoint
- Network connectivity between components and flowctl
- Sufficient gRPC connection limits on flowctl

## Timeline

| Phase | Duration | Dependencies | Deliverables |
|-------|----------|--------------|--------------|
| Phase 1: Shared Infrastructure | 1-2 days | None | Shared flowctl package, event definitions |
| Phase 2: stellar-arrow-source | 1-2 days | Phase 1 | FlowCtl integration for source |
| Phase 3: ttp-arrow-processor | 1-2 days | Phase 1 | FlowCtl integration for processor |
| Phase 4: arrow-analytics-sink | 2-3 days | Phase 1 | FlowCtl integration for sink |
| Phase 5: Configuration | 1 day | Phases 2-4 | Updated configs and scripts |
| Phase 6: Testing & Validation | 2-3 days | Phases 1-5 | Test suite and documentation |

**Total Estimated Duration**: 8-13 days

## Appendix

### A. FlowCtl Protocol Reference
See ttp-processor-demo implementation for complete protocol examples:
- `/home/tillman/Documents/ttp-processor-demo/stellar-live-source-datalake/go/server/flowctl.go`
- `/home/tillman/Documents/ttp-processor-demo/ttp-processor/flowctl.js`

### B. Service Type Definitions
```protobuf
enum ServiceType {
  SERVICE_TYPE_UNSPECIFIED = 0;
  SERVICE_TYPE_SOURCE = 1;      // stellar-arrow-source
  SERVICE_TYPE_PROCESSOR = 2;   // ttp-arrow-processor  
  SERVICE_TYPE_SINK = 3;        // arrow-analytics-sink
}
```

### C. Example Registration Response
```json
{
  "serviceId": "stellar-source-20250803141529",
  "assignedTopics": ["stellar_ledgers"],
  "connectionInfo": {
    "endpoint": "localhost:8816",
    "protocol": "arrow-flight"
  }
}
```

---

**Document Owner**: obsrvr-stellar-components team  
**Review Cycle**: Monthly  
**Next Review**: September 3, 2025
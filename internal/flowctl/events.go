package flowctl

import (
	flowctlpb "github.com/withobsrvr/obsrvr-stellar-components/proto/gen"
)

// Event Type Constants
// These define the standardized event types used in the obsrvr-stellar-components pipeline
const (
	// Input event types
	StellarLedgerEventType = "stellar_ledger_service.StellarLedger"
	TTPEventType          = "ttp_event_service.TTPEvent"
	RawLedgerEventType    = "raw_ledger_service.RawLedger"
	
	// Additional event types for future expansion
	StellarTransactionEventType = "stellar_transaction_service.StellarTransaction"
	StellarOperationEventType   = "stellar_operation_service.StellarOperation"
	AnalyticsEventType          = "analytics_service.AnalyticsEvent"
)

// Service Metadata Keys
// These define the standardized metadata keys for service registration
const (
	NetworkMetadataKey         = "network"
	StorageTypeMetadataKey     = "storage_type"
	OutputFormatsKey          = "output_formats"
	BackendTypeKey            = "backend_type"
	ProcessorTypeKey          = "processor_type"
	EventTypesKey             = "event_types"
	StoragePathsKey           = "storage_paths"
	WebSocketEnabledKey       = "websocket_enabled"
	APIEnabledKey             = "api_enabled"
	EndpointsKey              = "endpoints"
	CompressionKey            = "compression"
	BatchSizeKey              = "batch_size"
	ConcurrentReadersKey      = "concurrent_readers"
)

// Service Type Helpers
// These functions help create properly configured ServiceInfo objects

// CreateSourceServiceInfo creates ServiceInfo for a source component
func CreateSourceServiceInfo(outputEventTypes []string, healthEndpoint string, maxInflight uint32, metadata map[string]string) *flowctlpb.ServiceInfo {
	return &flowctlpb.ServiceInfo{
		ServiceType:      flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
		OutputEventTypes: outputEventTypes,
		HealthEndpoint:   healthEndpoint,
		MaxInflight:      int32(maxInflight),
		Metadata:         metadata,
	}
}

// CreateProcessorServiceInfo creates ServiceInfo for a processor component
func CreateProcessorServiceInfo(inputEventTypes, outputEventTypes []string, healthEndpoint string, maxInflight uint32, metadata map[string]string) *flowctlpb.ServiceInfo {
	return &flowctlpb.ServiceInfo{
		ServiceType:      flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		InputEventTypes:  inputEventTypes,
		OutputEventTypes: outputEventTypes,
		HealthEndpoint:   healthEndpoint,
		MaxInflight:      int32(maxInflight),
		Metadata:         metadata,
	}
}

// CreateSinkServiceInfo creates ServiceInfo for a sink component
func CreateSinkServiceInfo(inputEventTypes []string, healthEndpoint string, maxInflight uint32, metadata map[string]string) *flowctlpb.ServiceInfo {
	return &flowctlpb.ServiceInfo{
		ServiceType:     flowctlpb.ServiceType_SERVICE_TYPE_SINK,
		InputEventTypes: inputEventTypes,
		HealthEndpoint:  healthEndpoint,
		MaxInflight:     int32(maxInflight),
		Metadata:        metadata,
	}
}

// Common Metadata Builders
// These functions help build standardized metadata maps

// BuildStellarSourceMetadata creates metadata for stellar-arrow-source
func BuildStellarSourceMetadata(network, sourceType, backendType string, additionalMetadata map[string]string) map[string]string {
	metadata := map[string]string{
		NetworkMetadataKey:     network,
		StorageTypeMetadataKey: sourceType,
		BackendTypeKey:         backendType,
	}
	
	// Add additional metadata
	for k, v := range additionalMetadata {
		metadata[k] = v
	}
	
	return metadata
}

// BuildTTPProcessorMetadata creates metadata for ttp-arrow-processor
func BuildTTPProcessorMetadata(network, processorType string, eventTypes []string, additionalMetadata map[string]string) map[string]string {
	metadata := map[string]string{
		NetworkMetadataKey: network,
		ProcessorTypeKey:   processorType,
	}
	
	if len(eventTypes) > 0 {
		metadata[EventTypesKey] = joinStrings(eventTypes, ",")
	}
	
	// Add additional metadata
	for k, v := range additionalMetadata {
		metadata[k] = v
	}
	
	return metadata
}

// BuildAnalyticsSinkMetadata creates metadata for arrow-analytics-sink
func BuildAnalyticsSinkMetadata(outputFormats []string, endpoints []string, additionalMetadata map[string]string) map[string]string {
	metadata := map[string]string{
		OutputFormatsKey: joinStrings(outputFormats, ","),
		EndpointsKey:     joinStrings(endpoints, ","),
	}
	
	// Add boolean flags for common output types
	for _, format := range outputFormats {
		switch format {
		case "websocket":
			metadata[WebSocketEnabledKey] = "true"
		case "api":
			metadata[APIEnabledKey] = "true"
		}
	}
	
	// Add additional metadata
	for k, v := range additionalMetadata {
		metadata[k] = v
	}
	
	return metadata
}

// Helper Functions

// joinStrings joins a slice of strings with a delimiter
func joinStrings(strs []string, delimiter string) string {
	if len(strs) == 0 {
		return ""
	}
	if len(strs) == 1 {
		return strs[0]
	}
	
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += delimiter + strs[i]
	}
	return result
}

// containsString checks if a slice contains a string
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
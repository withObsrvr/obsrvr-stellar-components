package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// FlowCtlConfig holds flowctl integration configuration
type FlowCtlConfig struct {
	Enabled           bool          `mapstructure:"flowctl_enabled" json:"flowctl_enabled"`
	Endpoint          string        `mapstructure:"flowctl_endpoint" json:"flowctl_endpoint"`
	HeartbeatInterval time.Duration `mapstructure:"flowctl_heartbeat_interval" json:"flowctl_heartbeat_interval"`
}

// LoadFlowCtlConfig loads flowctl configuration from environment variables
func LoadFlowCtlConfig() *FlowCtlConfig {
	return &FlowCtlConfig{
		Enabled:           getEnvBool("ENABLE_FLOWCTL", false),
		Endpoint:          getEnv("FLOWCTL_ENDPOINT", "localhost:8080"),
		HeartbeatInterval: getEnvDuration("FLOWCTL_HEARTBEAT_INTERVAL", 10*time.Second),
	}
}

// LoadFlowCtlConfigWithPrefix loads flowctl configuration with a component-specific prefix
func LoadFlowCtlConfigWithPrefix(prefix string) *FlowCtlConfig {
	return &FlowCtlConfig{
		Enabled:           getEnvBool(prefix+"_ENABLE_FLOWCTL", false),
		Endpoint:          getEnv(prefix+"_FLOWCTL_ENDPOINT", "localhost:8080"),
		HeartbeatInterval: getEnvDuration(prefix+"_FLOWCTL_HEARTBEAT_INTERVAL", 10*time.Second),
	}
}

// Environment Variable Helpers

// getEnv retrieves an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvBool retrieves a boolean environment variable with a default value
func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

// getEnvInt retrieves an integer environment variable with a default value
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

// getEnvUint32 retrieves a uint32 environment variable with a default value
func getEnvUint32(key string, defaultValue uint32) uint32 {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.ParseUint(value, 10, 32); err == nil {
			return uint32(i)
		}
	}
	return defaultValue
}

// getEnvDuration retrieves a duration environment variable with a default value
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}

// Component-specific configuration helpers

// StellarArrowSourceConfig represents configuration for stellar-arrow-source
type StellarArrowSourceConfig struct {
	FlowCtl  *FlowCtlConfig
	Network  string
	Source   string
	Backend  string
	Endpoint string
}

// LoadStellarArrowSourceFlowCtlConfig loads stellar-arrow-source specific flowctl config
func LoadStellarArrowSourceFlowCtlConfig() *StellarArrowSourceConfig {
	return &StellarArrowSourceConfig{
		FlowCtl:  LoadFlowCtlConfigWithPrefix("STELLAR_ARROW_SOURCE"),
		Network:  getEnv("STELLAR_ARROW_SOURCE_NETWORK_PASSPHRASE", "Test SDF Network ; September 2015"),
		Source:   getEnv("STELLAR_ARROW_SOURCE_SOURCE_TYPE", "rpc"),
		Backend:  getEnv("STELLAR_ARROW_SOURCE_BACKEND_TYPE", "rpc"),
		Endpoint: getEnv("STELLAR_ARROW_SOURCE_RPC_ENDPOINT", "https://soroban-testnet.stellar.org"),
	}
}

// TTPArrowProcessorConfig represents configuration for ttp-arrow-processor
type TTPArrowProcessorConfig struct {
	FlowCtl       *FlowCtlConfig
	Network       string
	ProcessorType string
	EventTypes    string
}

// LoadTTPArrowProcessorFlowCtlConfig loads ttp-arrow-processor specific flowctl config
func LoadTTPArrowProcessorFlowCtlConfig() *TTPArrowProcessorConfig {
	return &TTPArrowProcessorConfig{
		FlowCtl:       LoadFlowCtlConfigWithPrefix("TTP_ARROW_PROCESSOR"),
		Network:       getEnv("TTP_ARROW_PROCESSOR_NETWORK_PASSPHRASE", "Test SDF Network ; September 2015"),
		ProcessorType: getEnv("TTP_ARROW_PROCESSOR_PROCESSOR_TYPE", "ttp_event_extraction"),
		EventTypes:    getEnv("TTP_ARROW_PROCESSOR_EVENT_TYPES", "payment,path_payment_strict_receive,path_payment_strict_send,create_account,account_merge"),
	}
}

// ArrowAnalyticsSinkConfig represents configuration for arrow-analytics-sink
type ArrowAnalyticsSinkConfig struct {
	FlowCtl       *FlowCtlConfig
	OutputFormats string
	WebSocketPort int
	APIPort       int
}

// LoadArrowAnalyticsSinkFlowCtlConfig loads arrow-analytics-sink specific flowctl config
func LoadArrowAnalyticsSinkFlowCtlConfig() *ArrowAnalyticsSinkConfig {
	return &ArrowAnalyticsSinkConfig{
		FlowCtl:       LoadFlowCtlConfigWithPrefix("ARROW_ANALYTICS_SINK"),
		OutputFormats: getEnv("ARROW_ANALYTICS_SINK_OUTPUT_FORMATS", "parquet,json,websocket"),
		WebSocketPort: getEnvInt("ARROW_ANALYTICS_SINK_WEBSOCKET_PORT", 8080),
		APIPort:       getEnvInt("ARROW_ANALYTICS_SINK_API_PORT", 8081),
	}
}

// Validation helpers

// Validate checks if the flowctl configuration is valid
func (c *FlowCtlConfig) Validate() error {
	if c.Enabled && c.Endpoint == "" {
		return fmt.Errorf("flowctl endpoint cannot be empty when enabled")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat interval must be positive")
	}
	return nil
}

// IsEnabled returns true if flowctl integration is enabled
func (c *FlowCtlConfig) IsEnabled() bool {
	return c.Enabled
}

// GetEndpoint returns the flowctl endpoint with default fallback
func (c *FlowCtlConfig) GetEndpoint() string {
	if c.Endpoint == "" {
		return "localhost:8080"
	}
	return c.Endpoint
}

// GetHeartbeatInterval returns the heartbeat interval with default fallback
func (c *FlowCtlConfig) GetHeartbeatInterval() time.Duration {
	if c.HeartbeatInterval <= 0 {
		return 10 * time.Second
	}
	return c.HeartbeatInterval
}
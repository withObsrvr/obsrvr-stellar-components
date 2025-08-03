package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/withobsrvr/obsrvr-stellar-components/internal/config"
	"github.com/withobsrvr/obsrvr-stellar-components/internal/flowctl"
	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

const (
	ComponentName    = "arrow-analytics-sink"
	ComponentVersion = "v1.0.0"
)

// Config holds the configuration for the analytics sink component
type Config struct {
	// Source configuration
	ProcessorEndpoint string `mapstructure:"processor_endpoint"`

	// Output configuration
	OutputFormats []string `mapstructure:"output_formats"`

	// Parquet configuration
	ParquetPath        string   `mapstructure:"parquet_path"`
	ParquetCompression string   `mapstructure:"parquet_compression"`
	PartitionBy        []string `mapstructure:"partition_by"`
	ParquetBatchSize   int      `mapstructure:"parquet_batch_size"`

	// JSON configuration
	JSONPath   string `mapstructure:"json_path"`
	JSONFormat string `mapstructure:"json_format"`

	// CSV configuration
	CSVPath      string `mapstructure:"csv_path"`
	CSVDelimiter string `mapstructure:"csv_delimiter"`

	// WebSocket configuration
	WebSocketPort            int `mapstructure:"websocket_port"`
	WebSocketPath            string `mapstructure:"websocket_path"`
	MaxWebSocketConnections  int `mapstructure:"max_websocket_connections"`

	// API configuration
	APIPort      int `mapstructure:"api_port"`
	APICacheSize int `mapstructure:"api_cache_size"`

	// Performance configuration
	BufferSize    int    `mapstructure:"buffer_size"`
	WriterThreads int    `mapstructure:"writer_threads"`
	FlushInterval string `mapstructure:"flush_interval"`

	// Real-time analytics
	RealTimeAnalytics bool   `mapstructure:"real_time_analytics"`
	AnalyticsWindow   string `mapstructure:"analytics_window"`

	// Server configuration
	FlightPort int `mapstructure:"flight_port"`
	HealthPort int `mapstructure:"health_port"`

	// Monitoring configuration
	MetricsEnabled bool   `mapstructure:"metrics_enabled"`
	LogLevel       string `mapstructure:"log_level"`

	// flowctl integration configuration
	FlowCtlConfig *config.FlowCtlConfig
}


// Default configuration values
func defaultConfig() *Config {
	return &Config{
		ProcessorEndpoint:       "localhost:8816",
		OutputFormats:           []string{"parquet", "json"},
		ParquetPath:             "./data/ttp_events",
		ParquetCompression:      "snappy",
		PartitionBy:             []string{"date"},
		ParquetBatchSize:        10000,
		JSONPath:                "./data/json",
		JSONFormat:              "jsonl",
		CSVPath:                 "./data/csv",
		CSVDelimiter:            ",",
		WebSocketPort:           8080,
		WebSocketPath:           "/ws",
		MaxWebSocketConnections: 1000,
		APIPort:                 8080,
		APICacheSize:            100000,
		BufferSize:              10000,
		WriterThreads:           4,
		FlushInterval:           "30s",
		RealTimeAnalytics:       false,
		AnalyticsWindow:         "1m",
		FlightPort:              8817,
		HealthPort:              8088,
		MetricsEnabled:          true,
		LogLevel:                "info",
	}
}

// Metrics
var (
	eventsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analytics_sink_events_received_total",
			Help: "Total number of events received",
		},
		[]string{"event_type", "asset_code"},
	)

	eventsWritten = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analytics_sink_events_written_total",
			Help: "Total number of events written",
		},
		[]string{"output_format", "status"},
	)

	filesWritten = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analytics_sink_files_written_total",
			Help: "Total number of files written",
		},
		[]string{"output_format"},
	)

	writeDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "analytics_sink_write_duration_seconds",
			Help: "Time spent writing data",
		},
		[]string{"output_format"},
	)

	bufferDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "analytics_sink_buffer_depth",
			Help: "Current buffer depth",
		},
		[]string{"buffer_type"},
	)

	websocketConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "analytics_sink_websocket_connections",
			Help: "Current number of WebSocket connections",
		},
	)
)

func init() {
	prometheus.MustRegister(eventsReceived)
	prometheus.MustRegister(eventsWritten)
	prometheus.MustRegister(filesWritten)
	prometheus.MustRegister(writeDuration)
	prometheus.MustRegister(bufferDepth)
	prometheus.MustRegister(websocketConnections)
}

func main() {
	// Load configuration
	config := loadConfig()

	// Setup logging
	setupLogging(config.LogLevel)

	log.Info().
		Str("component", ComponentName).
		Str("version", ComponentVersion).
		Str("processor_endpoint", config.ProcessorEndpoint).
		Strs("output_formats", config.OutputFormats).
		Bool("flowctl_enabled", config.FlowCtlConfig.IsEnabled()).
		Msg("Starting arrow-analytics-sink")

	// Create memory allocator
	pool := memory.NewGoAllocator()
	// Note: Go allocator is automatically garbage collected, no manual cleanup needed

	// Create schema registry
	registry := schemas.NewSchemaRegistry()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create and start the analytics service
	service, err := NewAnalyticsSinkService(config, pool, registry)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create analytics service")
	}

	// Initialize flowctl integration if enabled
	var flowctlController *flowctl.Controller
	if config.FlowCtlConfig.IsEnabled() {
		flowctlController = flowctl.NewController(
			config.FlowCtlConfig.GetEndpoint(),
			service, // service implements MetricsProvider
		)
		flowctlController.SetHeartbeatInterval(config.FlowCtlConfig.GetHeartbeatInterval())
		
		// Build endpoints list for metadata
		endpoints := []string{
			fmt.Sprintf("http://localhost:%d/health", config.HealthPort),
		}
		if contains(config.OutputFormats, "websocket") {
			endpoints = append(endpoints, fmt.Sprintf("ws://localhost:%d%s", config.WebSocketPort, config.WebSocketPath))
		}
		if contains(config.OutputFormats, "api") {
			endpoints = append(endpoints, fmt.Sprintf("http://localhost:%d/api", config.APIPort))
		}
		
		// Configure service registration
		serviceInfo := flowctl.CreateSinkServiceInfo(
			[]string{flowctl.TTPEventType},              // input event types
			fmt.Sprintf("http://localhost:%d/health", config.HealthPort),
			1000, // max inflight
			flowctl.BuildAnalyticsSinkMetadata(
				config.OutputFormats,
				endpoints,
				map[string]string{
					"buffer_size":     fmt.Sprintf("%d", config.BufferSize),
					"writer_threads":  fmt.Sprintf("%d", config.WriterThreads),
					"flush_interval":  config.FlushInterval,
					"real_time_analytics": fmt.Sprintf("%t", config.RealTimeAnalytics),
				},
			),
		)
		flowctlController.SetServiceInfo(serviceInfo)

		// Start flowctl integration
		if err := flowctlController.Start(); err != nil {
			log.Warn().Err(err).Msg("Failed to start flowctl integration, continuing without it")
		} else {
			log.Info().
				Str("endpoint", config.FlowCtlConfig.GetEndpoint()).
				Str("service_id", flowctlController.GetServiceID()).
				Msg("flowctl integration started")
		}

		// Set the controller on the service for dynamic discovery
		service.SetFlowCtlController(flowctlController)
	}

	// Start health server
	go func() {
		if err := startHealthServer(config, service); err != nil {
			log.Fatal().Err(err).Msg("Health server failed")
		}
	}()

	// Start WebSocket server if enabled
	if contains(config.OutputFormats, "websocket") || contains(config.OutputFormats, "api") {
		go func() {
			if err := service.StartWebSocketServer(ctx); err != nil {
				log.Error().Err(err).Msg("WebSocket server failed")
			}
		}()
	}

	// Start processing
	go func() {
		if err := service.Start(ctx); err != nil {
			log.Error().Err(err).Msg("Analytics service failed")
		}
	}()

	// Wait for shutdown signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Info().Msg("Shutting down arrow-analytics-sink")
	cancel()

	// Stop flowctl integration
	if flowctlController != nil {
		flowctlController.Stop()
	}

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	service.Stop(shutdownCtx)
	log.Info().Msg("Shutdown complete")
}

func loadConfig() *Config {
	cfg := defaultConfig()

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("/config")
	viper.AddConfigPath(".")

	// Environment variable binding
	viper.SetEnvPrefix("ARROW_ANALYTICS_SINK")
	viper.AutomaticEnv()

	// Read config file if it exists
	if err := viper.ReadInConfig(); err != nil {
		log.Debug().Err(err).Msg("No config file found, using environment variables and defaults")
	}

	// Unmarshal config
	if err := viper.Unmarshal(cfg); err != nil {
		log.Fatal().Err(err).Msg("Failed to unmarshal config")
	}

	// Load flowctl configuration
	flowCtlSinkConfig := config.LoadArrowAnalyticsSinkFlowCtlConfig()
	cfg.FlowCtlConfig = flowCtlSinkConfig.FlowCtl

	// Override with environment variables
	if port := os.Getenv("ARROW_ANALYTICS_SINK_PORT"); port != "" {
		viper.Set("flight_port", port)
	}
	if healthPort := os.Getenv("ARROW_ANALYTICS_SINK_HEALTH_PORT"); healthPort != "" {
		viper.Set("health_port", healthPort)
	}
	if wsPort := os.Getenv("ARROW_ANALYTICS_SINK_WEBSOCKET_PORT"); wsPort != "" {
		viper.Set("websocket_port", wsPort)
		viper.Set("api_port", wsPort)
	}
	if logLevel := os.Getenv("ARROW_ANALYTICS_SINK_LOG_LEVEL"); logLevel != "" {
		viper.Set("log_level", logLevel)
	}
	if dataPath := os.Getenv("ARROW_ANALYTICS_SINK_DATA_PATH"); dataPath != "" {
		viper.Set("parquet_path", dataPath+"/parquet")
		viper.Set("json_path", dataPath+"/json")
		viper.Set("csv_path", dataPath+"/csv")
	}

	// Re-unmarshal after environment overrides
	if err := viper.Unmarshal(cfg); err != nil {
		log.Fatal().Err(err).Msg("Failed to unmarshal config after env override")
	}

	return cfg
}

func setupLogging(level string) {
	// Parse log level
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}

	// Setup zerolog
	zerolog.SetGlobalLevel(logLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
}

func startHealthServer(config *Config, service *AnalyticsSinkService) error {
	router := mux.NewRouter()

	// Health endpoint
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"healthy","component":"%s","version":"%s","timestamp":"%s"}`,
			ComponentName, ComponentVersion, time.Now().Format(time.RFC3339))
	}).Methods("GET")

	// Schema endpoint
	router.HandleFunc("/schema", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"schema":"ttp_event","version":"%s","description":"Token Transfer Protocol events"}`,
			schemas.TTPEventSchemaVersion)
	}).Methods("GET")

	// Stats endpoint
	router.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := service.GetStats()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{
			"component": "%s",
			"version": "%s",
			"events_received": %d,
			"events_written": %d,
			"files_written": %d,
			"websocket_connections": %d,
			"uptime_seconds": %.0f
		}`,
			ComponentName, ComponentVersion,
			stats.EventsReceived, stats.EventsWritten, stats.FilesWritten,
			stats.WebSocketConnections, time.Since(stats.StartTime).Seconds())
	}).Methods("GET")

	// Metrics endpoint
	if config.MetricsEnabled {
		router.Handle("/metrics", promhttp.Handler()).Methods("GET")
	}

	log.Info().
		Int("port", config.HealthPort).
		Bool("metrics_enabled", config.MetricsEnabled).
		Msg("Starting analytics health server")

	return http.ListenAndServe(fmt.Sprintf(":%d", config.HealthPort), router)
}

// Helper function to check if slice contains string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if strings.EqualFold(s, item) {
			return true
		}
	}
	return false
}
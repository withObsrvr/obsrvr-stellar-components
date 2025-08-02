package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

const (
	ComponentName    = "ttp-arrow-processor"
	ComponentVersion = "v1.0.0"
)

// Config holds the configuration for the TTP processor component
type Config struct {
	// Source configuration
	SourceEndpoint string `mapstructure:"source_endpoint"`

	// Processing configuration
	EventTypes                []string     `mapstructure:"event_types"`
	AssetFilters             []AssetFilter `mapstructure:"asset_filters"`
	AmountFilters            AmountFilter  `mapstructure:"amount_filters"`
	ProcessorThreads         int           `mapstructure:"processor_threads"`
	BatchSize                int           `mapstructure:"batch_size"`
	BufferSize               int           `mapstructure:"buffer_size"`
	ComputeThreads           int           `mapstructure:"compute_threads"`
	MemoryPool               string        `mapstructure:"memory_pool"`

	// Output configuration
	IncludeRawXDR            bool `mapstructure:"include_raw_xdr"`
	IncludeTransactionDetails bool `mapstructure:"include_transaction_details"`
	DeduplicateEvents        bool `mapstructure:"deduplicate_events"`

	// Server configuration
	FlightPort int `mapstructure:"flight_port"`
	HealthPort int `mapstructure:"health_port"`

	// Monitoring configuration
	MetricsEnabled bool   `mapstructure:"metrics_enabled"`
	StatsInterval  string `mapstructure:"stats_interval"`
	LogLevel       string `mapstructure:"log_level"`
}

// AssetFilter represents a filter for specific assets
type AssetFilter struct {
	AssetCode   string `mapstructure:"asset_code"`
	AssetIssuer string `mapstructure:"asset_issuer"`
}

// AmountFilter represents amount range filters
type AmountFilter struct {
	MinAmount string `mapstructure:"min_amount"`
	MaxAmount string `mapstructure:"max_amount"`
}

// Default configuration values
func defaultConfig() *Config {
	return &Config{
		SourceEndpoint:           "localhost:8815",
		EventTypes:               []string{"payment", "path_payment_strict_receive", "path_payment_strict_send"},
		ProcessorThreads:         4,
		BatchSize:                1000,
		BufferSize:               10000,
		ComputeThreads:           0, // Auto-detect
		MemoryPool:               "default",
		IncludeRawXDR:            false,
		IncludeTransactionDetails: true,
		DeduplicateEvents:        true,
		FlightPort:               8816,
		HealthPort:               8088,
		MetricsEnabled:           true,
		StatsInterval:            "30s",
		LogLevel:                 "info",
	}
}

// Metrics
var (
	ledgersProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ttp_processor_ledgers_processed_total",
			Help: "Total number of ledgers processed",
		},
		[]string{"status"},
	)

	eventsExtracted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ttp_processor_events_extracted_total",
			Help: "Total number of TTP events extracted",
		},
		[]string{"event_type", "asset_code"},
	)

	batchesGenerated = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ttp_processor_batches_generated_total",
			Help: "Total number of Arrow batches generated",
		},
		[]string{"output_type"},
	)

	processingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "ttp_processor_processing_duration_seconds",
			Help: "Time spent processing operations",
		},
		[]string{"operation"},
	)

	currentLedger = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "ttp_processor_current_ledger",
			Help: "Current ledger being processed",
		},
	)

	queueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ttp_processor_queue_depth",
			Help: "Depth of processing queues",
		},
		[]string{"queue_type"},
	)
)

func init() {
	prometheus.MustRegister(ledgersProcessed)
	prometheus.MustRegister(eventsExtracted)
	prometheus.MustRegister(batchesGenerated)
	prometheus.MustRegister(processingDuration)
	prometheus.MustRegister(currentLedger)
	prometheus.MustRegister(queueDepth)
}

func main() {
	// Load configuration
	config := loadConfig()

	// Setup logging
	setupLogging(config.LogLevel)

	log.Info().
		Str("component", ComponentName).
		Str("version", ComponentVersion).
		Str("source_endpoint", config.SourceEndpoint).
		Strs("event_types", config.EventTypes).
		Msg("Starting ttp-arrow-processor")

	// Create memory allocator
	pool := memory.NewGoAllocator()
	defer pool.Destroy()

	// Create schema registry
	registry := schemas.NewSchemaRegistry()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create and start the processor service
	service, err := NewTTPProcessorService(config, pool, registry)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create processor service")
	}

	// Start Flight server
	flightServer := NewTTPFlightServer(service)
	go func() {
		if err := startFlightServer(flightServer, config.FlightPort); err != nil {
			log.Fatal().Err(err).Msg("Flight server failed")
		}
	}()

	// Start health server
	go func() {
		if err := startHealthServer(config.HealthPort, config.MetricsEnabled); err != nil {
			log.Fatal().Err(err).Msg("Health server failed")
		}
	}()

	// Start processing
	go func() {
		if err := service.Start(ctx); err != nil {
			log.Error().Err(err).Msg("Processor service failed")
		}
	}()

	// Wait for shutdown signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Info().Msg("Shutting down ttp-arrow-processor")
	cancel()

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	service.Stop(shutdownCtx)
	log.Info().Msg("Shutdown complete")
}

func loadConfig() *Config {
	config := defaultConfig()

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("/config")
	viper.AddConfigPath(".")

	// Environment variable binding
	viper.SetEnvPrefix("TTP_ARROW_PROCESSOR")
	viper.AutomaticEnv()

	// Read config file if it exists
	if err := viper.ReadInConfig(); err != nil {
		log.Debug().Err(err).Msg("No config file found, using environment variables and defaults")
	}

	// Unmarshal config
	if err := viper.Unmarshal(config); err != nil {
		log.Fatal().Err(err).Msg("Failed to unmarshal config")
	}

	// Override with environment variables
	if port := os.Getenv("TTP_ARROW_PROCESSOR_PORT"); port != "" {
		viper.Set("flight_port", port)
	}
	if healthPort := os.Getenv("TTP_ARROW_PROCESSOR_HEALTH_PORT"); healthPort != "" {
		viper.Set("health_port", healthPort)
	}
	if logLevel := os.Getenv("TTP_ARROW_PROCESSOR_LOG_LEVEL"); logLevel != "" {
		viper.Set("log_level", logLevel)
	}
	if threads := os.Getenv("TTP_ARROW_PROCESSOR_THREADS"); threads != "" {
		viper.Set("processor_threads", threads)
	}
	if sourceEndpoint := os.Getenv("TTP_ARROW_PROCESSOR_SOURCE_ENDPOINT"); sourceEndpoint != "" {
		viper.Set("source_endpoint", sourceEndpoint)
	}

	// Re-unmarshal after environment overrides
	if err := viper.Unmarshal(config); err != nil {
		log.Fatal().Err(err).Msg("Failed to unmarshal config after env override")
	}

	return config
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

func startFlightServer(server flight.Server, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	grpcServer := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcServer, server)

	// Add health check service
	healthServer := health.NewServer()
	healthServer.SetServingStatus("ttp-arrow-processor", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	log.Info().
		Int("port", port).
		Msg("Starting TTP Arrow Flight server")

	return grpcServer.Serve(lis)
}

func startHealthServer(port int, metricsEnabled bool) error {
	router := mux.NewRouter()

	// Health endpoint
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"healthy","component":"%s","version":"%s","timestamp":"%s"}`,
			ComponentName, ComponentVersion, time.Now().Format(time.RFC3339))
	}).Methods("GET")

	// Input schema endpoint
	router.HandleFunc("/schema/input", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"schema":"stellar_ledger","version":"%s","description":"Stellar ledger data input"}`,
			schemas.StellarLedgerSchemaVersion)
	}).Methods("GET")

	// Output schema endpoint
	router.HandleFunc("/schema/output", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"schema":"ttp_event","version":"%s","description":"Token Transfer Protocol events"}`,
			schemas.TTPEventSchemaVersion)
	}).Methods("GET")

	// Stats endpoint
	router.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"component":"%s","version":"%s","status":"processing"}`,
			ComponentName, ComponentVersion)
	}).Methods("GET")

	// Metrics endpoint
	if metricsEnabled {
		router.Handle("/metrics", promhttp.Handler()).Methods("GET")
	}

	log.Info().
		Int("port", port).
		Bool("metrics_enabled", metricsEnabled).
		Msg("Starting TTP health server")

	return http.ListenAndServe(fmt.Sprintf(":%d", port), router)
}
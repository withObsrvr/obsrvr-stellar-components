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
	ComponentName    = "stellar-arrow-source"
	ComponentVersion = "v1.0.0"
)

// Config holds the configuration for the stellar-arrow-source component
type Config struct {
	// Source configuration
	SourceType       string `mapstructure:"source_type"`
	RPCEndpoint      string `mapstructure:"rpc_endpoint"`
	NetworkPassphrase string `mapstructure:"network_passphrase"`

	// Data lake configuration
	StorageBackend string `mapstructure:"storage_backend"`
	BucketName     string `mapstructure:"bucket_name"`
	AWSRegion      string `mapstructure:"aws_region"`
	GCPProject     string `mapstructure:"gcp_project"`
	StoragePath    string `mapstructure:"storage_path"`

	// Processing configuration
	StartLedger       uint32 `mapstructure:"start_ledger"`
	EndLedger         uint32 `mapstructure:"end_ledger"`
	BatchSize         int    `mapstructure:"batch_size"`
	BufferSize        int    `mapstructure:"buffer_size"`
	ConcurrentReaders int    `mapstructure:"concurrent_readers"`

	// Arrow configuration
	ArrowMemoryPool string `mapstructure:"arrow_memory_pool"`
	Compression     string `mapstructure:"compression"`

	// Server configuration
	FlightPort int `mapstructure:"flight_port"`
	HealthPort int `mapstructure:"health_port"`

	// Monitoring configuration
	MetricsEnabled bool   `mapstructure:"metrics_enabled"`
	LogLevel       string `mapstructure:"log_level"`
}

// Default configuration values
func defaultConfig() *Config {
	return &Config{
		SourceType:        "rpc",
		NetworkPassphrase: "Test SDF Network ; September 2015",
		StartLedger:       0,
		EndLedger:         0,
		BatchSize:         1000,
		BufferSize:        10000,
		ConcurrentReaders: 4,
		ArrowMemoryPool:   "default",
		Compression:       "none",
		FlightPort:        8815,
		HealthPort:        8088,
		MetricsEnabled:    true,
		LogLevel:          "info",
		StoragePath:       "./data/ledgers",
		AWSRegion:         "us-west-2",
	}
}

// Metrics
var (
	ledgersProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "stellar_source_ledgers_processed_total",
			Help: "Total number of ledgers processed",
		},
		[]string{"source_type", "status"},
	)

	batchesGenerated = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "stellar_source_batches_generated_total",
			Help: "Total number of Arrow batches generated",
		},
		[]string{"source_type"},
	)

	processingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "stellar_source_processing_duration_seconds",
			Help: "Time spent processing ledgers",
		},
		[]string{"source_type", "operation"},
	)

	currentLedger = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "stellar_source_current_ledger",
			Help: "Current ledger being processed",
		},
		[]string{"source_type"},
	)
)

func init() {
	prometheus.MustRegister(ledgersProcessed)
	prometheus.MustRegister(batchesGenerated)
	prometheus.MustRegister(processingDuration)
	prometheus.MustRegister(currentLedger)
}

func main() {
	// Load configuration
	config := loadConfig()

	// Setup logging
	setupLogging(config.LogLevel)

	log.Info().
		Str("component", ComponentName).
		Str("version", ComponentVersion).
		Str("source_type", config.SourceType).
		Msg("Starting stellar-arrow-source")

	// Create memory allocator
	pool := memory.NewGoAllocator()
	defer pool.Destroy()

	// Create schema registry
	registry := schemas.NewSchemaRegistry()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create and start the source service
	service, err := NewStellarSourceService(config, pool, registry)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create source service")
	}

	// Start Flight server
	flightServer := NewFlightServer(service)
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
			log.Error().Err(err).Msg("Source service failed")
		}
	}()

	// Wait for shutdown signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Info().Msg("Shutting down stellar-arrow-source")
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
	viper.SetEnvPrefix("STELLAR_ARROW_SOURCE")
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
	if port := os.Getenv("STELLAR_ARROW_SOURCE_PORT"); port != "" {
		viper.Set("flight_port", port)
	}
	if healthPort := os.Getenv("STELLAR_ARROW_SOURCE_HEALTH_PORT"); healthPort != "" {
		viper.Set("health_port", healthPort)
	}
	if logLevel := os.Getenv("STELLAR_ARROW_SOURCE_LOG_LEVEL"); logLevel != "" {
		viper.Set("log_level", logLevel)
	}
	if batchSize := os.Getenv("STELLAR_ARROW_SOURCE_BATCH_SIZE"); batchSize != "" {
		viper.Set("batch_size", batchSize)
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
	healthServer.SetServingStatus("stellar-arrow-source", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	log.Info().
		Int("port", port).
		Msg("Starting Arrow Flight server")

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

	// Schema endpoint
	router.HandleFunc("/schema", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"schema":"stellar_ledger","version":"%s","description":"Stellar ledger data with Arrow format"}`,
			schemas.StellarLedgerSchemaVersion)
	}).Methods("GET")

	// Metrics endpoint
	if metricsEnabled {
		router.Handle("/metrics", promhttp.Handler()).Methods("GET")
	}

	log.Info().
		Int("port", port).
		Bool("metrics_enabled", metricsEnabled).
		Msg("Starting health server")

	return http.ListenAndServe(fmt.Sprintf(":%d", port), router)
}
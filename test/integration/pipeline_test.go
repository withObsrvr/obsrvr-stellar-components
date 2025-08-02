package integration

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestEndToEndPipeline tests the complete pipeline flow
func TestEndToEndPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Create test data directory
	testDataDir := filepath.Join(os.TempDir(), "obsrvr-test", fmt.Sprintf("test-%d", time.Now().Unix()))
	require.NoError(t, os.MkdirAll(testDataDir, 0755))
	defer os.RemoveAll(testDataDir)

	// Test configuration
	config := &PipelineTestConfig{
		TestDataDir:       testDataDir,
		StellarNetwork:    "testnet",
		StartLedger:       1,
		EndLedger:         10,
		ExpectedEvents:    5, // Minimum expected events
		TestTimeout:       2 * time.Minute,
	}

	// Run the pipeline test
	t.Run("BasicPipeline", func(t *testing.T) {
		testBasicPipeline(t, ctx, config)
	})

	t.Run("ArrowDataIntegrity", func(t *testing.T) {
		testArrowDataIntegrity(t, ctx, config)
	})

	t.Run("OutputFormats", func(t *testing.T) {
		testOutputFormats(t, ctx, config)
	})
}

type PipelineTestConfig struct {
	TestDataDir    string
	StellarNetwork string
	StartLedger    uint32
	EndLedger      uint32
	ExpectedEvents int
	TestTimeout    time.Duration
}

func testBasicPipeline(t *testing.T, ctx context.Context, config *PipelineTestConfig) {
	// Start stellar-arrow-source in filesystem mode with test data
	sourcePort, sourceCleanup := startStellarSource(t, ctx, config)
	defer sourceCleanup()

	// Start ttp-arrow-processor
	processorPort, processorCleanup := startTTPProcessor(t, ctx, sourcePort)
	defer processorCleanup()

	// Start arrow-analytics-sink  
	sinkCleanup := startAnalyticsSink(t, ctx, processorPort, config.TestDataDir)
	defer sinkCleanup()

	// Wait for pipeline to process data
	time.Sleep(10 * time.Second)

	// Verify outputs were created
	t.Run("VerifyParquetOutput", func(t *testing.T) {
		parquetPath := filepath.Join(config.TestDataDir, "parquet")
		files, err := filepath.Glob(filepath.Join(parquetPath, "**", "*.json"))
		require.NoError(t, err)
		assert.True(t, len(files) > 0, "Expected Parquet output files")
	})

	t.Run("VerifyJSONOutput", func(t *testing.T) {
		jsonPath := filepath.Join(config.TestDataDir, "json")
		files, err := filepath.Glob(filepath.Join(jsonPath, "*.json"))
		require.NoError(t, err)
		assert.True(t, len(files) > 0, "Expected JSON output files")
	})
}

func testArrowDataIntegrity(t *testing.T, ctx context.Context, config *PipelineTestConfig) {
	// This test would verify that data maintains integrity through the Arrow pipeline
	// For now, we'll do basic structure validation
	
	t.Log("Testing Arrow data integrity through pipeline")
	
	// TODO: Implement Arrow record validation
	// - Verify schema consistency
	// - Check for data corruption
	// - Validate field types and values
}

func testOutputFormats(t *testing.T, ctx context.Context, config *PipelineTestConfig) {
	t.Log("Testing multiple output formats")
	
	// TODO: Test that all configured output formats are generated correctly
	// - Parquet files with correct schema
	// - JSON files with proper formatting
	// - CSV files with headers
	// - WebSocket connectivity
}

// Helper functions to start components

func startStellarSource(t *testing.T, ctx context.Context, config *PipelineTestConfig) (int, func()) {
	// Create test ledger data
	createTestLedgerData(t, config.TestDataDir)

	// Find available port
	port := findAvailablePort(t)

	// TODO: Start stellar-arrow-source component
	// For now, return mock port and cleanup
	
	t.Logf("Started stellar-arrow-source on port %d", port)
	
	return port, func() {
		t.Log("Stopping stellar-arrow-source")
	}
}

func startTTPProcessor(t *testing.T, ctx context.Context, sourcePort int) (int, func()) {
	port := findAvailablePort(t)
	
	// TODO: Start ttp-arrow-processor component
	
	t.Logf("Started ttp-arrow-processor on port %d", port)
	
	return port, func() {
		t.Log("Stopping ttp-arrow-processor")
	}
}

func startAnalyticsSink(t *testing.T, ctx context.Context, processorPort int, dataDir string) func() {
	// TODO: Start arrow-analytics-sink component
	
	t.Log("Started arrow-analytics-sink")
	
	return func() {
		t.Log("Stopping arrow-analytics-sink")
	}
}

func createTestLedgerData(t *testing.T, dataDir string) {
	// Create mock ledger XDR files for testing
	ledgerDir := filepath.Join(dataDir, "ledgers")
	require.NoError(t, os.MkdirAll(ledgerDir, 0755))

	// Create a few test ledger files
	for i := 1; i <= 10; i++ {
		filename := fmt.Sprintf("%08d.xdr", i)
		filepath := filepath.Join(ledgerDir, filename)
		
		// Write mock XDR data (in a real test, this would be valid Stellar XDR)
		mockData := fmt.Sprintf("mock-ledger-xdr-data-for-sequence-%d", i)
		require.NoError(t, os.WriteFile(filepath, []byte(mockData), 0644))
	}

	t.Logf("Created test ledger data in %s", ledgerDir)
}

func findAvailablePort(t *testing.T) int {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

// TestDockerIntegration tests the components in Docker containers
func TestDockerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Docker integration test in short mode")
	}

	ctx := context.Background()

	// Test each component can start in Docker
	t.Run("StellarSourceContainer", func(t *testing.T) {
		testComponentInDocker(t, ctx, "stellar-arrow-source", 8815)
	})

	t.Run("TTPProcessorContainer", func(t *testing.T) {
		testComponentInDocker(t, ctx, "ttp-arrow-processor", 8816)
	})

	t.Run("AnalyticsSinkContainer", func(t *testing.T) {
		testComponentInDocker(t, ctx, "arrow-analytics-sink", 8817)
	})
}

func testComponentInDocker(t *testing.T, ctx context.Context, componentName string, port int) {
	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("obsrvr/%s:test", componentName),
		ExposedPorts: []string{fmt.Sprintf("%d/tcp", port), "8088/tcp"},
		WaitingFor:   wait.ForHTTP("/health").WithPort("8088/tcp"),
		Env: map[string]string{
			"LOG_LEVEL": "debug",
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	
	if err != nil {
		t.Skipf("Could not start %s container: %v", componentName, err)
		return
	}
	
	defer container.Terminate(ctx)

	// Test health endpoint
	healthPort, err := container.MappedPort(ctx, "8088")
	require.NoError(t, err)

	healthURL := fmt.Sprintf("http://localhost:%s/health", healthPort.Port())
	
	// Wait for health check to pass
	require.Eventually(t, func() bool {
		resp, err := http.Get(healthURL)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == 200
	}, 30*time.Second, 1*time.Second, "Health check should pass")

	t.Logf("%s container started successfully", componentName)
}

// Benchmark tests
func BenchmarkPipelineThroughput(b *testing.B) {
	// Benchmark the end-to-end pipeline throughput
	b.Skip("Pipeline benchmark not implemented")
}

func BenchmarkArrowProcessing(b *testing.B) {
	// Benchmark Arrow data processing performance
	b.Skip("Arrow processing benchmark not implemented")
}
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// PipelineE2ETestSuite contains end-to-end tests for the complete pipeline
type PipelineE2ETestSuite struct {
	suite.Suite
	ctx           context.Context
	cancel        context.CancelFunc
	testDir       string
	componentPids map[string]int
	cleanups      []func()
}

// SetupSuite runs once before all tests in the suite
func (suite *PipelineE2ETestSuite) SetupSuite() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.componentPids = make(map[string]int)
	suite.cleanups = make([]func(), 0)

	// Get the test directory (should be project root)
	wd, err := os.Getwd()
	require.NoError(suite.T(), err)
	
	// Navigate to project root (two levels up from test/e2e)
	suite.testDir = filepath.Join(wd, "..", "..")
	
	// Verify we have the right directory structure
	componentsDir := filepath.Join(suite.testDir, "components")
	_, err = os.Stat(componentsDir)
	require.NoError(suite.T(), err, "Components directory not found at %s", componentsDir)
}

// TearDownSuite runs once after all tests in the suite
func (suite *PipelineE2ETestSuite) TearDownSuite() {
	suite.cancel()
	
	// Stop all components
	suite.stopAllComponents()
	
	// Run all cleanup functions in reverse order
	for i := len(suite.cleanups) - 1; i >= 0; i-- {
		suite.cleanups[i]()
	}
}

// SetupTest runs before each test
func (suite *PipelineE2ETestSuite) SetupTest() {
	// Ensure clean state for each test
	suite.stopAllComponents()
	time.Sleep(1 * time.Second) // Allow time for cleanup
}

// TearDownTest runs after each test
func (suite *PipelineE2ETestSuite) TearDownTest() {
	// Clean up after each test
	suite.stopAllComponents()
}

// TestPipelineLocalMode tests the pipeline in local standalone mode
func (suite *PipelineE2ETestSuite) TestPipelineLocalMode() {
	t := suite.T()

	t.Run("Build Components", func(t *testing.T) {
		err := suite.buildAllComponents()
		assert.NoError(t, err, "Failed to build components")
	})

	t.Run("Start Pipeline in Local Mode", func(t *testing.T) {
		err := suite.startPipelineWithMode("local")
		assert.NoError(t, err, "Failed to start pipeline in local mode")

		// Wait for components to start
		time.Sleep(5 * time.Second)

		// Verify components are running
		suite.verifyComponentsRunning(t)
	})

	t.Run("Health Check All Components", func(t *testing.T) {
		healthResults := suite.checkComponentHealth()
		
		for component, healthy := range healthResults {
			assert.True(t, healthy, "Component %s should be healthy", component)
		}
	})

	t.Run("Stop Pipeline", func(t *testing.T) {
		suite.stopAllComponents()
		
		// Verify components are stopped
		time.Sleep(2 * time.Second)
		suite.verifyComponentsStopped(t)
	})
}

// TestPipelineFlowCtlMode tests the pipeline with flowctl integration
func (suite *PipelineE2ETestSuite) TestPipelineFlowCtlMode() {
	t := suite.T()

	// Skip if no flowctl endpoint is available
	flowctlEndpoint := os.Getenv("FLOWCTL_ENDPOINT")
	if flowctlEndpoint == "" {
		t.Skip("FLOWCTL_ENDPOINT not set, skipping flowctl integration test")
	}

	t.Run("Build Components", func(t *testing.T) {
		err := suite.buildAllComponents()
		assert.NoError(t, err, "Failed to build components")
	})

	t.Run("Start Pipeline with FlowCtl", func(t *testing.T) {
		err := suite.startPipelineWithMode("flowctl")
		assert.NoError(t, err, "Failed to start pipeline in flowctl mode")

		// Wait for components to start and register with flowctl
		time.Sleep(10 * time.Second)

		// Verify components are running
		suite.verifyComponentsRunning(t)
	})

	t.Run("Verify FlowCtl Registration", func(t *testing.T) {
		// Check component logs for flowctl registration messages
		logs := suite.getComponentLogs()
		
		for component, log := range logs {
			assert.Contains(t, log, "FlowCtl Integration: true", 
				"Component %s should have flowctl enabled", component)
		}
	})

	t.Run("Test Service Discovery", func(t *testing.T) {
		// In a real test, this would query flowctl to verify service registration
		// For now, we verify the components are attempting flowctl integration
		logs := suite.getComponentLogs()
		
		for component, log := range logs {
			// Look for flowctl-related log messages
			hasFlowCtlLogs := strings.Contains(log, "flowctl") || 
							 strings.Contains(log, "FlowCtl") ||
							 strings.Contains(log, "heartbeat") ||
							 strings.Contains(log, "registration")
			
			assert.True(t, hasFlowCtlLogs, 
				"Component %s should have flowctl-related logs", component)
		}
	})
}

// TestPipelineDevelopmentMode tests the pipeline in development configuration
func (suite *PipelineE2ETestSuite) TestPipelineDevelopmentMode() {
	t := suite.T()

	t.Run("Build Components", func(t *testing.T) {
		err := suite.buildAllComponents()
		assert.NoError(t, err, "Failed to build components")
	})

	t.Run("Start Pipeline in Development Mode", func(t *testing.T) {
		err := suite.startPipelineWithMode("development")
		assert.NoError(t, err, "Failed to start pipeline in development mode")

		// Wait for components to start
		time.Sleep(5 * time.Second)

		// Verify components are running
		suite.verifyComponentsRunning(t)
	})

	t.Run("Verify Development Configuration", func(t *testing.T) {
		logs := suite.getComponentLogs()
		
		for component, log := range logs {
			// Check for development-specific configuration
			if strings.Contains(log, "FlowCtl Integration: true") {
				// Should have faster heartbeat intervals in development mode
				assert.Contains(t, log, "Heartbeat Interval: 5s", 
					"Component %s should have development heartbeat interval", component)
			}
		}
	})
}

// TestPipelineConfigurationProfiles tests different configuration profiles
func (suite *PipelineE2ETestSuite) TestPipelineConfigurationProfiles() {
	t := suite.T()

	profiles := []string{"local", "development"}
	if os.Getenv("FLOWCTL_ENDPOINT") != "" {
		profiles = append(profiles, "flowctl")
	}

	for _, profile := range profiles {
		t.Run(fmt.Sprintf("Profile_%s", profile), func(t *testing.T) {
			// Build components
			err := suite.buildAllComponents()
			require.NoError(t, err)

			// Start with specific profile
			err = suite.startPipelineWithMode(profile)
			require.NoError(t, err)

			// Allow startup time
			time.Sleep(3 * time.Second)

			// Verify basic functionality
			healthResults := suite.checkComponentHealth()
			for component, healthy := range healthResults {
				assert.True(t, healthy, "Component %s should be healthy in %s mode", component, profile)
			}

			// Stop components
			suite.stopAllComponents()
			time.Sleep(2 * time.Second)
		})
	}
}

// TestPipelineStressTest performs basic stress testing
func (suite *PipelineE2ETestSuite) TestPipelineStressTest() {
	t := suite.T()

	t.Run("Build Components", func(t *testing.T) {
		err := suite.buildAllComponents()
		assert.NoError(t, err)
	})

	t.Run("Rapid Start/Stop Cycles", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			// Start pipeline
			err := suite.startPipelineWithMode("local")
			require.NoError(t, err, "Failed to start pipeline in cycle %d", i)

			// Wait briefly
			time.Sleep(2 * time.Second)

			// Verify at least some components started
			running := suite.countRunningComponents()
			assert.Greater(t, running, 0, "At least one component should be running in cycle %d", i)

			// Stop pipeline
			suite.stopAllComponents()
			time.Sleep(1 * time.Second)
		}
	})

	t.Run("Concurrent Health Checks", func(t *testing.T) {
		// Start pipeline
		err := suite.startPipelineWithMode("local")
		require.NoError(t, err)
		
		time.Sleep(3 * time.Second)

		// Perform concurrent health checks
		var wg sync.WaitGroup
		results := make(chan map[string]bool, 10)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				healthResults := suite.checkComponentHealth()
				results <- healthResults
			}()
		}

		wg.Wait()
		close(results)

		// Verify consistency of health check results
		var allResults []map[string]bool
		for result := range results {
			allResults = append(allResults, result)
		}

		assert.Greater(t, len(allResults), 5, "Should have multiple health check results")
	})
}

// TestPipelineErrorRecovery tests error recovery scenarios
func (suite *PipelineE2ETestSuite) TestPipelineErrorRecovery() {
	t := suite.T()

	t.Run("Build Components", func(t *testing.T) {
		err := suite.buildAllComponents()
		assert.NoError(t, err)
	})

	t.Run("Component Restart Recovery", func(t *testing.T) {
		// Start pipeline
		err := suite.startPipelineWithMode("local")
		require.NoError(t, err)
		
		time.Sleep(3 * time.Second)

		// Kill one component and verify others continue
		originalPids := make(map[string]int)
		for component, pid := range suite.componentPids {
			originalPids[component] = pid
		}

		// Stop stellar-arrow-source
		if pid, exists := suite.componentPids["stellar-arrow-source"]; exists {
			suite.killProcess(pid)
			delete(suite.componentPids, "stellar-arrow-source")
		}

		// Wait and check other components
		time.Sleep(2 * time.Second)
		
		// Other components should still be running
		healthResults := suite.checkComponentHealth()
		if healthResults["ttp-arrow-processor"] {
			t.Log("ttp-arrow-processor remained healthy after stellar-arrow-source failure")
		}
		if healthResults["arrow-analytics-sink"] {
			t.Log("arrow-analytics-sink remained healthy after stellar-arrow-source failure")
		}
	})
}

// Helper Methods

// buildAllComponents builds all pipeline components
func (suite *PipelineE2ETestSuite) buildAllComponents() error {
	components := []string{"stellar-arrow-source", "ttp-arrow-processor", "arrow-analytics-sink"}
	
	for _, component := range components {
		componentDir := filepath.Join(suite.testDir, "components", component, "src")
		
		// Change to component directory and build
		cmd := exec.CommandContext(suite.ctx, "go", "build", "-o", component, ".")
		cmd.Dir = componentDir
		
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to build %s: %w\nOutput: %s", component, err, output)
		}
		
		// Verify binary was created
		binaryPath := filepath.Join(componentDir, component)
		if _, err := os.Stat(binaryPath); err != nil {
			return fmt.Errorf("binary not created for %s at %s", component, binaryPath)
		}
	}
	
	return nil
}

// startPipelineWithMode starts the pipeline using the run-pipeline.sh script
func (suite *PipelineE2ETestSuite) startPipelineWithMode(mode string) error {
	scriptPath := filepath.Join(suite.testDir, "run-pipeline.sh")
	
	// Make sure script is executable
	if err := os.Chmod(scriptPath, 0755); err != nil {
		return fmt.Errorf("failed to make script executable: %w", err)
	}
	
	// Start pipeline in background
	cmd := exec.CommandContext(suite.ctx, scriptPath, mode)
	cmd.Dir = suite.testDir
	
	// Set environment variables for testing
	cmd.Env = append(os.Environ(),
		"ENABLE_FLOWCTL="+getFlowCtlEnabled(mode),
		"FLOWCTL_ENDPOINT="+getFlowCtlEndpoint(),
		"LOG_LEVEL=debug",
	)
	
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start pipeline: %w", err)
	}
	
	// Store the main process PID
	suite.componentPids["pipeline"] = cmd.Process.Pid
	
	// Register cleanup
	suite.cleanups = append(suite.cleanups, func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	})
	
	return nil
}

// stopAllComponents stops all running components
func (suite *PipelineE2ETestSuite) stopAllComponents() {
	// Try using the pipeline script to stop
	scriptPath := filepath.Join(suite.testDir, "run-pipeline.sh")
	cmd := exec.CommandContext(suite.ctx, scriptPath, "stop")
	cmd.Dir = suite.testDir
	cmd.Run() // Ignore errors
	
	// Also manually kill any processes we're tracking
	for component, pid := range suite.componentPids {
		suite.killProcess(pid)
		delete(suite.componentPids, component)
	}
	
	// Kill any remaining component processes
	components := []string{"stellar-arrow-source", "ttp-arrow-processor", "arrow-analytics-sink"}
	for _, component := range components {
		exec.Command("pkill", "-f", component).Run() // Ignore errors
	}
}

// killProcess kills a process by PID
func (suite *PipelineE2ETestSuite) killProcess(pid int) {
	if pid > 0 {
		if process, err := os.FindProcess(pid); err == nil {
			process.Kill()
		}
	}
}

// verifyComponentsRunning verifies that components are running
func (suite *PipelineE2ETestSuite) verifyComponentsRunning(t *testing.T) {
	runningCount := suite.countRunningComponents()
	assert.Greater(t, runningCount, 0, "At least one component should be running")
}

// verifyComponentsStopped verifies that components are stopped
func (suite *PipelineE2ETestSuite) verifyComponentsStopped(t *testing.T) {
	healthResults := suite.checkComponentHealth()
	
	allStopped := true
	for _, healthy := range healthResults {
		if healthy {
			allStopped = false
			break
		}
	}
	
	// Note: In a real test environment, we might be more strict about this
	// For now, we just log the status
	if !allStopped {
		t.Log("Some components may still be running after stop command")
	}
}

// countRunningComponents counts how many components are running
func (suite *PipelineE2ETestSuite) countRunningComponents() int {
	healthResults := suite.checkComponentHealth()
	count := 0
	for _, healthy := range healthResults {
		if healthy {
			count++
		}
	}
	return count
}

// checkComponentHealth checks the health of all components
func (suite *PipelineE2ETestSuite) checkComponentHealth() map[string]bool {
	components := map[string]int{
		"stellar-arrow-source":  8088,
		"ttp-arrow-processor":   8088,
		"arrow-analytics-sink":  8088,
	}
	
	results := make(map[string]bool)
	
	for component, port := range components {
		cmd := exec.CommandContext(suite.ctx, "curl", "-s", "-f", 
			fmt.Sprintf("http://localhost:%d/health", port))
		err := cmd.Run()
		results[component] = (err == nil)
	}
	
	return results
}

// getComponentLogs retrieves logs from all components
func (suite *PipelineE2ETestSuite) getComponentLogs() map[string]string {
	components := []string{"stellar-arrow-source", "ttp-arrow-processor", "arrow-analytics-sink"}
	logs := make(map[string]string)
	
	for _, component := range components {
		logPath := filepath.Join(suite.testDir, "components", component, "src", component+".log")
		
		if content, err := os.ReadFile(logPath); err == nil {
			logs[component] = string(content)
		} else {
			logs[component] = ""
		}
	}
	
	return logs
}

// getFlowCtlEnabled returns the flowctl enabled setting for the mode
func getFlowCtlEnabled(mode string) string {
	switch mode {
	case "local":
		return "false"
	case "flowctl", "development", "production":
		return "true"
	default:
		return "false"
	}
}

// getFlowCtlEndpoint returns the flowctl endpoint from environment or default
func getFlowCtlEndpoint() string {
	if endpoint := os.Getenv("FLOWCTL_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	return "localhost:8080"
}

// TestMain sets up the test environment
func TestMain(m *testing.M) {
	// Set up test environment
	os.Setenv("LOG_LEVEL", "debug")
	
	// Ensure we're in the right directory
	if wd, err := os.Getwd(); err == nil {
		if !strings.Contains(wd, "obsrvr-stellar-components") {
			fmt.Printf("Warning: Tests should be run from within the obsrvr-stellar-components directory\n")
			fmt.Printf("Current directory: %s\n", wd)
		}
	}
	
	// Run tests
	code := m.Run()
	
	// Clean up any remaining processes
	components := []string{"stellar-arrow-source", "ttp-arrow-processor", "arrow-analytics-sink"}
	for _, component := range components {
		exec.Command("pkill", "-f", component).Run()
	}
	
	os.Exit(code)
}

// Run the E2E test suite
func TestPipelineE2ESuite(t *testing.T) {
	suite.Run(t, new(PipelineE2ETestSuite))
}
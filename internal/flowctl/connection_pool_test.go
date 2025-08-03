package flowctl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test ConnectionPoolConfig
func TestDefaultConnectionPoolConfig(t *testing.T) {
	config := DefaultConnectionPoolConfig()
	
	assert.Equal(t, 10, config.MaxConnections)
	assert.Equal(t, 2, config.MinConnections)
	assert.Equal(t, 5*time.Minute, config.IdleTimeout)
	assert.Equal(t, 10*time.Second, config.ConnectionTimeout)
	assert.Equal(t, 3, config.MaxRetries)
	assert.Equal(t, 30*time.Second, config.HealthCheckInterval)
	assert.True(t, config.EnableLoadBalancing)
}

// Test ConnectionPool Creation
func TestNewConnectionPool(t *testing.T) {
	t.Run("With default config", func(t *testing.T) {
		pool := NewConnectionPool(nil)
		
		assert.NotNil(t, pool)
		assert.NotNil(t, pool.config)
		assert.NotNil(t, pool.pools)
		assert.NotNil(t, pool.stopChan)
		assert.False(t, pool.isRunning)
		
		// Verify default config was used
		assert.Equal(t, 10, pool.config.MaxConnections)
		assert.Equal(t, 2, pool.config.MinConnections)
	})
	
	t.Run("With custom config", func(t *testing.T) {
		customConfig := &ConnectionPoolConfig{
			MaxConnections:      15,
			MinConnections:      3,
			IdleTimeout:         2 * time.Minute,
			ConnectionTimeout:   5 * time.Second,
			MaxRetries:          2,
			HealthCheckInterval: 20 * time.Second,
			EnableLoadBalancing: false,
		}
		
		pool := NewConnectionPool(customConfig)
		
		assert.Equal(t, customConfig, pool.config)
		assert.Equal(t, 15, pool.config.MaxConnections)
		assert.Equal(t, 3, pool.config.MinConnections)
		assert.False(t, pool.config.EnableLoadBalancing)
	})
}

// Test ConnectionPool Lifecycle
func TestConnectionPool_Lifecycle(t *testing.T) {
	pool := NewConnectionPool(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	t.Run("Start pool", func(t *testing.T) {
		err := pool.Start(ctx)
		assert.NoError(t, err)
		assert.True(t, pool.isRunning)
	})
	
	t.Run("Start already running pool", func(t *testing.T) {
		err := pool.Start(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already running")
	})
	
	t.Run("Stop pool", func(t *testing.T) {
		pool.Stop()
		assert.False(t, pool.isRunning)
	})
	
	t.Run("Stop already stopped pool", func(t *testing.T) {
		// Should not panic or error
		pool.Stop()
		assert.False(t, pool.isRunning)
	})
}

// Test Endpoint Pool Creation
func TestConnectionPool_EndpointPoolCreation(t *testing.T) {
	pool := NewConnectionPool(&ConnectionPoolConfig{
		MaxConnections:      5,
		MinConnections:      1,
		IdleTimeout:         1 * time.Minute,
		ConnectionTimeout:   2 * time.Second,
		MaxRetries:          2,
		HealthCheckInterval: 10 * time.Second,
		EnableLoadBalancing: true,
	})
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	err := pool.Start(ctx)
	require.NoError(t, err)
	defer pool.Stop()
	
	// Note: This test would require a running gRPC server to test actual connections
	// For unit testing, we test the pool structure creation
	
	endpoint := "localhost:9999" // Non-existent endpoint for testing
	
	// Attempting to get connection should create endpoint pool
	_, err = pool.GetConnection(endpoint)
	assert.Error(t, err) // Expected to fail since no server is running
	
	// But the endpoint pool should be created
	pool.mu.RLock()
	endpointPool, exists := pool.pools[endpoint]
	pool.mu.RUnlock()
	
	assert.True(t, exists)
	assert.NotNil(t, endpointPool)
	assert.Equal(t, endpoint, endpointPool.endpoint)
	assert.Equal(t, pool.config, endpointPool.config)
}

// Test Pool Statistics
func TestConnectionPool_GetPoolStats(t *testing.T) {
	pool := NewConnectionPool(&ConnectionPoolConfig{
		MaxConnections:      5,
		MinConnections:      1,
		IdleTimeout:         1 * time.Minute,
		ConnectionTimeout:   1 * time.Second, // Short timeout for testing
		MaxRetries:          1,
		HealthCheckInterval: 10 * time.Second,
		EnableLoadBalancing: true,
	})
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	err := pool.Start(ctx)
	require.NoError(t, err)
	defer pool.Stop()
	
	// Try to get connections (will fail but create pools)
	endpoints := []string{"localhost:9991", "localhost:9992"}
	for _, endpoint := range endpoints {
		pool.GetConnection(endpoint) // Ignore errors
	}
	
	stats := pool.GetPoolStats()
	
	// Should have stats for both endpoints
	assert.Len(t, stats, 2)
	
	for _, endpoint := range endpoints {
		stat, exists := stats[endpoint]
		assert.True(t, exists)
		assert.Equal(t, endpoint, stat.Endpoint)
		// Note: Actual connection counts will be 0 since no servers are running
	}
}

// Test PooledConnection Usage Tracking
func TestPooledConnection_UsageTracking(t *testing.T) {
	conn := &PooledConnection{
		endpoint:  "test:8080",
		created:   time.Now(),
		lastUsed:  time.Now().Add(-1 * time.Hour),
		useCount:  0,
		isHealthy: true,
	}
	
	originalLastUsed := conn.lastUsed
	originalUseCount := conn.useCount
	
	// Record usage
	conn.recordUsage()
	
	// Verify usage was recorded
	assert.True(t, conn.lastUsed.After(originalLastUsed))
	assert.Equal(t, originalUseCount+1, conn.useCount)
}

// Test Connection Health Checking
func TestPooledConnection_HealthCheck(t *testing.T) {
	t.Run("Nil connection", func(t *testing.T) {
		conn := &PooledConnection{
			conn:      nil,
			isHealthy: true,
		}
		
		healthy := conn.isConnectionHealthy()
		assert.False(t, healthy)
		assert.False(t, conn.isHealthy)
	})
	
	// Note: Testing with actual gRPC connections would require more complex setup
	// The health checking logic primarily depends on gRPC connection state
}

// Test EndpointPool Round Robin Load Balancing
func TestEndpointPool_RoundRobinLoadBalancing(t *testing.T) {
	config := &ConnectionPoolConfig{
		MaxConnections:      3,
		MinConnections:      1,
		EnableLoadBalancing: true,
	}
	
	pool := &endpointPool{
		endpoint:    "test:8080",
		connections: make([]*PooledConnection, 0),
		roundRobin:  0,
		config:      config,
	}
	
	// Create mock connections (without actual gRPC connections)
	for i := 0; i < 3; i++ {
		conn := &PooledConnection{
			endpoint:  pool.endpoint,
			created:   time.Now(),
			lastUsed:  time.Now(),
			useCount:  0,
			isHealthy: true,
		}
		pool.connections = append(pool.connections, conn)
	}
	
	// Test round-robin selection
	selectedConnections := make([]*PooledConnection, 0)
	for i := 0; i < 6; i++ { // Test two full rounds
		conn := pool.getHealthyConnectionRoundRobin()
		assert.NotNil(t, conn)
		selectedConnections = append(selectedConnections, conn)
	}
	
	// Verify round-robin behavior
	assert.Equal(t, selectedConnections[0], selectedConnections[3]) // Should cycle back
	assert.Equal(t, selectedConnections[1], selectedConnections[4])
	assert.Equal(t, selectedConnections[2], selectedConnections[5])
	
	// Verify different connections are selected in sequence
	assert.NotEqual(t, selectedConnections[0], selectedConnections[1])
	assert.NotEqual(t, selectedConnections[1], selectedConnections[2])
}

// Test EndpointPool Least Used Selection
func TestEndpointPool_LeastUsedSelection(t *testing.T) {
	config := &ConnectionPoolConfig{
		MaxConnections: 3,
		MinConnections: 1,
	}
	
	pool := &endpointPool{
		endpoint:    "test:8080",
		connections: make([]*PooledConnection, 0),
		config:      config,
	}
	
	// Create connections with different use counts
	useCounts := []int32{10, 5, 15}
	for i, useCount := range useCounts {
		conn := &PooledConnection{
			endpoint:  pool.endpoint,
			created:   time.Now(),
			lastUsed:  time.Now(),
			useCount:  useCount,
			isHealthy: true,
		}
		pool.connections = append(pool.connections, conn)
	}
	
	// Get least used connection
	leastUsed := pool.getLeastUsedConnection()
	assert.NotNil(t, leastUsed)
	assert.Equal(t, int32(5), leastUsed.useCount) // Should be the one with use count 5
}

// Test EndpointPool Statistics
func TestEndpointPool_Statistics(t *testing.T) {
	config := &ConnectionPoolConfig{
		MaxConnections: 5,
		MinConnections: 2,
	}
	
	pool := &endpointPool{
		endpoint:    "test:8080",
		connections: make([]*PooledConnection, 0),
		config:      config,
	}
	
	// Create test connections
	now := time.Now()
	connections := []*PooledConnection{
		{
			endpoint:  pool.endpoint,
			created:   now.Add(-10 * time.Minute),
			lastUsed:  now.Add(-2 * time.Minute), // Idle
			useCount:  25,
			isHealthy: true,
		},
		{
			endpoint:  pool.endpoint,
			created:   now.Add(-5 * time.Minute),
			lastUsed:  now.Add(-30 * time.Second), // Active
			useCount:  15,
			isHealthy: true,
		},
		{
			endpoint:  pool.endpoint,
			created:   now.Add(-15 * time.Minute),
			lastUsed:  now.Add(-3 * time.Minute), // Idle
			useCount:  40,
			isHealthy: false, // Unhealthy
		},
	}
	
	pool.connections = connections
	
	stats := pool.getStats()
	
	assert.Equal(t, "test:8080", stats.Endpoint)
	assert.Equal(t, 3, stats.TotalConnections)
	assert.Equal(t, 2, stats.HealthyConnections) // 2 healthy connections
	assert.Equal(t, 2, stats.IdleConnections)    // 2 connections idle > 1 minute
	assert.Equal(t, int32(80), stats.TotalUseCount) // 25 + 15 + 40
	assert.Equal(t, float64(80)/float64(3), stats.AverageUseCount)
	assert.True(t, stats.OldestConnection >= 15*time.Minute)
}

// Test Maintenance Operations
func TestEndpointPool_Maintenance(t *testing.T) {
	config := &ConnectionPoolConfig{
		MaxConnections: 5,
		MinConnections: 2,
		IdleTimeout:    2 * time.Minute,
	}
	
	pool := &endpointPool{
		endpoint:    "test:8080",
		connections: make([]*PooledConnection, 0),
		config:      config,
	}
	
	now := time.Now()
	
	// Create connections with different states
	connections := []*PooledConnection{
		{
			endpoint:  pool.endpoint,
			created:   now,
			lastUsed:  now.Add(-5 * time.Minute), // Old idle - should be removed
			useCount:  10,
			isHealthy: true,
		},
		{
			endpoint:  pool.endpoint,
			created:   now,
			lastUsed:  now.Add(-30 * time.Second), // Recent - should be kept
			useCount:  5,
			isHealthy: true,
		},
		{
			endpoint:  pool.endpoint,
			created:   now,
			lastUsed:  now.Add(-1 * time.Minute), // Recent - should be kept
			useCount:  8,
			isHealthy: true,
		},
	}
	
	pool.connections = connections
	
	// Run maintenance
	pool.maintenance()
	
	// Should keep at least MinConnections (2) even if they're idle
	// In this case, we have one old idle connection that should be removed,
	// leaving 2 connections which meets the minimum
	assert.LessOrEqual(t, len(pool.connections), 2)
	assert.GreaterOrEqual(t, len(pool.connections), config.MinConnections)
	
	// Verify remaining connections are the more recent ones
	for _, conn := range pool.connections {
		assert.True(t, now.Sub(conn.lastUsed) <= 2*time.Minute)
	}
}

// Test Connection Pool Resource Management
func TestConnectionPool_ResourceManagement(t *testing.T) {
	// Test with very small limits for easier testing
	config := &ConnectionPoolConfig{
		MaxConnections:      2,
		MinConnections:      1,
		IdleTimeout:         1 * time.Second,
		ConnectionTimeout:   100 * time.Millisecond, // Very short for quick failure
		MaxRetries:          1,
		HealthCheckInterval: 500 * time.Millisecond,
		EnableLoadBalancing: true,
	}
	
	pool := NewConnectionPool(config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	err := pool.Start(ctx)
	require.NoError(t, err)
	defer pool.Stop()
	
	endpoint := "localhost:9999" // Non-existent endpoint
	
	// Try to get multiple connections
	for i := 0; i < 3; i++ {
		_, err := pool.GetConnection(endpoint)
		// Should fail due to no server, but pool structure should be created
		assert.Error(t, err)
	}
	
	// Verify endpoint pool was created with limits
	pool.mu.RLock()
	endpointPool, exists := pool.pools[endpoint]
	pool.mu.RUnlock()
	
	assert.True(t, exists)
	assert.NotNil(t, endpointPool)
	
	// Check that pool respects MaxConnections limit
	endpointPool.mu.RLock()
	connectionCount := len(endpointPool.connections)
	endpointPool.mu.RUnlock()
	
	assert.LessOrEqual(t, connectionCount, config.MaxConnections)
}

// Test Concurrent Access
func TestConnectionPool_ConcurrentAccess(t *testing.T) {
	pool := NewConnectionPool(&ConnectionPoolConfig{
		MaxConnections:      10,
		MinConnections:      2,
		IdleTimeout:         1 * time.Minute,
		ConnectionTimeout:   100 * time.Millisecond,
		MaxRetries:          1,
		HealthCheckInterval: 1 * time.Second,
		EnableLoadBalancing: true,
	})
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	err := pool.Start(ctx)
	require.NoError(t, err)
	defer pool.Stop()
	
	endpoint := "localhost:9999"
	
	// Launch multiple goroutines to access the pool concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()
			
			// Try to get connection multiple times
			for j := 0; j < 5; j++ {
				pool.GetConnection(endpoint)
				time.Sleep(10 * time.Millisecond)
			}
			
			// Get pool stats
			pool.GetPoolStats()
		}()
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent operations")
		}
	}
	
	// Verify pool state is still consistent
	stats := pool.GetPoolStats()
	assert.NotNil(t, stats)
}

// Benchmark Tests
func BenchmarkConnectionPool_GetConnection(b *testing.B) {
	pool := NewConnectionPool(&ConnectionPoolConfig{
		MaxConnections:      10,
		MinConnections:      2,
		ConnectionTimeout:   50 * time.Millisecond, // Short timeout for quick failure
	})
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	pool.Start(ctx)
	defer pool.Stop()
	
	endpoint := "localhost:9999" // Non-existent endpoint
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.GetConnection(endpoint) // Will fail but tests the pool logic
	}
}

func BenchmarkConnectionPool_GetPoolStats(b *testing.B) {
	pool := NewConnectionPool(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	pool.Start(ctx)
	defer pool.Stop()
	
	// Create some endpoint pools
	endpoints := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	for _, endpoint := range endpoints {
		pool.GetConnection(endpoint) // Will fail but creates pools
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.GetPoolStats()
	}
}

func BenchmarkEndpointPool_RoundRobin(b *testing.B) {
	config := &ConnectionPoolConfig{
		MaxConnections:      10,
		EnableLoadBalancing: true,
	}
	
	pool := &endpointPool{
		endpoint:    "test:8080",
		connections: make([]*PooledConnection, 10),
		config:      config,
	}
	
	// Create mock healthy connections
	for i := 0; i < 10; i++ {
		pool.connections[i] = &PooledConnection{
			isHealthy: true,
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.getHealthyConnectionRoundRobin()
	}
}
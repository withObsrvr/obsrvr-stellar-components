package flowctl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// ========================================================================
// Connection Pooling and Optimization for Arrow Flight Services
// ========================================================================

// ConnectionPoolConfig configures connection pool behavior
type ConnectionPoolConfig struct {
	MaxConnections      int           // Maximum connections per endpoint
	MinConnections      int           // Minimum connections to maintain
	IdleTimeout         time.Duration // How long to keep idle connections
	ConnectionTimeout   time.Duration // Timeout for establishing connections
	MaxRetries          int           // Maximum retry attempts
	HealthCheckInterval time.Duration // How often to check connection health
	EnableLoadBalancing bool          // Whether to load balance across connections
}

// DefaultConnectionPoolConfig returns sensible defaults
func DefaultConnectionPoolConfig() *ConnectionPoolConfig {
	return &ConnectionPoolConfig{
		MaxConnections:      10,
		MinConnections:      2,
		IdleTimeout:         5 * time.Minute,
		ConnectionTimeout:   10 * time.Second,
		MaxRetries:          3,
		HealthCheckInterval: 30 * time.Second,
		EnableLoadBalancing: true,
	}
}

// PooledConnection represents a connection in the pool
type PooledConnection struct {
	conn        *grpc.ClientConn
	client      flight.FlightServiceClient
	endpoint    string
	created     time.Time
	lastUsed    time.Time
	useCount    int32
	isHealthy   bool
	mu          sync.RWMutex
}

// ConnectionPool manages a pool of gRPC connections to Arrow Flight services
type ConnectionPool struct {
	config      *ConnectionPoolConfig
	pools       map[string]*endpointPool // endpoint -> pool
	mu          sync.RWMutex
	stopChan    chan struct{}
	wg          sync.WaitGroup
	isRunning   bool
}

// endpointPool manages connections for a specific endpoint
type endpointPool struct {
	endpoint    string
	connections []*PooledConnection
	roundRobin  int
	mu          sync.RWMutex
	config      *ConnectionPoolConfig
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(config *ConnectionPoolConfig) *ConnectionPool {
	if config == nil {
		config = DefaultConnectionPoolConfig()
	}
	
	return &ConnectionPool{
		config:    config,
		pools:     make(map[string]*endpointPool),
		stopChan:  make(chan struct{}),
		isRunning: false,
	}
}

// Start begins the connection pool maintenance routines
func (cp *ConnectionPool) Start(ctx context.Context) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	if cp.isRunning {
		return fmt.Errorf("connection pool is already running")
	}
	
	cp.isRunning = true
	
	// Start maintenance routine
	cp.wg.Add(1)
	go cp.maintenanceLoop(ctx)
	
	log.Info().
		Int("max_connections", cp.config.MaxConnections).
		Dur("idle_timeout", cp.config.IdleTimeout).
		Msg("Started connection pool")
	
	return nil
}

// Stop gracefully shuts down the connection pool
func (cp *ConnectionPool) Stop() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	if !cp.isRunning {
		return
	}
	
	cp.isRunning = false
	close(cp.stopChan)
	cp.wg.Wait()
	
	// Close all connections
	for _, pool := range cp.pools {
		pool.closeAll()
	}
	
	log.Info().Msg("Stopped connection pool")
}

// GetConnection retrieves a connection from the pool or creates a new one
func (cp *ConnectionPool) GetConnection(endpoint string) (flight.FlightServiceClient, error) {
	cp.mu.RLock()
	pool, exists := cp.pools[endpoint]
	cp.mu.RUnlock()
	
	if !exists {
		cp.mu.Lock()
		// Double-check after acquiring write lock
		if pool, exists = cp.pools[endpoint]; !exists {
			pool = &endpointPool{
				endpoint:    endpoint,
				connections: make([]*PooledConnection, 0, cp.config.MaxConnections),
				config:      cp.config,
			}
			cp.pools[endpoint] = pool
		}
		cp.mu.Unlock()
	}
	
	return pool.getConnection()
}

// ReturnConnection returns a connection to the pool (currently a no-op for simplicity)
func (cp *ConnectionPool) ReturnConnection(endpoint string, client flight.FlightServiceClient) {
	// In a more sophisticated implementation, this could track usage and potentially
	// close connections that are no longer needed
	log.Debug().Str("endpoint", endpoint).Msg("Connection returned to pool")
}

// GetPoolStats returns statistics for all connection pools
func (cp *ConnectionPool) GetPoolStats() map[string]PoolStats {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	stats := make(map[string]PoolStats)
	for endpoint, pool := range cp.pools {
		stats[endpoint] = pool.getStats()
	}
	
	return stats
}

// PoolStats represents statistics for a connection pool
type PoolStats struct {
	Endpoint        string
	TotalConnections int
	HealthyConnections int
	IdleConnections   int
	AverageUseCount   float64
	OldestConnection  time.Duration
	TotalUseCount     int32
}

// maintenanceLoop performs periodic maintenance on connection pools
func (cp *ConnectionPool) maintenanceLoop(ctx context.Context) {
	defer cp.wg.Done()
	
	ticker := time.NewTicker(cp.config.HealthCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-cp.stopChan:
			return
		case <-ticker.C:
			cp.performMaintenance()
		}
	}
}

// performMaintenance cleans up idle connections and checks health
func (cp *ConnectionPool) performMaintenance() {
	cp.mu.RLock()
	pools := make([]*endpointPool, 0, len(cp.pools))
	for _, pool := range cp.pools {
		pools = append(pools, pool)
	}
	cp.mu.RUnlock()
	
	for _, pool := range pools {
		pool.maintenance()
	}
}

// endpointPool methods

// getConnection gets a connection from this endpoint's pool
func (ep *endpointPool) getConnection() (flight.FlightServiceClient, error) {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	
	// Try to find an existing healthy connection
	if ep.config.EnableLoadBalancing {
		if conn := ep.getHealthyConnectionRoundRobin(); conn != nil {
			conn.recordUsage()
			return conn.client, nil
		}
	} else {
		if conn := ep.getHealthyConnection(); conn != nil {
			conn.recordUsage()
			return conn.client, nil
		}
	}
	
	// Create new connection if under limit
	if len(ep.connections) < ep.config.MaxConnections {
		conn, err := ep.createConnection()
		if err != nil {
			return nil, fmt.Errorf("failed to create connection to %s: %w", ep.endpoint, err)
		}
		
		ep.connections = append(ep.connections, conn)
		conn.recordUsage()
		return conn.client, nil
	}
	
	// All connections are unhealthy or at limit, try to use least-used one
	if conn := ep.getLeastUsedConnection(); conn != nil {
		conn.recordUsage()
		return conn.client, nil
	}
	
	return nil, fmt.Errorf("no available connections to %s", ep.endpoint)
}

// getHealthyConnection returns the first healthy connection
func (ep *endpointPool) getHealthyConnection() *PooledConnection {
	for _, conn := range ep.connections {
		if conn.isConnectionHealthy() {
			return conn
		}
	}
	return nil
}

// getHealthyConnectionRoundRobin returns a healthy connection using round-robin
func (ep *endpointPool) getHealthyConnectionRoundRobin() *PooledConnection {
	if len(ep.connections) == 0 {
		return nil
	}
	
	start := ep.roundRobin
	for i := 0; i < len(ep.connections); i++ {
		idx := (start + i) % len(ep.connections)
		conn := ep.connections[idx]
		if conn.isConnectionHealthy() {
			ep.roundRobin = (idx + 1) % len(ep.connections)
			return conn
		}
	}
	
	return nil
}

// getLeastUsedConnection returns the connection with the lowest use count
func (ep *endpointPool) getLeastUsedConnection() *PooledConnection {
	if len(ep.connections) == 0 {
		return nil
	}
	
	var bestConn *PooledConnection
	var minUseCount int32 = -1
	
	for _, conn := range ep.connections {
		if minUseCount == -1 || conn.useCount < minUseCount {
			minUseCount = conn.useCount
			bestConn = conn
		}
	}
	
	return bestConn
}

// createConnection creates a new connection to the endpoint
func (ep *endpointPool) createConnection() (*PooledConnection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ep.config.ConnectionTimeout)
	defer cancel()
	
	// Create gRPC connection
	conn, err := grpc.DialContext(ctx, ep.endpoint, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Wait for connection to be ready
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", ep.endpoint, err)
	}
	
	// Create Flight client
	client := flight.NewFlightServiceClient(conn)
	
	pooledConn := &PooledConnection{
		conn:      conn,
		client:    client,
		endpoint:  ep.endpoint,
		created:   time.Now(),
		lastUsed:  time.Now(),
		useCount:  0,
		isHealthy: true,
	}
	
	log.Debug().
		Str("endpoint", ep.endpoint).
		Msg("Created new pooled connection")
	
	return pooledConn, nil
}

// maintenance performs maintenance on this endpoint's connections
func (ep *endpointPool) maintenance() {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	
	now := time.Now()
	var healthyConnections []*PooledConnection
	
	// Check each connection
	for _, conn := range ep.connections {
		// Remove idle connections
		if now.Sub(conn.lastUsed) > ep.config.IdleTimeout && len(ep.connections) > ep.config.MinConnections {
			conn.close()
			log.Debug().
				Str("endpoint", ep.endpoint).
				Dur("idle_time", now.Sub(conn.lastUsed)).
				Msg("Closed idle connection")
			continue
		}
		
		// Check connection health
		if !conn.isConnectionHealthy() {
			log.Warn().
				Str("endpoint", ep.endpoint).
				Msg("Detected unhealthy connection, attempting to reconnect")
			
			if err := conn.reconnect(); err != nil {
				conn.close()
				log.Error().
					Err(err).
					Str("endpoint", ep.endpoint).
					Msg("Failed to reconnect, closing connection")
				continue
			}
		}
		
		healthyConnections = append(healthyConnections, conn)
	}
	
	ep.connections = healthyConnections
	
	// Ensure minimum connections
	for len(ep.connections) < ep.config.MinConnections {
		conn, err := ep.createConnection()
		if err != nil {
			log.Error().
				Err(err).
				Str("endpoint", ep.endpoint).
				Msg("Failed to create minimum connection")
			break
		}
		ep.connections = append(ep.connections, conn)
	}
}

// getStats returns statistics for this endpoint pool
func (ep *endpointPool) getStats() PoolStats {
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	
	stats := PoolStats{
		Endpoint:         ep.endpoint,
		TotalConnections: len(ep.connections),
	}
	
	if len(ep.connections) == 0 {
		return stats
	}
	
	var totalUseCount int32
	var healthyCount int
	var idleCount int
	var oldestAge time.Duration
	now := time.Now()
	
	for _, conn := range ep.connections {
		totalUseCount += conn.useCount
		
		if conn.isConnectionHealthy() {
			healthyCount++
		}
		
		if now.Sub(conn.lastUsed) > time.Minute {
			idleCount++
		}
		
		age := now.Sub(conn.created)
		if age > oldestAge {
			oldestAge = age
		}
	}
	
	stats.HealthyConnections = healthyCount
	stats.IdleConnections = idleCount
	stats.TotalUseCount = totalUseCount
	stats.AverageUseCount = float64(totalUseCount) / float64(len(ep.connections))
	stats.OldestConnection = oldestAge
	
	return stats
}

// closeAll closes all connections in this pool
func (ep *endpointPool) closeAll() {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	
	for _, conn := range ep.connections {
		conn.close()
	}
	ep.connections = ep.connections[:0]
}

// PooledConnection methods

// recordUsage updates usage statistics for this connection
func (pc *PooledConnection) recordUsage() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	
	pc.lastUsed = time.Now()
	pc.useCount++
}

// isConnectionHealthy checks if the underlying gRPC connection is healthy
func (pc *PooledConnection) isConnectionHealthy() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	
	if pc.conn == nil {
		pc.isHealthy = false
		return false
	}
	
	state := pc.conn.GetState()
	healthy := state == connectivity.Ready || state == connectivity.Idle
	pc.isHealthy = healthy
	
	return healthy
}

// reconnect attempts to reconnect this connection
func (pc *PooledConnection) reconnect() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	
	// Close existing connection
	if pc.conn != nil {
		pc.conn.Close()
	}
	
	// Create new connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	conn, err := grpc.DialContext(ctx, pc.endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to reconnect to %s: %w", pc.endpoint, err)
	}
	
	pc.conn = conn
	pc.client = flight.NewFlightServiceClient(conn)
	pc.isHealthy = true
	pc.lastUsed = time.Now()
	
	log.Info().
		Str("endpoint", pc.endpoint).
		Msg("Successfully reconnected pooled connection")
	
	return nil
}

// close closes this connection
func (pc *PooledConnection) close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
		pc.client = nil
		pc.isHealthy = false
	}
}
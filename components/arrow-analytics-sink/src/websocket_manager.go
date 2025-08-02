package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

// WebSocketManager manages WebSocket connections for real-time streaming
type WebSocketManager struct {
	// Configuration
	config *WebSocketConfig
	
	// Connection management
	connections    map[string]*WebSocketConnection
	connectionsMu  sync.RWMutex
	connectionPool chan *WebSocketConnection
	
	// Broadcasting
	broadcast    chan *BroadcastMessage
	
	// Metrics and monitoring
	metrics      *WebSocketMetrics
	
	// Lifecycle management
	ctx          context.Context
	cancel       context.CancelFunc
	shutdownOnce sync.Once
	
	// Connection authentication
	authenticator ConnectionAuthenticator
	
	// Message routing
	messageRouter *MessageRouter
}

// WebSocketConfig configures the WebSocket manager
type WebSocketConfig struct {
	// Server configuration
	Port            int    `mapstructure:"port"`
	Host            string `mapstructure:"host"`
	Path            string `mapstructure:"path"`
	
	// Connection limits
	MaxConnections          int           `mapstructure:"max_connections"`
	MaxConnectionsPerIP     int           `mapstructure:"max_connections_per_ip"`
	ConnectionTimeout       time.Duration `mapstructure:"connection_timeout"`
	IdleTimeout            time.Duration `mapstructure:"idle_timeout"`
	
	// Message configuration
	MaxMessageSize         int64         `mapstructure:"max_message_size"`
	WriteTimeout          time.Duration `mapstructure:"write_timeout"`
	ReadTimeout           time.Duration `mapstructure:"read_timeout"`
	PingInterval          time.Duration `mapstructure:"ping_interval"`
	PongTimeout           time.Duration `mapstructure:"pong_timeout"`
	
	// Buffer configuration
	WriteBufferSize       int `mapstructure:"write_buffer_size"`
	ReadBufferSize        int `mapstructure:"read_buffer_size"`
	BroadcastBufferSize   int `mapstructure:"broadcast_buffer_size"`
	
	// Compression
	EnableCompression     bool   `mapstructure:"enable_compression"`
	CompressionLevel      int    `mapstructure:"compression_level"`
	CompressionThreshold  int    `mapstructure:"compression_threshold"`
	
	// Authentication
	RequireAuth          bool   `mapstructure:"require_auth"`
	AuthTimeout          time.Duration `mapstructure:"auth_timeout"`
	
	// Rate limiting
	RateLimitEnabled     bool    `mapstructure:"rate_limit_enabled"`
	MessagesPerSecond    float64 `mapstructure:"messages_per_second"`
	BurstSize           int     `mapstructure:"burst_size"`
	
	// TLS configuration
	TLSEnabled          bool   `mapstructure:"tls_enabled"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
}

// WebSocketConnection represents a single WebSocket connection
type WebSocketConnection struct {
	ID              string
	Conn            *websocket.Conn
	ClientInfo      *ClientInfo
	Subscriptions   map[string]*Subscription
	RateLimiter     *ConnectionRateLimiter
	LastActivity    time.Time
	Connected       time.Time
	
	// Channels
	Send            chan []byte
	
	// State
	authenticated   bool
	mu              sync.RWMutex
	
	// Metrics
	MessagesSent    int64
	MessagesReceived int64
	BytesSent       int64
	BytesReceived   int64
}

// ClientInfo contains information about the WebSocket client
type ClientInfo struct {
	IPAddress    string                 `json:"ip_address"`
	UserAgent    string                 `json:"user_agent"`
	Origin       string                 `json:"origin"`
	UserID       string                 `json:"user_id,omitempty"`
	Roles        []string               `json:"roles,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// Subscription represents a client subscription to data feeds
type Subscription struct {
	Type       string                 `json:"type"`
	Filters    map[string]interface{} `json:"filters"`
	Format     string                 `json:"format"` // "json", "arrow", "compressed"
	CreatedAt  time.Time              `json:"created_at"`
}

// BroadcastMessage represents a message to broadcast to connections
type BroadcastMessage struct {
	Type        string                 `json:"type"`
	Data        interface{}            `json:"data"`
	Timestamp   time.Time              `json:"timestamp"`
	Filters     map[string]interface{} `json:"filters,omitempty"`
	TargetType  string                 `json:"target_type,omitempty"` // "all", "filtered", "specific"
	TargetIDs   []string               `json:"target_ids,omitempty"`
}

// WebSocketMetrics tracks WebSocket performance metrics
type WebSocketMetrics struct {
	// Connection metrics
	ActiveConnections     int64
	TotalConnections      int64
	ConnectionsPerSecond  float64
	FailedConnections     int64
	
	// Message metrics
	MessagesSent          int64
	MessagesReceived      int64
	BroadcastsSent        int64
	MessageErrors         int64
	
	// Data metrics
	BytesSent            int64
	BytesReceived        int64
	
	// Performance metrics
	AverageLatency       time.Duration
	MaxLatency          time.Duration
	
	// Error metrics
	AuthenticationErrors  int64
	RateLimitErrors      int64
	TimeoutErrors        int64
	
	mu                   sync.RWMutex
}

// ConnectionAuthenticator handles WebSocket authentication
type ConnectionAuthenticator interface {
	Authenticate(conn *websocket.Conn, request *http.Request) (*ClientInfo, error)
	IsAuthorized(clientInfo *ClientInfo, subscription *Subscription) bool
}

// DefaultAuthenticator provides basic authentication
type DefaultAuthenticator struct {
	apiKeys map[string]*ClientInfo
	mu      sync.RWMutex
}

// MessageRouter handles message routing and filtering
type MessageRouter struct {
	routes map[string]RouteHandler
	mu     sync.RWMutex
}

// RouteHandler processes messages for specific routes
type RouteHandler func(conn *WebSocketConnection, message []byte) error

// ConnectionRateLimiter limits connection message rate
type ConnectionRateLimiter struct {
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastUpdate time.Time
	mu         sync.Mutex
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Configure based on CORS policy
		return true // In production, implement proper origin checking
	},
	EnableCompression: true,
}

// NewWebSocketManager creates a new WebSocket manager
func NewWebSocketManager(config *WebSocketConfig) *WebSocketManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	manager := &WebSocketManager{
		config:        config,
		connections:   make(map[string]*WebSocketConnection),
		connectionPool: make(chan *WebSocketConnection, config.MaxConnections),
		broadcast:     make(chan *BroadcastMessage, config.BroadcastBufferSize),
		metrics:       &WebSocketMetrics{},
		ctx:           ctx,
		cancel:        cancel,
		authenticator: NewDefaultAuthenticator(),
		messageRouter: NewMessageRouter(),
	}
	
	// Configure upgrader based on config
	upgrader.ReadBufferSize = config.ReadBufferSize
	upgrader.WriteBufferSize = config.WriteBufferSize
	upgrader.EnableCompression = config.EnableCompression
	
	// Start background goroutines
	go manager.broadcastWorker()
	go manager.connectionCleanupWorker()
	go manager.metricsCollector()
	
	log.Info().
		Int("max_connections", config.MaxConnections).
		Bool("auth_required", config.RequireAuth).
		Bool("rate_limit_enabled", config.RateLimitEnabled).
		Msg("WebSocket manager initialized")
	
	return manager
}

// Start starts the WebSocket server
func (wm *WebSocketManager) Start() error {
	// Set up HTTP handler
	mux := http.NewServeMux()
	mux.HandleFunc(wm.config.Path, wm.handleWebSocket)
	mux.HandleFunc("/ws/metrics", wm.handleMetrics)
	mux.HandleFunc("/ws/health", wm.handleHealth)
	
	addr := fmt.Sprintf("%s:%d", wm.config.Host, wm.config.Port)
	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  wm.config.ReadTimeout,
		WriteTimeout: wm.config.WriteTimeout,
		IdleTimeout:  wm.config.IdleTimeout,
	}
	
	log.Info().
		Str("address", addr).
		Str("path", wm.config.Path).
		Msg("Starting WebSocket server")
	
	if wm.config.TLSEnabled {
		return server.ListenAndServeTLS(wm.config.CertFile, wm.config.KeyFile)
	}
	
	return server.ListenAndServe()
}

// handleWebSocket handles WebSocket connection upgrades
func (wm *WebSocketManager) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Check connection limits
	if !wm.canAcceptConnection(r) {
		http.Error(w, "Connection limit exceeded", http.StatusServiceUnavailable)
		atomic.AddInt64(&wm.metrics.FailedConnections, 1)
		return
	}
	
	// Upgrade connection
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to upgrade WebSocket connection")
		atomic.AddInt64(&wm.metrics.FailedConnections, 1)
		return
	}
	
	// Create connection object
	wsConn := wm.createConnection(conn, r)
	
	// Authenticate if required
	if wm.config.RequireAuth {
		clientInfo, err := wm.authenticator.Authenticate(conn, r)
		if err != nil {
			log.Warn().Err(err).Str("ip", r.RemoteAddr).Msg("WebSocket authentication failed")
			conn.Close()
			atomic.AddInt64(&wm.metrics.AuthenticationErrors, 1)
			return
		}
		wsConn.ClientInfo = clientInfo
		wsConn.authenticated = true
	}
	
	// Register connection
	wm.registerConnection(wsConn)
	
	// Start connection handlers
	go wm.connectionReader(wsConn)
	go wm.connectionWriter(wsConn)
	
	log.Info().
		Str("connection_id", wsConn.ID).
		Str("client_ip", wsConn.ClientInfo.IPAddress).
		Msg("WebSocket connection established")
	
	atomic.AddInt64(&wm.metrics.TotalConnections, 1)
	atomic.AddInt64(&wm.metrics.ActiveConnections, 1)
}

// createConnection creates a new WebSocket connection
func (wm *WebSocketManager) createConnection(conn *websocket.Conn, r *http.Request) *WebSocketConnection {
	connectionID := fmt.Sprintf("ws-%d-%d", time.Now().UnixNano(), len(wm.connections))
	
	clientInfo := &ClientInfo{
		IPAddress: r.RemoteAddr,
		UserAgent: r.UserAgent(),
		Origin:    r.Header.Get("Origin"),
		Metadata:  make(map[string]interface{}),
	}
	
	wsConn := &WebSocketConnection{
		ID:            connectionID,
		Conn:          conn,
		ClientInfo:    clientInfo,
		Subscriptions: make(map[string]*Subscription),
		Send:          make(chan []byte, 256),
		Connected:     time.Now(),
		LastActivity:  time.Now(),
	}
	
	// Set up rate limiter if enabled
	if wm.config.RateLimitEnabled {
		wsConn.RateLimiter = &ConnectionRateLimiter{
			tokens:     float64(wm.config.BurstSize),
			maxTokens:  float64(wm.config.BurstSize),
			refillRate: wm.config.MessagesPerSecond,
			lastUpdate: time.Now(),
		}
	}
	
	return wsConn
}

// registerConnection registers a new connection
func (wm *WebSocketManager) registerConnection(conn *WebSocketConnection) {
	wm.connectionsMu.Lock()
	defer wm.connectionsMu.Unlock()
	
	wm.connections[conn.ID] = conn
}

// unregisterConnection removes a connection
func (wm *WebSocketManager) unregisterConnection(connectionID string) {
	wm.connectionsMu.Lock()
	defer wm.connectionsMu.Unlock()
	
	if conn, exists := wm.connections[connectionID]; exists {
		close(conn.Send)
		delete(wm.connections, connectionID)
		atomic.AddInt64(&wm.metrics.ActiveConnections, -1)
		
		log.Debug().
			Str("connection_id", connectionID).
			Msg("WebSocket connection unregistered")
	}
}

// connectionReader handles incoming messages from a WebSocket connection
func (wm *WebSocketManager) connectionReader(conn *WebSocketConnection) {
	defer func() {
		wm.unregisterConnection(conn.ID)
		conn.Conn.Close()
	}()
	
	// Set read limits and timeouts
	conn.Conn.SetReadLimit(wm.config.MaxMessageSize)
	conn.Conn.SetReadDeadline(time.Now().Add(wm.config.ReadTimeout))
	conn.Conn.SetPongHandler(func(string) error {
		conn.Conn.SetReadDeadline(time.Now().Add(wm.config.ReadTimeout))
		conn.LastActivity = time.Now()
		return nil
	})
	
	for {
		// Read message
		messageType, message, err := conn.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Error().Err(err).Str("connection_id", conn.ID).Msg("WebSocket read error")
			}
			break
		}
		
		// Update activity and metrics
		conn.LastActivity = time.Now()
		atomic.AddInt64(&conn.MessagesReceived, 1)
		atomic.AddInt64(&conn.BytesReceived, int64(len(message)))
		atomic.AddInt64(&wm.metrics.MessagesReceived, 1)
		atomic.AddInt64(&wm.metrics.BytesReceived, int64(len(message)))
		
		// Check rate limiting
		if conn.RateLimiter != nil && !conn.RateLimiter.Allow() {
			log.Warn().
				Str("connection_id", conn.ID).
				Msg("Rate limit exceeded")
			atomic.AddInt64(&wm.metrics.RateLimitErrors, 1)
			wm.sendError(conn, "Rate limit exceeded")
			continue
		}
		
		// Handle different message types
		switch messageType {
		case websocket.TextMessage:
			wm.handleTextMessage(conn, message)
		case websocket.BinaryMessage:
			wm.handleBinaryMessage(conn, message)
		case websocket.PingMessage:
			wm.handlePing(conn)
		}
	}
}

// connectionWriter handles outgoing messages to a WebSocket connection
func (wm *WebSocketManager) connectionWriter(conn *WebSocketConnection) {
	ticker := time.NewTicker(wm.config.PingInterval)
	defer func() {
		ticker.Stop()
		conn.Conn.Close()
	}()
	
	for {
		select {
		case message, ok := <-conn.Send:
			conn.Conn.SetWriteDeadline(time.Now().Add(wm.config.WriteTimeout))
			if !ok {
				conn.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			
			// Send message
			if err := conn.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				log.Error().Err(err).Str("connection_id", conn.ID).Msg("WebSocket write error")
				return
			}
			
			// Update metrics
			atomic.AddInt64(&conn.MessagesSent, 1)
			atomic.AddInt64(&conn.BytesSent, int64(len(message)))
			atomic.AddInt64(&wm.metrics.MessagesSent, 1)
			atomic.AddInt64(&wm.metrics.BytesSent, int64(len(message)))
			
		case <-ticker.C:
			conn.Conn.SetWriteDeadline(time.Now().Add(wm.config.WriteTimeout))
			if err := conn.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// handleTextMessage processes incoming text messages
func (wm *WebSocketManager) handleTextMessage(conn *WebSocketConnection, message []byte) {
	var msg map[string]interface{}
	if err := json.Unmarshal(message, &msg); err != nil {
		log.Error().Err(err).Str("connection_id", conn.ID).Msg("Failed to parse JSON message")
		wm.sendError(conn, "Invalid JSON message")
		return
	}
	
	msgType, ok := msg["type"].(string)
	if !ok {
		wm.sendError(conn, "Message type required")
		return
	}
	
	// Route message to appropriate handler
	if handler, exists := wm.messageRouter.routes[msgType]; exists {
		if err := handler(conn, message); err != nil {
			log.Error().Err(err).
				Str("connection_id", conn.ID).
				Str("message_type", msgType).
				Msg("Message handler error")
			wm.sendError(conn, fmt.Sprintf("Handler error: %s", err.Error()))
		}
	} else {
		wm.sendError(conn, fmt.Sprintf("Unknown message type: %s", msgType))
	}
}

// handleBinaryMessage processes incoming binary messages
func (wm *WebSocketManager) handleBinaryMessage(conn *WebSocketConnection, message []byte) {
	// Handle Arrow-formatted messages
	log.Debug().
		Str("connection_id", conn.ID).
		Int("size", len(message)).
		Msg("Received binary message")
	
	// Process Arrow data (placeholder implementation)
	wm.sendConfirmation(conn, "Binary message received")
}

// handlePing responds to ping messages
func (wm *WebSocketManager) handlePing(conn *WebSocketConnection) {
	conn.LastActivity = time.Now()
}

// BroadcastArrowData broadcasts Arrow record data to subscribed connections
func (wm *WebSocketManager) BroadcastArrowData(record arrow.Record, eventType string) {
	// Convert Arrow record to JSON for broadcasting
	jsonData, err := wm.convertArrowToJSON(record)
	if err != nil {
		log.Error().Err(err).Msg("Failed to convert Arrow record to JSON")
		return
	}
	
	message := &BroadcastMessage{
		Type:      eventType,
		Data:      jsonData,
		Timestamp: time.Now(),
		TargetType: "filtered",
		Filters:   map[string]interface{}{"event_type": eventType},
	}
	
	select {
	case wm.broadcast <- message:
		atomic.AddInt64(&wm.metrics.BroadcastsSent, 1)
	default:
		log.Warn().Msg("Broadcast channel full, dropping message")
	}
}

// broadcastWorker processes broadcast messages
func (wm *WebSocketManager) broadcastWorker() {
	for {
		select {
		case message := <-wm.broadcast:
			wm.processBroadcast(message)
		case <-wm.ctx.Done():
			return
		}
	}
}

// processBroadcast sends a broadcast message to appropriate connections
func (wm *WebSocketManager) processBroadcast(message *BroadcastMessage) {
	wm.connectionsMu.RLock()
	defer wm.connectionsMu.RUnlock()
	
	messageBytes, err := json.Marshal(message)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal broadcast message")
		return
	}
	
	sentCount := 0
	for _, conn := range wm.connections {
		if wm.shouldSendToConnection(conn, message) {
			select {
			case conn.Send <- messageBytes:
				sentCount++
			default:
				log.Warn().
					Str("connection_id", conn.ID).
					Msg("Connection send buffer full, dropping message")
			}
		}
	}
	
	log.Debug().
		Str("message_type", message.Type).
		Int("sent_to", sentCount).
		Int("total_connections", len(wm.connections)).
		Msg("Broadcast message processed")
}

// shouldSendToConnection determines if a message should be sent to a connection
func (wm *WebSocketManager) shouldSendToConnection(conn *WebSocketConnection, message *BroadcastMessage) bool {
	// Check target type
	switch message.TargetType {
	case "all":
		return true
	case "specific":
		for _, targetID := range message.TargetIDs {
			if conn.ID == targetID {
				return true
			}
		}
		return false
	case "filtered":
		return wm.matchesFilters(conn, message.Filters)
	default:
		return false
	}
}

// matchesFilters checks if a connection matches broadcast filters
func (wm *WebSocketManager) matchesFilters(conn *WebSocketConnection, filters map[string]interface{}) bool {
	// Check subscriptions
	for _, subscription := range conn.Subscriptions {
		if wm.filtersMatch(subscription.Filters, filters) {
			return true
		}
	}
	return false
}

// filtersMatch checks if two filter sets match
func (wm *WebSocketManager) filtersMatch(subFilters, msgFilters map[string]interface{}) bool {
	for key, value := range msgFilters {
		if subValue, exists := subFilters[key]; exists {
			if subValue != value {
				return false
			}
		}
	}
	return true
}

// convertArrowToJSON converts Arrow record to JSON format
func (wm *WebSocketManager) convertArrowToJSON(record arrow.Record) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, record.NumRows())
	
	for i := int64(0); i < record.NumRows(); i++ {
		row := make(map[string]interface{})
		
		for j, field := range record.Schema().Fields() {
			col := record.Column(j)
			if col.IsValid(int(i)) {
				switch field.Type.ID() {
				case arrow.STRING:
					arr := col.(*array.String)
					row[field.Name] = arr.Value(int(i))
				case arrow.INT64:
					arr := col.(*array.Int64)
					row[field.Name] = arr.Value(int(i))
				case arrow.UINT32:
					arr := col.(*array.Uint32)
					row[field.Name] = arr.Value(int(i))
				case arrow.BOOL:
					arr := col.(*array.Boolean)
					row[field.Name] = arr.Value(int(i))
				case arrow.TIMESTAMP:
					arr := col.(*array.Timestamp)
					timestamp := arr.Value(int(i))
					row[field.Name] = time.Unix(0, int64(timestamp)*1000).Format(time.RFC3339)
				default:
					row[field.Name] = "unsupported_type"
				}
			} else {
				row[field.Name] = nil
			}
		}
		
		result[i] = row
	}
	
	return result, nil
}

// Utility methods
func (wm *WebSocketManager) sendError(conn *WebSocketConnection, message string) {
	errorMsg := map[string]interface{}{
		"type":    "error",
		"message": message,
		"timestamp": time.Now().Format(time.RFC3339),
	}
	
	if data, err := json.Marshal(errorMsg); err == nil {
		select {
		case conn.Send <- data:
		default:
			log.Warn().
				Str("connection_id", conn.ID).
				Msg("Failed to send error message - buffer full")
		}
	}
}

func (wm *WebSocketManager) sendConfirmation(conn *WebSocketConnection, message string) {
	confirmMsg := map[string]interface{}{
		"type":    "confirmation",
		"message": message,
		"timestamp": time.Now().Format(time.RFC3339),
	}
	
	if data, err := json.Marshal(confirmMsg); err == nil {
		select {
		case conn.Send <- data:
		default:
			log.Warn().
				Str("connection_id", conn.ID).
				Msg("Failed to send confirmation - buffer full")
		}
	}
}

// canAcceptConnection checks if a new connection can be accepted
func (wm *WebSocketManager) canAcceptConnection(r *http.Request) bool {
	wm.connectionsMu.RLock()
	defer wm.connectionsMu.RUnlock()
	
	// Check total connection limit
	if len(wm.connections) >= wm.config.MaxConnections {
		return false
	}
	
	// Check per-IP connection limit
	if wm.config.MaxConnectionsPerIP > 0 {
		ipCount := 0
		clientIP := r.RemoteAddr
		for _, conn := range wm.connections {
			if conn.ClientInfo.IPAddress == clientIP {
				ipCount++
			}
		}
		if ipCount >= wm.config.MaxConnectionsPerIP {
			return false
		}
	}
	
	return true
}

// connectionCleanupWorker periodically cleans up idle connections
func (wm *WebSocketManager) connectionCleanupWorker() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			wm.cleanupIdleConnections()
		case <-wm.ctx.Done():
			return
		}
	}
}

func (wm *WebSocketManager) cleanupIdleConnections() {
	wm.connectionsMu.Lock()
	defer wm.connectionsMu.Unlock()
	
	now := time.Now()
	for id, conn := range wm.connections {
		if now.Sub(conn.LastActivity) > wm.config.IdleTimeout {
			log.Info().
				Str("connection_id", id).
				Dur("idle_time", now.Sub(conn.LastActivity)).
				Msg("Closing idle WebSocket connection")
			
			conn.Conn.Close()
			close(conn.Send)
			delete(wm.connections, id)
			atomic.AddInt64(&wm.metrics.ActiveConnections, -1)
		}
	}
}

// metricsCollector updates performance metrics
func (wm *WebSocketManager) metricsCollector() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			wm.updateMetrics()
		case <-wm.ctx.Done():
			return
		}
	}
}

func (wm *WebSocketManager) updateMetrics() {
	// Update connection metrics
	wm.connectionsMu.RLock()
	activeCount := len(wm.connections)
	wm.connectionsMu.RUnlock()
	
	atomic.StoreInt64(&wm.metrics.ActiveConnections, int64(activeCount))
}

// HTTP handlers for monitoring
func (wm *WebSocketManager) handleMetrics(w http.ResponseWriter, r *http.Request) {
	wm.metrics.mu.RLock()
	defer wm.metrics.mu.RUnlock()
	
	metrics := map[string]interface{}{
		"active_connections":     atomic.LoadInt64(&wm.metrics.ActiveConnections),
		"total_connections":      atomic.LoadInt64(&wm.metrics.TotalConnections),
		"failed_connections":     atomic.LoadInt64(&wm.metrics.FailedConnections),
		"messages_sent":          atomic.LoadInt64(&wm.metrics.MessagesSent),
		"messages_received":      atomic.LoadInt64(&wm.metrics.MessagesReceived),
		"broadcasts_sent":        atomic.LoadInt64(&wm.metrics.BroadcastsSent),
		"bytes_sent":            atomic.LoadInt64(&wm.metrics.BytesSent),
		"bytes_received":        atomic.LoadInt64(&wm.metrics.BytesReceived),
		"authentication_errors": atomic.LoadInt64(&wm.metrics.AuthenticationErrors),
		"rate_limit_errors":     atomic.LoadInt64(&wm.metrics.RateLimitErrors),
		"timeout_errors":        atomic.LoadInt64(&wm.metrics.TimeoutErrors),
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (wm *WebSocketManager) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":            "healthy",
		"active_connections": atomic.LoadInt64(&wm.metrics.ActiveConnections),
		"uptime":           time.Since(time.Now()).String(),
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// NewDefaultAuthenticator creates a default authenticator
func NewDefaultAuthenticator() *DefaultAuthenticator {
	return &DefaultAuthenticator{
		apiKeys: make(map[string]*ClientInfo),
	}
}

func (auth *DefaultAuthenticator) Authenticate(conn *websocket.Conn, request *http.Request) (*ClientInfo, error) {
	// Check for API key in query parameters or headers
	apiKey := request.URL.Query().Get("api_key")
	if apiKey == "" {
		apiKey = request.Header.Get("Authorization")
	}
	
	if apiKey == "" {
		return nil, fmt.Errorf("authentication required")
	}
	
	auth.mu.RLock()
	clientInfo, exists := auth.apiKeys[apiKey]
	auth.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("invalid API key")
	}
	
	return clientInfo, nil
}

func (auth *DefaultAuthenticator) IsAuthorized(clientInfo *ClientInfo, subscription *Subscription) bool {
	// Implement authorization logic based on client roles
	return true // Placeholder - implement proper authorization
}

// NewMessageRouter creates a new message router
func NewMessageRouter() *MessageRouter {
	router := &MessageRouter{
		routes: make(map[string]RouteHandler),
	}
	
	// Register default handlers
	router.RegisterHandler("subscribe", router.handleSubscribe)
	router.RegisterHandler("unsubscribe", router.handleUnsubscribe)
	router.RegisterHandler("ping", router.handlePing)
	
	return router
}

func (mr *MessageRouter) RegisterHandler(messageType string, handler RouteHandler) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.routes[messageType] = handler
}

func (mr *MessageRouter) handleSubscribe(conn *WebSocketConnection, message []byte) error {
	// Parse subscription request
	var req map[string]interface{}
	if err := json.Unmarshal(message, &req); err != nil {
		return err
	}
	
	// Create subscription
	subscription := &Subscription{
		Type:      req["subscription_type"].(string),
		Filters:   req["filters"].(map[string]interface{}),
		Format:    "json", // Default format
		CreatedAt: time.Now(),
	}
	
	// Add to connection subscriptions
	conn.mu.Lock()
	conn.Subscriptions[subscription.Type] = subscription
	conn.mu.Unlock()
	
	log.Info().
		Str("connection_id", conn.ID).
		Str("subscription_type", subscription.Type).
		Msg("WebSocket subscription added")
	
	return nil
}

func (mr *MessageRouter) handleUnsubscribe(conn *WebSocketConnection, message []byte) error {
	var req map[string]interface{}
	if err := json.Unmarshal(message, &req); err != nil {
		return err
	}
	
	subscriptionType := req["subscription_type"].(string)
	
	conn.mu.Lock()
	delete(conn.Subscriptions, subscriptionType)
	conn.mu.Unlock()
	
	log.Info().
		Str("connection_id", conn.ID).
		Str("subscription_type", subscriptionType).
		Msg("WebSocket subscription removed")
	
	return nil
}

func (mr *MessageRouter) handlePing(conn *WebSocketConnection, message []byte) error {
	// Respond with pong
	pongMsg := map[string]interface{}{
		"type":      "pong",
		"timestamp": time.Now().Format(time.RFC3339),
	}
	
	if data, err := json.Marshal(pongMsg); err == nil {
		select {
		case conn.Send <- data:
		default:
			return fmt.Errorf("failed to send pong - buffer full")
		}
	}
	
	return nil
}

// Allow method for rate limiter
func (rl *ConnectionRateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	
	now := time.Now()
	elapsed := now.Sub(rl.lastUpdate).Seconds()
	
	// Refill tokens based on elapsed time
	rl.tokens = min(rl.maxTokens, rl.tokens+elapsed*rl.refillRate)
	rl.lastUpdate = now
	
	if rl.tokens >= 1.0 {
		rl.tokens--
		return true
	}
	
	return false
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// Shutdown gracefully shuts down the WebSocket manager
func (wm *WebSocketManager) Shutdown() {
	wm.shutdownOnce.Do(func() {
		log.Info().Msg("Shutting down WebSocket manager")
		
		wm.cancel()
		
		// Close all connections
		wm.connectionsMu.Lock()
		for id, conn := range wm.connections {
			conn.Conn.Close()
			close(conn.Send)
			delete(wm.connections, id)
		}
		wm.connectionsMu.Unlock()
		
		log.Info().Msg("WebSocket manager shutdown completed")
	})
}
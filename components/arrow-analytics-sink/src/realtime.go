package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

// WebSocketHub manages WebSocket connections and broadcasts events
type WebSocketHub struct {
	connections    map[*websocket.Conn]bool
	broadcast      chan *TTPEvent
	register       chan *websocket.Conn
	unregister     chan *websocket.Conn
	maxConnections int
	mu             sync.RWMutex
}

// NewWebSocketHub creates a new WebSocket hub
func NewWebSocketHub(maxConnections int) *WebSocketHub {
	return &WebSocketHub{
		connections:    make(map[*websocket.Conn]bool),
		broadcast:      make(chan *TTPEvent),
		register:       make(chan *websocket.Conn),
		unregister:     make(chan *websocket.Conn),
		maxConnections: maxConnections,
	}
}

// Run starts the WebSocket hub
func (h *WebSocketHub) Run(ctx context.Context) {
	log.Info().Msg("Starting WebSocket hub")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("WebSocket hub stopped")
			return

		case conn := <-h.register:
			h.mu.Lock()
			if len(h.connections) < h.maxConnections {
				h.connections[conn] = true
				websocketConnections.Set(float64(len(h.connections)))
				log.Debug().
					Int("connections", len(h.connections)).
					Msg("WebSocket client connected")
			} else {
				// Close connection if at capacity
				conn.Close()
				log.Warn().Msg("WebSocket connection rejected: at capacity")
			}
			h.mu.Unlock()

		case conn := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.connections[conn]; ok {
				delete(h.connections, conn)
				conn.Close()
				websocketConnections.Set(float64(len(h.connections)))
				log.Debug().
					Int("connections", len(h.connections)).
					Msg("WebSocket client disconnected")
			}
			h.mu.Unlock()

		case event := <-h.broadcast:
			h.mu.RLock()
			for conn := range h.connections {
				select {
				case <-ctx.Done():
					h.mu.RUnlock()
					return
				default:
					if err := conn.WriteJSON(event); err != nil {
						log.Error().Err(err).Msg("Failed to write to WebSocket")
						delete(h.connections, conn)
						conn.Close()
					}
				}
			}
			websocketConnections.Set(float64(len(h.connections)))
			h.mu.RUnlock()
		}
	}
}

// AddConnection adds a new WebSocket connection
func (h *WebSocketHub) AddConnection(conn *websocket.Conn) {
	// Set connection timeouts
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	// Handle pong messages to keep connection alive
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	h.register <- conn

	// Start a goroutine to handle connection cleanup
	go func() {
		defer func() {
			h.unregister <- conn
		}()

		// Send ping messages to keep connection alive
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}

			// Read messages from client (mostly for close detection)
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

// Broadcast sends an event to all connected WebSocket clients
func (h *WebSocketHub) Broadcast(event *TTPEvent) {
	select {
	case h.broadcast <- event:
	default:
		// Drop event if broadcast channel is full
		log.Warn().Msg("WebSocket broadcast channel full, dropping event")
	}
}

// GetConnectionCount returns the current number of connections
func (h *WebSocketHub) GetConnectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.connections)
}

// Close closes the WebSocket hub
func (h *WebSocketHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for conn := range h.connections {
		conn.Close()
	}
	
	close(h.broadcast)
	close(h.register)
	close(h.unregister)
	
	log.Info().Msg("WebSocket hub closed")
}

// APICache manages a cache of recent events for REST API access
type APICache struct {
	events      []*TTPEvent
	eventsByID  map[string]*TTPEvent
	maxSize     int
	mu          sync.RWMutex
}

// NewAPICache creates a new API cache
func NewAPICache(maxSize int) *APICache {
	return &APICache{
		events:     make([]*TTPEvent, 0, maxSize),
		eventsByID: make(map[string]*TTPEvent),
		maxSize:    maxSize,
	}
}

// Add adds an event to the cache
func (c *APICache) Add(event *TTPEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Add to slice
	c.events = append(c.events, event)
	
	// Add to map for ID lookup
	c.eventsByID[event.EventID] = event

	// Maintain size limit
	if len(c.events) > c.maxSize {
		// Remove oldest event
		oldEvent := c.events[0]
		delete(c.eventsByID, oldEvent.EventID)
		c.events = c.events[1:]
	}
}

// GetRecent returns the most recent N events
func (c *APICache) GetRecent(n int) []*TTPEvent {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if n > len(c.events) {
		n = len(c.events)
	}

	// Return the last N events
	start := len(c.events) - n
	result := make([]*TTPEvent, n)
	copy(result, c.events[start:])
	
	// Reverse to get newest first
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	
	return result
}

// GetByID returns an event by its ID
func (c *APICache) GetByID(eventID string) *TTPEvent {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return c.eventsByID[eventID]
}

// GetStats returns cache statistics
func (c *APICache) GetStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]interface{}{
		"total_events": len(c.events),
		"max_size":     c.maxSize,
		"utilization":  float64(len(c.events)) / float64(c.maxSize),
	}
}

// RealTimeAnalytics provides real-time analytics on event streams
type RealTimeAnalytics struct {
	windowSize    time.Duration
	eventCounts   map[string]int
	assetCounts   map[string]int
	volumeByAsset map[string]float64
	mu            sync.RWMutex
	lastReset     time.Time
}

// NewRealTimeAnalytics creates a new real-time analytics processor
func NewRealTimeAnalytics(windowSize time.Duration) *RealTimeAnalytics {
	return &RealTimeAnalytics{
		windowSize:    windowSize,
		eventCounts:   make(map[string]int),
		assetCounts:   make(map[string]int),
		volumeByAsset: make(map[string]float64),
		lastReset:     time.Now(),
	}
}

// ProcessEvent processes an event for real-time analytics
func (a *RealTimeAnalytics) ProcessEvent(event *TTPEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if we need to reset the window
	if time.Since(a.lastReset) > a.windowSize {
		a.resetCounters()
	}

	// Count by event type
	a.eventCounts[event.EventType]++

	// Count by asset
	assetKey := "XLM"
	if event.AssetCode != nil {
		assetKey = *event.AssetCode
	}
	a.assetCounts[assetKey]++

	// Track volume (convert amount string to float for simplicity)
	// In production, use proper decimal handling
	if amount := parseAmountString(event.AmountStr); amount > 0 {
		a.volumeByAsset[assetKey] += amount
	}
}

// GetCurrentStats returns current analytics statistics
func (a *RealTimeAnalytics) GetCurrentStats() map[string]interface{} {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return map[string]interface{}{
		"window_size_seconds": a.windowSize.Seconds(),
		"event_counts":        copyStringIntMap(a.eventCounts),
		"asset_counts":        copyStringIntMap(a.assetCounts),
		"volume_by_asset":     copyStringFloatMap(a.volumeByAsset),
		"last_reset":          a.lastReset,
	}
}

// resetCounters resets all analytics counters
func (a *RealTimeAnalytics) resetCounters() {
	a.eventCounts = make(map[string]int)
	a.assetCounts = make(map[string]int)
	a.volumeByAsset = make(map[string]float64)
	a.lastReset = time.Now()
}

// Helper functions

func parseAmountString(amountStr string) float64 {
	// Simplified amount parsing - in production use proper decimal handling
	var amount float64
	if _, err := fmt.Sscanf(amountStr, "%f", &amount); err != nil {
		return 0
	}
	return amount
}

func copyStringIntMap(original map[string]int) map[string]int {
	copy := make(map[string]int)
	for k, v := range original {
		copy[k] = v
	}
	return copy
}

func copyStringFloatMap(original map[string]float64) map[string]float64 {
	copy := make(map[string]float64)
	for k, v := range original {
		copy[k] = v
	}
	return copy
}
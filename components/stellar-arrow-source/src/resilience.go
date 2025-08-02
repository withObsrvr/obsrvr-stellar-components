package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// ResilienceConfig configures resilience patterns
type ResilienceConfig struct {
	// Circuit Breaker
	CircuitBreakerEnabled     bool          `mapstructure:"circuit_breaker_enabled"`
	CircuitBreakerThreshold   int           `mapstructure:"circuit_breaker_threshold"`
	CircuitBreakerTimeout     time.Duration `mapstructure:"circuit_breaker_timeout"`
	CircuitBreakerResetTime   time.Duration `mapstructure:"circuit_breaker_reset_time"`
	
	// Retry Policy
	RetryEnabled        bool          `mapstructure:"retry_enabled"`
	RetryMaxAttempts    int           `mapstructure:"retry_max_attempts"`
	RetryInitialDelay   time.Duration `mapstructure:"retry_initial_delay"`
	RetryMaxDelay       time.Duration `mapstructure:"retry_max_delay"`
	RetryMultiplier     float64       `mapstructure:"retry_multiplier"`
	RetryJitter         bool          `mapstructure:"retry_jitter"`
	
	// Timeout Configuration
	DefaultTimeout      time.Duration `mapstructure:"default_timeout"`
	LedgerTimeout       time.Duration `mapstructure:"ledger_timeout"`
	ConnectionTimeout   time.Duration `mapstructure:"connection_timeout"`
	
	// Health Check
	HealthCheckEnabled  bool          `mapstructure:"health_check_enabled"`
	HealthCheckInterval time.Duration `mapstructure:"health_check_interval"`
	HealthCheckTimeout  time.Duration `mapstructure:"health_check_timeout"`
	
	// Bulkhead Pattern
	BulkheadEnabled     bool `mapstructure:"bulkhead_enabled"`
	MaxConcurrentRPC    int  `mapstructure:"max_concurrent_rpc"`
	MaxConcurrentData   int  `mapstructure:"max_concurrent_data"`
	
	// Rate Limiting
	RateLimitEnabled    bool    `mapstructure:"rate_limit_enabled"`
	RateLimitRPS        float64 `mapstructure:"rate_limit_rps"`
	RateLimitBurst      int     `mapstructure:"rate_limit_burst"`
}

// ResilienceManager manages all resilience patterns
type ResilienceManager struct {
	config           *ResilienceConfig
	circuitBreakers  map[string]*CircuitBreaker
	retryPolicies    map[string]*RetryPolicy
	healthCheckers   map[string]*HealthChecker
	bulkheads        map[string]*Bulkhead
	rateLimiters     map[string]*RateLimiter
	mu               sync.RWMutex
	
	// Metrics
	totalRequests    int64
	failedRequests   int64
	retriedRequests  int64
	timeoutRequests  int64
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	name            string
	threshold       int
	timeout         time.Duration
	resetTime       time.Duration
	state           CircuitBreakerState
	failures        int64
	lastFailureTime time.Time
	nextRetryTime   time.Time
	mu              sync.RWMutex
}

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int

const (
	CircuitBreakerClosed CircuitBreakerState = iota
	CircuitBreakerOpen
	CircuitBreakerHalfOpen
)

// RetryPolicy defines retry behavior
type RetryPolicy struct {
	name         string
	maxAttempts  int
	initialDelay time.Duration
	maxDelay     time.Duration
	multiplier   float64
	jitter       bool
}

// HealthChecker monitors component health
type HealthChecker struct {
	name         string
	checkFunc    func(context.Context) error
	interval     time.Duration
	timeout      time.Duration
	lastCheck    time.Time
	isHealthy    bool
	errorCount   int64
	mu           sync.RWMutex
	stopChan     chan struct{}
}

// Bulkhead implements the bulkhead pattern for resource isolation
type Bulkhead struct {
	name     string
	semaphore chan struct{}
	active   int64
	rejected int64
}

// RateLimiter implements token bucket rate limiting
type RateLimiter struct {
	name       string
	rps        float64
	burst      int
	tokens     float64
	lastUpdate time.Time
	mu         sync.Mutex
}

// NewResilienceManager creates a new resilience manager
func NewResilienceManager(config *ResilienceConfig) *ResilienceManager {
	rm := &ResilienceManager{
		config:          config,
		circuitBreakers: make(map[string]*CircuitBreaker),
		retryPolicies:   make(map[string]*RetryPolicy),
		healthCheckers:  make(map[string]*HealthChecker),
		bulkheads:       make(map[string]*Bulkhead),
		rateLimiters:    make(map[string]*RateLimiter),
	}
	
	// Initialize default components
	rm.initializeDefaults()
	
	log.Info().
		Bool("circuit_breaker_enabled", config.CircuitBreakerEnabled).
		Bool("retry_enabled", config.RetryEnabled).
		Bool("health_check_enabled", config.HealthCheckEnabled).
		Bool("bulkhead_enabled", config.BulkheadEnabled).
		Bool("rate_limit_enabled", config.RateLimitEnabled).
		Msg("Resilience manager initialized")
	
	return rm
}

// initializeDefaults sets up default resilience components
func (rm *ResilienceManager) initializeDefaults() {
	// Default circuit breakers
	if rm.config.CircuitBreakerEnabled {
		rm.circuitBreakers["rpc"] = NewCircuitBreaker("rpc", 
			rm.config.CircuitBreakerThreshold,
			rm.config.CircuitBreakerTimeout,
			rm.config.CircuitBreakerResetTime)
		
		rm.circuitBreakers["datalake"] = NewCircuitBreaker("datalake",
			rm.config.CircuitBreakerThreshold,
			rm.config.CircuitBreakerTimeout,
			rm.config.CircuitBreakerResetTime)
	}
	
	// Default retry policies
	if rm.config.RetryEnabled {
		rm.retryPolicies["default"] = &RetryPolicy{
			name:         "default",
			maxAttempts:  rm.config.RetryMaxAttempts,
			initialDelay: rm.config.RetryInitialDelay,
			maxDelay:     rm.config.RetryMaxDelay,
			multiplier:   rm.config.RetryMultiplier,
			jitter:       rm.config.RetryJitter,
		}
		
		rm.retryPolicies["ledger"] = &RetryPolicy{
			name:         "ledger",
			maxAttempts:  3,
			initialDelay: 100 * time.Millisecond,
			maxDelay:     5 * time.Second,
			multiplier:   2.0,
			jitter:       true,
		}
	}
	
	// Default bulkheads
	if rm.config.BulkheadEnabled {
		rm.bulkheads["rpc"] = NewBulkhead("rpc", rm.config.MaxConcurrentRPC)
		rm.bulkheads["datalake"] = NewBulkhead("datalake", rm.config.MaxConcurrentData)
	}
	
	// Default rate limiters
	if rm.config.RateLimitEnabled {
		rm.rateLimiters["default"] = NewRateLimiter("default", 
			rm.config.RateLimitRPS, rm.config.RateLimitBurst)
	}
}

// ExecuteWithResilience executes a function with all resilience patterns applied
func (rm *ResilienceManager) ExecuteWithResilience(ctx context.Context, name string, fn func(context.Context) error) error {
	atomic.AddInt64(&rm.totalRequests, 1)
	
	// Apply rate limiting
	if rm.config.RateLimitEnabled {
		if rateLimiter, exists := rm.getRateLimiter(name); exists {
			if !rateLimiter.Allow() {
				atomic.AddInt64(&rm.failedRequests, 1)
				return fmt.Errorf("rate limit exceeded for %s", name)
			}
		}
	}
	
	// Apply bulkhead pattern
	if rm.config.BulkheadEnabled {
		if bulkhead, exists := rm.getBulkhead(name); exists {
			if !bulkhead.TryAcquire() {
				atomic.AddInt64(&rm.failedRequests, 1)
				return fmt.Errorf("bulkhead capacity exceeded for %s", name)
			}
			defer bulkhead.Release()
		}
	}
	
	// Apply circuit breaker
	if rm.config.CircuitBreakerEnabled {
		if cb, exists := rm.getCircuitBreaker(name); exists {
			if !cb.CanExecute() {
				atomic.AddInt64(&rm.failedRequests, 1)
				return fmt.Errorf("circuit breaker is open for %s", name)
			}
		}
	}
	
	// Apply timeout
	timeout := rm.getTimeout(name)
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	
	// Execute with retry policy
	if rm.config.RetryEnabled {
		if retryPolicy, exists := rm.getRetryPolicy(name); exists {
			return rm.executeWithRetry(ctx, name, fn, retryPolicy)
		}
	}
	
	// Execute without retry
	return rm.executeOnce(ctx, name, fn)
}

// executeWithRetry executes a function with retry logic
func (rm *ResilienceManager) executeWithRetry(ctx context.Context, name string, fn func(context.Context) error, policy *RetryPolicy) error {
	var lastErr error
	
	for attempt := 1; attempt <= policy.maxAttempts; attempt++ {
		if attempt > 1 {
			atomic.AddInt64(&rm.retriedRequests, 1)
			
			// Calculate delay with exponential backoff
			delay := rm.calculateDelay(policy, attempt-1)
			
			log.Debug().
				Str("operation", name).
				Int("attempt", attempt).
				Dur("delay", delay).
				Msg("Retrying operation")
			
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		
		err := rm.executeOnce(ctx, name, fn)
		if err == nil {
			if attempt > 1 {
				log.Info().
					Str("operation", name).
					Int("attempt", attempt).
					Msg("Operation succeeded after retry")
			}
			return nil
		}
		
		lastErr = err
		
		// Check if error is retryable
		if !rm.isRetryableError(err) {
			log.Debug().
				Str("operation", name).
				Err(err).
				Msg("Non-retryable error, stopping retries")
			break
		}
		
		// Check context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	
	log.Warn().
		Str("operation", name).
		Int("max_attempts", policy.maxAttempts).
		Err(lastErr).
		Msg("Operation failed after all retry attempts")
	
	return fmt.Errorf("operation %s failed after %d attempts: %w", name, policy.maxAttempts, lastErr)
}

// executeOnce executes a function once and handles circuit breaker state
func (rm *ResilienceManager) executeOnce(ctx context.Context, name string, fn func(context.Context) error) error {
	err := fn(ctx)
	
	// Update circuit breaker state
	if rm.config.CircuitBreakerEnabled {
		if cb, exists := rm.getCircuitBreaker(name); exists {
			if err != nil {
				cb.RecordFailure()
			} else {
				cb.RecordSuccess()
			}
		}
	}
	
	// Track metrics
	if err != nil {
		atomic.AddInt64(&rm.failedRequests, 1)
		
		// Check if it's a timeout error
		if errors.Is(err, context.DeadlineExceeded) {
			atomic.AddInt64(&rm.timeoutRequests, 1)
		}
	}
	
	return err
}

// calculateDelay calculates retry delay with exponential backoff and jitter
func (rm *ResilienceManager) calculateDelay(policy *RetryPolicy, attempt int) time.Duration {
	delay := time.Duration(float64(policy.initialDelay) * math.Pow(policy.multiplier, float64(attempt)))
	
	if delay > policy.maxDelay {
		delay = policy.maxDelay
	}
	
	if policy.jitter {
		// Add random jitter (±25%)
		jitterRange := float64(delay) * 0.25
		jitter := (float64(time.Now().UnixNano()%1000) / 1000.0) * jitterRange * 2 - jitterRange
		delay = time.Duration(float64(delay) + jitter)
	}
	
	return delay
}

// isRetryableError determines if an error is retryable
func (rm *ResilienceManager) isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	// Context cancellation is not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	
	// Check for specific error types that shouldn't be retried
	errorStr := err.Error()
	nonRetryableErrors := []string{
		"invalid argument",
		"unauthorized",
		"forbidden",
		"not found",
		"conflict",
		"unprocessable entity",
	}
	
	for _, nonRetryable := range nonRetryableErrors {
		if contains(errorStr, nonRetryable) {
			return false
		}
	}
	
	// Default to retryable for network, timeout, and server errors
	return true
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(name string, threshold int, timeout, resetTime time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		name:      name,
		threshold: threshold,
		timeout:   timeout,
		resetTime: resetTime,
		state:     CircuitBreakerClosed,
	}
}

// CanExecute checks if the circuit breaker allows execution
func (cb *CircuitBreaker) CanExecute() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	now := time.Now()
	
	switch cb.state {
	case CircuitBreakerClosed:
		return true
	case CircuitBreakerOpen:
		if now.After(cb.nextRetryTime) {
			cb.state = CircuitBreakerHalfOpen
			return true
		}
		return false
	case CircuitBreakerHalfOpen:
		return true
	default:
		return false
	}
}

// RecordSuccess records a successful execution
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	cb.failures = 0
	
	if cb.state == CircuitBreakerHalfOpen {
		cb.state = CircuitBreakerClosed
		log.Info().Str("circuit_breaker", cb.name).Msg("Circuit breaker closed")
	}
}

// RecordFailure records a failed execution
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	cb.failures++
	cb.lastFailureTime = time.Now()
	
	if cb.state == CircuitBreakerClosed && int(cb.failures) >= cb.threshold {
		cb.state = CircuitBreakerOpen
		cb.nextRetryTime = time.Now().Add(cb.resetTime)
		log.Warn().
			Str("circuit_breaker", cb.name).
			Int64("failures", cb.failures).
			Time("next_retry", cb.nextRetryTime).
			Msg("Circuit breaker opened")
	} else if cb.state == CircuitBreakerHalfOpen {
		cb.state = CircuitBreakerOpen
		cb.nextRetryTime = time.Now().Add(cb.resetTime)
		log.Warn().
			Str("circuit_breaker", cb.name).
			Msg("Circuit breaker re-opened from half-open state")
	}
}

// NewBulkhead creates a new bulkhead
func NewBulkhead(name string, capacity int) *Bulkhead {
	return &Bulkhead{
		name:      name,
		semaphore: make(chan struct{}, capacity),
	}
}

// TryAcquire tries to acquire a resource from the bulkhead
func (b *Bulkhead) TryAcquire() bool {
	select {
	case b.semaphore <- struct{}{}:
		atomic.AddInt64(&b.active, 1)
		return true
	default:
		atomic.AddInt64(&b.rejected, 1)
		return false
	}
}

// Release releases a resource back to the bulkhead
func (b *Bulkhead) Release() {
	select {
	case <-b.semaphore:
		atomic.AddInt64(&b.active, -1)
	default:
		// This shouldn't happen in normal circumstances
		log.Warn().Str("bulkhead", b.name).Msg("Attempted to release without acquire")
	}
}

// NewRateLimiter creates a new token bucket rate limiter
func NewRateLimiter(name string, rps float64, burst int) *RateLimiter {
	return &RateLimiter{
		name:       name,
		rps:        rps,
		burst:      burst,
		tokens:     float64(burst),
		lastUpdate: time.Now(),
	}
}

// Allow checks if a request is allowed by the rate limiter
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	
	now := time.Now()
	elapsed := now.Sub(rl.lastUpdate).Seconds()
	
	// Add tokens based on elapsed time
	rl.tokens = math.Min(float64(rl.burst), rl.tokens+elapsed*rl.rps)
	rl.lastUpdate = now
	
	if rl.tokens >= 1.0 {
		rl.tokens--
		return true
	}
	
	return false
}

// Getter methods for accessing resilience components
func (rm *ResilienceManager) getCircuitBreaker(name string) (*CircuitBreaker, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	cb, exists := rm.circuitBreakers[name]
	if !exists {
		// Try default circuit breaker
		cb, exists = rm.circuitBreakers["default"]
	}
	return cb, exists
}

func (rm *ResilienceManager) getRetryPolicy(name string) (*RetryPolicy, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	policy, exists := rm.retryPolicies[name]
	if !exists {
		policy, exists = rm.retryPolicies["default"]
	}
	return policy, exists
}

func (rm *ResilienceManager) getBulkhead(name string) (*Bulkhead, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	bulkhead, exists := rm.bulkheads[name]
	return bulkhead, exists
}

func (rm *ResilienceManager) getRateLimiter(name string) (*RateLimiter, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	limiter, exists := rm.rateLimiters[name]
	if !exists {
		limiter, exists = rm.rateLimiters["default"]
	}
	return limiter, exists
}

// getTimeout returns the appropriate timeout for an operation
func (rm *ResilienceManager) getTimeout(name string) time.Duration {
	switch name {
	case "ledger":
		return rm.config.LedgerTimeout
	case "connection":
		return rm.config.ConnectionTimeout
	default:
		return rm.config.DefaultTimeout
	}
}

// GetResilienceMetrics returns resilience-related metrics
func (rm *ResilienceManager) GetResilienceMetrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"total_requests":    atomic.LoadInt64(&rm.totalRequests),
		"failed_requests":   atomic.LoadInt64(&rm.failedRequests),
		"retried_requests":  atomic.LoadInt64(&rm.retriedRequests),
		"timeout_requests":  atomic.LoadInt64(&rm.timeoutRequests),
		"circuit_breakers":  make(map[string]interface{}),
		"bulkheads":         make(map[string]interface{}),
	}
	
	// Circuit breaker metrics
	rm.mu.RLock()
	for name, cb := range rm.circuitBreakers {
		cb.mu.RLock()
		metrics["circuit_breakers"].(map[string]interface{})[name] = map[string]interface{}{
			"state":    cb.state,
			"failures": cb.failures,
		}
		cb.mu.RUnlock()
	}
	
	// Bulkhead metrics
	for name, bulkhead := range rm.bulkheads {
		metrics["bulkheads"].(map[string]interface{})[name] = map[string]interface{}{
			"active":   atomic.LoadInt64(&bulkhead.active),
			"rejected": atomic.LoadInt64(&bulkhead.rejected),
			"capacity": cap(bulkhead.semaphore),
		}
	}
	rm.mu.RUnlock()
	
	return metrics
}

// Utility function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(s) > len(substr) && 
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || 
		 strings.Contains(s, substr))))
}


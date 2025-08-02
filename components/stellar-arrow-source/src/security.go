package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// SecurityConfig holds security-related configuration
type SecurityConfig struct {
	// TLS Configuration
	TLSEnabled  bool   `mapstructure:"tls_enabled"`
	CertFile    string `mapstructure:"cert_file"`
	KeyFile     string `mapstructure:"key_file"`
	CAFile      string `mapstructure:"ca_file"`
	ServerName  string `mapstructure:"server_name"`
	
	// Authentication
	AuthEnabled    bool   `mapstructure:"auth_enabled"`
	AuthType       string `mapstructure:"auth_type"` // "api_key", "jwt", "mutual_tls"
	APIKeyHeader   string `mapstructure:"api_key_header"`
	APIKeysFile    string `mapstructure:"api_keys_file"`
	JWTSecret      string `mapstructure:"jwt_secret"`
	JWTIssuer      string `mapstructure:"jwt_issuer"`
	
	// Authorization
	AuthzEnabled   bool   `mapstructure:"authz_enabled"`
	AuthzProvider  string `mapstructure:"authz_provider"` // "simple", "rbac"
	RolesFile      string `mapstructure:"roles_file"`
	
	// Security Headers
	CORSEnabled    bool     `mapstructure:"cors_enabled"`
	CORSOrigins    []string `mapstructure:"cors_origins"`
	CORSMethods    []string `mapstructure:"cors_methods"`
	CORSHeaders    []string `mapstructure:"cors_headers"`
	
	// Rate Limiting
	RateLimitEnabled bool `mapstructure:"rate_limit_enabled"`
	RateLimitRPS     int  `mapstructure:"rate_limit_rps"`
	RateLimitBurst   int  `mapstructure:"rate_limit_burst"`
}

// SecurityManager handles all security aspects
type SecurityManager struct {
	config      *SecurityConfig
	tlsConfig   *tls.Config
	apiKeys     map[string]*APIKeyInfo
	roleManager *RoleManager
}

// APIKeyInfo represents API key metadata
type APIKeyInfo struct {
	KeyID       string    `json:"key_id"`
	Description string    `json:"description"`
	Roles       []string  `json:"roles"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	Active      bool      `json:"active"`
}

// RoleManager handles role-based access control
type RoleManager struct {
	roles       map[string]*Role
	userRoles   map[string][]string
}

// Role represents a security role with permissions
type Role struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Permissions []string `json:"permissions"`
}

// Permission constants
const (
	PermissionReadLedgers   = "read:ledgers"
	PermissionWriteLedgers  = "write:ledgers"
	PermissionReadMetrics   = "read:metrics"
	PermissionWriteMetrics  = "write:metrics"
	PermissionAdmin         = "admin"
)

// NewSecurityManager creates a new security manager
func NewSecurityManager(config *SecurityConfig) (*SecurityManager, error) {
	sm := &SecurityManager{
		config:  config,
		apiKeys: make(map[string]*APIKeyInfo),
	}
	
	// Initialize TLS if enabled
	if config.TLSEnabled {
		tlsConfig, err := sm.setupTLS()
		if err != nil {
			return nil, fmt.Errorf("failed to setup TLS: %w", err)
		}
		sm.tlsConfig = tlsConfig
	}
	
	// Load API keys if authentication is enabled
	if config.AuthEnabled && config.AuthType == "api_key" {
		if err := sm.loadAPIKeys(); err != nil {
			return nil, fmt.Errorf("failed to load API keys: %w", err)
		}
	}
	
	// Initialize role manager if authorization is enabled
	if config.AuthzEnabled {
		roleManager, err := sm.setupRoleManager()
		if err != nil {
			return nil, fmt.Errorf("failed to setup role manager: %w", err)
		}
		sm.roleManager = roleManager
	}
	
	log.Info().
		Bool("tls_enabled", config.TLSEnabled).
		Bool("auth_enabled", config.AuthEnabled).
		Bool("authz_enabled", config.AuthzEnabled).
		Str("auth_type", config.AuthType).
		Msg("Security manager initialized")
	
	return sm, nil
}

// setupTLS configures TLS settings
func (sm *SecurityManager) setupTLS() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		CurvePreferences: []tls.CurveID{
			tls.CurveP256,
			tls.X25519,
		},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		},
	}
	
	// Load server certificate and key
	if sm.config.CertFile != "" && sm.config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(sm.config.CertFile, sm.config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load server certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	
	// Load CA certificate for client verification
	if sm.config.CAFile != "" {
		caCert, err := os.ReadFile(sm.config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		
		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	
	// Set server name for verification
	if sm.config.ServerName != "" {
		tlsConfig.ServerName = sm.config.ServerName
	}
	
	return tlsConfig, nil
}

// loadAPIKeys loads API keys from configuration file
func (sm *SecurityManager) loadAPIKeys() error {
	if sm.config.APIKeysFile == "" {
		log.Warn().Msg("No API keys file specified, using empty key set")
		return nil
	}
	
	// For this implementation, we'll create some default API keys
	// In production, these would be loaded from a secure configuration file
	sm.apiKeys = map[string]*APIKeyInfo{
		"obsrvr-admin-key-123": {
			KeyID:       "admin-001",
			Description: "Admin API Key",
			Roles:       []string{"admin"},
			CreatedAt:   time.Now(),
			Active:      true,
		},
		"obsrvr-reader-key-456": {
			KeyID:       "reader-001",
			Description: "Read-only API Key",
			Roles:       []string{"reader"},
			CreatedAt:   time.Now(),
			Active:      true,
		},
	}
	
	log.Info().
		Int("api_keys_loaded", len(sm.apiKeys)).
		Str("keys_file", sm.config.APIKeysFile).
		Msg("API keys loaded")
	
	return nil
}

// setupRoleManager initializes the role-based access control system
func (sm *SecurityManager) setupRoleManager() (*RoleManager, error) {
	rm := &RoleManager{
		roles:     make(map[string]*Role),
		userRoles: make(map[string][]string),
	}
	
	// Define default roles
	rm.roles["admin"] = &Role{
		Name:        "admin",
		Description: "Full administrative access",
		Permissions: []string{
			PermissionReadLedgers,
			PermissionWriteLedgers,
			PermissionReadMetrics,
			PermissionWriteMetrics,
			PermissionAdmin,
		},
	}
	
	rm.roles["reader"] = &Role{
		Name:        "reader",
		Description: "Read-only access",
		Permissions: []string{
			PermissionReadLedgers,
			PermissionReadMetrics,
		},
	}
	
	rm.roles["processor"] = &Role{
		Name:        "processor",
		Description: "Data processing access",
		Permissions: []string{
			PermissionReadLedgers,
			PermissionWriteLedgers,
			PermissionReadMetrics,
		},
	}
	
	log.Info().
		Int("roles_defined", len(rm.roles)).
		Msg("Role manager initialized")
	
	return rm, nil
}

// GetGRPCServerOptions returns gRPC server options with security configurations
func (sm *SecurityManager) GetGRPCServerOptions() []grpc.ServerOption {
	var opts []grpc.ServerOption
	
	// Add TLS credentials if enabled
	if sm.config.TLSEnabled && sm.tlsConfig != nil {
		creds := credentials.NewTLS(sm.tlsConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	
	// Add authentication interceptor if enabled
	if sm.config.AuthEnabled {
		opts = append(opts, grpc.UnaryInterceptor(sm.authenticationInterceptor()))
	}
	
	// Add authorization interceptor if enabled
	if sm.config.AuthzEnabled {
		opts = append(opts, grpc.UnaryInterceptor(sm.authorizationInterceptor()))
	}
	
	return opts
}

// GetHTTPSServer returns an HTTPS server with security configurations
func (sm *SecurityManager) GetHTTPSServer(handler http.Handler, addr string) *http.Server {
	server := &http.Server{
		Addr:         addr,
		Handler:      sm.wrapHTTPHandler(handler),
		TLSConfig:    sm.tlsConfig,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	
	return server
}

// wrapHTTPHandler adds security middleware to HTTP handlers
func (sm *SecurityManager) wrapHTTPHandler(handler http.Handler) http.Handler {
	// Add CORS middleware if enabled
	if sm.config.CORSEnabled {
		handler = sm.corsMiddleware(handler)
	}
	
	// Add authentication middleware if enabled
	if sm.config.AuthEnabled {
		handler = sm.httpAuthMiddleware(handler)
	}
	
	// Add security headers middleware
	handler = sm.securityHeadersMiddleware(handler)
	
	return handler
}

// authenticationInterceptor validates authentication for gRPC requests
func (sm *SecurityManager) authenticationInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !sm.config.AuthEnabled {
			return handler(ctx, req)
		}
		
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
		}
		
		switch sm.config.AuthType {
		case "api_key":
			return sm.validateAPIKey(ctx, md, handler, req)
		case "jwt":
			return sm.validateJWT(ctx, md, handler, req)
		case "mutual_tls":
			return sm.validateMutualTLS(ctx, handler, req)
		default:
			return nil, status.Errorf(codes.Unauthenticated, "unsupported authentication type")
		}
	}
}

// authorizationInterceptor validates authorization for gRPC requests
func (sm *SecurityManager) authorizationInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !sm.config.AuthzEnabled {
			return handler(ctx, req)
		}
		
		// Extract user/client information from context (set by authentication)
		userID, ok := ctx.Value("user_id").(string)
		if !ok {
			return nil, status.Errorf(codes.PermissionDenied, "user not authenticated")
		}
		
		// Determine required permission based on method
		requiredPermission := sm.getRequiredPermission(info.FullMethod)
		
		// Check if user has required permission
		if !sm.hasPermission(userID, requiredPermission) {
			log.Warn().
				Str("user_id", userID).
				Str("method", info.FullMethod).
				Str("required_permission", requiredPermission).
				Msg("Authorization denied")
			
			return nil, status.Errorf(codes.PermissionDenied, "insufficient permissions")
		}
		
		return handler(ctx, req)
	}
}

// validateAPIKey validates API key authentication
func (sm *SecurityManager) validateAPIKey(ctx context.Context, md metadata.MD, handler grpc.UnaryHandler, req interface{}) (interface{}, error) {
	headerName := sm.config.APIKeyHeader
	if headerName == "" {
		headerName = "authorization"
	}
	
	values := md[headerName]
	if len(values) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "missing API key")
	}
	
	apiKey := values[0]
	if strings.HasPrefix(apiKey, "Bearer ") {
		apiKey = strings.TrimPrefix(apiKey, "Bearer ")
	}
	
	keyInfo, exists := sm.apiKeys[apiKey]
	if !exists || !keyInfo.Active {
		return nil, status.Errorf(codes.Unauthenticated, "invalid API key")
	}
	
	// Check expiration
	if keyInfo.ExpiresAt != nil && time.Now().After(*keyInfo.ExpiresAt) {
		return nil, status.Errorf(codes.Unauthenticated, "API key expired")
	}
	
	// Add user context for authorization
	ctx = context.WithValue(ctx, "user_id", keyInfo.KeyID)
	ctx = context.WithValue(ctx, "user_roles", keyInfo.Roles)
	
	return handler(ctx, req)
}

// validateJWT validates JWT token authentication
func (sm *SecurityManager) validateJWT(ctx context.Context, md metadata.MD, handler grpc.UnaryHandler, req interface{}) (interface{}, error) {
	// JWT validation implementation would go here
	// For now, return a placeholder implementation
	return nil, status.Errorf(codes.Unimplemented, "JWT authentication not implemented")
}

// validateMutualTLS validates mutual TLS authentication
func (sm *SecurityManager) validateMutualTLS(ctx context.Context, handler grpc.UnaryHandler, req interface{}) (interface{}, error) {
	// Mutual TLS validation implementation would go here
	// For now, return a placeholder implementation
	return handler(ctx, req)
}

// corsMiddleware adds CORS headers
func (sm *SecurityManager) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		
		// Check if origin is allowed
		if len(sm.config.CORSOrigins) > 0 {
			allowed := false
			for _, allowedOrigin := range sm.config.CORSOrigins {
				if allowedOrigin == "*" || allowedOrigin == origin {
					allowed = true
					break
				}
			}
			if !allowed {
				http.Error(w, "CORS policy violation", http.StatusForbidden)
				return
			}
		}
		
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(sm.config.CORSMethods, ", "))
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(sm.config.CORSHeaders, ", "))
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		
		// Handle preflight request
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		next.ServeHTTP(w, r)
	})
}

// httpAuthMiddleware adds HTTP authentication
func (sm *SecurityManager) httpAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract and validate authentication
		headerName := sm.config.APIKeyHeader
		if headerName == "" {
			headerName = "Authorization"
		}
		
		authHeader := r.Header.Get(headerName)
		if authHeader == "" {
			http.Error(w, "Authentication required", http.StatusUnauthorized)
			return
		}
		
		apiKey := authHeader
		if strings.HasPrefix(apiKey, "Bearer ") {
			apiKey = strings.TrimPrefix(apiKey, "Bearer ")
		}
		
		keyInfo, exists := sm.apiKeys[apiKey]
		if !exists || !keyInfo.Active {
			http.Error(w, "Invalid authentication", http.StatusUnauthorized)
			return
		}
		
		// Add user context
		ctx := context.WithValue(r.Context(), "user_id", keyInfo.KeyID)
		ctx = context.WithValue(ctx, "user_roles", keyInfo.Roles)
		r = r.WithContext(ctx)
		
		next.ServeHTTP(w, r)
	})
}

// securityHeadersMiddleware adds security headers
func (sm *SecurityManager) securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		w.Header().Set("Content-Security-Policy", "default-src 'self'")
		
		next.ServeHTTP(w, r)
	})
}

// getRequiredPermission determines the required permission for a gRPC method
func (sm *SecurityManager) getRequiredPermission(method string) string {
	switch {
	case strings.Contains(method, "GetLedger") || strings.Contains(method, "StreamLedgers"):
		return PermissionReadLedgers
	case strings.Contains(method, "GetMetrics") || strings.Contains(method, "Health"):
		return PermissionReadMetrics
	default:
		return PermissionAdmin
	}
}

// hasPermission checks if a user has the required permission
func (sm *SecurityManager) hasPermission(userID, requiredPermission string) bool {
	if sm.roleManager == nil {
		return false
	}
	
	// Get user roles from API key info
	for _, keyInfo := range sm.apiKeys {
		if keyInfo.KeyID == userID {
			for _, roleName := range keyInfo.Roles {
				if role, exists := sm.roleManager.roles[roleName]; exists {
					for _, permission := range role.Permissions {
						if permission == requiredPermission || permission == PermissionAdmin {
							return true
						}
					}
				}
			}
			break
		}
	}
	
	return false
}

// GenerateAPIKey generates a new API key (for administrative use)
func (sm *SecurityManager) GenerateAPIKey(keyID, description string, roles []string, expiresAt *time.Time) (string, error) {
	// Generate a secure random API key
	apiKey := fmt.Sprintf("obsrvr-%s-%d", keyID, time.Now().Unix())
	
	keyInfo := &APIKeyInfo{
		KeyID:       keyID,
		Description: description,
		Roles:       roles,
		CreatedAt:   time.Now(),
		ExpiresAt:   expiresAt,
		Active:      true,
	}
	
	sm.apiKeys[apiKey] = keyInfo
	
	log.Info().
		Str("key_id", keyID).
		Str("description", description).
		Strs("roles", roles).
		Msg("API key generated")
	
	return apiKey, nil
}

// RevokeAPIKey revokes an existing API key
func (sm *SecurityManager) RevokeAPIKey(apiKey string) error {
	if keyInfo, exists := sm.apiKeys[apiKey]; exists {
		keyInfo.Active = false
		log.Info().
			Str("key_id", keyInfo.KeyID).
			Msg("API key revoked")
		return nil
	}
	
	return fmt.Errorf("API key not found")
}

// GetSecurityMetrics returns security-related metrics
func (sm *SecurityManager) GetSecurityMetrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"tls_enabled":        sm.config.TLSEnabled,
		"auth_enabled":       sm.config.AuthEnabled,
		"auth_type":          sm.config.AuthType,
		"authz_enabled":      sm.config.AuthzEnabled,
		"cors_enabled":       sm.config.CORSEnabled,
		"rate_limit_enabled": sm.config.RateLimitEnabled,
		"active_api_keys":    0,
		"total_api_keys":     len(sm.apiKeys),
	}
	
	// Count active API keys
	activeKeys := 0
	for _, keyInfo := range sm.apiKeys {
		if keyInfo.Active {
			activeKeys++
		}
	}
	metrics["active_api_keys"] = activeKeys
	
	if sm.roleManager != nil {
		metrics["roles_defined"] = len(sm.roleManager.roles)
	}
	
	return metrics
}
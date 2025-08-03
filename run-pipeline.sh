#!/usr/bin/env bash

# Pipeline Runner for obsrvr-stellar-components
# Starts all components with optional flowctl integration

set -e

# Configuration
PIPELINE_MODE=${1:-local}  # local, flowctl, development, production
COMPONENTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/components"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

success() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

# Pipeline configuration profiles
configure_local() {
    export ENABLE_FLOWCTL=false
    log "Configured for local standalone mode"
}

configure_flowctl() {
    export ENABLE_FLOWCTL=true
    export FLOWCTL_ENDPOINT=${FLOWCTL_ENDPOINT:-localhost:8080}
    export FLOWCTL_HEARTBEAT_INTERVAL=${FLOWCTL_HEARTBEAT_INTERVAL:-10s}
    log "Configured for flowctl integration mode"
    log "FlowCtl endpoint: $FLOWCTL_ENDPOINT"
}

configure_development() {
    export ENABLE_FLOWCTL=true
    export FLOWCTL_ENDPOINT=${FLOWCTL_ENDPOINT:-localhost:8080}
    export FLOWCTL_HEARTBEAT_INTERVAL=5s
    export CONNECTION_POOL_MAX_CONNECTIONS=5
    export CONNECTION_POOL_MIN_CONNECTIONS=1
    export CONNECTION_POOL_IDLE_TIMEOUT=2m
    export MONITORING_INTERVAL=3s
    log "Configured for development mode with fast monitoring"
}

configure_production() {
    export ENABLE_FLOWCTL=true
    export FLOWCTL_ENDPOINT=${FLOWCTL_ENDPOINT:-flowctl-prod.example.com:8080}
    export FLOWCTL_HEARTBEAT_INTERVAL=15s
    export CONNECTION_POOL_MAX_CONNECTIONS=20
    export CONNECTION_POOL_MIN_CONNECTIONS=5
    export CONNECTION_POOL_IDLE_TIMEOUT=10m
    export MONITORING_INTERVAL=5s
    log "Configured for production mode"
}

# Function to check if a component binary exists
check_component() {
    local component=$1
    local binary_path="${COMPONENTS_DIR}/${component}/src/${component}"
    
    if [ ! -f "$binary_path" ]; then
        error "Binary not found: $binary_path"
        error "Please build the component first: cd ${COMPONENTS_DIR}/${component}/src && go build"
        return 1
    fi
    return 0
}

# Function to start a component in background
start_component() {
    local component=$1
    local component_dir="${COMPONENTS_DIR}/${component}/src"
    
    log "Starting $component..."
    
    if ! check_component "$component"; then
        return 1
    fi
    
    # Start component in background
    cd "$component_dir"
    nohup ./run.sh > "${component}.log" 2>&1 &
    local pid=$!
    echo $pid > "${component}.pid"
    
    # Wait a moment and check if it's still running
    sleep 2
    if kill -0 $pid 2>/dev/null; then
        success "$component started successfully (PID: $pid)"
        log "Log file: ${component_dir}/${component}.log"
        return 0
    else
        error "$component failed to start"
        return 1
    fi
}

# Function to stop all components
stop_components() {
    log "Stopping all components..."
    
    for component in stellar-arrow-source ttp-arrow-processor arrow-analytics-sink; do
        local component_dir="${COMPONENTS_DIR}/${component}/src"
        local pid_file="${component_dir}/${component}.pid"
        
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if kill -0 $pid 2>/dev/null; then
                log "Stopping $component (PID: $pid)..."
                kill $pid
                # Wait for graceful shutdown
                for i in {1..10}; do
                    if ! kill -0 $pid 2>/dev/null; then
                        success "$component stopped"
                        break
                    fi
                    sleep 1
                done
                # Force kill if still running
                if kill -0 $pid 2>/dev/null; then
                    warn "Force killing $component"
                    kill -9 $pid
                fi
            fi
            rm -f "$pid_file"
        fi
    done
}

# Function to check component health
check_health() {
    local ports=(8088 8088 8088)  # Health ports for all components
    local components=(stellar-arrow-source ttp-arrow-processor arrow-analytics-sink)
    
    log "Checking component health..."
    
    for i in "${!components[@]}"; do
        local component="${components[$i]}"
        local port="${ports[$i]}"
        
        if curl -s -f "http://localhost:${port}/health" >/dev/null 2>&1; then
            success "$component is healthy"
        else
            warn "$component health check failed"
        fi
    done
}

# Function to show pipeline status
show_status() {
    log "Pipeline Status:"
    echo ""
    
    for component in stellar-arrow-source ttp-arrow-processor arrow-analytics-sink; do
        local component_dir="${COMPONENTS_DIR}/${component}/src"
        local pid_file="${component_dir}/${component}.pid"
        
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if kill -0 $pid 2>/dev/null; then
                echo -e "  ${GREEN}●${NC} $component (PID: $pid)"
            else
                echo -e "  ${RED}●${NC} $component (stopped)"
            fi
        else
            echo -e "  ${RED}●${NC} $component (not started)"
        fi
    done
    echo ""
}

# Function to follow logs
follow_logs() {
    local component=${1:-all}
    
    if [ "$component" = "all" ]; then
        log "Following all component logs..."
        tail -f "${COMPONENTS_DIR}"/*/src/*.log
    else
        log "Following $component logs..."
        tail -f "${COMPONENTS_DIR}/${component}/src/${component}.log"
    fi
}

# Main execution
case "$PIPELINE_MODE" in
    "local")
        configure_local
        ;;
    "flowctl")
        configure_flowctl
        ;;
    "development"|"dev")
        configure_development
        ;;
    "production"|"prod")
        configure_production
        ;;
    "stop")
        stop_components
        exit 0
        ;;
    "status")
        show_status
        exit 0
        ;;
    "health")
        check_health
        exit 0
        ;;
    "logs")
        follow_logs "$2"
        exit 0
        ;;
    *)
        echo "Usage: $0 [local|flowctl|development|production|stop|status|health|logs]"
        echo ""
        echo "Pipeline Modes:"
        echo "  local       - Standalone mode without flowctl"
        echo "  flowctl     - Basic flowctl integration"
        echo "  development - Development mode with fast monitoring"
        echo "  production  - Production mode with optimized settings"
        echo ""
        echo "Management Commands:"
        echo "  stop        - Stop all components"
        echo "  status      - Show pipeline status"
        echo "  health      - Check component health"
        echo "  logs [comp] - Follow component logs (all by default)"
        echo ""
        echo "Examples:"
        echo "  $0 local                    # Start in standalone mode"
        echo "  $0 flowctl                  # Start with flowctl integration"
        echo "  $0 development              # Start in development mode"
        echo "  $0 logs stellar-arrow-source # Follow specific component logs"
        exit 1
        ;;
esac

# Trap to handle cleanup on exit
trap 'log "Received interrupt signal, stopping components..."; stop_components; exit 0' INT TERM

log "Starting obsrvr-stellar-components pipeline in $PIPELINE_MODE mode..."
echo ""

# Start components in order
if start_component "stellar-arrow-source"; then
    sleep 3  # Allow time for source to initialize
    
    if start_component "ttp-arrow-processor"; then
        sleep 3  # Allow time for processor to initialize
        
        if start_component "arrow-analytics-sink"; then
            sleep 5  # Allow time for sink to initialize
            
            log "Pipeline startup complete!"
            echo ""
            show_status
            
            if [ "$ENABLE_FLOWCTL" = "true" ]; then
                log "FlowCtl integration enabled - components will register with control plane"
            fi
            
            log "Monitoring pipeline... (Ctrl+C to stop)"
            
            # Keep the script running and monitor components
            while true; do
                sleep 30
                
                # Check if all components are still running
                all_running=true
                for component in stellar-arrow-source ttp-arrow-processor arrow-analytics-sink; do
                    local component_dir="${COMPONENTS_DIR}/${component}/src"
                    local pid_file="${component_dir}/${component}.pid"
                    
                    if [ -f "$pid_file" ]; then
                        local pid=$(cat "$pid_file")
                        if ! kill -0 $pid 2>/dev/null; then
                            error "$component has stopped unexpectedly"
                            all_running=false
                        fi
                    else
                        error "$component PID file missing"
                        all_running=false
                    fi
                done
                
                if [ "$all_running" = false ]; then
                    error "One or more components have failed, stopping pipeline"
                    stop_components
                    exit 1
                fi
            done
        else
            error "Failed to start arrow-analytics-sink"
            stop_components
            exit 1
        fi
    else
        error "Failed to start ttp-arrow-processor"
        stop_components
        exit 1
    fi
else
    error "Failed to start stellar-arrow-source"
    exit 1
fi
#!/usr/bin/env bash
set -euo pipefail

# Component testing script for obsrvr-stellar-components
# This script runs comprehensive tests for all components

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] ✓${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] ⚠${NC} $*"
}

log_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ✗${NC} $*"
}

# Test configuration
TEST_MODE=${1:-"all"}
PARALLEL=${PARALLEL:-"true"}
COVERAGE=${COVERAGE:-"true"}
INTEGRATION=${INTEGRATION:-"true"}
DOCKER_TESTS=${DOCKER_TESTS:-"false"}

log "Starting component tests - Mode: ${TEST_MODE}"
log "Project root: ${PROJECT_ROOT}"

cd "${PROJECT_ROOT}"

# Ensure test environment is set up
setup_test_environment() {
    log "Setting up test environment..."
    
    # Create test data directories
    mkdir -p test/data/{ledgers,output}/{parquet,json,csv}
    
    # Generate test data if needed
    if [[ ! -f "test/data/ledgers/00000001.xdr" ]]; then
        log "Generating test ledger data..."
        "${SCRIPT_DIR}/generate-test-data.sh"
    fi
    
    log_success "Test environment ready"
}

# Run schema tests
test_schemas() {
    log "Running schema tests..."
    
    cd schemas
    if go test -v -race -coverprofile=coverage.out ./...; then
        log_success "Schema tests passed"
        if [[ "${COVERAGE}" == "true" ]]; then
            go tool cover -html=coverage.out -o coverage.html
            log "Coverage report: schemas/coverage.html"
        fi
    else
        log_error "Schema tests failed"
        return 1
    fi
    cd ..
}

# Test individual component
test_component() {
    local component=$1
    log "Testing component: ${component}"
    
    cd "components/${component}/src"
    
    # Check if component builds
    if ! go build -o "../../../build/${component}" .; then
        log_error "Failed to build ${component}"
        return 1
    fi
    
    # Run unit tests if they exist
    if find . -name "*_test.go" -type f | grep -q .; then
        if go test -v -race -coverprofile=coverage.out ./...; then
            log_success "Unit tests passed for ${component}"
            if [[ "${COVERAGE}" == "true" ]]; then
                go tool cover -html=coverage.out -o coverage.html
                log "Coverage report: components/${component}/src/coverage.html"
            fi
        else
            log_error "Unit tests failed for ${component}"
            return 1
        fi
    else
        log_warning "No unit tests found for ${component}"
    fi
    
    cd ../../..
}

# Run integration tests
test_integration() {
    if [[ "${INTEGRATION}" != "true" ]]; then
        log "Skipping integration tests"
        return 0
    fi
    
    log "Running integration tests..."
    
    cd test
    
    # Set test environment variables
    export TEST_DATA_DIR="${PROJECT_ROOT}/test/data"
    export STELLAR_NETWORK="testnet"
    export LOG_LEVEL="debug"
    
    if go test -v -race -timeout=10m ./integration/...; then
        log_success "Integration tests passed"
    else
        log_error "Integration tests failed"
        return 1
    fi
    
    cd ..
}

# Run Docker tests
test_docker() {
    if [[ "${DOCKER_TESTS}" != "true" ]]; then
        log "Skipping Docker tests"
        return 0
    fi
    
    log "Running Docker tests..."
    
    # Build Docker images
    for component in stellar-arrow-source ttp-arrow-processor arrow-analytics-sink; do
        log "Building Docker image for ${component}..."
        if ! make "docker-${component}"; then
            log_error "Failed to build Docker image for ${component}"
            return 1
        fi
    done
    
    # Run Docker integration tests
    cd test
    export TESTCONTAINERS_RYUK_DISABLED=true
    if go test -v -timeout=15m -tags=docker ./integration/...; then
        log_success "Docker tests passed"
    else
        log_error "Docker tests failed"
        return 1
    fi
    cd ..
}

# Validate component configurations
validate_configurations() {
    log "Validating component configurations..."
    
    # Validate bundle
    if make validate-bundle; then
        log_success "Bundle validation passed"
    else
        log_error "Bundle validation failed"
        return 1
    fi
    
    # Validate pipeline templates
    if make validate-pipelines; then
        log_success "Pipeline validation passed"
    else
        log_error "Pipeline validation failed"
        return 1
    fi
}

# Performance benchmarks
run_benchmarks() {
    log "Running performance benchmarks..."
    
    cd test
    if go test -v -bench=. -benchmem -timeout=5m ./...; then
        log_success "Benchmarks completed"
    else
        log_warning "Some benchmarks failed"
    fi
    cd ..
}

# Main test execution
main() {
    setup_test_environment
    
    case "${TEST_MODE}" in
        "schemas")
            test_schemas
            ;;
        "component")
            if [[ -z "${2:-}" ]]; then
                log_error "Component name required for component test mode"
                exit 1
            fi
            test_component "$2"
            ;;
        "integration")
            test_integration
            ;;
        "docker")
            test_docker
            ;;
        "validate")
            validate_configurations
            ;;
        "bench")
            run_benchmarks
            ;;
        "all")
            validate_configurations
            test_schemas
            
            # Test all components
            for component in stellar-arrow-source ttp-arrow-processor arrow-analytics-sink; do
                test_component "${component}"
            done
            
            test_integration
            
            if [[ "${DOCKER_TESTS}" == "true" ]]; then
                test_docker
            fi
            
            run_benchmarks
            ;;
        *)
            log_error "Unknown test mode: ${TEST_MODE}"
            log "Available modes: schemas, component, integration, docker, validate, bench, all"
            exit 1
            ;;
    esac
    
    log_success "All tests completed successfully!"
}

# Error handling
trap 'log_error "Test script failed at line $LINENO"' ERR

# Run main function
main "$@"
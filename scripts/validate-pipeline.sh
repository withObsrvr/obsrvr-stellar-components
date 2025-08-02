#!/bin/bash

# validate-pipeline.sh - Validate pipeline template configuration
# Usage: ./scripts/validate-pipeline.sh templates/pipeline.yaml

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PIPELINE_FILE="${1:-}"

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Usage function
usage() {
    echo "Usage: $0 <pipeline.yaml>"
    echo
    echo "Validate pipeline template configuration"
    echo
    echo "Arguments:"
    echo "  pipeline.yaml    Path to pipeline configuration file"
    echo
    echo "Options:"
    echo "  -h, --help       Show this help message"
    exit 0
}

# Check arguments
if [[ -z "$PIPELINE_FILE" || "$PIPELINE_FILE" == "--help" || "$PIPELINE_FILE" == "-h" ]]; then
    usage
fi

# Check if file exists
if [[ ! -f "$PIPELINE_FILE" ]]; then
    log_error "Pipeline file not found: $PIPELINE_FILE"
    exit 1
fi

log_info "Validating pipeline configuration: $PIPELINE_FILE"

# Validation functions
validate_yaml_syntax() {
    log_info "Checking YAML syntax..."
    
    if command -v yq >/dev/null 2>&1; then
        if yq eval '.' "$PIPELINE_FILE" >/dev/null 2>&1; then
            log_success "YAML syntax is valid"
        else
            log_error "YAML syntax is invalid"
            return 1
        fi
    else
        log_warning "yq not found, skipping YAML syntax validation"
    fi
}

validate_pipeline_structure() {
    log_info "Validating pipeline structure..."
    
    # Check required fields
    local required_fields=(
        ".apiVersion"
        ".kind"
        ".metadata.name"
        ".spec"
    )
    
    for field in "${required_fields[@]}"; do
        if ! yq eval "$field" "$PIPELINE_FILE" >/dev/null 2>&1; then
            log_error "Missing required field: $field"
            return 1
        fi
    done
    
    # Check kind is Pipeline
    local kind
    kind=$(yq eval '.kind' "$PIPELINE_FILE" 2>/dev/null || echo "")
    if [[ "$kind" != "Pipeline" ]]; then
        log_error "Invalid kind: expected 'Pipeline', got '$kind'"
        return 1
    fi
    
    log_success "Pipeline structure is valid"
}

validate_component_references() {
    log_info "Validating component references..."
    
    # Check sources
    local sources
    sources=$(yq eval '.spec.sources[].component' "$PIPELINE_FILE" 2>/dev/null || echo "")
    
    if [[ -n "$sources" ]]; then
        while IFS= read -r component; do
            if [[ -n "$component" && "$component" != "null" ]]; then
                # Extract component name (handle versioned references)
                local component_name
                component_name=$(echo "$component" | cut -d'@' -f1 | cut -d'/' -f2)
                
                if [[ ! -d "components/$component_name" ]]; then
                    log_warning "Component directory not found: components/$component_name (referenced as $component)"
                else
                    log_success "Source component validated: $component"
                fi
            fi
        done <<< "$sources"
    fi
    
    # Check processors
    local processors
    processors=$(yq eval '.spec.processors[].component' "$PIPELINE_FILE" 2>/dev/null || echo "")
    
    if [[ -n "$processors" ]]; then
        while IFS= read -r component; do
            if [[ -n "$component" && "$component" != "null" ]]; then
                local component_name
                component_name=$(echo "$component" | cut -d'@' -f1 | cut -d'/' -f2)
                
                if [[ ! -d "components/$component_name" ]]; then
                    log_warning "Component directory not found: components/$component_name (referenced as $component)"
                else
                    log_success "Processor component validated: $component"
                fi
            fi
        done <<< "$processors"
    fi
    
    # Check sinks
    local sinks
    sinks=$(yq eval '.spec.sinks[].component' "$PIPELINE_FILE" 2>/dev/null || echo "")
    
    if [[ -n "$sinks" ]]; then
        while IFS= read -r component; do
            if [[ -n "$component" && "$component" != "null" ]]; then
                local component_name
                component_name=$(echo "$component" | cut -d'@' -f1 | cut -d'/' -f2)
                
                if [[ ! -d "components/$component_name" ]]; then
                    log_warning "Component directory not found: components/$component_name (referenced as $component)"
                else
                    log_success "Sink component validated: $component"
                fi
            fi
        done <<< "$sinks"
    fi
}

validate_port_configurations() {
    log_info "Validating port configurations..."
    
    local ports_used=()
    
    # Extract ports from networking configuration
    local network_ports
    network_ports=$(yq eval '.spec.networking.ports | to_entries | .[] | .value' "$PIPELINE_FILE" 2>/dev/null || echo "")
    
    while IFS= read -r port; do
        if [[ -n "$port" && "$port" != "null" ]]; then
            # Remove any variable substitution syntax
            port=$(echo "$port" | sed 's/\${[^}]*:-\([^}]*\)}/\1/g' | sed 's/\${[^}]*}/8080/g')
            
            if [[ " ${ports_used[*]} " =~ " ${port} " ]]; then
                log_error "Port conflict detected: $port"
                return 1
            fi
            ports_used+=("$port")
        fi
    done <<< "$network_ports"
    
    log_success "No port conflicts detected"
}

validate_environment_variables() {
    log_info "Validating environment variable usage..."
    
    # Check for common environment variable patterns
    local env_vars_found=()
    
    # Extract environment variables from the file
    while IFS= read -r line; do
        if [[ "$line" =~ \$\{([A-Z_][A-Z0-9_]*) ]]; then
            local var_name="${BASH_REMATCH[1]}"
            if [[ ! " ${env_vars_found[*]} " =~ " ${var_name} " ]]; then
                env_vars_found+=("$var_name")
            fi
        fi
    done < "$PIPELINE_FILE"
    
    if [[ ${#env_vars_found[@]} -gt 0 ]]; then
        log_info "Environment variables used:"
        for var in "${env_vars_found[@]}"; do
            echo "  - $var"
        done
    fi
    
    log_success "Environment variable usage documented"
}

validate_resource_specifications() {
    log_info "Validating resource specifications..."
    
    # Check if resource specifications are present
    local has_resources=false
    
    # Check sources
    if yq eval '.spec.sources[].resources' "$PIPELINE_FILE" 2>/dev/null | grep -q "requests\|limits"; then
        has_resources=true
    fi
    
    # Check processors
    if yq eval '.spec.processors[].resources' "$PIPELINE_FILE" 2>/dev/null | grep -q "requests\|limits"; then
        has_resources=true
    fi
    
    # Check sinks
    if yq eval '.spec.sinks[].resources' "$PIPELINE_FILE" 2>/dev/null | grep -q "requests\|limits"; then
        has_resources=true
    fi
    
    if [[ "$has_resources" == "true" ]]; then
        log_success "Resource specifications found"
    else
        log_warning "No resource specifications found - consider adding resource limits"
    fi
}

validate_health_checks() {
    log_info "Validating health check configurations..."
    
    local health_checks_found=false
    
    # Check for health check configurations
    if yq eval '.spec.sources[].health' "$PIPELINE_FILE" 2>/dev/null | grep -q "readiness\|liveness"; then
        health_checks_found=true
    fi
    
    if yq eval '.spec.processors[].health' "$PIPELINE_FILE" 2>/dev/null | grep -q "readiness\|liveness"; then
        health_checks_found=true
    fi
    
    if yq eval '.spec.sinks[].health' "$PIPELINE_FILE" 2>/dev/null | grep -q "readiness\|liveness"; then
        health_checks_found=true
    fi
    
    if [[ "$health_checks_found" == "true" ]]; then
        log_success "Health check configurations found"
    else
        log_warning "No health check configurations found - consider adding health checks"
    fi
}

validate_environment_overrides() {
    log_info "Validating environment-specific overrides..."
    
    # Check for environment configurations
    local environments
    environments=$(yq eval '.spec.environments | keys | .[]' "$PIPELINE_FILE" 2>/dev/null || echo "")
    
    if [[ -n "$environments" ]]; then
        log_info "Environment overrides found:"
        while IFS= read -r env; do
            if [[ -n "$env" && "$env" != "null" ]]; then
                echo "  - $env"
            fi
        done <<< "$environments"
        log_success "Environment overrides validated"
    else
        log_warning "No environment-specific overrides found"
    fi
}

validate_monitoring_configuration() {
    log_info "Validating monitoring configuration..."
    
    local has_monitoring=false
    
    # Check for monitoring configuration
    if yq eval '.spec.monitoring' "$PIPELINE_FILE" 2>/dev/null | grep -q "metrics\|logging\|tracing"; then
        has_monitoring=true
        log_success "Monitoring configuration found"
    fi
    
    # Check for dashboard configuration
    if yq eval '.spec.dashboards' "$PIPELINE_FILE" 2>/dev/null | grep -q "enabled\|grafana"; then
        log_success "Dashboard configuration found"
    fi
    
    # Check for analytics configuration
    if yq eval '.spec.analytics' "$PIPELINE_FILE" 2>/dev/null | grep -q "streaming\|real_time"; then
        log_success "Analytics configuration found"
    fi
    
    if [[ "$has_monitoring" == "false" ]]; then
        log_warning "No monitoring configuration found - consider adding monitoring"
    fi
}

# Main validation workflow
main() {
    log_info "Starting pipeline validation..."
    echo
    
    # Run all validations
    local exit_code=0
    
    validate_yaml_syntax || exit_code=1
    echo
    
    validate_pipeline_structure || exit_code=1
    echo
    
    validate_component_references || exit_code=1
    echo
    
    validate_port_configurations || exit_code=1
    echo
    
    validate_environment_variables || exit_code=1
    echo
    
    validate_resource_specifications || exit_code=1
    echo
    
    validate_health_checks || exit_code=1
    echo
    
    validate_environment_overrides || exit_code=1
    echo
    
    validate_monitoring_configuration || exit_code=1
    echo
    
    # Final result
    if [[ $exit_code -eq 0 ]]; then
        log_success "Pipeline validation completed successfully!"
        echo
        log_info "Pipeline summary:"
        echo "  Name: $(yq eval '.metadata.name' "$PIPELINE_FILE")"
        echo "  Description: $(yq eval '.metadata.description // "N/A"' "$PIPELINE_FILE")"
        
        local source_count
        source_count=$(yq eval '.spec.sources | length' "$PIPELINE_FILE" 2>/dev/null || echo "0")
        echo "  Sources: $source_count"
        
        local processor_count
        processor_count=$(yq eval '.spec.processors | length' "$PIPELINE_FILE" 2>/dev/null || echo "0")
        echo "  Processors: $processor_count"
        
        local sink_count
        sink_count=$(yq eval '.spec.sinks | length' "$PIPELINE_FILE" 2>/dev/null || echo "0")
        echo "  Sinks: $sink_count"
    else
        log_error "Pipeline validation failed!"
        echo
        log_info "Please fix the errors above and run validation again."
    fi
    
    exit $exit_code
}

# Install yq if not available
install_yq() {
    if ! command -v yq >/dev/null 2>&1; then
        log_info "Installing yq..."
        
        case "$(uname -s)" in
            Linux*)
                sudo curl -sSL https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -o /usr/local/bin/yq
                sudo chmod +x /usr/local/bin/yq
                ;;
            Darwin*)
                if command -v brew >/dev/null 2>&1; then
                    brew install yq
                else
                    sudo curl -sSL https://github.com/mikefarah/yq/releases/latest/download/yq_darwin_amd64 -o /usr/local/bin/yq
                    sudo chmod +x /usr/local/bin/yq
                fi
                ;;
            *)
                log_error "Unsupported operating system for automatic yq installation"
                log_info "Please install yq manually: https://github.com/mikefarah/yq"
                exit 1
                ;;
        esac
        
        log_success "yq installed successfully"
    fi
}

# Install yq if needed
install_yq

# Change to project root
cd "$PROJECT_ROOT"

# Run main validation
main
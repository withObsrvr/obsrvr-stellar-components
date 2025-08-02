#!/bin/bash

# validate-bundle.sh - Validate component bundle configuration
# Usage: ./scripts/validate-bundle.sh bundle.yaml

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
BUNDLE_FILE="${1:-bundle.yaml}"

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

# Check if file exists
if [[ ! -f "$BUNDLE_FILE" ]]; then
    log_error "Bundle file not found: $BUNDLE_FILE"
    exit 1
fi

log_info "Validating bundle configuration: $BUNDLE_FILE"

# Validation functions
validate_yaml_syntax() {
    log_info "Checking YAML syntax..."
    
    if command -v yq >/dev/null 2>&1; then
        if yq eval '.' "$BUNDLE_FILE" >/dev/null 2>&1; then
            log_success "YAML syntax is valid"
        else
            log_error "YAML syntax is invalid"
            return 1
        fi
    else
        log_warning "yq not found, skipping YAML syntax validation"
    fi
}

validate_bundle_structure() {
    log_info "Validating bundle structure..."
    
    # Check required fields
    local required_fields=(
        ".apiVersion"
        ".kind"
        ".metadata.name"
        ".metadata.version"
        ".spec.components"
    )
    
    for field in "${required_fields[@]}"; do
        if ! yq eval "$field" "$BUNDLE_FILE" >/dev/null 2>&1; then
            log_error "Missing required field: $field"
            return 1
        fi
    done
    
    log_success "Bundle structure is valid"
}

validate_component_references() {
    log_info "Validating component references..."
    
    # Get component names from bundle
    local components
    components=$(yq eval '.spec.components[].name' "$BUNDLE_FILE" 2>/dev/null || echo "")
    
    if [[ -z "$components" ]]; then
        log_error "No components found in bundle"
        return 1
    fi
    
    # Check if component directories exist  
    while IFS= read -r component; do
        local component_path="components/$component"
        local component_yaml="$component_path/component.yaml"
        
        if [[ ! -d "$component_path" ]]; then
            log_error "Component directory not found: $component_path"
            return 1
        fi
        
        if [[ ! -f "$component_yaml" ]]; then
            log_error "Component specification not found: $component_yaml"
            return 1
        fi
        
        # Validate component.yaml structure
        if ! yq eval '.spec.type' "$component_yaml" >/dev/null 2>&1; then
            log_error "Invalid component specification: $component_yaml"
            return 1
        fi
        
        log_success "Component validated: $component"
    done <<< "$components"
}

validate_template_references() {
    log_info "Validating template references..."
    
    # Get template names from bundle
    local templates
    templates=$(yq eval '.spec.templates[].name' "$BUNDLE_FILE" 2>/dev/null || echo "")
    
    if [[ -z "$templates" ]]; then
        log_warning "No templates found in bundle"
        return 0
    fi
    
    # Check if template files exist
    while IFS= read -r template; do
        local template_file="templates/$template.yaml"
        
        if [[ ! -f "$template_file" ]]; then
            log_error "Template file not found: $template_file"
            return 1
        fi
        
        # Basic template validation
        if ! yq eval '.apiVersion' "$template_file" >/dev/null 2>&1; then
            log_error "Invalid template file: $template_file"
            return 1
        fi
        
        log_success "Template validated: $template"
    done <<< "$templates"
}

validate_schema_references() {
    log_info "Validating schema references..."
    
    # Check if schemas directory exists
    if [[ ! -d "schemas" ]]; then
        log_error "Schemas directory not found"
        return 1
    fi
    
    # Check for schema files
    local schema_files=(
        "schemas/stellar_schemas.go"
        "schemas/schema_utils.go"
        "schemas/go.mod"
    )
    
    for schema_file in "${schema_files[@]}"; do
        if [[ ! -f "$schema_file" ]]; then
            log_error "Schema file not found: $schema_file"
            return 1
        fi
    done
    
    log_success "Schema files validated"
}

validate_version_consistency() {
    log_info "Validating version consistency..."
    
    local bundle_version
    bundle_version=$(yq eval '.metadata.version' "$BUNDLE_FILE" 2>/dev/null || echo "")
    
    if [[ -z "$bundle_version" ]]; then
        log_error "Bundle version not found"
        return 1
    fi
    
    # Check component versions match bundle version
    local components
    components=$(yq eval '.spec.components[].name' "$BUNDLE_FILE" 2>/dev/null || echo "")
    
    while IFS= read -r component; do
        local component_yaml="components/$component/component.yaml"
        local component_version
        component_version=$(yq eval '.metadata.version' "$component_yaml" 2>/dev/null || echo "")
        
        if [[ "$component_version" != "$bundle_version" ]]; then
            log_warning "Version mismatch in component $component: $component_version != $bundle_version"
        fi
    done <<< "$components"
    
    log_success "Version consistency checked"
}

validate_port_conflicts() {
    log_info "Validating port configurations..."
    
    # Extract port configurations
    local ports_used=()
    
    # Get ports from communication configuration
    local comm_ports
    comm_ports=$(yq eval '.spec.communication.ports | to_entries | .[] | .value' "$BUNDLE_FILE" 2>/dev/null || echo "")
    
    while IFS= read -r port; do
        if [[ -n "$port" && "$port" != "null" ]]; then
            if [[ " ${ports_used[*]} " =~ " ${port} " ]]; then
                log_error "Port conflict detected: $port"
                return 1
            fi
            ports_used+=("$port")
        fi
    done <<< "$comm_ports"
    
    log_success "No port conflicts detected"
}

validate_dependencies() {
    log_info "Validating dependencies..."
    
    # Check required tools
    local required_tools=("go" "docker")
    
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            log_error "Required tool not found: $tool"
            return 1
        fi
    done
    
    # Check Go version
    local go_version
    go_version=$(go version | grep -oE 'go[0-9]+\.[0-9]+')
    local required_go_version="go1.23"
    
    if [[ "$go_version" < "$required_go_version" ]]; then
        log_error "Go version $required_go_version or higher required, found: $go_version"
        return 1
    fi
    
    log_success "Dependencies validated"
}

# Main validation workflow
main() {
    log_info "Starting bundle validation..."
    echo
    
    # Run all validations
    local exit_code=0
    
    validate_yaml_syntax || exit_code=1
    echo
    
    validate_bundle_structure || exit_code=1
    echo
    
    validate_component_references || exit_code=1
    echo
    
    validate_template_references || exit_code=1
    echo
    
    validate_schema_references || exit_code=1
    echo
    
    validate_version_consistency || exit_code=1
    echo
    
    validate_port_conflicts || exit_code=1
    echo
    
    validate_dependencies || exit_code=1
    echo
    
    # Final result
    if [[ $exit_code -eq 0 ]]; then
        log_success "Bundle validation completed successfully!"
        echo
        log_info "Bundle summary:"
        echo "  Name: $(yq eval '.metadata.name' "$BUNDLE_FILE")"
        echo "  Version: $(yq eval '.metadata.version' "$BUNDLE_FILE")"
        echo "  Components: $(yq eval '.spec.components | length' "$BUNDLE_FILE")"
        echo "  Templates: $(yq eval '.spec.templates | length' "$BUNDLE_FILE")"
    else
        log_error "Bundle validation failed!"
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
                curl -sSL https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -o /usr/local/bin/yq
                chmod +x /usr/local/bin/yq
                ;;
            Darwin*)
                if command -v brew >/dev/null 2>&1; then
                    brew install yq
                else
                    curl -sSL https://github.com/mikefarah/yq/releases/latest/download/yq_darwin_amd64 -o /usr/local/bin/yq
                    chmod +x /usr/local/bin/yq
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

# Check for help flag
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo "Usage: $0 [bundle.yaml]"
    echo
    echo "Validate component bundle configuration"
    echo
    echo "Arguments:"
    echo "  bundle.yaml    Path to bundle configuration file (default: bundle.yaml)"
    echo
    echo "Options:"
    echo "  -h, --help     Show this help message"
    exit 0
fi

# Install yq if needed
install_yq

# Change to project root
cd "$PROJECT_ROOT"

# Run main validation
main
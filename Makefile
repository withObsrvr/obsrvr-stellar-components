# obsrvr-stellar-components Makefile
# Build system for Apache Arrow-native Stellar processing components

# Project configuration
PROJECT_NAME := obsrvr-stellar-components
VERSION := v1.0.0
GO_VERSION := 1.23

# Components
COMPONENTS := stellar-arrow-source ttp-arrow-processor arrow-analytics-sink
COMPONENT_DIRS := $(addprefix components/,$(COMPONENTS))

# Build configuration
BUILD_DIR := build
DIST_DIR := dist
DOCKER_REGISTRY := ghcr.io/withobsrvr
PLATFORMS := linux/amd64,linux/arm64

# Go build configuration
CGO_ENABLED := 1
GOOS := linux
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.buildTime=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)
BUILD_FLAGS := -a -installsuffix cgo -ldflags "$(LDFLAGS)"

# Arrow configuration
ARROW_VERSION := 17.0.0

# Colors for output
CYAN := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m

.PHONY: help
help: ## Display this help message
	@echo "$(CYAN)obsrvr-stellar-components Build System$(RESET)"
	@echo ""
	@echo "$(YELLOW)Available targets:$(RESET)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ Development

.PHONY: deps
deps: ## Install development dependencies
	@echo "$(CYAN)Installing dependencies...$(RESET)"
	@go mod download
	@cd schemas && go mod download

.PHONY: dev-setup
dev-setup: deps ## Set up development environment
	@echo "$(CYAN)Setting up development environment...$(RESET)"
	@mkdir -p $(BUILD_DIR) $(DIST_DIR) data logs
	@echo "Development environment ready"

.PHONY: clean
clean: ## Clean build artifacts
	@echo "$(CYAN)Cleaning build artifacts...$(RESET)"
	@rm -rf $(BUILD_DIR) $(DIST_DIR)
	@find . -name "*.test" -delete
	@find . -name "coverage.out" -delete
	@docker system prune -f 2>/dev/null || true

##@ Building

.PHONY: build
build: $(COMPONENTS) ## Build all components
	@echo "$(GREEN)All components built successfully$(RESET)"

.PHONY: build-schemas
build-schemas: ## Build and validate Arrow schemas
	@echo "$(CYAN)Building Arrow schemas...$(RESET)"
	@cd schemas && go build -o ../$(BUILD_DIR)/schema-tools .
	@$(BUILD_DIR)/schema-tools validate
	@echo "$(GREEN)Schema validation passed$(RESET)"

.PHONY: $(COMPONENTS)
$(COMPONENTS): build-schemas
	@echo "$(CYAN)Building component: $@$(RESET)"
	@mkdir -p $(BUILD_DIR)/$@
	@cd components/$@/src && \
		CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) \
		go build $(BUILD_FLAGS) -o ../../../$(BUILD_DIR)/$@/$@ .
	@echo "$(GREEN)Built $@ successfully$(RESET)"

##@ Testing

.PHONY: test
test: ## Run all tests
	@echo "$(CYAN)Running tests...$(RESET)"
	@go test -v ./schemas/...
	@for component in $(COMPONENTS); do \
		echo "Testing $$component..."; \
		cd components/$$component/src && go test -v ./...; \
		cd ../../..; \
	done
	@echo "$(GREEN)All tests passed$(RESET)"

.PHONY: test-coverage
test-coverage: ## Run tests with coverage
	@echo "$(CYAN)Running tests with coverage...$(RESET)"
	@mkdir -p $(BUILD_DIR)/coverage
	@go test -coverprofile=$(BUILD_DIR)/coverage/schemas.out ./schemas/...
	@for component in $(COMPONENTS); do \
		echo "Testing $$component with coverage..."; \
		cd components/$$component/src && \
		go test -coverprofile=../../../$(BUILD_DIR)/coverage/$$component.out ./...; \
		cd ../../..; \
	done
	@echo "$(GREEN)Coverage reports generated in $(BUILD_DIR)/coverage$(RESET)"

.PHONY: benchmark
benchmark: ## Run benchmarks
	@echo "$(CYAN)Running benchmarks...$(RESET)"
	@go test -bench=. -benchmem ./schemas/...
	@for component in $(COMPONENTS); do \
		echo "Benchmarking $$component..."; \
		cd components/$$component/src && go test -bench=. -benchmem ./...; \
		cd ../../..; \
	done

##@ Code Quality

.PHONY: lint
lint: ## Run linters
	@echo "$(CYAN)Running linters...$(RESET)"
	@golangci-lint run ./schemas/...
	@for component in $(COMPONENTS); do \
		echo "Linting $$component..."; \
		cd components/$$component/src && golangci-lint run ./...; \
		cd ../../..; \
	done
	@echo "$(GREEN)Linting passed$(RESET)"

.PHONY: fmt
fmt: ## Format code
	@echo "$(CYAN)Formatting code...$(RESET)"
	@go fmt ./schemas/...
	@for component in $(COMPONENTS); do \
		cd components/$$component/src && go fmt ./...; \
		cd ../../..; \
	done
	@echo "$(GREEN)Code formatted$(RESET)"

.PHONY: vet
vet: ## Run go vet
	@echo "$(CYAN)Running go vet...$(RESET)"
	@go vet ./schemas/...
	@for component in $(COMPONENTS); do \
		cd components/$$component/src && go vet ./...; \
		cd ../../..; \
	done
	@echo "$(GREEN)Vet checks passed$(RESET)"

##@ Docker

.PHONY: docker-build
docker-build: ## Build Docker images for all components
	@echo "$(CYAN)Building Docker images...$(RESET)"
	@for component in $(COMPONENTS); do \
		echo "Building Docker image for $$component..."; \
		docker build \
			--build-arg GO_VERSION=$(GO_VERSION) \
			--build-arg ARROW_VERSION=$(ARROW_VERSION) \
			--build-arg VERSION=$(VERSION) \
			-t $(DOCKER_REGISTRY)/$$component:$(VERSION) \
			-t $(DOCKER_REGISTRY)/$$component:latest \
			-f components/$$component/Dockerfile \
			.; \
	done
	@echo "$(GREEN)Docker images built successfully$(RESET)"

.PHONY: docker-push
docker-push: docker-build ## Push Docker images to registry
	@echo "$(CYAN)Pushing Docker images...$(RESET)"
	@for component in $(COMPONENTS); do \
		echo "Pushing $$component..."; \
		docker push $(DOCKER_REGISTRY)/$$component:$(VERSION); \
		docker push $(DOCKER_REGISTRY)/$$component:latest; \
	done
	@echo "$(GREEN)Docker images pushed successfully$(RESET)"

.PHONY: docker-run-dev
docker-run-dev: ## Run development environment with Docker Compose
	@echo "$(CYAN)Starting development environment...$(RESET)"
	@docker-compose -f docker-compose.dev.yml up -d
	@echo "$(GREEN)Development environment started$(RESET)"
	@echo "WebSocket: http://localhost:8080/ws"
	@echo "Health: http://localhost:8088/health"
	@echo "Metrics: http://localhost:9090/metrics"

.PHONY: docker-stop-dev
docker-stop-dev: ## Stop development environment
	@echo "$(CYAN)Stopping development environment...$(RESET)"
	@docker-compose -f docker-compose.dev.yml down
	@echo "$(GREEN)Development environment stopped$(RESET)"

##@ Release

.PHONY: dist
dist: build ## Create distribution packages
	@echo "$(CYAN)Creating distribution packages...$(RESET)"
	@mkdir -p $(DIST_DIR)
	@for component in $(COMPONENTS); do \
		echo "Packaging $$component..."; \
		tar -czf $(DIST_DIR)/$$component-$(VERSION)-linux-amd64.tar.gz \
			-C $(BUILD_DIR)/$$component $$component; \
	done
	@echo "$(GREEN)Distribution packages created in $(DIST_DIR)$(RESET)"

.PHONY: release
release: clean test lint dist docker-build ## Create a full release
	@echo "$(CYAN)Creating release $(VERSION)...$(RESET)"
	@echo "$(GREEN)Release $(VERSION) ready$(RESET)"

##@ Development Workflows

.PHONY: dev
dev: dev-setup build ## Start development mode
	@echo "$(CYAN)Starting development mode...$(RESET)"
	@echo "Built components available in $(BUILD_DIR)/"
	@echo ""
	@echo "$(YELLOW)Quick start:$(RESET)"
	@echo "  1. Configure environment: cp examples/dev.env .env"
	@echo "  2. Run RPC pipeline: make run-rpc-pipeline"
	@echo "  3. View real-time data: http://localhost:8080/ws"

.PHONY: run-rpc-pipeline
run-rpc-pipeline: build ## Run RPC pipeline template
	@echo "$(CYAN)Running RPC pipeline...$(RESET)"
	@export STELLAR_NETWORK=testnet && \
	export STELLAR_RPC_URL=https://soroban-testnet.stellar.org && \
	export DATA_DIR=./data && \
	./scripts/run-pipeline.sh templates/rpc-pipeline.yaml

.PHONY: run-analytics-pipeline
run-analytics-pipeline: build ## Run analytics pipeline template
	@echo "$(CYAN)Running analytics pipeline...$(RESET)"
	@export STELLAR_NETWORK=mainnet && \
	export STELLAR_RPC_URL=https://horizon.stellar.org && \
	export DATA_DIR=./data && \
	export ENABLE_DASHBOARDS=true && \
	./scripts/run-pipeline.sh templates/analytics-pipeline.yaml

.PHONY: test-data
test-data: ## Generate test data for development
	@echo "$(CYAN)Generating test data...$(RESET)"
	@mkdir -p data/test
	@$(BUILD_DIR)/schema-tools generate-test-data \
		--output data/test \
		--ledgers 1000 \
		--events 5000
	@echo "$(GREEN)Test data generated in data/test$(RESET)"

##@ Utilities

.PHONY: validate-bundle
validate-bundle: ## Validate component bundle configuration
	@echo "$(CYAN)Validating bundle configuration...$(RESET)"
	@./scripts/validate-bundle.sh bundle.yaml
	@for template in templates/*.yaml; do \
		echo "Validating $$template..."; \
		./scripts/validate-pipeline.sh "$$template"; \
	done
	@echo "$(GREEN)Bundle validation passed$(RESET)"

.PHONY: generate-docs
generate-docs: ## Generate documentation
	@echo "$(CYAN)Generating documentation...$(RESET)"
	@./scripts/generate-docs.sh
	@echo "$(GREEN)Documentation generated in docs/$(RESET)"

.PHONY: check-deps
check-deps: ## Check for outdated dependencies
	@echo "$(CYAN)Checking dependencies...$(RESET)"
	@go list -u -m all | grep '\[.*\]'
	@cd schemas && go list -u -m all | grep '\[.*\]'

.PHONY: update-deps
update-deps: ## Update dependencies
	@echo "$(CYAN)Updating dependencies...$(RESET)"
	@go get -u ./...
	@go mod tidy
	@cd schemas && go get -u ./... && go mod tidy

##@ CI/CD

.PHONY: ci-test
ci-test: deps test lint vet ## Run CI test suite
	@echo "$(GREEN)CI test suite passed$(RESET)"

.PHONY: ci-build
ci-build: ci-test build docker-build ## Run CI build pipeline
	@echo "$(GREEN)CI build pipeline completed$(RESET)"

.PHONY: security-scan
security-scan: ## Run security scans
	@echo "$(CYAN)Running security scans...$(RESET)"
	@gosec ./schemas/...
	@for component in $(COMPONENTS); do \
		echo "Scanning $$component..."; \
		cd components/$$component/src && gosec ./...; \
		cd ../../..; \
	done
	@echo "$(GREEN)Security scan completed$(RESET)"

##@ Environment Management

.PHONY: env-dev
env-dev: ## Create development environment file
	@echo "$(CYAN)Creating development environment...$(RESET)"
	@cp examples/dev.env .env
	@echo "$(GREEN)Development environment created (.env)$(RESET)"

.PHONY: env-prod
env-prod: ## Create production environment template
	@echo "$(CYAN)Creating production environment template...$(RESET)"
	@cp examples/prod.env .env.prod
	@echo "$(YELLOW)Review and customize .env.prod before use$(RESET)"

##@ Monitoring

.PHONY: health-check
health-check: ## Check component health
	@echo "$(CYAN)Checking component health...$(RESET)"
	@./scripts/health-check.sh

.PHONY: metrics
metrics: ## Display metrics
	@echo "$(CYAN)Fetching metrics...$(RESET)"
	@curl -s http://localhost:9090/metrics | head -20

.PHONY: logs
logs: ## Show component logs
	@echo "$(CYAN)Component logs:$(RESET)"
	@tail -f logs/*.log 2>/dev/null || echo "No log files found. Start components first."

# Default target
.DEFAULT_GOAL := help
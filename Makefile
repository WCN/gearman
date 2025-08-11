# Go library package Makefile
.PHONY: help test lint vet staticcheck clean deps check

# Go related variables
GO ?= go
GOPATH ?= ${HOME}/go
STATICCHECK_PATH ?= ${GOPATH}/bin/staticcheck

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Available targets:"
	@echo "  test       - Run tests with coverage"
	@echo "  lint       - Run vet and staticcheck"
	@echo "  vet        - Run go vet"
	@echo "  staticcheck - Run staticcheck"
	@echo "  clean      - Remove build artifacts"
	@echo "  deps       - Download and tidy dependencies"
	@echo "  check      - Run all checks (test + lint)"

test: ## Run tests with coverage
	@echo "Running tests with coverage..."
	${GO} test -cover -coverprofile=coverage.out -covermode=count -v ./...
	@echo "Generating coverage report..."
	${GO} tool cover -html coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

lint: vet staticcheck ## Run all linting checks

vet: ## Run go vet
	${GO} vet ./...

staticcheck: ## Run staticcheck
	@echo "Running staticcheck..."
	@if [ ! -f "$(STATICCHECK_PATH)" ]; then \
		echo "Installing staticcheck..."; \
		${GO} install honnef.co/go/tools/cmd/staticcheck@latest; \
	fi
	$(STATICCHECK_PATH) ./...

fmt: ## Check code formatting
	@echo "Checking code formatting..."
	@if [ "$$(${GO} fmt -l . | wc -l)" -gt 0 ]; then \
		echo "Code is not formatted. Run 'make fmt-fix' to fix."; \
		${GO} fmt -l .; \
		exit 1; \
	fi
	@echo "Code is properly formatted."

fmt-fix: ## Fix code formatting
	@echo "Fixing code formatting..."
	${GO} fmt ./...

clean: ## Clean build artifacts and temporary files
	@echo "Cleaning..."
	rm -f coverage.out coverage.html
	${GO} clean

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	${GO} mod download
	${GO} mod tidy

deps-update: ## Update dependencies
	@echo "Updating dependencies..."
	${GO} get -u ./...
	${GO} mod tidy

deps-check: ## Check for outdated dependencies
	@echo "Checking for outdated dependencies..."
	${GO} list -u -m all

# CI/CD targets
check: test lint ## Run all checks (test + lint)
	@echo "All checks completed successfully"

# Documentation targets
docs: ## Generate documentation
	@echo "Generating documentation..."
	${GO} doc -all ./...

# Development helpers
dev-setup: deps ## Set up development environment
	@echo "Setting up development environment..."
	@echo "Installing staticcheck..."
	${GO} install honnef.co/go/tools/cmd/staticcheck@latest
	@echo "Development environment ready!"

# Pre-commit hook (can be symlinked to .git/hooks/pre-commit)
pre-commit: fmt lint test ## Run pre-commit checks
	@echo "Pre-commit checks passed!"

# Module verification
mod-verify: ## Verify module dependencies
	@echo "Verifying module dependencies..."
	${GO} mod verify

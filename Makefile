# Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
# This is licensed software from AccelByte Inc, for limitations
# and restrictions contact your company contract manager.

SHELL := /bin/bash

.PHONY: proto build lint lint-fix test test-unit test-coverage test-all test-integration test-integration-setup test-integration-teardown test-integration-run

proto:
	docker run --tty --rm --user $$(id -u):$$(id -g) \
		--volume $$(pwd):/build \
		--workdir /build \
		--entrypoint /bin/bash \
		rvolosatovs/protoc:4.1.0 \
			proto.sh

build: proto

# Linting targets
lint:
	@echo "Running golangci-lint..."
	@golangci-lint run ./...

lint-fix:
	@echo "Running golangci-lint with auto-fix..."
	@golangci-lint run --fix ./...

# Unit testing targets
test:
	@echo "Running unit tests..."
	@go test $$(go list ./... | grep -v /integration) -v

test-unit: test

test-coverage:
	@echo "Running unit tests with coverage..."
	@go test $$(go list ./... | grep -v /integration) -coverprofile=coverage.out
	@go tool cover -func=coverage.out | grep total

# Integration testing targets
test-integration-setup:
	@echo "Starting test database..."
	@docker-compose -f docker-compose.test.yml up -d postgres-test
	@echo "Waiting for database to be ready..."
	@sleep 5

test-integration-teardown:
	@echo "Stopping test database..."
	@docker-compose -f docker-compose.test.yml down -v

test-integration-run:
	@echo "Running integration tests..."
	@go test ./pkg/processor -v -run TestE2E

test-integration:
	@$(MAKE) test-integration-setup
	@if ! $(MAKE) test-integration-run; then \
		$(MAKE) test-integration-teardown; \
		exit 1; \
	fi
	@$(MAKE) test-integration-teardown
	@echo "✅ Integration tests complete!"

# Run all checks (lint + unit tests + integration tests)
test-all: lint test-coverage test-integration
	@echo "✅ All checks passed!"


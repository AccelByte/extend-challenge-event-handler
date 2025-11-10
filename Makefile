# Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
# This is licensed software from AccelByte Inc, for limitations
# and restrictions contact your company contract manager.

SHELL := /bin/bash

.PHONY: proto build lint lint-fix test test-coverage test-all

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

# Testing targets
test:
	@echo "Running tests..."
	@go test ./... -v

test-coverage:
	@echo "Running tests with coverage..."
	@go test ./... -coverprofile=coverage.out
	@go tool cover -func=coverage.out | grep total

# Run all checks (lint + test with coverage)
test-all: lint test-coverage
	@echo "✅ All checks passed!"


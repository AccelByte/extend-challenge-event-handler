# Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
# This is licensed software from AccelByte Inc, for limitations
# and restrictions contact your company contract manager.

# ----------------------------------------
# Stage 1: Protoc Code Generation
# ----------------------------------------
FROM --platform=$BUILDPLATFORM rvolosatovs/protoc:4.0.0 AS proto-builder

# Set working directory.
WORKDIR /build

# Copy proto sources and generator script.
COPY proto.sh .
COPY pkg/proto/ pkg/proto/

# Make script executable and run it.
RUN chmod +x proto.sh && \
    ./proto.sh



# ----------------------------------------
# Stage 2: Builder
# ----------------------------------------
FROM --platform=$BUILDPLATFORM golang:alpine AS builder

# Set the value for the target OS and architecture.
ARG TARGETOS
ARG TARGETARCH

# Set the value for GOCACHE and GOMODCACHE.
ARG GOCACHE=/tmp/build-cache/go/cache
ARG GOMODCACHE=/tmp/build-cache/go/modcache

# Set working directory.
WORKDIR /build

# Copy and download the dependencies for application.
COPY go.mod go.sum ./
RUN go mod download

# Copy application code.
COPY . .

# Copy generated protobuf files from stage 1.
COPY --from=proto-builder /build/pkg/pb pkg/pb

# Build the Go application binary for the target OS and architecture.
RUN env GOOS=$TARGETOS GOARCH=$TARGETARCH go build -modcacherw -o extend-challenge-event-handler_$TARGETOS-$TARGETARCH


# ----------------------------------------
# Stage 3: Runtime Container
# ----------------------------------------
FROM alpine:3.22

# Set the value for the target OS and architecture.
ARG TARGETOS
ARG TARGETARCH

# Set working directory.
WORKDIR /app

# Copy build from stage 2.
COPY --from=builder /build/extend-challenge-event-handler_$TARGETOS-$TARGETARCH extend-challenge-event-handler

# Copy config for local development
COPY --from=builder /build/config /app/config

# Plugin Arch gRPC Server Port.
EXPOSE 6565

# Prometheus /metrics Web Server Port.
EXPOSE 8080

# Entrypoint.
CMD [ "/app/extend-challenge-event-handler" ]
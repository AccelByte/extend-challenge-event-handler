#!/bin/bash

set -eou pipefail

shopt -s globstar

PROTO_DIR="${1:-pkg/proto}"
OUT_DIR="${2:-pkg/pb}"

# Ensure output directory exists
mkdir -p "${OUT_DIR}"

# Clean previously generated files
find "${OUT_DIR}" -type f \( -name '*.go' \) -delete

# Generate protobuf files.
protoc-wrapper \
  --proto_path="${PROTO_DIR}" \
  --go_out="${OUT_DIR}" \
  --go_opt=paths=source_relative \
  --go-grpc_out="${OUT_DIR}" \
  --go-grpc_opt=paths=source_relative \
  "${PROTO_DIR}"/**/*.proto

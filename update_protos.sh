#!/usr/bin/env bash
set -euo pipefail

protoc --go_out=. --go_opt=paths=source_relative fdb-layer/annotations.proto

PLUGIN_BIN="$(mktemp)"
trap 'rm -f "$PLUGIN_BIN"' EXIT
go build -o "$PLUGIN_BIN" .

protoc \
  --plugin=protoc-gen-fdb="$PLUGIN_BIN" \
  --go_out=. --go_opt=paths=source_relative \
  --fdb_out=. --fdb_opt=paths=source_relative \
  tests/store/store.proto \
  tests/atomic/atomic.proto

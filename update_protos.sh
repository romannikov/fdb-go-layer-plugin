#!/usr/bin/env bash
set -euo pipefail

mkdir -p tests/go/store tests/go/atomic tests/rust/src/gen

protoc -I proto \
  --go_out=proto --go_opt=paths=source_relative \
  proto/fdb-layer/annotations.proto

PLUGIN_GO="$(mktemp)"
PLUGIN_RUST="$(mktemp)"
trap 'rm -f "$PLUGIN_GO" "$PLUGIN_RUST"' EXIT

go build -o "$PLUGIN_GO" ./compiler/cmd/protoc-gen-fdb-go
go build -o "$PLUGIN_RUST" ./compiler/cmd/protoc-gen-fdb-rust

protoc -I proto -I tests/proto \
  --plugin=protoc-gen-fdb-go="$PLUGIN_GO" \
  --plugin=protoc-gen-fdb-rust="$PLUGIN_RUST" \
  --go_out=. --go_opt=module=github.com/romannikov/fdb-layer \
  --fdb-go_out=. --fdb-go_opt=module=github.com/romannikov/fdb-layer \
  --fdb-rust_out=tests/rust/src/gen --fdb-rust_opt=generate_messages=true \
  tests/proto/store.proto \
  tests/proto/atomic.proto

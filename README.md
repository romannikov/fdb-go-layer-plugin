# FoundationDB Protobuf Layer (`fdb-layer`)

A `protoc` compiler (`protoc-gen-fdb-go`) and runtime library that generates FoundationDB data access layer code from shared Protobuf annotations (`proto/fdb-layer/annotations.proto`).

The compiler is structured around a **language-neutral Intermediate Representation (`compiler/ir`)** and a **pluggable backend interface (`compiler/backend`)** so new target languages can be added without duplicating schema parsing or validation logic. Currently, the **Go** backend (`compiler/backend/golang`) and runtime (`runtimes/go`) are implemented.

---

## Supported Features

- **Shared Protobuf Annotations (`proto/fdb-layer/annotations.proto`)**: Single source of truth for compile-time message `type_id`s, primary keys (`PrimaryKey`), secondary/fan-out indexes (`SecondaryIndex`), versionstamped primary keys (`is_versionstamp`), and FIFO queues (`is_queue`). Reserved options (`mutation`, `interleave`, `split_large_records`, `ttl_field`, and `SecondaryIndex` options `unique`, `storing`, `sparse`, `aggregate`, `aggregate_field`) are documented in `annotations.proto` with `// [NOT IMPLEMENTED YET]` and rejected at compile time if specified.
- **Extensible Compiler Architecture**:
  - `compiler/ir`: Extracts `FileSpec`, `MessageSpec`, `FieldSpec`, and `SecondaryIndexSpec` from `protogen.File`, and validates all schema invariants (including mandatory non-zero `type_id`, `secondary_index.id` uniqueness, and duplicate field-sequence rejection).
  - `compiler/backend`: Defines the language-agnostic `Generator` interface, `Registry`, `Options`, and `RunProtogen` helper.
  - `compiler/backend/golang`: Emits zero-allocation `*.fdb.go` repositories for Go implementing the uniform `fdblayer.Repository[*Msg]` (or `fdblayer.QueueRepository[*Msg]`) interface, along with compile-time `<MsgName>TypeID` and `<MsgName>By<Fields>` index constants.
- **Uniform `Repository[T]` Operations**:
  - `Insert(ctx, tx, entity)`: Inserts a new record, returning `fdblayer.ErrAlreadyExists` if the primary key already exists.
  - `Upsert(ctx, tx, entity)`: Inserts or updates a record in-place with minimal-write index delta maintenance.
  - `Delete(ctx, tx, key)`: Deletes a record by `fdblayer.Key` and clears all secondary/fan-out index entries.
  - `ReadRow(ctx, tx, key)`: Reads a single record by `fdblayer.Key` (`fdblayer.ErrNotFound` if missing).
  - `ReadRows(ctx, tx, keys)`: Pipelined parallel batch fetch by `[]fdblayer.Key` in 1 RTT.
  - `Scan(ctx, tx, rng, opts)`: Cursor-paginated primary key scan over `fdblayer.AllKeys()`, `fdblayer.Prefix(...)`, or `fdblayer.Range(begin, end)`.
  - `ScanIndex(ctx, tx, index, rng, opts)`: Cursor-paginated secondary/fan-out index scan over `fdblayer.AllKeys()`, `fdblayer.Prefix(...)` (exact or partial leading prefix), or `fdblayer.Range(begin, end)` using generated `<Msg>By<Fields>` constants.
- **Primary Keys**: Single-field and compound primary keys (`option (annotations.primary_key) = { fields: [...] };`) composed of scalar, `bytes`, or `enum` fields.
- **Secondary & Fan-Out Indexes**:
  - Single-field and composite secondary indexes with explicit `id` (`> 0`) and automatic maintenance on `Insert`, `Upsert`, and `Delete`.
  - **Fan-out** indexes on `repeated` fields with deduplication and minimal-write delta updates on `Upsert`.
- **Versionstamps & FIFO Queues**: Built-in support for `[(annotations.is_versionstamp) = true]` primary keys and `option (annotations.is_queue) = true` FIFO queues (`Enqueue`, `Dequeue`, `DequeueBatch` via `fdblayer.QueueRepository[T]`) with per-transaction user version monotonicity scoped to `*fdblayer.Tx` (`fdblayer.NewTx(tr, dir)`).

---

## Repository Structure

```text
fdb-layer/
├── proto/
│   └── fdb-layer/
│       └── annotations.proto          # Shared Protobuf options (tags 50000..50008)
├── compiler/                          # Extensible protoc compiler written in Go
│   ├── ir/                            # Language-neutral Intermediate Representation (IR)
│   ├── backend/                       # Common backend Generator interface & Registry
│   │   └── golang/                    # Go backend emitting *.fdb.go
│   └── cmd/
│       └── protoc-gen-fdb-go/         # CLI entrypoint for Go codegen
├── runtimes/
│   └── go/                            # Go runtime package (github.com/romannikov/fdb-layer/runtimes/go)
└── tests/
    ├── proto/                         # Shared test schemas (store.proto)
    └── go/                            # Go unit (MockKV) and live FDB integration tests
```

---

## Installation & Code Generation

### Install the `protoc` Plugin

```bash
go install github.com/romannikov/fdb-layer/compiler/cmd/protoc-gen-fdb-go@latest
```

Ensure `$(go env GOPATH)/bin` is in your `PATH`:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

### Generate Go Code

```bash
protoc -I proto -I . \
  --go_out=. --go_opt=paths=source_relative \
  --fdb-go_out=. --fdb-go_opt=paths=source_relative \
  generated/store.proto
```

To regenerate all internal annotations and test schemas in this repository, run:
```bash
./update_protos.sh
```

---

## Binary Data Storage Layout

All keys are packed inside the caller-provided `directory.DirectorySubspace`.

### Subspace Namespaces & Tuple Type Normalization

1. **Namespaces** are packed as signed 64-bit integers (`int64`):
   - `DataNamespace` = `0`
   - `IndexNamespace` = `1`
2. **Integer & Enum Normalization**:
   - Signed integers (`int32`, `sint32`, `sfixed32`, `int64`, `sint64`, `sfixed64`) and `enum` values are widened to `int64` before tuple packing.
   - Unsigned integers (`uint32`, `fixed32`, `uint64`, `fixed64`) are widened to `uint64` before tuple packing.
3. **Byte Strings (`bytes` fields)**:
   - Protobuf `bytes` fields in primary keys and secondary/fan-out indexes are packed as raw FDB Tuple Byte Strings (`[]byte`).

### Key-Value Layout

Each generated message requires a mandatory non-zero compile-time `TypeID` (`option (annotations.type_id) = <N>;`), and each secondary index requires a mandatory non-zero `id` (`id: <N>`). Values `<= 255` use a compact 2-byte FDB Tuple integer encoding.

Assume `User` has `TypeID = 1` and `TaskMessage` has `TypeID = 4`:

1. **Standard Data Record**:
   - **Key**: `[Data Subspace] + (1, 0, "u123")` (`TypeID = 1`, `DataNamespace = 0`, primary key `"u123"`)
   - **Value**: `[Serialized User Protobuf Bytes]`
2. **Secondary & Fan-Out Index**:
   - **Key**: `[Data Subspace] + (1, 1, index_id, "user@example.com", "u123")`
     - `index_id`: Explicit `SecondaryIndex.id` (`> 0`).
   - **Value**: `[]` (empty byte slice; primary key elements trail the index key elements).
3. **FIFO Queue Message**:
   - **Key**: `[Data Subspace] + (4, 0, queue_name, versionstamp, shard_id)`
     - The compiler automatically orders `versionstamp` immediately after the queue partition key (`queue_name`) and before tie-breaker fields (`shard_id`).
   - **Value**: `[Serialized TaskMessage Protobuf Bytes]`

---

## Running Tests

### 1. Compiler Unit Tests (IR + Backend Registry + Go Backend)
```bash
go test -race -v ./compiler/...
```

### 2. Go Runtime Unit & Integration Tests
```bash
# Unit tests (in-memory MockKV)
go test -race -v ./runtimes/go/... ./tests/go/...

# Live FoundationDB integration tests
go test -p 1 -v -tags=integration ./tests/go/...
```

---

## License

MIT

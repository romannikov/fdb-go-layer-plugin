# FoundationDB Protobuf Layer (`fdb-layer`)

A `protoc` compiler (`protoc-gen-fdb-go`) and runtime library that generates FoundationDB data access layer code from shared Protobuf annotations (`proto/fdb-layer/annotations.proto`).

The compiler is structured around a **language-neutral Intermediate Representation (`compiler/ir`)** and a **pluggable backend interface (`compiler/backend`)** so new target languages can be added without duplicating schema parsing, validation, or index hashing logic. Currently, the **Go** backend (`compiler/backend/golang`) and runtime (`runtimes/go`) are implemented.

---

## Supported Features

- **Shared Protobuf Annotations (`proto/fdb-layer/annotations.proto`)**: Single source of truth for primary keys, secondary/fan-out indexes, atomic field mutations, versionstamped primary keys, and FIFO queues.
- **Extensible Compiler Architecture**:
  - `compiler/ir`: Extracts `FileSpec`, `MessageSpec`, `FieldSpec`, `SecondaryIndexSpec`, and `IndexPrefixSpec` from `protogen.File`, validates all schema invariants, and computes deterministic 32-bit FNV-1a index IDs.
  - `compiler/backend`: Defines the language-agnostic `Generator` interface, `Registry`, `Options`, and `RunProtogen` helper.
  - `compiler/backend/golang`: Emits strongly-typed `*.fdb.go` repositories for Go.
- **CRUD Operations**: Full support for `Create` (with `ErrAlreadyExists` protection), `Get` (with `ErrNotFound`), `Set` (with minimal-write index delta updates), `Delete`, strongly-typed `BatchGet`, and cursor-paginated `List` (including compound primary key `opts.Prefix` filtering).
- **Primary Keys**: Single-field and compound primary keys composed of scalar, `bytes`, or `enum` fields.
- **Secondary & Fan-Out Indexes**:
  - Single-field and composite secondary indexes with automatic maintenance on `Create`, `Set`, and `Delete`.
  - Automatic generation of **partial-prefix query methods** (`GetBy<Prefix>`) for leading prefixes of composite secondary indexes.
  - **Fan-out** indexes on `repeated` fields with deduplication and minimal-write delta updates on `Set`.
- **Atomic Operations**: Support for FoundationDB atomic mutations (`MUTATION_ADD`, `MUTATION_MAX`, `MUTATION_MIN`) stored in an isolated `FieldNamespace` (`2`) so `Set` never overwrites concurrent atomic updates, plus `Set<Entity><Field>` for explicit resets.
- **Versionstamps & FIFO Queues**: Built-in support for `[(annotations.is_versionstamp) = true]` primary keys and `option (annotations.is_queue) = true` FIFO queues (`Enqueue`, `Dequeue`, `DequeueBatch`) with automatic per-transaction user version monotonicity.

---

## Repository Structure

```text
fdb-layer/
├── proto/
│   └── fdb-layer/
│       └── annotations.proto          # Shared Protobuf options (tags 50001..50005)
├── compiler/                          # Extensible protoc compiler written in Go
│   ├── ir/                            # Language-neutral Intermediate Representation (IR)
│   ├── backend/                       # Common backend Generator interface & Registry
│   │   └── golang/                    # Go backend emitting *.fdb.go
│   └── cmd/
│       └── protoc-gen-fdb-go/         # CLI entrypoint for Go codegen
├── runtimes/
│   └── go/                            # Go runtime package (github.com/romannikov/fdb-layer/runtimes/go)
└── tests/
    ├── proto/                         # Shared test schemas (store.proto, atomic.proto)
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
   - `FieldNamespace` = `2`
2. **Integer & Enum Normalization**:
   - Signed integers (`int32`, `sint32`, `sfixed32`, `int64`, `sint64`, `sfixed64`) and `enum` values are widened to `int64` before tuple packing.
   - Unsigned integers (`uint32`, `fixed32`, `uint64`, `fixed64`) are widened to `uint64` before tuple packing.
3. **Byte Strings (`bytes` fields)**:
   - Protobuf `bytes` fields in primary keys and secondary/fan-out indexes are packed as raw FDB Tuple Byte Strings (`[]byte`).

### Key-Value Layout

Assume `User` has `TypeID = 1`, `TaskMessage` has `TypeID = 2`, and `Counter` has `TypeID = 3`:

1. **Metadata Registry**:
   - **Key**: `[Meta Subspace] + ("User")`
   - **Value**: `(1)` (packed 1-element tuple containing `int64` `TypeID`)
2. **Standard Data Record**:
   - **Key**: `[Data Subspace] + (1, 0, "u123")` (`TypeID = 1`, `DataNamespace = 0`, primary key `"u123"`)
   - **Value**: `[Serialized User Protobuf Bytes]` (atomic `mutation` fields are zeroed on a shallow copy before serialization without mutating the caller's struct, and overlaid from `FieldNamespace` on read).
3. **Secondary & Fan-Out Index**:
   - **Key**: `[Data Subspace] + (1, 1, index_id, "user@example.com", "u123")`
     - `index_id`: Explicit `SecondaryIndex.id` if non-zero, otherwise `int64(fnv32a(lowercase_comma_joined_field_names))`.
   - **Value**: `[]` (empty byte slice; primary key elements trail the index key elements).
4. **FIFO Queue Message**:
   - **Key**: `[Data Subspace] + (2, 0, queue_name, versionstamp, shard_id)`
     - The compiler automatically orders `versionstamp` immediately after the queue partition key (`queue_name`) and before tie-breaker fields (`shard_id`).
   - **Value**: `[Serialized TaskMessage Protobuf Bytes]`
5. **Atomic Mutation Field Record**:
   - **Key**: `[Data Subspace] + (3, 2, "cnt1", field_number)` (`TypeID = 3`, `FieldNamespace = 2`, primary key `"cnt1"`, `int64(field_number)`)
   - **Value**:
     - `MUTATION_ADD` (and unsigned `MUTATION_MAX` / `MUTATION_MIN`): 8-byte little-endian `uint64`.
     - Signed `MUTATION_MAX` / `MUTATION_MIN`: 8-byte little-endian sign-bit-flipped `uint64` (`uint64(val) ^ (1 << 63)`), preserving signed two's-complement ordering under FoundationDB's unsigned little-endian `MAX`/`MIN` atomic operations.

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

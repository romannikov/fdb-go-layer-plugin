# FoundationDB Protobuf Record Layer (`fdb-layer`)

A `protoc` compiler (`protoc-gen-fdb-go`) and runtime library that generates a high-performance, strongly-typed FoundationDB Record Layer from shared Protobuf annotations (`proto/fdb-layer/annotations.proto`).

The compiler is structured around a **language-neutral Intermediate Representation (`compiler/ir`)** and a **pluggable backend interface (`compiler/backend`)** so new target languages can be added without duplicating schema parsing, validation, or index resolution logic. Currently, the **Go** backend (`compiler/backend/golang`) and runtime (`runtimes/go`) are implemented.

---

## Supported Features

- **Shared Protobuf Annotations (`proto/fdb-layer/annotations.proto`)**:
  - Mandatory non-zero message `option (annotations.type_id) = <N>;` (`N > 0`) and `SecondaryIndex.id` (`> 0`) for compact FDB tuple prefixes and zero-accident schema evolution.
  - Message-level primary key definition: `option (annotations.primary_key) = { fields: ["user_id", "order_id"] };`.
  - Reserved future options (`split_large_records = 50007`, `ttl_field = 50008`) that fail compilation with a `"not implemented yet"` error if set.
- **Extensible Compiler Architecture**:
  - `compiler/ir`: Extracts `FileSpec`, `MessageSpec`, `FieldSpec`, `SecondaryIndexSpec`, `IndexPrefixSpec`, `IndexRangeScanSpec`, and `InterleaveSpec` from `protogen.File`, and validates all schema invariants at compile time.
  - `compiler/backend`: Defines the language-agnostic `Generator` interface, `Registry`, `Options`, and `RunProtogen` helper.
  - `compiler/backend/golang`: Modular Go backend (`generator.go`, `crud.go`, `mutations.go`, `indexes.go`, `aggregates.go`, `interleave.go`, `queues.go`, `atomics.go`) emitting strongly-typed, zero-reflection `<file>_<msg>.fdb.go` repositories.
- **Record Store & Read/Write Transaction Separation (`fdblayer.Store`, `fdblayer.ReadTx`, `*fdblayer.Tx`)**:
  - `Store.Transact(ctx, func(ctx, tx *fdblayer.Tx) error)`: Automatic retry loop, per-attempt user-version sequence reset, and automatic flush of buffered mutations before commit.
  - `Store.ReadTransact(ctx, func(ctx, rtx fdblayer.ReadTx) error)`: Read-only transaction runner. All generated read methods accept `fdblayer.ReadTx` and support conflict-free snapshot reads via `rtx.Snapshot()`.
  - `Store.Apply(ctx, muts ...fdblayer.Mutation)`: One-liner helper to apply a batch of pipelined mutations in a transaction.
- **Pipelined Buffered Mutations (`tx.Buffer` / `tx.Flush`)**:
  - `Insert(entity)`, `Upsert(entity)`, `Replace(entity)` (0-RTT blind write for messages without secondary/aggregate indexes or interleaved children), `DeleteMutation(pk)`, and atomic field mutations (`Add<Field>Mutation`, `Set<Field>Mutation`, etc.).
  - Executes all pre-write reads (`tr.Get` for old records and unique index checks) concurrently in Phase 1 (1 network RTT total across the entire batch) and detects intra-batch primary key and unique index collisions via `tx.ClaimKey`.
- **Rich Secondary Indexes**:
  - **Unique Indexes (`unique: true`)**: Enforces uniqueness on `Create`, `Set`, `Insert`, and `Upsert` (returning `fdblayer.ErrUniqueConstraintViolation`, which wraps `fdblayer.ErrAlreadyExists`), and generates `GetUniqueBy<Fields>(ctx, rtx, ...) (*Msg, error)`.
  - **Covering Indexes (`storing: ["field_a", ...]`)**: Stores a compact Protobuf projection of non-key fields in the index value and generates 1-RTT `GetCoveredBy<Fields>` and `ScanCoveredBy<Fields>` methods that reconstruct projected entities without reading `DataNamespace`.
  - **Sparse Indexes (`sparse: true`)**: Omits the index entry when any indexed scalar field has its zero/empty value.
  - **Fan-Out Indexes**: Indexes `repeated` fields with automatic deduplication and minimal-write delta updates on `Set` / `Upsert`.
  - **Aggregate Indexes (`AGGREGATE_COUNT`, `AGGREGATE_SUM`, `AGGREGATE_MIN`, `AGGREGATE_MAX`)**: Maintains 8-byte little-endian aggregates in `AggregateNamespace` (`3`) using FoundationDB atomic operations and generates $O(1)$ point-read getters (`GetCountBy<Fields>`, `GetSum<AggField>By<Fields>`, `GetMin<AggField>By<Fields>`, `GetMax<AggField>By<Fields>`).
- **Typed Range Scans (`KeyRange[K]`) & Opaque `PageToken` Pagination**:
  - `fdblayer.KeyRange[K]` (`Between`, `HalfOpen`, `OpenClosedRange`, `OpenInterval`, `GreaterOrEqual`, `GreaterThan`, `LessThan`, `LessOrEqual`, `AllKeys`) on both primary keys (`ScanBy<PKFields>`, `ListBy<PKPrefix>`) and secondary indexes (`ScanBy<Fields>`, `ScanCoveredBy<Fields>`).
  - `PaginationOptions.PageToken` and `PaginatedResult.NextPageToken` (packed FDB tuple bytes) alongside `Begin` / `NextKey`.
- **Hierarchical Interleaved Tables (`interleave`)**:
  - `option (annotations.interleave) = { parent: "Customer", on_delete_cascade: true };` co-locates child records directly under their parent's data key prefix `(ParentTypeID, DataNamespace, parent_pk..., ChildTypeID, child_pk_suffix...)`.
  - Generates `GetWithChildren(ctx, rtx, parentPK) (*<Parent>WithChildren, error)` (1-RTT single range read for parent + all interleaved child types) and automatic cascade delete when `on_delete_cascade = true`.
- **Atomic Field Operations & Versionstamped Queues**:
  - Field-level atomic mutations (`MUTATION_ADD`, `MUTATION_MAX`, `MUTATION_MIN`) stored in `FieldNamespace` (`2`).
  - Versionstamped primary keys (`[(annotations.is_versionstamp) = true]`) and `option (annotations.is_queue) = true` FIFO queues (`Enqueue`, `Dequeue`, `DequeueBatch`).

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
│   │   └── golang/                    # Modular Go backend emitting <file>_<msg>.fdb.go
│   └── cmd/
│       └── protoc-gen-fdb-go/         # CLI entrypoint for Go codegen
├── runtimes/
│   └── go/                            # Go runtime package (github.com/romannikov/fdb-layer/runtimes/go)
└── tests/
    ├── proto/                         # Shared test schemas (store.proto, atomic.proto)
    └── go/                            # Go unit (MockKV) and live FDB integration tests
```

---

## Schema Definition Example

```protobuf
syntax = "proto3";
package store;
option go_package = "example/store;store";

import "fdb-layer/annotations.proto";

enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_PENDING = 1;
  ORDER_STATUS_SHIPPED = 2;
}

message Customer {
  option (annotations.type_id) = 200;
  option (annotations.primary_key) = {
    fields: ["customer_id"]
  };
  option (annotations.secondary_index) = {
    id: 1
    fields: ["email"]
    unique: true
  };
  option (annotations.secondary_index) = {
    id: 2
    fields: ["referral_code"]
    sparse: true
  };

  string customer_id = 1;
  string email = 2;
  string name = 3;
  string referral_code = 4;
}

message CustomerInvoice {
  option (annotations.type_id) = 201;
  option (annotations.primary_key) = {
    fields: ["customer_id", "invoice_id"]
  };
  option (annotations.interleave) = {
    parent: "Customer"
    on_delete_cascade: true
  };
  option (annotations.secondary_index) = {
    id: 1
    fields: ["status", "issued_at"]
    storing: ["amount_cents"]
  };
  option (annotations.secondary_index) = {
    id: 2
    fields: ["customer_id", "status"]
    aggregate: AGGREGATE_COUNT
  };
  option (annotations.secondary_index) = {
    id: 3
    fields: ["customer_id", "status"]
    aggregate: AGGREGATE_SUM
    aggregate_field: "amount_cents"
  };

  string customer_id = 1;
  int64 invoice_id = 2;
  OrderStatus status = 3;
  int64 issued_at = 4;
  int64 amount_cents = 5;
  string note = 6;
}
```

---

## Binary Data Storage Layout

All keys are packed inside the caller-provided `directory.DirectorySubspace`.

### Subspace Namespaces

Namespaces are packed as signed 64-bit integers (`int64`):
- `DataNamespace` = `0`: Primary records and interleaved child records.
- `IndexNamespace` = `1`: Secondary, unique, sparse, covering, and fan-out index entries.
- `FieldNamespace` = `2`: Per-record atomic mutation fields (`MUTATION_ADD`, `MUTATION_MIN`, `MUTATION_MAX`).
- `AggregateNamespace` = `3`: Grouped aggregate index counters/extrema (`AGGREGATE_COUNT`, `AGGREGATE_SUM`, `AGGREGATE_MIN`, `AGGREGATE_MAX`).

### Key-Value Layout

1. **Root Data Record (`DataNamespace = 0`)**:
   - **Key**: `dir.Pack(tuple.Tuple{TypeID, 0, pk_1, ..., pk_k})`
   - **Value**: Serialized Protobuf message bytes.
2. **Interleaved Child Data Record (`DataNamespace = 0`)**:
   - **Key**: `dir.Pack(tuple.Tuple{ParentTypeID, 0, parent_pk_1, ..., parent_pk_p, ChildTypeID, child_pk_1, ..., child_pk_c})`
   - **Value**: Serialized child Protobuf message bytes.
3. **Standard & Covering Secondary Index (`IndexNamespace = 1`, `unique = false`)**:
   - **Key**: `dir.Pack(tuple.Tuple{TypeID, 1, IndexID, idx_1, ..., idx_m, pk_1, ..., pk_k})`
   - **Value**: Empty byte slice `[]byte{}` (or `proto.Marshal(projectedEntity)` when `storing` is non-empty).
4. **Unique Secondary Index (`IndexNamespace = 1`, `unique = true`)**:
   - **Key**: `dir.Pack(tuple.Tuple{TypeID, 1, IndexID, idx_1, ..., idx_m})`
   - **Value**: `tuple.Tuple{pk_1, ..., pk_k}.Pack()` (or `tuple.Tuple{pk_1, ..., pk_k, projectedBytes}.Pack()` when `storing` is also set).
5. **Atomic Field Record (`FieldNamespace = 2`)**:
   - **Key**: `dir.Pack(tuple.Tuple{TypeID, 2, pk_1, ..., pk_k, FieldNumber})`
   - **Value**: 8-byte little-endian `uint64` (sign-bit flipped `^ (1 << 63)` for signed `MIN`/`MAX`).
6. **Aggregate Index Record (`AggregateNamespace = 3`)**:
   - **Key**: `dir.Pack(tuple.Tuple{TypeID, 3, IndexID, group_1, ..., group_m})`
   - **Value**: 8-byte little-endian `uint64` updated via FDB atomic `Add`, `Min`, or `Max`.

---

## Running Tests

### 1. Regenerate Protobuf & Record Layer Code
```bash
./update_protos.sh
```

### 2. Compiler & Runtime Unit Tests (In-Memory `MockKV`)
```bash
go test -race -v ./...
```

### 3. Live FoundationDB Integration Tests
```bash
go test -p 1 -v -tags=integration ./tests/go/suite/...
```

---

## License

MIT

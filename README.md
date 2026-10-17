# FoundationDB Protobuf Layer (`fdb-layer`)

A `protoc` compiler (`protoc-gen-fdb-go`) and runtime library that generates FoundationDB data access layer code from shared Protobuf annotations (`proto/fdb-layer/annotations.proto`). It simplifies building applications on top of FoundationDB by handling the boilerplate of mapping Protobuf messages to keys, secondary indexes, and FIFO queues.

The compiler is structured around a **language-neutral Intermediate Representation (`compiler/ir`)** and a **pluggable backend interface (`compiler/backend`)** so new target languages can be added without duplicating schema parsing or validation logic. Currently, the **Go** backend (`compiler/backend/golang`) and runtime (`runtimes/go`) are implemented.

---

## Supported Features

- **Uniform `Repository[T]` Operations**: Every standard record repository is constructed with `New<Msg>Repository(dir)` and implements `fdblayer.Repository[*Msg]` with 8 methods taking `(ctx context.Context, tr fdblayer.Transaction, ...)`:
  - `Insert(ctx, tr, entity, opts ...fdblayer.WriteOption)`: Inserts a new record, returning `fdblayer.ErrAlreadyExists` if the primary key already exists.
  - `Upsert(ctx, tr, entity, opts ...fdblayer.WriteOption)`: Inserts or updates a record in-place with minimal-write index delta maintenance.
  - `Delete(ctx, tr, key)`: Deletes a record by `fdblayer.Key` and clears all secondary/fan-out index entries (and decrements `AGGREGATE_COUNT` / `AGGREGATE_SUM` aggregate indexes).
  - `ReadRow(ctx, tr, key)`: Reads a single record by `fdblayer.Key` (`fdblayer.ErrNotFound` if missing).
  - `ReadRows(ctx, tr, keys)`: Pipelined parallel batch fetch by `[]fdblayer.Key` in 1 RTT.
  - `Scan(ctx, tr, rng, opts ...fdblayer.ScanOption)`: Cursor-paginated primary key scan over `fdblayer.AllKeys()`, `fdblayer.Prefix(...)`, or `fdblayer.Range(begin, end)` with optional `fdblayer.WithLimit(n)`, `fdblayer.WithReverse()`, and `fdblayer.WithCursor(c)`.
  - `ScanIndex(ctx, tr, index, rng, opts ...fdblayer.ScanOption)`: Cursor-paginated secondary/fan-out index scan over `fdblayer.AllKeys()`, `fdblayer.Prefix(...)` (exact or partial leading prefix), or `fdblayer.Range(begin, end)` using generated `<Msg>By<Fields>` `fdblayer.IndexID` constants.
  - `ReadAggregate(ctx, tr, index, groupKey)`: Reads an $O(1)$ atomic aggregate secondary index (`AGGREGATE_COUNT`, `AGGREGATE_SUM`, `AGGREGATE_MIN`, `AGGREGATE_MAX`) for `groupKey` (`fdblayer.Key`).
- **Direct `fdb.Transaction` Compatibility**: Repository methods accept `fdblayer.Transaction` directly, which is satisfied out-of-the-box by `fdb.Transaction` inside standard `db.Transact(func(tr fdb.Transaction) (any, error) { ... })` blocks as well as by in-memory `MockKV` for unit tests.
- **Primary Keys**: Support for custom primary keys (`option (annotations.primary_key) = { fields: [...] };`), including single-field and compound keys composed of scalar, `bytes`, or `enum` fields.
- **Secondary & Fan-Out Indexes**: Support for single-field and composite secondary indexes with explicit `id` (`> 0`), automatic maintenance on `Insert`, `Upsert`, and `Delete`, exact/prefix/range queries via `ScanIndex`, and **fan-out** indexes on `repeated` fields with deduplication and minimal-write delta updates.
- **Aggregate Secondary Indexes (`AGGREGATE_COUNT`, `AGGREGATE_SUM`, `AGGREGATE_MIN`, `AGGREGATE_MAX`)**: Materialized group-by rollups maintained automatically on `Insert`, `Upsert`, and `Delete` using FoundationDB conflict-free atomic mutations (`tr.Add`, `tr.Min`, `tr.Max`) and queried in $O(1)$ via `ReadAggregate`.
- **Time Versionstamps**: Support for FDB 12-byte versionstamps (`[(annotations.is_versionstamp) = true]`) to order records by commit time, with optional 16-bit user versions (`fdblayer.WithUserVersion(n)` passed to `Insert`, `Upsert`, or `Enqueue`) to disambiguate multiple writes within the same transaction.
- **Queue Support**: Built-in support for FIFO queues (`option (annotations.is_queue) = true;`) implementing `fdblayer.QueueRepository[*Msg]` (`Enqueue(ctx, tr, msg, opts ...fdblayer.WriteOption)`, `Dequeue(ctx, tr, partitionKey)`, `DequeueBatch(ctx, tr, partitionKey, limit)`) ordered by commit versionstamp.
- **Unified Store**: Support for both standard data records and queue messages within the same system and `directory.DirectorySubspace`.
- **Shared Protobuf Annotations (`proto/fdb-layer/annotations.proto`)**: Single source of truth for compile-time message `type_id`s, primary keys, secondary/fan-out/aggregate indexes, versionstamped primary keys, and FIFO queues. Reserved options (`mutation`, `interleave`, `split_large_records`, `ttl_field`, and `SecondaryIndex` options `unique`, `storing`, `sparse`) are documented in `annotations.proto` with `// [NOT IMPLEMENTED YET]` and rejected at compile time if specified.

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

Ensure that your `GOPATH/bin` is in your `PATH` so that `protoc` can find the plugin:

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

## Data Storage Layout

The plugin stores data in FoundationDB using a deterministic key-value layout. All keys are prefixed by a `directory.DirectorySubspace` bound to the repository constructor `New<Msg>Repository(dir)`.

### Data Locality

Using a directory subspace ensures that all data for a specific application or layer is stored in a contiguous key range in FoundationDB. This provides strong data locality, making range reads efficient and keeping related data physically close in the cluster.

### Key Components

- **Subspace**: The root `directory.DirectorySubspace` for your application data.
- **TypeID**: A mandatory non-zero compile-time integer ID (`option (annotations.type_id) = <N>;`) for each message type. Values `<= 255` use a compact 2-byte FDB Tuple integer encoding, minimizing key size in FDB compared to using full message names and eliminating any runtime metadata registry lookup.
- **Namespaces & Tuple Type Normalization**:
  1. **Namespaces** are packed as signed 64-bit integers (`int64`):
     - `DataNamespace` = `0`
     - `IndexNamespace` = `1`
  2. **Integer & Enum Normalization**:
     - Signed integers (`int32`, `sint32`, `sfixed32`, `int64`, `sint64`, `sfixed64`) and `enum` values are widened to `int64` before tuple packing.
     - Unsigned integers (`uint32`, `fixed32`, `uint64`, `fixed64`) are widened to `uint64` before tuple packing.
  3. **Byte Strings (`bytes` fields)**:
     - Protobuf `bytes` fields in primary keys and secondary/fan-out indexes are packed as raw FDB Tuple Byte Strings (`[]byte`).

### Layout Examples

Assume we have a `User` message with `TypeID = 1` and a `Task` message with `TypeID = 2`.

#### 1. Standard Data Record
- **Purpose**: Stores the serialized Protobuf message.
- **Key**: `[Data Subspace] + (1, 0, "u123")` (Uses `TypeID = 1` and namespace constant `0` for data)
- **Value**: `[Serialized User Protobuf Blob]`
- *Example*: A user with ID `"u123"` is stored at key `app_data/1/0/"u123"`.

#### 2. Secondary & Fan-Out Index
- **Purpose**: Enables lookup and range scans by indexed scalar or `repeated` fields.
- **Key**: `[Data Subspace] + (1, 1, indexID, "user@example.com", "u123")` (Uses `TypeID = 1`, namespace constant `1` for index, and the explicit non-zero `SecondaryIndex.id`)
- **Value**: `[]` (empty)
- *Note*: The plugin does NOT store the whole record in the index. It stores a **reference** to the record (the primary key `"u123"`) within the index key itself.
- *Example*: Scanning `generated.UserByEmail` with `fdblayer.Prefix("user@example.com")` finds this index key, extracts the trailing `"u123"` primary key, and fetches the corresponding `User` record in a pipelined batch read.

#### 3. Aggregate Secondary Index (`AGGREGATE_COUNT`, `AGGREGATE_SUM`, `AGGREGATE_MIN`, `AGGREGATE_MAX`)
- **Purpose**: Maintains $O(1)$ conflict-free group-by rollups (`COUNT`, `SUM`, `MIN`, `MAX`) across records using FoundationDB atomic mutations (`tr.Add`, `tr.Min`, `tr.Max`).
- **Key**: `[Data Subspace] + (1, 1, indexID, group_1, ..., group_m)` (No primary key suffix; all records sharing the same grouping tuple map to a single accumulator key)
- **Value**: `[8-byte Little-Endian uint64]` (two's complement `int64` for `COUNT`/`SUM`; sign-bit-flipped `int64` or raw `uint64` for `MIN`/`MAX`)
- *Example*: Calling `orderRepo.ReadAggregate(ctx, tr, generated.OrderCountByTenantIdStatus, fdblayer.Key{"acme", "PENDING"})` reads the single 8-byte counter key in 1 RTT without scanning matching rows.

#### 4. Queue Message
- **Purpose**: Stores messages in a FIFO queue ordered by commit versionstamp.
- **Key**: `[Data Subspace] + (2, 0, queue_name, versionstamp, shard_id)` (Uses `TypeID = 2` and namespace constant `0` for data; `versionstamp` is placed immediately after `queue_name` to guarantee global FIFO order)
- **Value**: `[Serialized Task Protobuf Blob]`
- *Example*: A task in the `"high-priority"` queue (`queue_name`) with `shard_id = 1` is stored at `app_data/2/0/"high-priority"/versionstamp/1`.

### How to Make Data User-Local

If you want to ensure that all data belonging to a specific user is stored together (e.g., for GDPR compliance, multi-tenant isolation, or efficient cleanup), you can structure your directory subspaces by user ID:

`app_state/user_id/...`

In code, create the subspace and pass it to the repository constructor:

```go
userDir, _ := directory.CreateOrOpen(db, []string{"app_state", "u123"}, nil)
userRepo := generated.NewUserRepository(userDir)
_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
	// Pass ctx and tr to repository methods
	return nil, nil
})
```

This ensures all records and secondary indexes for `"u123"` are stored in a single contiguous key range.

---

## Proto and Code Examples

Here are examples of how to define your messages and use the generated code for common use cases.

### 1. Standard Record

This example shows basic CRUD operations (`Insert`, `Upsert`, `ReadRow`, `ReadRows`, `Delete`) and scans (`Scan`, `ScanIndex`) for a standard record.

#### Define Proto

```protobuf
syntax = "proto3";

import "fdb-layer/annotations.proto";

message User {
  option (annotations.type_id) = 1;
  option (annotations.primary_key) = {
    fields: ["id"]
  };
  option (annotations.secondary_index) = {
    id: 1
    fields: ["email"]
  };

  string id = 1;
  string name = 2;
  string email = 3;
}
```

#### Use Generated Code

```go
package main

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	"your/package/generated"
)

func main() {
	fdb.MustAPIVersion(730)
	db := fdb.MustOpenDefault()
	ctx := context.Background()

	dataDir, _ := directory.CreateOrOpen(db, []string{"app_data"}, nil)
	userRepo := generated.NewUserRepository(dataDir)

	_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		// Insert a new user (fails with fdblayer.ErrAlreadyExists if PK exists)
		user := &generated.User{Id: "u123", Email: "user@example.com", Name: "John"}
		if err := userRepo.Insert(ctx, tr, user); err != nil {
			return nil, err
		}

		// Read a single user by Primary Key
		u, err := userRepo.ReadRow(ctx, tr, fdblayer.Key{"u123"})
		if err != nil {
			return nil, err
		}
		fmt.Println("Found user:", u.Name)

		// Scan by Secondary Index (exact or leading prefix match)
		page, err := userRepo.ScanIndex(
			ctx,
			tr,
			generated.UserByEmail,
			fdblayer.Prefix("user@example.com"),
			fdblayer.WithLimit(10),
		)
		if err != nil {
			return nil, err
		}
		fmt.Println("Found users by email:", len(page.Items))

		// Upsert user (updates record and maintains secondary index deltas)
		user.Name = "John Doe"
		return nil, userRepo.Upsert(ctx, tr, user)
	})
	if err != nil {
		panic(err)
	}
}
```

---

### 2. Queue

This example shows how to insert a user and enqueue `"send-email"` tasks in the **same transaction**, ensuring atomicity. When enqueuing multiple messages within the same transaction, pass `fdblayer.WithUserVersion(n)` with a distinct 16-bit user version (`0..65535`).

#### Define Proto

```protobuf
message Task {
  option (annotations.type_id) = 2;
  option (annotations.is_queue) = true;
  option (annotations.primary_key) = {
    fields: ["queue_name", "versionstamp", "shard_id"]
  };

  string queue_name = 1;
  uint32 shard_id = 2; // Tie-breaker / producer shard identifier
  bytes versionstamp = 3 [(annotations.is_versionstamp) = true];
  bytes payload = 4;
}
```

#### Use Generated Code

```go
package main

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	"your/package/generated"
)

func main() {
	fdb.MustAPIVersion(730)
	db := fdb.MustOpenDefault()
	ctx := context.Background()

	dataDir, _ := directory.CreateOrOpen(db, []string{"app_data"}, nil)
	userRepo := generated.NewUserRepository(dataDir)
	taskRepo := generated.NewTaskRepository(dataDir)

	_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		// Insert a user
		user := &generated.User{Id: "u123", Email: "user@example.com", Name: "John"}
		if err := userRepo.Insert(ctx, tr, user); err != nil {
			return nil, err
		}

		// Enqueue a task to send a welcome email in the same transaction (defaults to userVersion 0)
		task1 := &generated.Task{
			QueueName: "send-email",
			// shard_id is stored after the commit versionstamp as a tie-breaker / producer tag.
			// (To partition a high-throughput queue across independent shards, include the shard
			// in QueueName, e.g. fmt.Sprintf("send-email-%d", rand.Intn(10))).
			ShardId: uint32(rand.Intn(10)),
			Payload: []byte("u123"), // Reference the user ID in payload
		}
		if err := taskRepo.Enqueue(ctx, tr, task1); err != nil {
			return nil, err
		}

		// When enqueuing a second task in the same transaction, pass fdblayer.WithUserVersion(1)
		task2 := &generated.Task{
			QueueName: "send-email",
			ShardId:   uint32(rand.Intn(10)),
			Payload:   []byte("u123-audit"),
		}
		return nil, taskRepo.Enqueue(ctx, tr, task2, fdblayer.WithUserVersion(1))
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("User created and email tasks enqueued atomically.")
}
```

---

### 3. Dequeue

This example shows how to pop and process the oldest task (or a batch of tasks) from the queue.

#### Use Generated Code

```go
package main

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	"your/package/generated"
)

func main() {
	fdb.MustAPIVersion(730)
	db := fdb.MustOpenDefault()
	ctx := context.Background()

	dataDir, _ := directory.CreateOrOpen(db, []string{"app_data"}, nil)
	taskRepo := generated.NewTaskRepository(dataDir)

	_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		// Dequeue the oldest task (returns nil, nil if the queue is empty)
		task, err := taskRepo.Dequeue(ctx, tr, fdblayer.Key{"send-email"})
		if err != nil {
			return nil, err
		}

		if task != nil {
			userID := string(task.Payload)
			fmt.Println("Processing: Send welcome email to user", userID)
			// Actual email sending logic would go here
		} else {
			fmt.Println("No tasks in queue.")
		}
		return nil, nil
	})
	if err != nil {
		panic(err)
	}
}
```

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

# Live FoundationDB integration tests (requires a running FDB cluster)
go test -p 1 -v -tags=integration ./tests/go/...
```

---

## Contributing

We welcome contributions to improve this plugin! Here is the process:

1. **Fork the repository** on GitHub.
2. **Clone your fork** locally.
3. **Create a new branch** for your feature or bugfix.
4. **Make your changes** and ensure code is well-formatted.
5. **Run unit and integration tests**:
   ```bash
   go test -race -v ./...
   go test -p 1 -v -tags=integration ./tests/go/...
   ```
6. **Update Protos** if you modified `annotations.proto` or `tests/proto/store.proto`:
   ```bash
   ./update_protos.sh
   ```
7. **Commit your changes** and push to your fork.
8. **Submit a Pull Request** with a clear description of what you did.

---

## License

MIT

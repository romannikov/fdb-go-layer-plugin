# FoundationDB Protobuf Layer (`fdb-record-layer`)

A `protoc` compiler and runtime library that generates FoundationDB data access layer code from shared Protobuf annotations. It simplifies building applications on top of FoundationDB by handling the boilerplate of mapping Protobuf messages to keys, secondary indexes, atomic aggregations, and FIFO queues.

---

## Supported Features

- **Type-Safe CRUD & Batch Reads**: Insert, upsert, delete, and fetch Protobuf records by single-field or composite primary keys, including pipelined batch reads that fetch multiple records in a single network round-trip.
- **Range Scans & Cursor Pagination**: Query primary keys or secondary indexes by exact match, leading tuple prefix, or bounded `[begin, end)` range with configurable page limits, reverse iteration, and resume cursors.
- **Secondary & Fan-Out Indexes**: Index scalar, composite, or `repeated` (fan-out) fields directly from Protobuf annotations. The generated repository automatically maintains index entries on `Insert`, `Upsert` (updating only changed index deltas), and `Delete`.
- **Automatic Aggregate Indexes (`COUNT`, `SUM`, `MIN`, `MAX`)**: Maintain conflict-free group-by rollups (or global table-wide totals) automatically whenever records are inserted, upserted, or deleted, and read the aggregated value in $O(1)$ via `ReadAggregate`.
- **Manual Atomic Mutation Indexes (`COUNT`, `ADD`, `MIN`, `MAX`)**: Update high-contention counters and watermarks (such as view counts, likes, or peak latency) in 0 RTTs via `Mutate` without reading or rewriting the underlying Protobuf record.
- **Versionstamped Keys & FIFO Queues**: Order records by commit time or turn a message into a transactional FIFO queue (`Enqueue`, `Dequeue`, `DequeueBatch`) powered by FoundationDB 12-byte versionstamps.

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

---

## Data Storage Layout

The layer stores data in FoundationDB using a deterministic tuple key-value layout. All keys are prefixed by the `directory.DirectorySubspace` passed to the repository constructor `New<Msg>Repository(dir)`.

### Data Locality

Using a directory subspace ensures that all data for a specific application or tenant is stored in a contiguous key range in FoundationDB. This provides strong data locality, making range reads efficient and keeping related data physically close in the cluster.

### Key Components

- **Subspace**: The root `directory.DirectorySubspace` for your application data.
- **Type ID**: A mandatory non-zero integer ID (`option (annotations.type_id) = <N>;`) for each message type. Values `<= 255` use a compact 2-byte FDB Tuple integer encoding, keeping keys small and surviving message renames safely.
- **Namespaces**:
  - `0` — Primary data records and queue messages.
  - `1` — Secondary, fan-out, aggregate, and atomic mutation indexes.

### Layout Examples

Assume a `User` message with `type_id = 1`, an `Order` message with `type_id = 2`, and a `Task` queue message with `type_id = 3`:

#### 1. Standard Data Record
- **Purpose**: Stores the serialized Protobuf message under its primary key.
- **Key**: `[Subspace] + (1, 0, "u123")`
- **Value**: `[Serialized User Protobuf Bytes]`

#### 2. Secondary & Fan-Out Index
- **Purpose**: Enables lookups and range scans by indexed scalar or `repeated` fields.
- **Key**: `[Subspace] + (1, 1, indexID, "user@example.com", "u123")`
- **Value**: `[]` (empty)
- *Note*: The index key appends the record's primary key (`"u123"`). Scanning the index extracts the primary keys from matching index entries and fetches the full records in a single pipelined batch read.

#### 3. Aggregate & Atomic Mutation Index
- **Purpose**: Maintains $O(1)$ conflict-free counters, sums, and min/max watermarks using FoundationDB atomic operations (`Add`, `Min`, `Max`).
- **Key**: `[Subspace] + (2, 1, indexID, "acme", "PENDING")`
- **Value**: `[8-byte Little-Endian Integer]`
- *Note*: All records sharing the same grouping key update the same 8-byte accumulator key atomically, allowing `ReadAggregate` to return the result in a single key read without scanning records.

#### 4. Queue Message
- **Purpose**: Stores messages in a FIFO queue ordered by commit versionstamp.
- **Key**: `[Subspace] + (3, 0, "send-email", versionstamp, shard_id)`
- **Value**: `[Serialized Task Protobuf Bytes]`

### How to Make Data User-Local

To isolate all records and indexes belonging to a specific user or tenant into a dedicated contiguous key range (e.g., for multi-tenant isolation or fast tenant deletion), open a per-user directory subspace:

```go
userDir, _ := directory.CreateOrOpen(db, []string{"app_state", "u123"}, nil)
userRepo := generated.NewUserRepository(userDir)
```

---

## Proto and Code Examples

### 1. Standard Record & Secondary Indexes

This example shows CRUD operations (`Insert`, `Upsert`, `ReadRow`, `ReadRows`, `Delete`) and scans (`Scan`, `ScanIndex`) for a standard record.

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
		// Insert a new user (returns fdblayer.ErrAlreadyExists if the PK already exists)
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

		// Scan by Secondary Index (supports Prefix, Range, WithLimit, WithReverse, WithCursor)
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

### 2. Automatic Aggregate & Manual Atomic Mutation Indexes

- **Automatic Aggregate Indexes (`aggregate`)** are maintained automatically on `Insert`, `Upsert`, and `Delete` based on the record's fields.
- **Manual Atomic Mutation Indexes (`mutation`)** are updated explicitly via `Mutate` in 0 RTTs without rewriting the record, and primary-key-scoped mutation entries are cleaned up automatically when the record is deleted.
- Both are read in $O(1)$ via `ReadAggregate`.

#### Define Proto

```protobuf
syntax = "proto3";

import "fdb-layer/annotations.proto";

message Post {
  option (annotations.type_id) = 2;
  option (annotations.primary_key) = {
    fields: ["id"]
  };

  // Automatic aggregates (updated on Insert / Upsert / Delete)
  // Generates: PostCountByAuthorId
  option (annotations.secondary_index) = {
    id: 1
    fields: ["author_id"]
    aggregate: AGGREGATE_COUNT
  };

  // Generates: PostSumWordCountByAuthorId
  option (annotations.secondary_index) = {
    id: 2
    fields: ["author_id"]
    aggregate: AGGREGATE_SUM
    aggregate_field: "word_count"
  };

  // Manual atomic mutations (updated explicitly via repo.Mutate)
  // Generates: PostCountViewsById
  option (annotations.secondary_index) = {
    id: 3
    fields: ["id"]
    mutation: MUTATION_COUNT
    name: "views"
  };

  // Generates: PostMaxLatencyMsById
  option (annotations.secondary_index) = {
    id: 4
    fields: ["id"]
    mutation: MUTATION_MAX
    name: "latency_ms"
  };

  string id = 1;
  string author_id = 2;
  int64 word_count = 3;
}
```

#### Use Generated Code

```go
_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
	// 1. Insert a post — automatically updates PostCountByAuthorId (+1)
	//    and PostSumWordCountByAuthorId (+1200)
	post := &generated.Post{
		Id:        "post-1",
		AuthorId:  "alice",
		WordCount: 1200,
	}
	if err := postRepo.Insert(ctx, tr, post); err != nil {
		return nil, err
	}

	// 2. Apply conflict-free manual atomic mutations in 0 RTTs without rewriting the Post record
	if err := postRepo.Mutate(ctx, tr, generated.PostCountViewsById, fdblayer.Key{"post-1"}, 1); err != nil {
		return nil, err
	}
	if err := postRepo.Mutate(ctx, tr, generated.PostMaxLatencyMsById, fdblayer.Key{"post-1"}, 42); err != nil {
		return nil, err
	}

	return nil, nil
})

// 3. Query both automatic aggregates and manual mutations via ReadAggregate
_, err = db.Transact(func(tr fdb.Transaction) (any, error) {
	postCount, err := postRepo.ReadAggregate(ctx, tr, generated.PostCountByAuthorId, fdblayer.Key{"alice"})
	if err != nil {
		return nil, err
	}
	totalWords, err := postRepo.ReadAggregate(ctx, tr, generated.PostSumWordCountByAuthorId, fdblayer.Key{"alice"})
	if err != nil {
		return nil, err
	}
	views, err := postRepo.ReadAggregate(ctx, tr, generated.PostCountViewsById, fdblayer.Key{"post-1"})
	if err != nil {
		return nil, err
	}
	maxLatency, err := postRepo.ReadAggregate(ctx, tr, generated.PostMaxLatencyMsById, fdblayer.Key{"post-1"})
	if err != nil {
		return nil, err
	}
	fmt.Printf("author posts=%d words=%d | post views=%d maxLatency=%dms\n",
		postCount, totalWords, views, maxLatency)
	return nil, nil
})
```

---

### 3. Transactional FIFO Queue

This example shows how to insert a user and enqueue `"send-email"` tasks in the **same transaction**, and later dequeue tasks in FIFO commit order. When enqueuing multiple messages within the same transaction, pass `fdblayer.WithUserVersion(n)` with a distinct 16-bit user version (`0..65535`).

#### Define Proto

```protobuf
message Task {
  option (annotations.type_id) = 3;
  option (annotations.is_queue) = true;
  option (annotations.primary_key) = {
    fields: ["queue_name", "versionstamp", "shard_id"]
  };

  string queue_name = 1;
  uint32 shard_id = 2; // Tie-breaker / producer identifier
  bytes versionstamp = 3 [(annotations.is_versionstamp) = true];
  bytes payload = 4;
}
```

#### Enqueue Tasks

```go
_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
	// Insert a user
	user := &generated.User{Id: "u123", Email: "user@example.com", Name: "John"}
	if err := userRepo.Insert(ctx, tr, user); err != nil {
		return nil, err
	}

	// Enqueue a welcome email task in the same transaction (defaults to userVersion 0)
	task1 := &generated.Task{
		QueueName: "send-email",
		ShardId:   uint32(rand.Intn(10)),
		Payload:   []byte("u123"),
	}
	if err := taskRepo.Enqueue(ctx, tr, task1); err != nil {
		return nil, err
	}

	// Enqueue a second task in the same transaction using fdblayer.WithUserVersion(1)
	task2 := &generated.Task{
		QueueName: "send-email",
		ShardId:   uint32(rand.Intn(10)),
		Payload:   []byte("u123-audit"),
	}
	return nil, taskRepo.Enqueue(ctx, tr, task2, fdblayer.WithUserVersion(1))
})
```

#### Dequeue Tasks

```go
_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
	// Dequeue the oldest task (returns nil, nil if the queue is empty)
	task, err := taskRepo.Dequeue(ctx, tr, fdblayer.Key{"send-email"})
	if err != nil {
		return nil, err
	}
	if task != nil {
		fmt.Println("Processing task for user:", string(task.Payload))
	}

	// Or pop a batch of up to 10 oldest tasks in a single range read:
	batch, err := taskRepo.DequeueBatch(ctx, tr, fdblayer.Key{"send-email"}, 10)
	if err != nil {
		return nil, err
	}
	fmt.Println("Dequeued batch size:", len(batch))
	return nil, nil
})
```

---

## Running Tests

```bash
# Run compiler and runtime unit tests
go test -race -v ./compiler/... ./runtimes/go/... ./tests/go/...

# Run live FoundationDB integration tests (requires a running FDB cluster)
go test -p 1 -v -tags=integration ./tests/go/...
```

---

## Contributing

1. **Fork the repository** on GitHub and **clone your fork** locally.
2. **Create a new branch** for your feature or bugfix.
3. **Make your changes** and run the unit and integration tests:
   ```bash
   go test -race -v ./...
   go test -p 1 -v -tags=integration ./tests/go/...
   ```
4. **Regenerate Protos** if you modified schema definitions:
   ```bash
   ./update_protos.sh
   ```
5. **Commit your changes**, push to your fork, and **submit a Pull Request**.

---

## License

MIT

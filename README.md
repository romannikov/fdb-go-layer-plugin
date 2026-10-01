# FoundationDB Multi-Language Layer (`fdb-layer`)

A multi-language `protoc` compiler suite (`protoc-gen-fdb-go` and `protoc-gen-fdb-rust`) and runtime library that generates FoundationDB data access layer code for Protobuf messages in **Go** and **Rust** with **100% binary key-value storage compatibility**.

Records, secondary indexes, fan-out indexes, atomic counters, and versionstamped FIFO queues written by Go services can be read, queried, and mutated directly by Rust services (and vice versa) within the same FoundationDB cluster and subspace.

---

## Supported Features

- **Multi-Language Code Generation**: Shared Intermediate Representation (IR) with backends for Go (`*.fdb.go`) and Rust (`*.fdb.rs`, targeting `prost` + `foundationdb`).
- **100% Binary Compatibility**: Identical FDB Tuple key encoding, integer/enum width normalization (`i64`/`u64`), FNV-1a 32-bit index hashing, 8-byte little-endian atomic counters, and 12-byte FDB versionstamps across Go and Rust.
- **CRUD Operations**: Full support for `Create` (with `AlreadyExists` protection), `Get`, `Set` (with index delta updates), `Delete`, `BatchGet`, and cursor-paginated `List`.
- **Primary Keys**: Single-field and compound primary keys composed of multiple scalar, bytes, or enum fields.
- **Secondary Indexes**: Automatic index maintenance on `Create`, `Set`, and `Delete`, including **fan-out** indexes on `repeated` fields with minimal-write delta updates.
- **Atomic Operations**: Support for FoundationDB atomic mutations (`ADD`, `MAX`, `MIN`) via field annotations, stored in an isolated field namespace so `Set` never clobbers concurrent atomic updates.
- **Time Versionstamps & FIFO Queues**: Built-in support for `[(annotations.is_versionstamp) = true]` primary keys and `option (annotations.is_queue) = true` FIFO queues with automatic per-transaction user version monotonicity.
- **Generic Repository Abstractions**: Generated repositories implement `fdblayer.GenericRepository[T, ID]` in Go and `fdb_layer::GenericRepository<T, PK>` in Rust.

---

## Repository Structure

```text
fdb-layer/
├── proto/
│   └── fdb-layer/
│       └── annotations.proto          # Shared Protobuf options (tags 50001..50005)
├── compiler/                          # Shared protoc compiler written in Go
│   ├── ir/                            # Language-neutral Intermediate Representation (IR)
│   ├── backend/
│   │   ├── golang/                    # Go backend emitting *.fdb.go
│   │   └── rust/                      # Rust backend emitting *.fdb.rs (prost + foundationdb)
│   └── cmd/
│       ├── protoc-gen-fdb-go/         # CLI entrypoint for Go codegen
│       └── protoc-gen-fdb-rust/       # CLI entrypoint for Rust codegen
├── runtimes/
│   ├── go/                            # Go runtime package (github.com/romannikov/fdb-layer/runtimes/go)
│   └── rust/                          # Rust runtime crate (fdb-layer)
└── tests/
    ├── proto/                         # Shared test schemas (store.proto, atomic.proto)
    ├── go/                            # Go unit (mock) and FDB integration tests
    ├── rust/                          # Rust FDB integration tests
    └── conformance/                   # Cross-language test (Go writes -> Rust mutates -> Go reads)
```

---

## Installation & Code Generation

### Install the `protoc` Plugins

```bash
go install github.com/romannikov/fdb-layer/compiler/cmd/protoc-gen-fdb-go@latest
go install github.com/romannikov/fdb-layer/compiler/cmd/protoc-gen-fdb-rust@latest
```

Ensure `$(go env GOPATH)/bin` is in your `PATH`:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

### Generate Go and Rust Code

```bash
protoc -I proto -I . \
  --go_out=. --go_opt=paths=source_relative \
  --fdb-go_out=. --fdb-go_opt=paths=source_relative \
  --fdb-rust_out=rust/src --fdb-rust_opt=generate_messages=true \
  generated/store.proto
```

- `--fdb-rust_opt=generate_messages=true`: Emits `#[derive(Clone, PartialEq, ::prost::Message)]` struct definitions alongside the generated Rust repositories in `*.fdb.rs`. Omit this flag (default `false`) if you already generate `prost` structs separately via `prost-build` and `include!()` the `.fdb.rs` file into the same module.
- To regenerate all internal annotations and test schemas in this repository, run:
  ```bash
  ./update_protos.sh
  ```

---

## Binary Data Storage Layout

All keys are packed inside the caller-provided subspace (`directory.DirectorySubspace` in Go, `fdb_layer::Subspace` in Rust).

### Data Locality & User-Local Subspaces

Using a subspace ensures that all data for an application or tenant is stored in a contiguous key range in FoundationDB. To isolate data per user or tenant (e.g., for multi-tenancy or fast tenant deletion), open a subspace scoped to the user:

- **Go**:
  ```go
  userDir, _ := directory.CreateOrOpen(db, []string{"app_state", "u123"}, nil)
  ```
- **Rust**:
  ```rust
  let user_dir = Subspace::all().subspace(&("app_state", "u123"));
  ```

### Subspace Namespaces & Tuple Type Normalization

To guarantee byte-for-byte identical keys between Go and Rust:
1. **Namespaces** are packed as signed 64-bit integers (`int64` / `i64`):
   - `DataNamespace` / `DATA_NAMESPACE` = `0`
   - `IndexNamespace` / `INDEX_NAMESPACE` = `1`
   - `FieldNamespace` / `FIELD_NAMESPACE` = `2`
2. **Integer & Enum Normalization**:
   - Signed integers (`int32`, `sint32`, `sfixed32`, `int64`, `sint64`, `sfixed64`) and `enum` values are widened to `int64` / `i64` before tuple packing.
   - Unsigned integers (`uint32`, `fixed32`, `uint64`, `fixed64`) are widened to `uint64` / `u64` before tuple packing.

### Key-Value Layout Examples

Assume `User` has `TypeID = 1`, `TaskMessage` has `TypeID = 2`, and `Counter` has `TypeID = 3`:

#### 1. Metadata Registry
- **Purpose**: Maps Protobuf message names to compact integer `TypeID`s.
- **Key**: `[Meta Subspace] + ("User")`
- **Value**: `(1)` (packed 1-element tuple containing `i64` `TypeID`)

#### 2. Standard Data Record
- **Purpose**: Stores the serialized Protobuf message.
- **Key**: `[Data Subspace] + (1, 0, "u123")` (`TypeID = 1`, `DataNamespace = 0`, primary key `"u123"`)
- **Value**: `[Serialized User Protobuf Bytes]` (any atomic `mutation` fields are zeroed before serialization so `Set` never overwrites atomic state).

#### 3. Secondary & Fan-Out Index
- **Purpose**: Enables fast lookups by non-primary-key scalar or `repeated` fields.
- **Key**: `[Data Subspace] + (1, 1, index_id, "user@example.com", "u123")`
  - `index_id`: Explicit `SecondaryIndex.id` if non-zero, otherwise `int64(fnv32a(lowercase_comma_joined_field_names))` (e.g., `fnv32a("email") = 2324124615`).
- **Value**: `[]` (empty byte slice; the primary key elements trail the index key elements).

#### 4. FIFO Queue Message
- **Purpose**: Stores messages in a global FIFO queue ordered by commit versionstamp.
- **Key**: `[Data Subspace] + (2, 0, queue_name, versionstamp, shard_id)`
  - The compiler automatically orders `versionstamp` immediately after the queue partition key (`queue_name`) and before tie-breaker fields (`shard_id`).
  - Each `Enqueue` in a transaction increments a 16-bit user version counter on `RecordStore` (`IncompleteVersionstamp(store.NextUserVersion())`), preserving strict FIFO ordering even when multiple items are enqueued in the same transaction.
- **Value**: `[Serialized TaskMessage Protobuf Bytes]`

#### 5. Atomic Mutation Field Record
- **Purpose**: Stores atomic counter fields (`MUTATION_ADD`, `MUTATION_MAX`, `MUTATION_MIN`).
- **Key**: `[Data Subspace] + (3, 2, "cnt1", field_number)` (`TypeID = 3`, `FieldNamespace = 2`, primary key `"cnt1"`, `int64(field_number)`)
- **Value**: 8-byte little-endian unsigned 64-bit integer (`uint64` / `u64::to_le_bytes()`).

---

## Proto and Code Examples

Here are examples of how to define your messages and use the generated code in **Go** and **Rust** for common use cases.

### 1. Standard Record & Secondary Index

#### Define Proto
```protobuf
syntax = "proto3";

import "fdb-layer/annotations.proto";

message User {
    option (annotations.primary_key) = "id";
    option (annotations.secondary_index) = {
        fields: ["email"]
    };

    string id = 1;
    string name = 2;
    string email = 3;
}
```

#### Use Generated Code (Go)
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
	fdb.MustAPIVersion(710)
	db := fdb.MustOpenDefault()
	ctx := context.Background()

	store := fdblayer.NewRecordStore()
	dataDir, _ := directory.CreateOrOpen(db, []string{"app_data"}, nil)
	metaDir, _ := directory.CreateOrOpen(db, []string{"app_data", "_meta"}, nil)

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if err := store.SyncMetadata(ctx, tr, metaDir, []string{"User"}); err != nil {
			return nil, err
		}

		userRepo := generated.NewUserRepository(store)

		// Create a user
		user := &generated.User{Id: "u123", Email: "user@example.com", Name: "John"}
		if err := userRepo.Create(ctx, tr, dataDir, user); err != nil {
			return nil, err
		}

		// Get by PK
		u, err := userRepo.Get(ctx, tr, dataDir, "u123")
		if err != nil {
			return nil, err
		}
		fmt.Println("Found user:", u.Name)

		// Lookup by Secondary Index
		users, err := userRepo.GetUserByEmail(ctx, tr, dataDir, "user@example.com")
		if err != nil {
			return nil, err
		}
		fmt.Println("Found users by email:", len(users))
		return nil, nil
	})
	if err != nil {
		panic(err)
	}
}
```

#### Use Generated Code (Rust)
```rust
use fdb_layer::{Database, RecordStore, Subspace};
use std::sync::Arc;

mod generated {
    include!("generated/user.fdb.rs");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _network = unsafe { fdb_layer::foundationdb::boot() };
    let db = Database::default()?;

    let store = Arc::new(RecordStore::new());
    let data_dir = Subspace::all().subspace(&("app_data",));
    let meta_dir = Subspace::all().subspace(&("app_data", "_meta"));

    let tr = db.create_trx()?;
    store.sync_metadata(&tr, &meta_dir, &["User"]).await?;

    let user_repo = generated::UserRepository::new(store.clone());

    // Create a user
    let user = generated::User {
        id: "u123".into(),
        name: "John".into(),
        email: "user@example.com".into(),
    };
    user_repo.create(&tr, &data_dir, &user).await?;

    // Get by PK
    let u = user_repo.get(&tr, &data_dir, "u123").await?;
    println!("Found user: {}", u.name);

    // Lookup by Secondary Index
    let users = user_repo
        .get_user_by_email(&tr, &data_dir, "user@example.com")
        .await?;
    println!("Found users by email: {}", users.len());

    tr.commit().await?;
    Ok(())
}
```

---

### 2. Queue (`Enqueue` & `Dequeue`)

This example shows how to create a user and enqueue a "send email" task in the **same transaction**, and then dequeue and process the oldest task.

#### Define Proto
```protobuf
message Task {
    option (annotations.is_queue) = true;
    option (annotations.primary_key) = "queue_name";
    option (annotations.primary_key) = "versionstamp";
    option (annotations.primary_key) = "shard_id";

    string queue_name = 1;
    uint32 shard_id = 2; // Tie-breaker / producer shard identifier
    bytes versionstamp = 3 [(annotations.is_versionstamp) = true];
    bytes payload = 4;
}
```

#### Enqueue & Dequeue (Go)
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
	fdb.MustAPIVersion(710)
	db := fdb.MustOpenDefault()
	ctx := context.Background()

	store := fdblayer.NewRecordStore()
	dataDir, _ := directory.CreateOrOpen(db, []string{"app_data"}, nil)
	metaDir, _ := directory.CreateOrOpen(db, []string{"app_data", "_meta"}, nil)

	// 1. Create User and Enqueue Task atomically
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if err := store.SyncMetadata(ctx, tr, metaDir, []string{"User", "Task"}); err != nil {
			return nil, err
		}
		userRepo := generated.NewUserRepository(store)
		taskRepo := generated.NewTaskRepository(store)

		user := &generated.User{Id: "u123", Email: "user@example.com", Name: "John"}
		if err := userRepo.Create(ctx, tr, dataDir, user); err != nil {
			return nil, err
		}

		task := &generated.Task{
			QueueName: "send-email",
			ShardId:   uint32(rand.Intn(10)),
			Payload:   []byte("u123"),
		}
		return nil, taskRepo.Enqueue(ctx, tr, dataDir, task)
	})
	if err != nil {
		panic(err)
	}

	// 2. Dequeue the oldest Task
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		taskRepo := generated.NewTaskRepository(store)
		task, err := taskRepo.Dequeue(ctx, tr, dataDir, "send-email")
		if err != nil {
			return nil, err
		}
		if task != nil {
			fmt.Println("Processing: Send welcome email to user", string(task.Payload))
		}
		return nil, nil
	})
	if err != nil {
		panic(err)
	}
}
```

#### Enqueue & Dequeue (Rust)
```rust
use fdb_layer::{Database, RecordStore, Subspace};
use std::sync::Arc;

mod generated {
    include!("generated/task.fdb.rs");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _network = unsafe { fdb_layer::foundationdb::boot() };
    let db = Database::default()?;

    let store = Arc::new(RecordStore::new());
    let data_dir = Subspace::all().subspace(&("app_data",));
    let meta_dir = Subspace::all().subspace(&("app_data", "_meta"));

    let task_repo = generated::TaskRepository::new(store.clone());

    // 1. Enqueue Task
    {
        let tr = db.create_trx()?;
        store.sync_metadata(&tr, &meta_dir, &["Task"]).await?;
        task_repo
            .enqueue(
                &tr,
                &data_dir,
                &generated::Task {
                    queue_name: "send-email".into(),
                    shard_id: 1,
                    versionstamp: vec![],
                    payload: b"u123".to_vec(),
                },
            )
            .await?;
        tr.commit().await?;
    }

    // 2. Dequeue the oldest Task
    {
        let tr = db.create_trx()?;
        if let Some(task) = task_repo.dequeue(&tr, &data_dir, "send-email").await? {
            println!(
                "Processing: Send welcome email to user {}",
                String::from_utf8_lossy(&task.payload)
            );
        }
        tr.commit().await?;
    }

    Ok(())
}
```

---

### 3. Atomic Operations (`ADD`, `MAX`, `MIN`)

#### Define Proto
```protobuf
message Counter {
    option (annotations.primary_key) = "id";

    string id = 1;
    int64 value = 2 [(annotations.mutation) = MUTATION_ADD];
    int64 max_value = 3 [(annotations.mutation) = MUTATION_MAX];
    int64 min_value = 4 [(annotations.mutation) = MUTATION_MIN];
}
```

#### Use Generated Code (Go & Rust)
- **Go**:
  ```go
  counterRepo := generated.NewCounterRepository(store)
  _ = counterRepo.Create(ctx, tr, dataDir, &generated.Counter{Id: "c1", Value: 10, MaxValue: 100, MinValue: 5})
  _ = counterRepo.AddCounterValue(ctx, tr, dataDir, "c1", 5)
  _ = counterRepo.MaxCounterMaxValue(ctx, tr, dataDir, "c1", 250)
  _ = counterRepo.MinCounterMinValue(ctx, tr, dataDir, "c1", 2)
  ```
- **Rust**:
  ```rust
  let counter_repo = generated::CounterRepository::new(store.clone());
  counter_repo.create(&tr, &data_dir, &generated::Counter { id: "c1".into(), value: 10, max_value: 100, min_value: 5 }).await?;
  counter_repo.add_counter_value(&tr, &data_dir, "c1", 5).await?;
  counter_repo.max_counter_max_value(&tr, &data_dir, "c1", 250).await?;
  counter_repo.min_counter_min_value(&tr, &data_dir, "c1", 2).await?;
  ```

---

## Running Tests

### 1. Compiler Unit Tests (IR + Go & Rust Codegen Backends)
```bash
go test -v ./compiler/...
```

### 2. Go Runtime Unit & Integration Tests
```bash
# Unit tests (in-memory MockKV)
go test -v ./tests/go/...

# FDB integration tests
go test -p 1 -v -tags=integration ./tests/go/...
```

### 3. Rust Runtime Integration Tests
```bash
cargo test --manifest-path tests/rust/Cargo.toml -- --test-threads=1
```

### 4. Cross-Language Go <-> Rust Conformance Suite
Verifies that entities, secondary indexes, fan-out indexes, atomic counters, and versionstamped FIFO queues written by Go are read and mutated by Rust, and then read back and verified by Go against the same FoundationDB store:
```bash
go test -v -tags=integration ./tests/conformance/...
```

---

## License

MIT

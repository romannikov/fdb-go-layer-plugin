# FoundationDB Go Layer Plugin

A protoc plugin that generates FoundationDB data access layer code for Proto messages.

## Features

- **Multi-Type Record Stores** — multiple Protobuf messages share the same subspace with automatic integer-based type IDs
- **Runtime Metadata Registry** — stable type IDs are managed in an FDB meta-space via `SyncMetadata`
- **RecordStore Pattern** — no global state; all operations are methods on a `RecordStore` struct
- **Early Loop Cancellation** — explicit checks for `ctx.Err()` inside all pagination, index lookup, and batch operations to save FDB resources if a caller cancels their context
- Supports primary key and secondary index annotations
- Supports FDB atomic mutations (`ADD`, `MAX`, `MIN`) via field annotations
- Provides CRUD, batch, and paginated list operations
- Thread-safe operations using FoundationDB transactions
- Supports pagination for list operations
- Handles index management automatically

## Installation

```bash
go install github.com/romannikov/fdb-go-layer-plugin@latest
```

Ensure that your GOPATH/bin is in your PATH so that protoc can find the plugin:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

## Usage

### 1. Add annotations to your Proto messages

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

message Product {
    option (annotations.primary_key) = "id";
    option (annotations.secondary_index) = {
        fields: ["category"]
    };

    string id = 1;
    string name = 2;
    string category = 3;
    int32 price = 4;
}

// Fan-out index: repeated fields create one index entry per element
message Post {
    option (annotations.primary_key) = "id";
    option (annotations.secondary_index) = {
        fields: ["tags"]
    };

    string id = 1;
    repeated string tags = 2;
}
```

### 2. Generate Code

```bash
protoc \
  -I=. -I=$(go list -m -f '{{ .Dir }}' github.com/romannikov/fdb-go-layer-plugin) \
  --plugin=protoc-gen-fdb-layer=./fdb-go-layer-plugin \
  --fdb-layer_out=. \
  --fdb-layer_opt=paths=source_relative \
  --go_out=. \
  --go_opt=paths=source_relative \
  your/proto/file.proto
```

This generates:
- `*_metadata.go` — `RecordStore` struct, `Transaction` interface, and `SyncMetadata` method
- `*_<message>.go` — CRUD methods for each annotated message

## Generated Code

### Core Types

```go
// Transaction is a mockable interface that abstracts fdb.Transaction.
type Transaction interface {
    fdb.ReadTransaction
    Set(key fdb.KeyConvertible, value []byte)
    Clear(key fdb.KeyConvertible)
    Add(key fdb.KeyConvertible, param []byte)
    Max(key fdb.KeyConvertible, param []byte)
    Min(key fdb.KeyConvertible, param []byte)
}

// GenericRepository is a generic data access interface for entity T with primary key PK.
type GenericRepository[T any, PK any] interface {
    Create(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, entity T) error
    Get(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, pk PK) (T, error)
    Set(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, entity T) error
    Delete(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, pk PK) error
}

// RecordStore holds metadata mapping between message names and their integer type IDs.
type RecordStore struct { /* unexported metadata field */ }

// NewRecordStore creates a new RecordStore instance.
func NewRecordStore() *RecordStore

// SyncMetadata reads the existing metadata from FDB and assigns new IDs to any unmapped messages.
func (s *RecordStore) SyncMetadata(ctx context.Context, tr Transaction, metaDir directory.DirectorySubspace) error

// Metadata returns a read-only copy of the metadata mapping.
func (s *RecordStore) Metadata() map[string]int64
```

### Specific Repositories & Wrapper Pattern

For each message type, the plugin generates a type-safe specific repository interface extending `GenericRepository` and encapsulating all custom database methods (indexes, batch, pagination, atomic mutations).

For example, for the `User` message:

```go
// UserRepository defines the repository interface for User.
type UserRepository interface {
    GenericRepository[*User, string]

    BatchGetUser(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, ids []tuple.Tuple) (map[string]*User, error)
    ListUser(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, opts UserPaginationOptions) (*UserPaginatedResult, error)
    GetUserByEmail(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, Email string) ([]*User, error)
}

// NewUserRepository creates a new UserRepository instance wrapping the RecordStore.
func NewUserRepository(store *RecordStore) UserRepository
```

This makes it extremely simple to inject mock repositories in unit tests and wrap the generated repository interfaces to add custom business logic.


### CRUD Operations (methods on `*RecordStore`)

```go
// Create a new entity
store.CreateUser(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, entity *User) error

// Get an entity by primary key
store.GetUser(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, id string) (*User, error)

// Update an existing entity
store.SetUser(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, entity *User) error

// Delete an entity
store.DeleteUser(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, id string) error

// Batch get multiple entities by their primary keys
store.BatchGetUser(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, ids []tuple.Tuple) (map[string]*User, error)

// List entities with pagination
store.ListUser(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, opts UserPaginationOptions) (*UserPaginatedResult, error)
```

### Pagination

```go
type UserPaginationOptions struct {
    Begin tuple.Tuple  // Starting key for the query
    Limit int          // Maximum number of items to return
}

type UserPaginatedResult struct {
    Items   []*User     // List of items
    NextKey tuple.Tuple // Key for the next page
    HasMore bool        // Whether there are more items
}
```

### Secondary Index Operations

For each secondary index, the plugin generates a lookup method:

```go
// Standard index — lookup by a single scalar field
store.GetUserByEmail(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, email string) ([]*User, error)
```

#### Fan-Out Indexes

When a secondary index references a `repeated` field, the plugin generates a **fan-out index**.
One index entry is written per element in the repeated field, enabling efficient lookups by any single value:

```go
// Fan-out index — lookup posts by any one of their tags
store.GetPostByTags(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, tag string) ([]*Post, error)
```

On `Set` and `Delete`, all old fan-out entries are automatically cleared and re-written.

### Atomic Mutations

For fields marked with mutation annotations, the plugin generates specific atomic operation methods:

```go
// Apply atomic ADD to a field
store.AddCounterValue(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, id string, val int64) error

// Apply atomic MAX to a field
store.MaxCounterMaxValue(ctx context.Context, tr Transaction, id string, val int64) error

// Apply atomic MIN to a field
store.MinCounterMinValue(ctx context.Context, tr Transaction, id string, val int64) error
```

These operations do not require a read-modify-write cycle. Note that fields marked for atomic mutation are stored in separate keys and are excluded from the main serialized message blob.

## Example Usage

```go
package main

import (
    "context"
    "fmt"
    "github.com/apple/foundationdb/bindings/go/src/fdb"
    "github.com/apple/foundationdb/bindings/go/src/fdb/directory"
    db "your/package/generated"
)

func main() {
    fdb.MustAPIVersion(730)
    fdbConn := fdb.MustOpenDefault()
    ctx := context.Background()

    // Create directory subspaces for data and metadata
    dataDir, _ := directory.CreateOrOpen(fdbConn, []string{"app_data"}, nil)
    metaDir, _ := directory.CreateOrOpen(fdbConn, []string{"app_data", "_meta"}, nil)

    // Initialize the RecordStore and sync metadata
    store := db.NewRecordStore()
    fdbConn.Transact(func(tr fdb.Transaction) (interface{}, error) {
        return nil, store.SyncMetadata(ctx, tr, metaDir)
    })

    // Create a new user
    fdbConn.Transact(func(tr fdb.Transaction) (interface{}, error) {
        user := &db.User{
            Id:    "123",
            Email: "user@example.com",
            Name:  "John Doe",
        }
        return nil, store.CreateUser(ctx, tr, dataDir, user)
    })

    fmt.Println("User saved successfully")
}
```

## License

MIT

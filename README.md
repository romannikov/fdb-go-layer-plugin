# FDB Go Layer Plugin

A protoc plugin that generates FoundationDB data access layer code for Proto messages.

## Features

- Generates FoundationDB data access functions for Proto messages
- Supports primary key and secondary index annotations
- Provides CRUD operations and batch operations
- Handles index management automatically
- Thread-safe operations using FoundationDB transactions

## Installation

```bash
go install github.com/romannikov/fdb-go-layer-plugin@latest
```

Ensure that your GOPATH/bin is in your PATH so that protoc can find the plugin:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

## Usage

### Add annotations to your Proto messages:

```protobuf
syntax = "proto3"
import "fdb-layer/annotations.proto";

message User {
    option (fdb_layer.primary_key) = "id";
    option (fdb_layer.secondary_index) = {
        fields: ["email"]
    };
    option (fdb_layer.secondary_index) = {
        fields: ["name", "age"]
    };

    string id = 1;
    string email = 2;
    string name = 3;
    int32 age = 4;
}
```

### Generate Code
Run the `protoc` compiler with the plugin to generate Go code for your messages and repositories.
```bash
protoc \
  -I=. -I=$(go list -m -f '{{ .Dir }}' github.com/romannikov/fdb-go-layer-plugin) \
  --plugin=protoc-gen-fdb-go-layer-plugin=./fdb-go-layer-plugin \
  --fdb-go-layer-plugin_out=. \
  --go_out=. \
  --go_opt=paths=source_relative \
  user.proto
```

## Generated Code

The plugin generates the following functions for each message:

### Basic Operations

```go
// Create a new entity
CreateUser(tr fdb.Transaction, dir directory.DirectorySubspace, entity *pb.User) error

// Get an entity by primary key
GetUser(tr fdb.ReadTransaction, dir directory.DirectorySubspace, id string) (*pb.User, error)

// Update an existing entity
SetUser(tr fdb.Transaction, dir directory.DirectorySubspace, entity *pb.User) error

// Delete an entity
DeleteUser(tr fdb.Transaction, dir directory.DirectorySubspace, id string) error

// Batch get multiple entities by their primary keys
BatchGetUser(tr fdb.ReadTransaction, dir directory.DirectorySubspace, ids []tuple.Tuple) (map[string]*pb.User, error)
```

### Secondary Index Operations

For each secondary index, the plugin generates a lookup function:

```go
// Get by email index
GetUserByEmail(tr fdb.ReadTransaction, dir directory.DirectorySubspace, email string) ([]*pb.User, error)

// Get by name and age index
GetUserByNameAndAge(tr fdb.ReadTransaction, dir directory.DirectorySubspace, name string, age int32) ([]*pb.User, error)
```

## Example Usage

```go
package main

import (
    "github.com/apple/foundationdb/bindings/go/src/fdb"
    "github.com/apple/foundationdb/bindings/go/src/fdb/directory"
    "your/package/db"
    pb "your/package/proto"
)

func main() {
    // Initialize FDB
    fdb.MustAPIVersion(710)
    db := fdb.MustOpenDefault()

    // Create directory subspace
    dir, err := directory.CreateOrOpen(db, []string{"myapp"}, nil)
    if err != nil {
        panic(err)
    }

    // Start a transaction
    tr, err := db.CreateTransaction()
    if err != nil {
        panic(err)
    }

    // Create a new user
    user := &pb.User{
        Id:    "123",
        Email: "user@example.com",
        Name:  "John Doe",
        Age:   30,
    }
    err = db.CreateUser(tr, dir, user)
    if err != nil {
        panic(err)
    }

    // Commit the transaction
    err = tr.Commit().Get()
    if err != nil {
        panic(err)
    }

    fmt.Println("User saved successfully")
}
```

## License

MIT

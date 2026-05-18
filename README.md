# FoundationDB Go Layer Plugin
A protoc plugin that generates FoundationDB data access layer code for Proto messages. It simplifies building applications on top of FoundationDB by handling the boilerplate of mapping messages to keys and values.
## Supported Features
- **CRUD Operations**: Full support for Create, Get, Set (Update), and Delete operations on Protobuf messages.
- **Primary Keys**: Support for custom primary keys, including compound keys composed of multiple fields.
- **Secondary Indexes**: Support for secondary indexes for efficient lookups, including "fan-out" indexes for repeated fields.
- **Atomic Operations**: Support for FoundationDB atomic mutations (`ADD`, `MAX`, `MIN`) directly via field annotations.
- **Time Versionstamps**: Support for FDB versionstamps to order records by commit time.
- **Queue Support**: Built-in support for FIFO queues ordered by versionstamp.
- **Unified Store**: Support for both standard data records and queue messages within the same system and subspace.
## Installation
```bash
go install github.com/romannikov/fdb-go-layer-plugin@latest
```
Ensure that your `GOPATH/bin` is in your `PATH` so that `protoc` can find the plugin:
```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```
## Data Storage Layout
The plugin stores data in FoundationDB using a specific key-value layout. All keys are prefixed by a `directory.DirectorySubspace` assigned to the store.
### Data Locality
Using a directory subspace ensures that all data for a specific application or layer is stored in a contiguous key range in FoundationDB. This provides excellent data locality, making range reads very efficient and keeping related data physically close in the cluster.
### Key Components
- **Subspace**: The root directory subspace for your application data.
- **TypeID**: An auto-assigned integer ID for each message type. This minimizes key size in FDB compared to using full message names, saving storage and network bandwidth.
### Layout Examples
Assume we have a `User` message with `TypeID = 1` and a `Task` message with `TypeID = 2`.
#### 1. Metadata Registry
- **Purpose**: Maps message names to integer TypeIDs.
- **Key**: `[Meta Subspace] + ("User")`
- **Value**: `(1)` (packed tuple)
- *Example*: If the meta subspace is `app_meta`, the key might be `app_meta/"User"` and the value `(1)`.
#### 2. Standard Data Record
- **Purpose**: Stores the serialized message.
- **Key**: `[Data Subspace] + (1, "u123")`
- **Value**: `[Serialized User Protobuf Blob]`
- *Example*: A user with ID `u123` is stored at key `app_data/1/"u123"`.
#### 3. Secondary Index
- **Purpose**: Enables lookup by indexed fields.
- **Key**: `[Data Subspace] + (1, "index", "email", "user@example.com", "u123")`
- **Value**: `[]` (empty)
- *Note*: The plugin does NOT store the whole record in the index. It stores a **reference** to the record (the primary key `"u123"`) within the index key itself.
- *Example*: Searching for `user@example.com` will find this index key, and the trailing `"u123"` tells the plugin which record to fetch.
#### 4. Queue Message
- **Purpose**: Stores messages in a queue.
- **Key**: `[Data Subspace] + (2, queue_name, shard_id, versionstamp)`
- **Value**: `[Serialized Task Protobuf Blob]`
- *Example*: A task in the `"high-priority"` queue (queue_name) might be stored at `app_data/2/"high-priority"/1/versionstamp`.
### How to Make Data User-Local
If you want to ensure that all data belonging to a specific user is stored together (e.g., for GDPR compliance or efficient cleanup), you can structure your directory subspaces by user ID:
For instance, instead of a global `app_data` subspace, create a subspace per user:
`app_state/user_id/type/...`
In code, you would create the subspace like this:
```go
userDir, _ := directory.CreateOrOpen(db, []string{"app_state", "u123"}, nil)
// Pass userDir to repository methods
```
This ensures all data for `u123` is in a contiguous range.
## Proto and Code Examples
Here are examples of how to define your messages and use the generated code for common use cases.
### 1. Standard Record
This example shows basic CRUD operations and index lookup for a standard record.
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
#### Use Generated Code
```go
package main
import (
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"your/package/generated"
)
func main() {
	fdb.MustAPIVersion(730)
	db := fdb.MustOpenDefault()
	ctx := context.Background()
	store := generated.NewRecordStore()
	dataDir, _ := directory.CreateOrOpen(db, []string{"app_data"}, nil)
	metaDir, _ := directory.CreateOrOpen(db, []string{"app_data", "_meta"}, nil)
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err := store.SyncMetadata(ctx, tr, metaDir, []string{"User"})
		if err != nil {
			return nil, err
		}
		userRepo := generated.NewUserRepository(store)
		// Create a user
		user := &generated.User{Id: "u123", Email: "user@example.com", Name: "John"}
		err = userRepo.Create(ctx, tr, dataDir, user)
		if err != nil {
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
### 2. Queue
This example shows how to create a user and enqueue a "send email" task in the **same transaction**, ensuring atomicity.
#### Define Proto
```protobuf
message Task {
    option (annotations.is_queue) = true;
    option (annotations.primary_key) = "queue_name";
    option (annotations.primary_key) = "shard_id";
    option (annotations.primary_key) = "versionstamp";
    string queue_name = 1;
    uint32 shard_id = 2; // Used for sharding to avoid contention
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
	"your/package/generated"
)
func main() {
	fdb.MustAPIVersion(730)
	db := fdb.MustOpenDefault()
	ctx := context.Background()
	store := generated.NewRecordStore()
	dataDir, _ := directory.CreateOrOpen(db, []string{"app_data"}, nil)
	metaDir, _ := directory.CreateOrOpen(db, []string{"app_data", "_meta"}, nil)
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err := store.SyncMetadata(ctx, tr, metaDir, []string{"User", "Task"})
		if err != nil {
			return nil, err
		}
		userRepo := generated.NewUserRepository(store)
		taskRepo := generated.NewTaskRepository(store)
		// Create a user
		user := &generated.User{Id: "u123", Email: "user@example.com", Name: "John"}
		err = userRepo.Create(ctx, tr, dataDir, user)
		if err != nil {
			return nil, err
		}
		// Enqueue a task to send a welcome email
		task := &generated.Task{
			QueueName: "send-email",
			// shard_id can be set randomly to distribute load across multiple
			// writers and avoid contention on the queue tail.
			ShardId:   uint32(rand.Intn(10)), 
			Payload:   []byte("u123"), // Reference the user ID in payload
		}
		err = taskRepo.Enqueue(ctx, tr, dataDir, task)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("User created and email task enqueued atomically.")
}
```
### 3. Dequeue
This example shows how to read and process a task from the queue.
#### Use Generated Code
```go
package main
import (
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"your/package/generated"
)
func main() {
	fdb.MustAPIVersion(730)
	db := fdb.MustOpenDefault()
	ctx := context.Background()
	store := generated.NewRecordStore()
	dataDir, _ := directory.CreateOrOpen(db, []string{"app_data"}, nil)
	metaDir, _ := directory.CreateOrOpen(db, []string{"app_data", "_meta"}, nil)
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err := store.SyncMetadata(ctx, tr, metaDir, []string{"Task"})
		if err != nil {
			return nil, err
		}
		taskRepo := generated.NewTaskRepository(store)
		// Dequeue the oldest task
		task, err := taskRepo.Dequeue(ctx, tr, dataDir, "send-email")
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
## Contributing
We welcome contributions to improve this plugin! Here is the process:
1.  **Fork the repository** on GitHub.
2.  **Clone your fork** locally.
3.  **Create a new branch** for your feature or bugfix.
4.  **Make your changes** and ensure code is well-formatted.
5.  **Run integration tests** (requires a running FoundationDB cluster or simulator):
    ```bash
    go test ./...
    ```
6.  **Update Protos** if you modified `annotations.proto`:
    ```bash
    ./update_protos.sh
    ```
7.  **Commit your changes** and push to your fork.
8.  **Submit a Pull Request** with a clear description of what you did.
## License
MIT

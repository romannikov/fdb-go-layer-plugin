# FoundationDB Go Layer Plugin Enhancements

## 1. Functional Enhancements

### Support for Multi-Type Record Stores
Currently, the plugin seems to focus on single-message repositories. FoundationDB’s "Record Layer" (the Java inspiration) allows multiple message types to share the same subspace.

**Improvement:** Allow a single repository to manage multiple Protobuf messages, using a "type header" in the FDB key to distinguish between them.

### Complex Indexing (Function Indexes)
**Improvement:** Support for fan-out indexes (indexing individual elements of a repeated field) and versionstamp indexes (using FDB’s atomic versionstamps to track when a record was last modified).

### Atomic Operations
**Improvement:** Add support for FDB atomic mutations (like `ADD`, `MAX`, `MIN`) directly via Proto annotations. This would allow updating specific fields without a full read-modify-write cycle.

## 2. API Ergonomics & Go Idioms

### Context Support
**Improvement:** Update generated methods to accept `context.Context`. While FDB transactions handle their own state, passing a context is standard for Go libraries to handle cancellation and tracing (e.g., OpenTelemetry).

### Functional Options for Queries
**Improvement:** Replace the current pagination struct with a functional options pattern for `List` operations.

*Example:* `repo.ListUsers(ctx, tr, dir, fdb.WithLimit(10), fdb.WithReverse())`

### Generic Repository Interface
**Improvement:** With Go 1.18+, the plugin could generate a generic repository interface to reduce boilerplate when the user wants to wrap the generated code in custom logic.

## 3. Developer Experience (DX)

### Validation Logic
**Improvement:** Integrate with `protoc-gen-validate` (or `buf validate`). Automatically run validation checks before a `Set` or `Create` operation is committed to the database.

### Local Simulation Testing
**Improvement:** Generate a mock implementation or a "memory layer" for the repository. This would allow developers to write unit tests for their business logic without requiring a running FDB cluster or the C-library dependency.

### CLI Tool for Index Management
**Improvement:** A side-car CLI tool to "sync" indexes. If a developer adds a `secondary_index` annotation, the tool could scan the database and build the missing index entries for existing records in the background.

## 4. Technical Debt & Robustness

### Directory Layer Caching
**Improvement:** Resolving `directory.DirectorySubspace` on every call is expensive. Implement an internal cache within the generated client to store resolved subspaces.

### Tuple Packing Optimization
**Improvement:** Ensure that the generated code uses efficient `tuple.Tuple` packing, especially for primary keys, to minimize key size and improve performance.

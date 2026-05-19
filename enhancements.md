# FoundationDB Go Layer Plugin Enhancements

## 1. API Ergonomics & Go Idioms
### Functional Options for Queries
**Improvement:** Replace the current pagination struct with a functional options pattern for `List` operations.

*Example:* `repo.ListUsers(ctx, tr, dir, fdb.WithLimit(10), fdb.WithReverse())`

## 2. Developer Experience (DX)
### Local Simulation Testing
**Improvement:** Generate a mock implementation or a "memory layer" for the repository. This would allow developers to write unit tests for their business logic without requiring a running FDB cluster or the C-library dependency.

### CLI Tool for Index Management
**Improvement:** A side-car CLI tool to "sync" indexes. If a developer adds a `secondary_index` annotation, the tool could scan the database and build the missing index entries for existing records in the background.

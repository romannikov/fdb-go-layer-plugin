# FoundationDB Go Layer Plugin Enhancements & Roadmap

This document outlines the proposed enhancements for the FoundationDB Go Layer Plugin, categorized by API ergonomics, developer experience (DX), performance, operations, and technical debt.

---

## 1. API Ergonomics & Go Idioms

### Functional Options for Queries
* **Problem**: The current pagination struct (`*PaginationOptions`) generated for each message is boilerplate-heavy, inflexible, and duplicates options per model.
* **Solution**: Replace the pagination struct with a reusable functional options pattern in the core `fdblayer` package.
* **Example**:
  ```go
  // Before
  res, err := repo.ListUser(ctx, tr, dir, store.UserPaginationOptions{Limit: 10})

  // After
  res, err := repo.ListUser(ctx, tr, dir, fdblayer.WithLimit(10), fdblayer.WithReverse(true))
  ```

### Partial Prefix Querying for Composite Indexes
* **Problem**: Secondary indexes on compound fields (e.g. `["category", "status"]`) only generate lookups requiring both fields, meaning you cannot query just by the prefix `category`.
* **Solution**: Generate lookups for all left-prefixes of composite indexes.
* **Example**:
  For an index on `["category", "status"]`, generate:
  * `GetProductByCategoryAndStatus(ctx, tr, dir, category, status)`
  * `GetProductByCategory(ctx, tr, dir, category)`

---

## 2. Developer Experience (DX)

### pure-Go In-Memory Mocking & Simulator
* **Problem**: The internal `tests/mock.go` is not publicly accessible to downstream package consumers. Additionally, it imports the official FDB Go bindings, which require dynamic linking of the C library (`libfdb_c`), making it impossible to run unit tests in environments without an FDB installation.
* **Solution**: Move the mock KV store and transaction simulator to a dedicated public package `fdblayer/mock`. Decouple it from dynamic C bindings so tests can run in pure-Go (e.g., standard CI/CD pipelines).

### CLI Tool for Index Management & Syncing
* **Problem**: When a developer adds a `secondary_index` annotation, the tool does not build missing index entries for existing records, leaving index state inconsistent.
* **Solution**: A side-car CLI tool to "sync" indexes. The tool scans the database and builds the missing index entries in the background.

---

## 3. Operational Integrity & Migrations

### Chunked Background Index Builders
* **Problem**: Index building on existing live datasets can easily hit FoundationDB's strict 5-second transaction limit if performed in a single transaction.
* **Solution**: Generate chunked index rebuild helper methods within each repository that paginate through the dataset and write index entries across multiple transactions.
* **Example**:
  ```go
  totalIndexed, err := repo.RebuildEmailIndex(ctx, db, dir, batchSize)
  ```

---

## 4. Technical Debt & Performance Robustness

### Tuple Packing Optimization
* **Problem**: Single string or integer primary keys are always packed using `tuple.Tuple`, adding a 1-2 byte element-type overhead per key.
* **Solution**: Provide an option to pack simple single-field primary keys directly as raw byte slices or compact binary forms, reducing FDB storage footprint in high-volume clusters.

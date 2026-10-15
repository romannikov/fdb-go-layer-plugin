package fdblayer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// ErrAlreadyExists is returned when an entity with the same primary key or unique index already exists.
var ErrAlreadyExists = errors.New("already exists")

// ErrUniqueConstraintViolation is returned when a unique secondary index constraint is violated.
// It wraps ErrAlreadyExists so errors.Is(err, ErrAlreadyExists) and errors.Is(err, ErrUniqueConstraintViolation) both return true.
var ErrUniqueConstraintViolation = fmt.Errorf("unique constraint violation: %w", ErrAlreadyExists)

// ErrNotFound is returned by Get and GetUniqueBy* when no matching entity exists.
var ErrNotFound = errors.New("not found")

// ErrUserVersionOverflow is returned when versionstamped operations exceed 65,536 (0..65535) in a single transaction.
var ErrUserVersionOverflow = errors.New("user version overflow in transaction")

// Partition namespaces under the TypeID prefix.
const (
	DataNamespace      int64 = 0
	IndexNamespace     int64 = 1
	FieldNamespace     int64 = 2
	AggregateNamespace int64 = 3
)

// Transaction is a mockable interface that abstracts fdb.Transaction.
type Transaction interface {
	fdb.ReadTransaction
	Set(key fdb.KeyConvertible, value []byte)
	Clear(key fdb.KeyConvertible)
	ClearRange(er fdb.ExactRange)
	Add(key fdb.KeyConvertible, param []byte)
	Max(key fdb.KeyConvertible, param []byte)
	Min(key fdb.KeyConvertible, param []byte)
	SetVersionstampedKey(key fdb.KeyConvertible, value []byte)
}

// ReadTx is a read-only transaction context bound to a DirectorySubspace.
// Both *ReadOnlyTx and *Tx implement ReadTx, allowing all repository read methods
// to be called inside both ReadTransact and Transact callbacks.
type ReadTx interface {
	ReadTransaction() fdb.ReadTransaction
	Dir() directory.DirectorySubspace
	Snapshot() ReadTx
}

// ReadOnlyTx wraps an fdb.ReadTransaction and a DirectorySubspace for read-only operations.
type ReadOnlyTx struct {
	rtr      fdb.ReadTransaction
	dir      directory.DirectorySubspace
	snapshot bool
}

// NewReadOnlyTx creates a read-only transaction context binding rtr and dir.
func NewReadOnlyTx(rtr fdb.ReadTransaction, dir directory.DirectorySubspace) *ReadOnlyTx {
	return &ReadOnlyTx{rtr: rtr, dir: dir}
}

// ReadTransaction returns the underlying fdb.ReadTransaction.
func (r *ReadOnlyTx) ReadTransaction() fdb.ReadTransaction {
	if r.snapshot {
		if snap := r.rtr.Snapshot(); snap != (fdb.Snapshot{}) {
			return snap
		}
	}
	return r.rtr
}

// Dir returns the DirectorySubspace associated with this read transaction.
func (r *ReadOnlyTx) Dir() directory.DirectorySubspace {
	return r.dir
}

// Snapshot returns a conflict-free snapshot view of this read transaction.
func (r *ReadOnlyTx) Snapshot() ReadTx {
	return &ReadOnlyTx{rtr: r.rtr, dir: r.dir, snapshot: true}
}

// Mutation represents a buffered database write operation that executes in two pipelined phases:
//  1. Prepare issues any required asynchronous FDB reads (primary key existence, old record for index diffing,
//     or unique index conflict checks) without blocking, returning a resolve callback.
//  2. The returned resolve callback awaits the prepared futures (which resolve in parallel in 1 RTT)
//     and emits the underlying FDB Set/Clear/Add/Max/Min mutations.
type Mutation interface {
	Prepare(ctx context.Context, tx *Tx) (func(ctx context.Context) error, error)
}

// MutationFunc adapts a function into a Mutation.
type MutationFunc func(ctx context.Context, tx *Tx) (func(ctx context.Context) error, error)

// Prepare implements Mutation.
func (f MutationFunc) Prepare(ctx context.Context, tx *Tx) (func(ctx context.Context) error, error) {
	return f(ctx, tx)
}

// Tx bundles a FoundationDB Transaction, its target DirectorySubspace, the per-transaction
// userVersionSeq counter for incomplete versionstamps, and buffered pipelined Mutations.
type Tx struct {
	tr             Transaction
	dir            directory.DirectorySubspace
	userVersionSeq atomic.Uint32
	mutations      []Mutation
	claimedKeys    map[string]string
}

// NewTx creates a per-transaction context binding tr and dir with a fresh userVersionSeq counter.
func NewTx(tr Transaction, dir directory.DirectorySubspace) *Tx {
	return &Tx{tr: tr, dir: dir}
}

// Transaction returns the underlying FoundationDB Transaction.
func (tx *Tx) Transaction() Transaction {
	return tx.tr
}

// ReadTransaction implements ReadTx by returning the underlying fdb.ReadTransaction.
func (tx *Tx) ReadTransaction() fdb.ReadTransaction {
	return tx.tr
}

// Dir returns the DirectorySubspace associated with this transaction context.
func (tx *Tx) Dir() directory.DirectorySubspace {
	return tx.dir
}

// Snapshot implements ReadTx by returning a conflict-free snapshot view of the transaction.
func (tx *Tx) Snapshot() ReadTx {
	return &ReadOnlyTx{rtr: tx.tr, dir: tx.dir, snapshot: true}
}

// Reset resets the transaction's user version sequence and clears any buffered mutations.
func (tx *Tx) Reset() {
	tx.userVersionSeq.Store(0)
	tx.mutations = nil
	tx.claimedKeys = nil
}

// ResetUserVersion resets the transaction's user version sequence to 0.
func (tx *Tx) ResetUserVersion() {
	tx.userVersionSeq.Store(0)
}

// NextUserVersion returns a monotonically increasing 16-bit user version (0..65535)
// so that multiple versionstamped keys written within the same transaction receive
// distinct user versions instead of colliding at 0.
func (tx *Tx) NextUserVersion() (uint16, error) {
	v := tx.userVersionSeq.Add(1)
	if v > 65536 {
		return 0, ErrUserVersionOverflow
	}
	return uint16(v - 1), nil
}

// ClaimKey records a key claimed during a buffered Flush (e.g. for Insert PK or Unique Index checks)
// to detect intra-batch collisions before FDB commit. Returns false if already claimed by a different owner
// (or if either claim has an empty owner, which indicates an unconditional Insert PK claim).
func (tx *Tx) ClaimKey(key fdb.Key, owner string) bool {
	if tx.claimedKeys == nil {
		tx.claimedKeys = make(map[string]string)
	}
	k := string(key)
	if prevOwner, exists := tx.claimedKeys[k]; exists && (owner == "" || prevOwner == "" || prevOwner != owner) {
		return false
	}
	tx.claimedKeys[k] = owner
	return true
}

// Buffer appends one or more Mutations to be executed in a pipelined batch when Flush is called
// (or automatically at the end of a Store.Transact callback).
func (tx *Tx) Buffer(ms ...Mutation) {
	tx.mutations = append(tx.mutations, ms...)
}

// Flush executes all buffered Mutations in two pipelined phases:
// Phase 1 issues all pre-read futures in parallel, and Phase 2 resolves them in a single RTT and applies writes.
func (tx *Tx) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pending := tx.mutations
	tx.mutations = nil
	if len(pending) == 0 {
		return nil
	}
	tx.claimedKeys = make(map[string]string, len(pending)*2)
	defer func() {
		tx.claimedKeys = nil
	}()

	resolvers := make([]func(context.Context) error, 0, len(pending))
	for _, m := range pending {
		if m == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		resolve, err := m.Prepare(ctx, tx)
		if err != nil {
			return err
		}
		if resolve != nil {
			resolvers = append(resolvers, resolve)
		}
	}
	for _, resolve := range resolvers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := resolve(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Store binds a FoundationDB database (or mock runner) and a DirectorySubspace.
type Store struct {
	dir          directory.DirectorySubspace
	transact     func(func(Transaction) error) error
	readTransact func(func(fdb.ReadTransaction) error) error
}

// NewStore creates a Store bound to an fdb.Database and a DirectorySubspace.
func NewStore(db fdb.Database, dir directory.DirectorySubspace) *Store {
	return &Store{
		dir: dir,
		transact: func(fn func(Transaction) error) error {
			_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				return nil, fn(tr)
			})
			return err
		},
		readTransact: func(fn func(fdb.ReadTransaction) error) error {
			_, err := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
				return nil, fn(rtr)
			})
			return err
		},
	}
}

// NewStoreFromTransaction creates a Store backed by a single Transaction (useful for unit tests with MockTransaction).
func NewStoreFromTransaction(tr Transaction, dir directory.DirectorySubspace) *Store {
	return &Store{
		dir: dir,
		transact: func(fn func(Transaction) error) error {
			return fn(tr)
		},
		readTransact: func(fn func(fdb.ReadTransaction) error) error {
			return fn(tr)
		},
	}
}

// Dir returns the Store's DirectorySubspace.
func (s *Store) Dir() directory.DirectorySubspace {
	return s.dir
}

// Transact executes fn inside a read-write transaction retry loop.
// On each attempt, a fresh *Tx is created, fn is invoked, and any buffered Mutations are flushed before committing.
func (s *Store) Transact(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.transact(func(tr Transaction) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		tx := NewTx(tr, s.dir)
		if err := fn(ctx, tx); err != nil {
			return err
		}
		return tx.Flush(ctx)
	})
}

// ReadTransact executes fn inside a read-only transaction retry loop.
func (s *Store) ReadTransact(ctx context.Context, fn func(ctx context.Context, rtx ReadTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.readTransact(func(rtr fdb.ReadTransaction) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(ctx, NewReadOnlyTx(rtr, s.dir))
	})
}

// Apply buffers and executes a batch of Mutations in a single pipelined read-write transaction.
func (s *Store) Apply(ctx context.Context, ms ...Mutation) error {
	if len(ms) == 0 {
		return ctx.Err()
	}
	return s.Transact(ctx, func(ctx context.Context, tx *Tx) error {
		tx.Buffer(ms...)
		return tx.Flush(ctx)
	})
}

// BoundKind specifies whether the start and end bounds of a KeyRange are inclusive (Closed) or exclusive (Open).
type BoundKind int

const (
	// ClosedOpen represents [Start, End) — inclusive Start, exclusive End (default).
	ClosedOpen BoundKind = iota
	// ClosedClosed represents [Start, End] — inclusive Start, inclusive End.
	ClosedClosed
	// OpenClosed represents (Start, End] — exclusive Start, inclusive End.
	OpenClosed
	// OpenOpen represents (Start, End) — exclusive Start, exclusive End.
	OpenOpen
)

// KeyRange represents a typed range over key or index field values of type K.
type KeyRange[K any] struct {
	Start          K
	End            K
	Kind           BoundKind
	UnboundedStart bool
	UnboundedEnd   bool
}

// Between returns a closed interval [start, end] (ClosedClosed).
func Between[K any](start, end K) KeyRange[K] {
	return KeyRange[K]{Start: start, End: end, Kind: ClosedClosed}
}

// HalfOpen returns a half-open interval [start, end) (ClosedOpen).
func HalfOpen[K any](start, end K) KeyRange[K] {
	return KeyRange[K]{Start: start, End: end, Kind: ClosedOpen}
}

// OpenClosedRange returns an interval (start, end] (OpenClosed).
func OpenClosedRange[K any](start, end K) KeyRange[K] {
	return KeyRange[K]{Start: start, End: end, Kind: OpenClosed}
}

// OpenInterval returns an open interval (start, end) (OpenOpen).
func OpenInterval[K any](start, end K) KeyRange[K] {
	return KeyRange[K]{Start: start, End: end, Kind: OpenOpen}
}

// GreaterOrEqual returns an interval [start, +∞).
func GreaterOrEqual[K any](start K) KeyRange[K] {
	return KeyRange[K]{Start: start, Kind: ClosedOpen, UnboundedEnd: true}
}

// GreaterThan returns an interval (start, +∞).
func GreaterThan[K any](start K) KeyRange[K] {
	return KeyRange[K]{Start: start, Kind: OpenOpen, UnboundedEnd: true}
}

// LessThan returns an interval (-∞, end).
func LessThan[K any](end K) KeyRange[K] {
	return KeyRange[K]{End: end, Kind: ClosedOpen, UnboundedStart: true}
}

// LessOrEqual returns an interval (-∞, end].
func LessOrEqual[K any](end K) KeyRange[K] {
	return KeyRange[K]{End: end, Kind: ClosedClosed, UnboundedStart: true}
}

// AllKeys returns an unbounded interval (-∞, +∞).
func AllKeys[K any]() KeyRange[K] {
	return KeyRange[K]{UnboundedStart: true, UnboundedEnd: true}
}

// PaginationOptions represents options for paginated queries.
type PaginationOptions struct {
	Begin     tuple.Tuple
	Prefix    tuple.Tuple
	PageToken []byte
	Limit     int
	Reverse   bool
}

// EffectiveBegin returns opts.Begin if non-empty, or unpacks opts.PageToken when provided.
func (opts PaginationOptions) EffectiveBegin() (tuple.Tuple, error) {
	if len(opts.Begin) > 0 {
		return opts.Begin, nil
	}
	if len(opts.PageToken) == 0 {
		return nil, nil
	}
	tpl, err := tuple.Unpack(fdb.Key(opts.PageToken))
	if err != nil {
		return nil, fmt.Errorf("invalid page token: %w", err)
	}
	return tpl, nil
}

// PaginatedResult represents a paginated result set for entity T.
type PaginatedResult[T any] struct {
	Items         []T
	NextKey       tuple.Tuple
	NextPageToken []byte
	HasMore       bool
}

// ComputeBoundedKeyRange computes the [Begin, End) FDB KeyRange for a base tuple prefix,
// optional trailing field range bounds (startEl/endEl), and pagination cursor.
func ComputeBoundedKeyRange(
	dir directory.DirectorySubspace,
	baseTpl tuple.Tuple,
	startEl tuple.TupleElement,
	endEl tuple.TupleElement,
	unboundedStart bool,
	unboundedEnd bool,
	kind BoundKind,
	cursorTpl tuple.Tuple,
	reverse bool,
) (fdb.KeyRange, bool, error) {
	basePrefix := dir.Pack(baseTpl)
	baseRange, err := fdb.PrefixRange(basePrefix)
	if err != nil {
		return fdb.KeyRange{}, false, err
	}
	rangeBegin := baseRange.Begin.FDBKey()
	rangeEnd := baseRange.End.FDBKey()

	if !unboundedStart {
		startTpl := make(tuple.Tuple, len(baseTpl)+1)
		copy(startTpl, baseTpl)
		startTpl[len(baseTpl)] = NormalizeTupleElement(startEl)
		startPrefix := dir.Pack(startTpl)
		if kind == OpenClosed || kind == OpenOpen {
			sr, err := fdb.PrefixRange(startPrefix)
			if err != nil {
				return fdb.KeyRange{}, false, err
			}
			if bytes.Compare(sr.End.FDBKey(), rangeBegin) > 0 {
				rangeBegin = sr.End.FDBKey()
			}
		} else {
			if bytes.Compare(startPrefix, rangeBegin) > 0 {
				rangeBegin = startPrefix
			}
		}
	}

	if !unboundedEnd {
		endTpl := make(tuple.Tuple, len(baseTpl)+1)
		copy(endTpl, baseTpl)
		endTpl[len(baseTpl)] = NormalizeTupleElement(endEl)
		endPrefix := dir.Pack(endTpl)
		if kind == ClosedClosed || kind == OpenClosed {
			er, err := fdb.PrefixRange(endPrefix)
			if err != nil {
				return fdb.KeyRange{}, false, err
			}
			if bytes.Compare(er.End.FDBKey(), rangeEnd) < 0 {
				rangeEnd = er.End.FDBKey()
			}
		} else {
			if bytes.Compare(endPrefix, rangeEnd) < 0 {
				rangeEnd = endPrefix
			}
		}
	}

	if len(cursorTpl) > 0 {
		fullCursorTpl := make(tuple.Tuple, len(baseTpl)+len(cursorTpl))
		copy(fullCursorTpl, baseTpl)
		for j, el := range cursorTpl {
			fullCursorTpl[len(baseTpl)+j] = NormalizeTupleElement(el)
		}
		cursorKey := dir.Pack(fullCursorTpl)
		if reverse {
			cr, err := fdb.PrefixRange(cursorKey)
			if err != nil {
				return fdb.KeyRange{}, false, err
			}
			if bytes.Compare(cr.End.FDBKey(), rangeEnd) < 0 {
				rangeEnd = cr.End.FDBKey()
			}
		} else if bytes.Compare(cursorKey, rangeBegin) > 0 {
			rangeBegin = cursorKey
		}
	}

	if bytes.Compare(rangeBegin, rangeEnd) >= 0 {
		return fdb.KeyRange{}, false, nil
	}
	return fdb.KeyRange{Begin: rangeBegin, End: rangeEnd}, true, nil
}

// GenericRepository is a generic data access interface for entity T with primary key PK.
type GenericRepository[T any, PK any] interface {
	Create(ctx context.Context, tx *Tx, entity T) error
	Get(ctx context.Context, tx ReadTx, pk PK) (T, error)
	Set(ctx context.Context, tx *Tx, entity T) error
	Delete(ctx context.Context, tx *Tx, pk PK) error
	BatchGet(ctx context.Context, tx ReadTx, pks []PK) ([]T, error)
	List(ctx context.Context, tx ReadTx, opts PaginationOptions) (*PaginatedResult[T], error)

	Insert(entity T) Mutation
	Upsert(entity T) Mutation
	DeleteMutation(pk PK) Mutation
}

type rangeSliceReader interface {
	GetRangeSlice(r fdb.Range, options fdb.RangeOptions) []fdb.KeyValue
}

// RangeFuture wraps an asynchronous fdb.RangeResult (for real FoundationDB
// transactions, starting the range read immediately upon creation) or a slice
// returned by a mock transaction implementing GetRangeSlice.
type RangeFuture struct {
	rr     fdb.RangeResult
	slice  []fdb.KeyValue
	isMock bool
}

// GetRange starts an asynchronous range read on tr (or reads via GetRangeSlice
// when tr is a mock transaction in unit tests).
func GetRange(tr fdb.ReadTransaction, r fdb.Range, opts fdb.RangeOptions) RangeFuture {
	if mockTr, ok := tr.(rangeSliceReader); ok {
		return RangeFuture{slice: mockTr.GetRangeSlice(r, opts), isMock: true}
	}
	return RangeFuture{rr: tr.GetRange(r, opts)}
}

// GetSliceOrPanic blocks until the range read completes and returns all KeyValues.
func (rf RangeFuture) GetSliceOrPanic() []fdb.KeyValue {
	if rf.isMock {
		return rf.slice
	}
	return rf.rr.GetSliceOrPanic()
}

// NormalizeTupleElement widens signed integer/enum types (such as int32 or generated Protobuf
// enum types) to int64 and unsigned integer types (uint32, uint16, uint8) to uint64 so they
// match the canonical tuple encoding used by PackField and never panic in tuple.Pack.
func NormalizeTupleElement(el tuple.TupleElement) tuple.TupleElement {
	if el == nil {
		return nil
	}
	switch v := el.(type) {
	case string, []byte, int64, uint64, bool, float32, float64, tuple.Versionstamp, tuple.UUID:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case uint:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint8:
		return uint64(v)
	case tuple.Tuple:
		out := make(tuple.Tuple, len(v))
		for i, sub := range v {
			out[i] = NormalizeTupleElement(sub)
		}
		return out
	default:
		rv := reflect.ValueOf(el)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return rv.Uint()
		}
		return el
	}
}

// BytesToVersionstamp converts a 12-byte slice into a complete tuple.Versionstamp.
func BytesToVersionstamp(b []byte) tuple.Versionstamp {
	var vs tuple.Versionstamp
	copy(vs.TransactionVersion[:], b)
	if len(b) >= 12 {
		vs.UserVersion = uint16(b[10])<<8 | uint16(b[11])
	}
	return vs
}

package fdblayer

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// ErrAlreadyExists is returned by Create when an entity with the same primary key already exists.
var ErrAlreadyExists = errors.New("already exists")

// ErrNotFound is returned by Get when no entity exists for the given primary key.
var ErrNotFound = errors.New("not found")

// ErrUserVersionOverflow is returned when versionstamped operations exceed 65,536 (0..65535).
var ErrUserVersionOverflow = errors.New("user version overflow in transaction")

// Partition namespaces under the typeID prefix.
const (
	DataNamespace  int64 = 0
	IndexNamespace int64 = 1
)

// Transaction is a mockable interface that abstracts fdb.Transaction.
type Transaction interface {
	fdb.ReadTransaction
	Set(key fdb.KeyConvertible, value []byte)
	Clear(key fdb.KeyConvertible)
	SetVersionstampedKey(key fdb.KeyConvertible, value []byte)
}

// PaginationOptions represents options for paginated queries.
type PaginationOptions struct {
	Begin   tuple.Tuple
	Prefix  tuple.Tuple
	Limit   int
	Reverse bool
}

// PaginatedResult represents a paginated result set for entity T.
type PaginatedResult[T any] struct {
	Items   []T
	NextKey tuple.Tuple
	HasMore bool
}

// Tx bundles a FoundationDB Transaction, its target DirectorySubspace, and the
// per-transaction userVersionSeq counter for incomplete versionstamps.
// Create a new Tx inside each db.Transact / TransactContext callback via NewTx(tr, dir).
type Tx struct {
	tr             Transaction
	dir            directory.DirectorySubspace
	userVersionSeq atomic.Uint32
}

// NewTx creates a per-transaction context binding tr and dir with a fresh userVersionSeq counter.
func NewTx(tr Transaction, dir directory.DirectorySubspace) *Tx {
	return &Tx{tr: tr, dir: dir}
}

// Transaction returns the underlying FoundationDB Transaction.
func (tx *Tx) Transaction() Transaction {
	return tx.tr
}

// Dir returns the DirectorySubspace associated with this transaction context.
func (tx *Tx) Dir() directory.DirectorySubspace {
	return tx.dir
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

// GenericRepository is a generic data access interface for entity T with primary key PK.
type GenericRepository[T any, PK any] interface {
	Create(ctx context.Context, tx *Tx, entity T) error
	Get(ctx context.Context, tx *Tx, pk PK) (T, error)
	Set(ctx context.Context, tx *Tx, entity T) error
	Delete(ctx context.Context, tx *Tx, pk PK) error
	BatchGet(ctx context.Context, tx *Tx, pks []PK) ([]T, error)
	List(ctx context.Context, tx *Tx, opts PaginationOptions) (*PaginatedResult[T], error)
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


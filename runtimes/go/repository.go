package fdblayer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// ErrAlreadyExists is returned by Insert when an entity with the same primary key already exists.
var ErrAlreadyExists = errors.New("already exists")

// ErrNotFound is returned by ReadRow when no entity exists for the given primary key.
var ErrNotFound = errors.New("not found")

// ErrUnknownIndex is returned by ScanIndex when the given IndexID is not defined on the message.
var ErrUnknownIndex = errors.New("unknown index")

// ErrInvalidKey is returned when a Key or KeyRange has an invalid tuple length or element type.
var ErrInvalidKey = errors.New("invalid key")

// Partition namespaces under the typeID prefix.
const (
	DataNamespace  int64 = 0
	IndexNamespace int64 = 1
)

// Key represents a single or composite primary/index key tuple.
type Key = tuple.Tuple

// IndexID uniquely identifies a secondary index defined in protobuf annotations.
type IndexID int64

// KeyRange specifies the key bounds for Scan and ScanIndex.
type KeyRange struct {
	Prefix Key // Exact match or leading tuple prefix
	Begin  Key // Inclusive lower bound (for range queries)
	End    Key // Exclusive upper bound (for range queries)
}

// AllKeys returns a KeyRange that matches all keys in the entity or index subspace.
func AllKeys() KeyRange {
	return KeyRange{}
}

// Prefix returns a KeyRange that matches all keys starting with the given tuple prefix.
func Prefix(parts ...tuple.TupleElement) KeyRange {
	return KeyRange{Prefix: Key(parts)}
}

// Range returns a KeyRange scanning keys in [begin, end).
func Range(begin, end Key) KeyRange {
	return KeyRange{Begin: begin, End: end}
}

// WriteConfig holds optional configuration for versionstamped writes.
type WriteConfig struct {
	UserVersion uint16
}

// WriteOption configures an Insert, Upsert, or Enqueue call.
type WriteOption func(*WriteConfig)

// WithUserVersion sets the 16-bit transaction-local user version (default 0)
// when writing multiple versionstamped records in the same transaction.
func WithUserVersion(v uint16) WriteOption {
	return func(c *WriteConfig) {
		c.UserVersion = v
	}
}

// ApplyWriteOptions resolves a slice of WriteOption functions into a WriteConfig.
func ApplyWriteOptions(opts []WriteOption) WriteConfig {
	var cfg WriteConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// ScanConfig holds resolved pagination and direction settings for Scan and ScanIndex.
type ScanConfig struct {
	Limit   int
	Reverse bool
	Cursor  Key // Resume token from Page.NextCursor
}

// ScanOption configures pagination and scan direction for Scan and ScanIndex.
type ScanOption func(*ScanConfig)

// WithLimit caps the number of records returned in a single page.
func WithLimit(limit int) ScanOption {
	return func(c *ScanConfig) {
		c.Limit = limit
	}
}

// WithReverse scans keys in descending order instead of ascending order.
func WithReverse() ScanOption {
	return func(c *ScanConfig) {
		c.Reverse = true
	}
}

// WithCursor resumes a forward or reverse scan from a Page.NextCursor token.
func WithCursor(cursor Key) ScanOption {
	return func(c *ScanConfig) {
		c.Cursor = cursor
	}
}

// ApplyScanOptions resolves a slice of ScanOption functions into a ScanConfig.
func ApplyScanOptions(opts []ScanOption) ScanConfig {
	var cfg ScanConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// Page represents a single page of results from Scan or ScanIndex.
type Page[T any] struct {
	Items      []T
	NextCursor Key
	HasMore    bool
}

// Transaction is a mockable interface that abstracts fdb.Transaction.
type Transaction interface {
	fdb.ReadTransaction
	Set(key fdb.KeyConvertible, value []byte)
	Clear(key fdb.KeyConvertible)
	SetVersionstampedKey(key fdb.KeyConvertible, value []byte)
}

// Repository is the uniform data access interface implemented by all generated entity repositories.
type Repository[T any] interface {
	Insert(ctx context.Context, tr Transaction, entity T, opts ...WriteOption) error
	Upsert(ctx context.Context, tr Transaction, entity T, opts ...WriteOption) error
	Delete(ctx context.Context, tr Transaction, key Key) error

	ReadRow(ctx context.Context, tr Transaction, key Key) (T, error)
	ReadRows(ctx context.Context, tr Transaction, keys []Key) ([]T, error)

	Scan(ctx context.Context, tr Transaction, kr KeyRange, opts ...ScanOption) (*Page[T], error)
	ScanIndex(ctx context.Context, tr Transaction, index IndexID, kr KeyRange, opts ...ScanOption) (*Page[T], error)
}

// QueueRepository is the uniform interface implemented by all generated FIFO queue repositories (is_queue = true).
type QueueRepository[T any] interface {
	Enqueue(ctx context.Context, tr Transaction, msg T, opts ...WriteOption) error
	Dequeue(ctx context.Context, tr Transaction, partitionKey Key) (T, error)
	DequeueBatch(ctx context.Context, tr Transaction, partitionKey Key, limit int) ([]T, error)
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

var incompleteTxVersion = [10]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

func normalizeAndValidateElement(el tuple.TupleElement) (tuple.TupleElement, error) {
	norm := NormalizeTupleElement(el)
	if norm == nil {
		return nil, nil
	}
	switch v := norm.(type) {
	case string, []byte, int64, uint64, bool, float32, float64, tuple.UUID, *big.Int, big.Int, fdb.KeyConvertible:
		return v, nil
	case tuple.Versionstamp:
		if v.TransactionVersion == incompleteTxVersion {
			return nil, fmt.Errorf("%w: incomplete versionstamp cannot be used in a query key", ErrInvalidKey)
		}
		return v, nil
	case tuple.Tuple:
		out := make(tuple.Tuple, len(v))
		for i, sub := range v {
			subNorm, err := normalizeAndValidateElement(sub)
			if err != nil {
				return nil, err
			}
			out[i] = subNorm
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: unsupported key element type %T", ErrInvalidKey, el)
	}
}

// FieldKind describes the expected tuple element type for a schema key field.
type FieldKind uint8

const (
	FieldKindInt64 FieldKind = iota + 1
	FieldKindUint64
	FieldKindFloat32
	FieldKindFloat64
	FieldKindBool
	FieldKindString
	FieldKindBytes
	FieldKindVersionstamp
)

func normalizeElementWithKind(el tuple.TupleElement, kind FieldKind) (tuple.TupleElement, error) {
	switch kind {
	case FieldKindInt64:
		switch v := el.(type) {
		case int:
			return int64(v), nil
		case int8:
			return int64(v), nil
		case int16:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		default:
			if el != nil {
				rv := reflect.ValueOf(el)
				if rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Int64 {
					return rv.Int(), nil
				}
			}
			return nil, fmt.Errorf("%w: expected signed integer or enum, got %T", ErrInvalidKey, el)
		}
	case FieldKindUint64:
		switch v := el.(type) {
		case uint:
			return uint64(v), nil
		case uint8:
			return uint64(v), nil
		case uint16:
			return uint64(v), nil
		case uint32:
			return uint64(v), nil
		case uint64:
			return v, nil
		case int:
			if v >= 0 {
				return uint64(v), nil
			}
		case int64:
			if v >= 0 {
				return uint64(v), nil
			}
		default:
			if el != nil {
				rv := reflect.ValueOf(el)
				if rv.Kind() >= reflect.Uint && rv.Kind() <= reflect.Uint64 {
					return rv.Uint(), nil
				}
			}
		}
		return nil, fmt.Errorf("%w: expected unsigned integer, got %T(%v)", ErrInvalidKey, el, el)
	case FieldKindFloat32:
		if v, ok := el.(float32); ok {
			return v, nil
		}
		return nil, fmt.Errorf("%w: expected float32, got %T", ErrInvalidKey, el)
	case FieldKindFloat64:
		switch v := el.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		default:
			return nil, fmt.Errorf("%w: expected float64, got %T", ErrInvalidKey, el)
		}
	case FieldKindBool:
		if v, ok := el.(bool); ok {
			return v, nil
		}
		return nil, fmt.Errorf("%w: expected bool, got %T", ErrInvalidKey, el)
	case FieldKindString:
		if v, ok := el.(string); ok {
			return v, nil
		}
		return nil, fmt.Errorf("%w: expected string, got %T", ErrInvalidKey, el)
	case FieldKindBytes:
		if v, ok := el.([]byte); ok {
			return v, nil
		}
		return nil, fmt.Errorf("%w: expected []byte, got %T", ErrInvalidKey, el)
	case FieldKindVersionstamp:
		return PackVersionstampElement(el)
	default:
		return normalizeAndValidateElement(el)
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

// PackVersionstampElement converts a 12-byte []byte or tuple.Versionstamp into a complete tuple.Versionstamp.
func PackVersionstampElement(el tuple.TupleElement) (tuple.Versionstamp, error) {
	switch v := el.(type) {
	case tuple.Versionstamp:
		if v.TransactionVersion == incompleteTxVersion {
			return tuple.Versionstamp{}, fmt.Errorf("%w: incomplete versionstamp cannot be used in a query key", ErrInvalidKey)
		}
		return v, nil
	case []byte:
		if len(v) != 12 {
			return tuple.Versionstamp{}, fmt.Errorf("%w: invalid versionstamp length %d: must be 12 bytes", ErrInvalidKey, len(v))
		}
		vs := BytesToVersionstamp(v)
		if vs.TransactionVersion == incompleteTxVersion {
			return tuple.Versionstamp{}, fmt.Errorf("%w: incomplete versionstamp bytes cannot be used in a query key", ErrInvalidKey)
		}
		return vs, nil
	default:
		return tuple.Versionstamp{}, fmt.Errorf("%w: invalid versionstamp element type %T: expected 12-byte []byte", ErrInvalidKey, el)
	}
}

// PackPrimaryKey validates that key matches schema in length and element types, normalizes
// each element, and returns both the normalized PK tuple and the packed FDB data key.
func PackPrimaryKey(
	dir directory.DirectorySubspace,
	typeID int64,
	key Key,
	schema []FieldKind,
) (tuple.Tuple, fdb.Key, error) {
	pkLen := len(schema)
	if len(key) != pkLen {
		return nil, nil, fmt.Errorf("%w: expected %d primary key elements, got %d", ErrInvalidKey, pkLen, len(key))
	}
	fullTpl := make(tuple.Tuple, 2+pkLen)
	fullTpl[0] = typeID
	fullTpl[1] = DataNamespace
	for j, el := range key {
		norm, err := normalizeElementWithKind(el, schema[j])
		if err != nil {
			return nil, nil, err
		}
		fullTpl[2+j] = norm
	}
	return fullTpl[2:], dir.Pack(fullTpl), nil
}

// PackSubspaceTuple packs baseTpl followed by suffix into dir, validating and normalizing
// each element of suffix against schema.
func PackSubspaceTuple(
	dir directory.DirectorySubspace,
	baseTpl tuple.Tuple,
	suffix Key,
	schema []FieldKind,
) (fdb.Key, error) {
	if len(suffix) > len(schema) {
		return nil, fmt.Errorf("%w: key length %d exceeds maximum length %d", ErrInvalidKey, len(suffix), len(schema))
	}
	tpl := make(tuple.Tuple, len(baseTpl)+len(suffix))
	copy(tpl, baseTpl)
	for j, el := range suffix {
		norm, err := normalizeElementWithKind(el, schema[j])
		if err != nil {
			return nil, err
		}
		tpl[len(baseTpl)+j] = norm
	}
	return dir.Pack(tpl), nil
}

// ResolveScanBounds computes the [Begin, End) FDB key range and ScanConfig for Scan or ScanIndex.
// baseTpl is {typeID, DataNamespace} for Scan, or {typeID, IndexNamespace, int64(indexID)} for ScanIndex.
// schema is the FieldKind slice for the full key (PrimaryKeyFields for Scan, or IndexFields+PrimaryKeyFields for ScanIndex).
func ResolveScanBounds(
	dir directory.DirectorySubspace,
	baseTpl tuple.Tuple,
	kr KeyRange,
	opts []ScanOption,
	schema []FieldKind,
) (fdb.Key, fdb.Key, ScanConfig, error) {
	cfg := ApplyScanOptions(opts)
	fullKeyLen := len(schema)
	if len(kr.Prefix) > 0 && (len(kr.Begin) > 0 || len(kr.End) > 0) {
		return nil, nil, cfg, fmt.Errorf("%w: KeyRange cannot specify both Prefix and Begin/End", ErrInvalidKey)
	}
	if len(kr.Prefix) > fullKeyLen {
		return nil, nil, cfg, fmt.Errorf("%w: prefix length %d exceeds maximum key length %d", ErrInvalidKey, len(kr.Prefix), fullKeyLen)
	}
	if len(kr.Begin) > fullKeyLen {
		return nil, nil, cfg, fmt.Errorf("%w: range begin length %d exceeds maximum key length %d", ErrInvalidKey, len(kr.Begin), fullKeyLen)
	}
	if len(kr.End) > fullKeyLen {
		return nil, nil, cfg, fmt.Errorf("%w: range end length %d exceeds maximum key length %d", ErrInvalidKey, len(kr.End), fullKeyLen)
	}
	if len(cfg.Cursor) > fullKeyLen {
		return nil, nil, cfg, fmt.Errorf("%w: cursor length %d exceeds maximum key length %d", ErrInvalidKey, len(cfg.Cursor), fullKeyLen)
	}

	baseBytes := dir.Pack(baseTpl)
	rangeBegin := fdb.Key(append(append([]byte(nil), baseBytes...), 0x00))
	rangeEnd := fdb.Key(append(append([]byte(nil), baseBytes...), 0xFF))

	if len(kr.Prefix) > 0 {
		prefixKey, err := PackSubspaceTuple(dir, baseTpl, kr.Prefix, schema)
		if err != nil {
			return nil, nil, cfg, err
		}
		rangeBegin = prefixKey
		rangeEnd = fdb.Key(append(append([]byte(nil), prefixKey...), 0xFF))
	} else {
		if len(kr.Begin) > 0 {
			beginKey, err := PackSubspaceTuple(dir, baseTpl, kr.Begin, schema)
			if err != nil {
				return nil, nil, cfg, err
			}
			if string(beginKey) > string(rangeBegin) {
				rangeBegin = beginKey
			}
		}
		if len(kr.End) > 0 {
			endKey, err := PackSubspaceTuple(dir, baseTpl, kr.End, schema)
			if err != nil {
				return nil, nil, cfg, err
			}
			if string(endKey) < string(rangeEnd) {
				rangeEnd = endKey
			}
		}
	}

	if len(cfg.Cursor) > 0 {
		cursorKey, err := PackSubspaceTuple(dir, baseTpl, cfg.Cursor, schema)
		if err != nil {
			return nil, nil, cfg, err
		}
		if cfg.Reverse {
			sep := byte(0xFF)
			if len(cfg.Cursor) >= fullKeyLen {
				sep = 0x00
			}
			upperEnd := fdb.Key(append(append([]byte(nil), cursorKey...), sep))
			if string(upperEnd) < string(rangeEnd) {
				rangeEnd = upperEnd
			}
		} else if string(cursorKey) > string(rangeBegin) {
			rangeBegin = cursorKey
		}
	}

	return rangeBegin, rangeEnd, cfg, nil
}

// ExtractCursorKey slices tpl[startIdx:] and converts any tuple.Versionstamp
// elements to their 12-byte []byte representation for consistent cursor handling.
func ExtractCursorKey(tpl tuple.Tuple, startIdx int) Key {
	if len(tpl) <= startIdx {
		return nil
	}
	out := make(Key, len(tpl)-startIdx)
	for i, el := range tpl[startIdx:] {
		if vs, ok := el.(tuple.Versionstamp); ok {
			out[i] = vs.Bytes()
		} else {
			out[i] = el
		}
	}
	return out
}

package fdblayer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// ErrAlreadyExists is returned by Create when an entity with the same primary key already exists.
var ErrAlreadyExists = errors.New("already exists")

// ErrNotFound is returned by Get when no entity exists for the given primary key.
var ErrNotFound = errors.New("not found")

// ErrUserVersionOverflow is returned when a single transaction exceeds 65,536 versionstamped operations
// or would wrap the 16-bit userVersion counter from 65535 to 0 within the same transaction.
var ErrUserVersionOverflow = errors.New("user version overflow in transaction")

// Partition namespaces under the typeID prefix.
const (
	DataNamespace  int64 = 0
	IndexNamespace int64 = 1
	FieldNamespace int64 = 2
)

// Transaction is a mockable interface that abstracts fdb.Transaction.
type Transaction interface {
	fdb.ReadTransaction
	Set(key fdb.KeyConvertible, value []byte)
	Clear(key fdb.KeyConvertible)
	Add(key fdb.KeyConvertible, param []byte)
	Max(key fdb.KeyConvertible, param []byte)
	Min(key fdb.KeyConvertible, param []byte)
	SetVersionstampedKey(key fdb.KeyConvertible, value []byte)
}

// GenericRepository is a generic data access interface for entity T with primary key PK.
type GenericRepository[T any, PK any] interface {
	Create(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, entity T) error
	Get(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, pk PK) (T, error)
	Set(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, entity T) error
	Delete(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, pk PK) error
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

// RecordStore holds metadata mapping between message names and their integer type IDs.
type RecordStore struct {
	mu                sync.RWMutex
	metadata          map[string]int64
	metadataCommitted bool

	userVersionSeq atomic.Uint32
	uvMu              sync.Mutex
	txCounts          map[any]uint32
	txOrder           []any
	pendingUV         []uint16
}

// NewRecordStore creates a new RecordStore instance.
func NewRecordStore() *RecordStore {
	return &RecordStore{}
}

// ResetUserVersion resets the user version sequence and per-transaction counters to 0.
func (s *RecordStore) ResetUserVersion() {
	s.uvMu.Lock()
	defer s.uvMu.Unlock()
	s.userVersionSeq.Store(0)
	s.txCounts = nil
	s.txOrder = nil
	s.pendingUV = nil
}

// PrepareUserVersion ensures that allocating the next 16-bit userVersion for tr
// will not wrap around from 65535 (0xFFFF) to 0 (0x0000) within the same transaction.
// Each transaction gets its own independent [0..65535] sequence so concurrent or
// sequential transactions never interfere with each other.
func (s *RecordStore) PrepareUserVersion(tr Transaction) error {
	s.uvMu.Lock()
	defer s.uvMu.Unlock()

	isComparable := tr != nil && reflect.TypeOf(tr).Comparable()
	if !isComparable {
		if s.userVersionSeq.Load() >= 65536 {
			return ErrUserVersionOverflow
		}
		return nil
	}
	if s.txCounts == nil {
		s.txCounts = make(map[any]uint32)
	}
	count, exists := s.txCounts[tr]
	if count >= 65536 {
		return ErrUserVersionOverflow
	}
	if !exists {
		if len(s.txOrder) >= 1024 {
			oldest := s.txOrder[0]
			s.txOrder = s.txOrder[1:]
			delete(s.txCounts, oldest)
		}
		s.txOrder = append(s.txOrder, tr)
	}
	s.txCounts[tr] = count + 1
	s.pendingUV = append(s.pendingUV, uint16(count))
	return nil
}

// NextUserVersion returns a monotonically increasing 16-bit user version so
// that multiple versionstamped keys written within the same transaction receive
// distinct user versions instead of colliding at 0.
func (s *RecordStore) NextUserVersion() uint16 {
	s.uvMu.Lock()
	if len(s.pendingUV) > 0 {
		uv := s.pendingUV[0]
		s.pendingUV = s.pendingUV[1:]
		s.uvMu.Unlock()
		return uv
	}
	s.uvMu.Unlock()
	return uint16(s.userVersionSeq.Add(1) - 1)
}

// GetTypeID retrieves the type ID for a given message name.
func (s *RecordStore) GetTypeID(name string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.metadata == nil {
		return 0, fmt.Errorf("metadata not initialized (type %s not found in metadata), call SyncMetadata first", name)
	}
	typeID, ok := s.metadata[name]
	if !ok {
		return 0, fmt.Errorf("type %s not found in metadata", name)
	}
	return typeID, nil
}

// Metadata returns a read-only copy of the metadata mapping.
func (s *RecordStore) Metadata() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]int64, len(s.metadata))
	for k, v := range s.metadata {
		out[k] = v
	}
	return out
}

// HasAllMetadata returns true if all given message names have been committed and are present in memory.
func (s *RecordStore) HasAllMetadata(messages []string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.metadataCommitted || s.metadata == nil {
		return false
	}
	for _, msg := range messages {
		if _, ok := s.metadata[msg]; !ok {
			return false
		}
	}
	return true
}

// EnsureMetadata synchronizes metadata in its own committed FDB transaction and only
// updates the in-memory metadata cache after the transaction successfully commits.
// If all requested messages have already been committed in this RecordStore, it returns
// immediately under a read lock without touching FoundationDB.
func (s *RecordStore) EnsureMetadata(ctx context.Context, db fdb.Database, metaDir directory.DirectorySubspace, messages []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	if s.metadataCommitted && s.metadata != nil {
		allPresent := true
		for _, msg := range messages {
			if _, ok := s.metadata[msg]; !ok {
				allPresent = false
				break
			}
		}
		if allPresent {
			s.mu.RUnlock()
			return nil
		}
	}
	s.mu.RUnlock()

	res, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		return s.computeAndWriteMetadata(ctx, tr, metaDir, messages)
	})
	if err != nil {
		return err
	}
	committedMeta := res.(map[string]int64)

	s.mu.Lock()
	s.metadata = committedMeta
	s.metadataCommitted = true
	s.mu.Unlock()
	return nil
}

func (s *RecordStore) computeAndWriteMetadata(ctx context.Context, tr Transaction, metaDir directory.DirectorySubspace, messages []string) (map[string]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	newMeta := make(map[string]int64, len(messages))
	kvs := GetRange(tr, metaDir, fdb.RangeOptions{}).GetSliceOrPanic()

	maxID := int64(0)
	for _, kv := range kvs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tpl, err := metaDir.Unpack(kv.Key)
		if err != nil || len(tpl) != 1 {
			continue
		}
		msgName, ok := tpl[0].(string)
		if !ok {
			continue
		}
		valTpl, err := tuple.Unpack(kv.Value)
		if err != nil || len(valTpl) != 1 {
			continue
		}
		id, ok := valTpl[0].(int64)
		if !ok {
			continue
		}
		newMeta[msgName] = id
		if id > maxID {
			maxID = id
		}
	}

	for _, msg := range messages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, exists := newMeta[msg]; exists {
			continue
		}
		key := metaDir.Pack(tuple.Tuple{msg})
		if raw := tr.Get(key).MustGet(); raw != nil {
			if valTpl, err := tuple.Unpack(raw); err == nil && len(valTpl) == 1 {
				if id, ok := valTpl[0].(int64); ok {
					newMeta[msg] = id
					if id > maxID {
						maxID = id
					}
					continue
				}
			}
		}
		maxID++
		newMeta[msg] = maxID
		val := tuple.Tuple{int64(maxID)}.Pack()
		tr.Set(key, val)
	}
	return newMeta, nil
}

// SyncMetadata reads the existing metadata from FDB and assigns new IDs to any unmapped messages.
func (s *RecordStore) SyncMetadata(ctx context.Context, tr Transaction, metaDir directory.DirectorySubspace, messages []string) error {
	newMeta, err := s.computeAndWriteMetadata(ctx, tr, metaDir, messages)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.metadata = newMeta
	s.metadataCommitted = false
	s.mu.Unlock()
	return nil
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


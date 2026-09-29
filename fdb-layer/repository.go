package fdblayer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Partition namespaces under the typeID prefix.
const (
	DataNamespace  int64 = 0
	IndexNamespace int64 = 1
	FieldNamespace int64 = 2
)

// Transaction is a mockable interface that abstracts fdb.Transaction
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
	mu             sync.RWMutex
	metadata       map[string]int64
	userVersionSeq atomic.Uint32
}

// NewRecordStore creates a new RecordStore instance.
func NewRecordStore() *RecordStore {
	return &RecordStore{
		metadata: make(map[string]int64),
	}
}

// NextUserVersion returns a monotonically increasing 16-bit user version so
// that multiple versionstamped keys written within the same transaction receive
// distinct user versions instead of colliding at 0.
func (s *RecordStore) NextUserVersion() uint16 {
	return uint16(s.userVersionSeq.Add(1) - 1)
}

// GetTypeID retrieves the type ID for a given message name.
func (s *RecordStore) GetTypeID(name string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.metadata == nil {
		return 0, fmt.Errorf("metadata not initialized, call SyncMetadata first")
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

	copy := make(map[string]int64, len(s.metadata))
	for k, v := range s.metadata {
		copy[k] = v
	}
	return copy
}

// SyncMetadata reads the existing metadata from FDB and assigns new IDs to any unmapped messages.
func (s *RecordStore) SyncMetadata(ctx context.Context, tr Transaction, metaDir directory.DirectorySubspace, messages []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	s.metadata = make(map[string]int64, len(messages))
	kvs := tr.GetRange(metaDir, fdb.RangeOptions{}).GetSliceOrPanic()

	maxID := int64(0)
	for _, kv := range kvs {
		if err := ctx.Err(); err != nil {
			return err
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
		s.metadata[msgName] = id
		if id > maxID {
			maxID = id
		}
	}

	for _, msg := range messages {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, exists := s.metadata[msg]; exists {
			continue
		}
		key := metaDir.Pack(tuple.Tuple{msg})
		if raw := tr.Get(key).MustGet(); raw != nil {
			if valTpl, err := tuple.Unpack(raw); err == nil && len(valTpl) == 1 {
				if id, ok := valTpl[0].(int64); ok {
					s.metadata[msg] = id
					if id > maxID {
						maxID = id
					}
					continue
				}
			}
		}
		maxID++
		s.metadata[msg] = maxID
		val := tuple.Tuple{int64(maxID)}.Pack()
		tr.Set(key, val)
	}
	return nil
}

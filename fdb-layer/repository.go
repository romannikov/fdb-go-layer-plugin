package fdblayer

import (
	"context"
	"fmt"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Transaction is a mockable interface that abstracts fdb.Transaction
type Transaction interface {
	fdb.ReadTransaction
	Set(key fdb.KeyConvertible, value []byte)
	Clear(key fdb.KeyConvertible)
	Add(key fdb.KeyConvertible, param []byte)
	Max(key fdb.KeyConvertible, param []byte)
	Min(key fdb.KeyConvertible, param []byte)
}

// GenericRepository is a generic data access interface for entity T with primary key PK.
type GenericRepository[T any, PK any] interface {
	Create(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, entity T) error
	Get(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, pk PK) (T, error)
	Set(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, entity T) error
	Delete(ctx context.Context, tr Transaction, dir directory.DirectorySubspace, pk PK) error
}

// RecordStore holds metadata mapping between message names and their integer type IDs.
type RecordStore struct {
	mu       sync.RWMutex
	metadata map[string]int64
}

// NewRecordStore creates a new RecordStore instance.
func NewRecordStore() *RecordStore {
	return &RecordStore{
		metadata: make(map[string]int64),
	}
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
	kvs := tr.GetRange(metaDir, fdb.RangeOptions{}).GetSliceOrPanic()

	maxID := int64(0)
	for _, kv := range kvs {
		if err := ctx.Err(); err != nil {
			return err
		}
		tpl, err := metaDir.Unpack(kv.Key)
		if err != nil {
			return err
		}
		valTpl, err := tuple.Unpack(kv.Value)
		if err != nil {
			return err
		}
		msgName := tpl[0].(string)
		id := valTpl[0].(int64)
		s.metadata[msgName] = id
		if id > maxID {
			maxID = id
		}
	}

	for _, msg := range messages {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, exists := s.metadata[msg]; !exists {
			maxID++
			s.metadata[msg] = maxID
			key := metaDir.Pack(tuple.Tuple{msg})
			val := tuple.Tuple{int64(maxID)}.Pack()
			tr.Set(key, val)
		}
	}
	return nil
}

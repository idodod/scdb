package core

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sjy-dv/scdb/scdb/core/sysid"
	bytebufferpool "github.com/sjy-dv/scdb/scdb/storage/byte_buffer_pool"
)

type Batch struct {
	db            *DB
	pendingWrites []*LogRecord
	options       BatchOptions
	mu            sync.RWMutex
	iscommit      bool
	isrollback    bool
	batchId       *sysid.Node
	buffers       []*bytebufferpool.ByteBuffer
}

func (db *DB) NewBatch(opts BatchOptions) *Batch {
	batch := &Batch{
		db:         db,
		options:    opts,
		iscommit:   false,
		isrollback: false,
	}
	if !opts.ReadOnly {
		node, err := sysid.NewNode(
			rand.New(
				rand.NewSource(time.Now().UnixNano())).Int63n(4) + 1)
		if err != nil {
			panic(fmt.Sprintf("sysid.NewNode failed: %v", err))
		}
		batch.batchId = node
	}
	batch.lock()
	return batch
}

func newBatch() interface{} {
	node, err := sysid.NewNode(rand.New(
		rand.NewSource(time.Now().UnixNano())).Int63n(4) + 1)
	if err != nil {
		panic(fmt.Sprintf("sysid.NewNode failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchId: node,
	}
}

func newRecord() interface{} {
	return &LogRecord{}
}

func (b *Batch) init(rdonly, sync bool, db *DB) *Batch {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.db = db
	b.lock()
	return b
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = b.pendingWrites[:0]
	b.iscommit = false
	b.isrollback = false
	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	b.buffers = b.buffers[:0]
}

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

func (b *Batch) Save(key []byte, value []byte) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}
	if b.db.closed {
		return errors.New("db is closing")
	}
	if b.options.ReadOnly {
		return errors.New("batch is readonly")
	}

	b.mu.Lock()
	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	if record == nil {
		record = b.db.recordPool.Get().(*LogRecord)
		b.pendingWrites = append(b.pendingWrites, record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, 0
	b.mu.Unlock()

	return nil
}

func (b *Batch) SaveWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}
	if b.db.closed {
		return errors.New("db is closing")
	}
	if b.options.ReadOnly {
		return errors.New("batch is readonly")
	}

	b.mu.Lock()
	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	if record == nil {
		record = b.db.recordPool.Get().(*LogRecord)
		b.pendingWrites = append(b.pendingWrites, record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, time.Now().Add(ttl).UnixNano()
	b.mu.Unlock()

	return nil
}

func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("empty key")
	}
	if b.db.closed {
		return nil, errors.New("db is closed")
	}

	now := time.Now().UnixNano()
	b.mu.RLock()
	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	b.mu.RUnlock()

	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			return nil, errors.New("key not found in database")
		}
		return record.Value, nil
	}

	chunkPosition := b.db.registry.Get(key)
	if chunkPosition == nil {
		return nil, errors.New("key not found in database")
	}
	chunk, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}

	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	if record.IsExpired(now) {
		b.db.registry.Del(record.Key)
		return nil, errors.New("key not found in database")
	}
	return record.Value, nil
}

func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("key is empty")
	}
	if b.db.closed {
		return errors.New("db is closed")
	}
	if b.options.ReadOnly {
		return errors.New("batch is read-only")
	}

	b.mu.Lock()
	var exist bool
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			b.pendingWrites[i].Type = LogRecordDeleted
			b.pendingWrites[i].Value = nil
			b.pendingWrites[i].Expire = 0
			exist = true
			break
		}
	}
	if !exist {
		b.pendingWrites = append(b.pendingWrites, &LogRecord{
			Key:  key,
			Type: LogRecordDeleted,
		})
	}
	b.mu.Unlock()

	return nil
}

func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}

	now := time.Now().UnixNano()
	b.mu.RLock()
	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	b.mu.RUnlock()

	if record != nil {
		return record.Type != LogRecordDeleted && !record.IsExpired(now), nil
	}

	position := b.db.registry.Get(key)
	if position == nil {
		return false, nil
	}

	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return false, err
	}

	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.registry.Del(record.Key)
		return false, nil
	}
	return true, nil
}

func (b *Batch) Expire(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}

	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = time.Now().Add(ttl).UnixNano()
	} else {
		position := b.db.registry.Get(key)
		if position == nil {
			return ErrKeyNotFound
		}
		chunk, err := b.db.dataFiles.Read(position)
		if err != nil {
			return err
		}

		now := time.Now()
		record = decodeLogRecord(chunk)
		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			b.db.registry.Del(key)
			return ErrKeyNotFound
		}
		record.Expire = now.Add(ttl).UnixNano()
		b.pendingWrites = append(b.pendingWrites, record)
	}

	return nil
}

func (b *Batch) TTL(key []byte) (time.Duration, error) {
	if len(key) == 0 {
		return -1, ErrKeyIsEmpty
	}
	if b.db.closed {
		return -1, ErrDBClosed
	}

	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.pendingWrites) > 0 {
		var record *LogRecord
		for i := len(b.pendingWrites) - 1; i >= 0; i-- {
			if bytes.Equal(key, b.pendingWrites[i].Key) {
				record = b.pendingWrites[i]
				break
			}
		}
		if record != nil {
			if record.Expire == 0 {
				return -1, nil
			}
			if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
				return -1, ErrKeyNotFound
			}
			return time.Duration(record.Expire - now.UnixNano()), nil
		}
	}

	position := b.db.registry.Get(key)
	if position == nil {
		return -1, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return -1, err
	}

	record := decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		return -1, ErrKeyNotFound
	}
	if record.IsExpired(now.UnixNano()) {
		b.db.registry.Del(key)
		return -1, ErrKeyNotFound
	}

	if record.Expire > 0 {
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	return -1, nil
}

func (b *Batch) Persist(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}

	if record != nil {
		if record.Type == LogRecordDeleted && record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = 0
	} else {
		position := b.db.registry.Get(key)
		if position == nil {
			return ErrKeyNotFound
		}
		chunk, err := b.db.dataFiles.Read(position)
		if err != nil {
			return err
		}

		record := decodeLogRecord(chunk)
		now := time.Now().UnixNano()
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			b.db.registry.Del(record.Key)
			return ErrKeyNotFound
		}
		if record.Expire == 0 {
			return nil
		}

		record.Expire = 0
		b.pendingWrites = append(b.pendingWrites, record)
	}

	return nil
}

func (b *Batch) Commit() error {
	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.iscommit {
		return ErrBatchCommitted
	}
	if b.isrollback {
		return ErrBatchRollbacked
	}

	batchId := b.batchId.Generate()
	now := time.Now().UnixNano()
	for _, record := range b.pendingWrites {
		buf := bytebufferpool.Get()
		b.buffers = append(b.buffers, buf)
		record.BatchId = uint64(batchId)
		encRecord := encodeLogRecord(record, b.db.encodeHeader, buf)
		b.db.dataFiles.PendingWrites(encRecord)
	}

	buf := bytebufferpool.Get()
	b.buffers = append(b.buffers, buf)
	endRecord := encodeLogRecord(&LogRecord{
		Key:  batchId.Bytes(),
		Type: LogRecordBatchFinished,
	}, b.db.encodeHeader, buf)
	b.db.dataFiles.PendingWrites(endRecord)

	chunkPositions, err := b.db.dataFiles.WriteAll()
	if err != nil {
		b.db.dataFiles.ClearPendingWrites()
		return err
	}
	if len(chunkPositions) != len(b.pendingWrites)+1 {
		panic("chunk positions length is not equal to pending writes length")
	}

	if b.options.Sync && !b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}

	for i, record := range b.pendingWrites {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			b.db.registry.Del(record.Key)
		} else {
			b.db.registry.Save(record.Key, chunkPositions[i])
		}

		if b.db.options.WatchQueueSize > 0 {
			e := &Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
			if record.Type == LogRecordDeleted {
				e.Action = ObserveActionDelete
			} else {
				e.Action = ObserveActionPut
			}
			b.db.observer.putEvent(e)
		}
		b.db.recordPool.Put(record)
	}

	b.iscommit = true
	return nil
}

func (b *Batch) Rollback() error {
	defer b.unlock()

	if b.db.closed {
		return ErrDBClosed
	}

	if b.iscommit {
		return ErrBatchCommitted
	}
	if b.isrollback {
		return ErrBatchRollbacked
	}

	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}

	if !b.options.ReadOnly {
		for _, record := range b.pendingWrites {
			b.db.recordPool.Put(record)
		}
		b.pendingWrites = b.pendingWrites[:0]
	}

	b.isrollback = true
	return nil
}

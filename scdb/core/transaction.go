package core

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sjy-dv/scdb/scdb/core/sysid"
	bytebufferpool "github.com/sjy-dv/scdb/scdb/storage/byte_buffer_pool"
)

type Transaction struct {
	db            *DB
	pendingWrites []*LogRecord
	options       TxOptions
	mu            sync.RWMutex
	iscommit      bool
	isrollback    bool
	txId          *sysid.Node
	buffers       []*bytebufferpool.ByteBuffer
}

func (db *DB) NewTransaction(opts TxOptions) *Transaction {
	tx := &Transaction{
		db:         db,
		options:    opts,
		iscommit:   false,
		isrollback: false,
	}
	if !opts.ReadOnly {
		node, err := sysid.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("sysid.NewNode failed: %v", err))
		}
		tx.txId = node
	}
	tx.lock()
	return tx
}

func newTransaction() interface{} {
	node, err := sysid.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("sysid.NewNode failed: %v", err))
	}
	return &Transaction{
		options: DefaultTxOptions,
		txId:    node,
	}
}

func newRecord() interface{} {
	return &LogRecord{}
}

func (tx *Transaction) init(rdonly, sync bool, db *DB) *Transaction {
	tx.options.ReadOnly = rdonly
	tx.options.Sync = sync
	tx.db = db
	tx.lock()
	return tx
}

func (tx *Transaction) reset() {
	tx.db = nil
	tx.pendingWrites = tx.pendingWrites[:0]
	tx.iscommit = false
	tx.isrollback = false
	for _, buf := range tx.buffers {
		bytebufferpool.Put(buf)
	}
	tx.buffers = tx.buffers[:0]
}

func (tx *Transaction) lock() {
	if tx.options.ReadOnly {
		tx.db.mu.RLock()
	} else {
		tx.db.mu.Lock()
	}
}

func (tx *Transaction) unlock() {
	if tx.options.ReadOnly {
		tx.db.mu.RUnlock()
	} else {
		tx.db.mu.Unlock()
	}
}

func (tx *Transaction) Save(key []byte, value []byte) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}
	if tx.db.closed {
		return errors.New("db is closing")
	}
	if tx.options.ReadOnly {
		return errors.New("Transaction is readonly")
	}

	tx.mu.Lock()
	var record *LogRecord
	for i := len(tx.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, tx.pendingWrites[i].Key) {
			record = tx.pendingWrites[i]
			break
		}
	}
	if record == nil {
		record = tx.db.recordPool.Get().(*LogRecord)
		tx.pendingWrites = append(tx.pendingWrites, record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, 0
	tx.mu.Unlock()

	return nil
}

func (tx *Transaction) SaveWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}
	if tx.db.closed {
		return errors.New("db is closing")
	}
	if tx.options.ReadOnly {
		return errors.New("Transaction is readonly")
	}

	tx.mu.Lock()
	var record *LogRecord
	for i := len(tx.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, tx.pendingWrites[i].Key) {
			record = tx.pendingWrites[i]
			break
		}
	}
	if record == nil {
		record = tx.db.recordPool.Get().(*LogRecord)
		tx.pendingWrites = append(tx.pendingWrites, record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, time.Now().Add(ttl).UnixNano()
	tx.mu.Unlock()

	return nil
}

func (tx *Transaction) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("empty key")
	}
	if tx.db.closed {
		return nil, errors.New("db is closed")
	}

	now := time.Now().UnixNano()
	tx.mu.RLock()
	var record *LogRecord
	for i := len(tx.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, tx.pendingWrites[i].Key) {
			record = tx.pendingWrites[i]
			break
		}
	}
	tx.mu.RUnlock()

	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			return nil, errors.New("key not found in database")
		}
		return record.Value, nil
	}

	chunkPosition := tx.db.registry.Get(key)
	if chunkPosition == nil {
		return nil, errors.New("key not found in database")
	}
	chunk, err := tx.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}

	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	if record.IsExpired(now) {
		tx.db.registry.Del(record.Key)
		return nil, errors.New("key not found in database")
	}
	return record.Value, nil
}

func (tx *Transaction) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("key is empty")
	}
	if tx.db.closed {
		return errors.New("db is closed")
	}
	if tx.options.ReadOnly {
		return errors.New("Transaction is read-only")
	}

	tx.mu.Lock()
	var exist bool
	for i := len(tx.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, tx.pendingWrites[i].Key) {
			tx.pendingWrites[i].Type = LogRecordDeleted
			tx.pendingWrites[i].Value = nil
			tx.pendingWrites[i].Expire = 0
			exist = true
			break
		}
	}
	if !exist {
		tx.pendingWrites = append(tx.pendingWrites, &LogRecord{
			Key:  key,
			Type: LogRecordDeleted,
		})
	}
	tx.mu.Unlock()

	return nil
}

func (tx *Transaction) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if tx.db.closed {
		return false, ErrDBClosed
	}

	now := time.Now().UnixNano()
	tx.mu.RLock()
	var record *LogRecord
	for i := len(tx.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, tx.pendingWrites[i].Key) {
			record = tx.pendingWrites[i]
			break
		}
	}
	tx.mu.RUnlock()

	if record != nil {
		return record.Type != LogRecordDeleted && !record.IsExpired(now), nil
	}

	position := tx.db.registry.Get(key)
	if position == nil {
		return false, nil
	}

	chunk, err := tx.db.dataFiles.Read(position)
	if err != nil {
		return false, err
	}

	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		tx.db.registry.Del(record.Key)
		return false, nil
	}
	return true, nil
}

func (tx *Transaction) Expire(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if tx.db.closed {
		return ErrDBClosed
	}
	if tx.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	var record *LogRecord
	for i := len(tx.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, tx.pendingWrites[i].Key) {
			record = tx.pendingWrites[i]
			break
		}
	}

	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = time.Now().Add(ttl).UnixNano()
	} else {
		position := tx.db.registry.Get(key)
		if position == nil {
			return ErrKeyNotFound
		}
		chunk, err := tx.db.dataFiles.Read(position)
		if err != nil {
			return err
		}

		now := time.Now()
		record = decodeLogRecord(chunk)
		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			tx.db.registry.Del(key)
			return ErrKeyNotFound
		}
		record.Expire = now.Add(ttl).UnixNano()
		tx.pendingWrites = append(tx.pendingWrites, record)
	}

	return nil
}

func (tx *Transaction) TTL(key []byte) (time.Duration, error) {
	if len(key) == 0 {
		return -1, ErrKeyIsEmpty
	}
	if tx.db.closed {
		return -1, ErrDBClosed
	}

	now := time.Now()
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if len(tx.pendingWrites) > 0 {
		var record *LogRecord
		for i := len(tx.pendingWrites) - 1; i >= 0; i-- {
			if bytes.Equal(key, tx.pendingWrites[i].Key) {
				record = tx.pendingWrites[i]
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

	position := tx.db.registry.Get(key)
	if position == nil {
		return -1, ErrKeyNotFound
	}
	chunk, err := tx.db.dataFiles.Read(position)
	if err != nil {
		return -1, err
	}

	record := decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		return -1, ErrKeyNotFound
	}
	if record.IsExpired(now.UnixNano()) {
		tx.db.registry.Del(key)
		return -1, ErrKeyNotFound
	}

	if record.Expire > 0 {
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	return -1, nil
}

func (tx *Transaction) Persist(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if tx.db.closed {
		return ErrDBClosed
	}
	if tx.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	var record *LogRecord
	for i := len(tx.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, tx.pendingWrites[i].Key) {
			record = tx.pendingWrites[i]
			break
		}
	}

	if record != nil {
		if record.Type == LogRecordDeleted && record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = 0
	} else {
		position := tx.db.registry.Get(key)
		if position == nil {
			return ErrKeyNotFound
		}
		chunk, err := tx.db.dataFiles.Read(position)
		if err != nil {
			return err
		}

		record := decodeLogRecord(chunk)
		now := time.Now().UnixNano()
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			tx.db.registry.Del(record.Key)
			return ErrKeyNotFound
		}
		if record.Expire == 0 {
			return nil
		}

		record.Expire = 0
		tx.pendingWrites = append(tx.pendingWrites, record)
	}

	return nil
}

func (tx *Transaction) Commit() error {
	defer tx.unlock()
	if tx.db.closed {
		return ErrDBClosed
	}

	if tx.options.ReadOnly || len(tx.pendingWrites) == 0 {
		return nil
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.iscommit {
		return ErrTransactionCommitted
	}
	if tx.isrollback {
		return ErrTransactionRollbacked
	}

	TransactionId := tx.txId.Generate()
	now := time.Now().UnixNano()
	for _, record := range tx.pendingWrites {
		buf := bytebufferpool.Get()
		tx.buffers = append(tx.buffers, buf)
		record.TxId = uint64(TransactionId)
		encRecord := encodeLogRecord(record, tx.db.encodeHeader, buf)
		tx.db.dataFiles.PendingWrites(encRecord)
	}

	buf := bytebufferpool.Get()
	tx.buffers = append(tx.buffers, buf)
	endRecord := encodeLogRecord(&LogRecord{
		Key:  TransactionId.Bytes(),
		Type: LogRecordTransactionFinished,
	}, tx.db.encodeHeader, buf)
	tx.db.dataFiles.PendingWrites(endRecord)

	chunkPositions, err := tx.db.dataFiles.WriteAll()
	if err != nil {
		tx.db.dataFiles.ClearPendingWrites()
		return err
	}
	if len(chunkPositions) != len(tx.pendingWrites)+1 {
		panic("chunk positions length is not equal to pending writes length")
	}

	if tx.options.Sync && !tx.db.options.Sync {
		if err := tx.db.dataFiles.Sync(); err != nil {
			return err
		}
	}

	for i, record := range tx.pendingWrites {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			tx.db.registry.Del(record.Key)
		} else {
			tx.db.registry.Save(record.Key, chunkPositions[i])
		}

		if tx.db.options.WatchQueueSize > 0 {
			e := &Event{Key: record.Key, Value: record.Value, TxId: record.TxId}
			if record.Type == LogRecordDeleted {
				e.Action = ObserveActionDelete
			} else {
				e.Action = ObserveActionPut
			}
			tx.db.observer.putEvent(e)
		}
		tx.db.recordPool.Put(record)
	}

	tx.iscommit = true
	return nil
}

func (b *Transaction) Rollback() error {
	defer func() {
		if !b.iscommit {
			b.unlock()
		}
	}()

	if b.db.closed {
		return ErrDBClosed
	}

	if b.iscommit {
		return nil
	}
	if b.isrollback {
		return nil
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

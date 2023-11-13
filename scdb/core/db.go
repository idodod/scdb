package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/gofrs/flock"

	"github.com/sjy-dv/scdb/scdb/core/sysid"
	"github.com/sjy-dv/scdb/scdb/registry"
	"github.com/sjy-dv/scdb/scdb/storage"
)

const (
	fileLockName       = "FLOCK"
	dataFileNameSuffix = ".SEG"
	hintFileNameSuffix = ".HINT"
	mergeFinNameSuffix = ".MERGEFIN"
)

type DB struct {
	dataFiles        *storage.WAL
	hintFile         *storage.WAL
	registry         registry.Accessor
	options          Options
	fileLock         *flock.Flock
	mu               sync.RWMutex
	closed           bool
	mergeRunning     uint32
	transactionPool  sync.Pool
	recordPool       sync.Pool
	encodeHeader     []byte
	observingCh      chan *Event
	observer         *Observer
	expiredCursorKey []byte
}

type Stat struct {
	KeysNum  int
	DiskSize int64
}

func Open(opt Options) (*DB, error) {
	if err := checkOptions(opt); err != nil {
		return nil, err
	}
	if _, err := os.Stat(opt.DirPath); err != nil {
		if err := os.MkdirAll(opt.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	fileLock := flock.New(filepath.Join(opt.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, errors.New("directory is already held")
	}

	if err := loadMergeFiles(opt.DirPath); err != nil {
		return nil, err
	}

	db := &DB{
		registry:        registry.NewAccessor(),
		options:         opt,
		fileLock:        fileLock,
		transactionPool: sync.Pool{New: newTransaction},
		recordPool:      sync.Pool{New: newRecord},
		encodeHeader:    make([]byte, maxLogRecordHeaderSize),
	}

	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return nil, err
	}
	if err = db.loadIndex(); err != nil {
		return nil, err
	}
	if opt.WatchQueueSize > 0 {
		db.observingCh = make(chan *Event)
		db.observer = NewObserver(opt.WatchQueueSize)
		go db.observer.sendEvent(db.observingCh)
	}
	return db, nil
}

func (db *DB) openWalFiles() (*storage.WAL, error) {
	walFiles, err := storage.Open(storage.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    db.options.SegmentSize,
		SegmentFileExt: dataFileNameSuffix,
		BlockCache:     db.options.BlockCache,
		Sync:           db.options.Sync,
		BytesPerSync:   db.options.BytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	return walFiles, nil
}

func (db *DB) loadIndex() error {
	if err := db.loadIndexFromHintFile(); err != nil {
		return err
	}
	if err := db.loadIndexFromWAL(); err != nil {
		return err
	}
	return nil
}

func checkOptions(opt Options) error {
	if opt.DirPath == "" {
		return errors.New("scdb dir path is empty")
	}
	if opt.SegmentSize <= 0 {
		return errors.New("scdb file size must be larger than 0")
	}
	return nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.closeFiles(); err != nil {
		return err
	}

	// release file lock
	if err := db.fileLock.Unlock(); err != nil {
		return err
	}

	// close watch channel
	if db.options.WatchQueueSize > 0 {
		close(db.observingCh)
	}

	db.closed = true
	return nil
}

func (db *DB) closeFiles() error {
	// close wal
	if err := db.dataFiles.Close(); err != nil {
		return err
	}
	// close hint file if exists
	if db.hintFile != nil {
		if err := db.hintFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.dataFiles.Sync()
}

func (db *DB) Stat() *Stat {
	db.mu.Lock()
	defer db.mu.Unlock()

	diskSize, err := func(dirPath string) (int64, error) {
		var size int64
		err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				size += info.Size()
			}
			return nil
		})
		return size, err
	}(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("scdb: get database directory size error: %v", err))
	}

	return &Stat{
		KeysNum:  db.registry.Size(),
		DiskSize: diskSize,
	}
}

func (db *DB) Save(key []byte, value []byte) error {
	tx := db.transactionPool.Get().(*Transaction)
	defer func() {
		tx.reset()
		db.transactionPool.Put(tx)
	}()
	tx.init(false, false, db)
	if err := tx.Save(key, value); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) SaveWithTTL(key []byte, value []byte, ttl time.Duration) error {
	tx := db.transactionPool.Get().(*Transaction)
	defer func() {
		tx.reset()
		db.transactionPool.Put(tx)
	}()
	tx.init(false, false, db)
	if err := tx.SaveWithTTL(key, value, ttl); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	tx := db.transactionPool.Get().(*Transaction)
	tx.init(true, false, db)
	defer func() {
		_ = tx.Commit()
		tx.reset()
		db.transactionPool.Put(tx)
	}()
	return tx.Get(key)
}

func (db *DB) Delete(key []byte) error {
	tx := db.transactionPool.Get().(*Transaction)
	defer func() {
		tx.reset()
		db.transactionPool.Put(tx)
	}()
	tx.init(false, false, db)
	if err := tx.Delete(key); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) Exist(key []byte) (bool, error) {
	tx := db.transactionPool.Get().(*Transaction)
	tx.init(true, false, db)
	defer func() {
		_ = tx.Commit()
		tx.reset()
		db.transactionPool.Put(tx)
	}()
	return tx.Exist(key)
}

func (db *DB) Expire(key []byte, ttl time.Duration) error {
	tx := db.transactionPool.Get().(*Transaction)
	defer func() {
		tx.reset()
		db.transactionPool.Put(tx)
	}()
	tx.init(false, false, db)
	if err := tx.Expire(key, ttl); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) TTL(key []byte) (time.Duration, error) {
	tx := db.transactionPool.Get().(*Transaction)
	tx.init(true, false, db)
	defer func() {
		_ = tx.Commit()
		tx.reset()
		db.transactionPool.Put(tx)
	}()
	return tx.TTL(key)
}

func (db *DB) Persist(key []byte) error {
	tx := db.transactionPool.Get().(*Transaction)
	defer func() {
		tx.reset()
		db.transactionPool.Put(tx)
	}()
	tx.init(false, false, db)
	if err := tx.Persist(key); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) Observe() (chan *Event, error) {
	if db.options.WatchQueueSize <= 0 {
		return nil, ErrWatchDisabled
	}
	return db.observingCh, nil
}

func (db *DB) Ascend(callback func(k []byte, v []byte) (bool, error)) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.registry.Ascend(func(key []byte, pos *storage.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		if val := db.checkValue(chunk); val != nil {
			return callback(key, val)
		}
		return true, nil
	})
}

func (db *DB) AscendRange(startKey, endKey []byte, callback func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.registry.AscendRange(startKey, endKey, func(key []byte, pos *storage.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if val := db.checkValue(chunk); val != nil {
			return callback(key, val)
		}
		return true, nil
	})
}

func (db *DB) AscendGreaterOrEqual(key []byte, callback func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.registry.AscendGreaterOrEqual(key, func(key []byte, pos *storage.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if val := db.checkValue(chunk); val != nil {
			return callback(key, val)
		}
		return true, nil
	})
}

func (db *DB) AscendKeys(pattern []byte, filterExpr bool, callback func(key []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		reg = regexp.MustCompile(string(pattern))
	}
	db.registry.Ascend(func(key []byte, pos *storage.ChunkPosition) (bool, error) {
		if reg == nil || reg.Match(key) {
			var invalid bool
			if filterExpr {
				chunk, err := db.dataFiles.Read(pos)
				if err != nil {
					return false, err
				}
				if value := db.checkValue(chunk); value == nil {
					invalid = true
				}
			}
			if invalid {
				return true, nil
			}
			return callback(key)
		}
		return true, nil
	})
}

func (db *DB) Descend(callback func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.registry.Descend(func(key []byte, pos *storage.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return callback(key, value)
		}
		return true, nil
	})
}

func (db *DB) DescendRange(startKey, endKey []byte, callback func(key []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	db.registry.DescendRange(startKey, endKey, func(key []byte, pos *storage.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return callback(key, value)
		}
		return true, nil
	})
}

func (db *DB) DescendLessOrEqual(key []byte, callback func(key []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	db.registry.DescendLessOrEqual(key, func(key []byte, pos *storage.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return callback(key, value)
		}
		return true, nil
	})
}

func (db *DB) DescendKeys(pattern []byte, filterExpr bool, callback func(key []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var reg *regexp.Regexp
	if len(pattern) > 0 {
		reg = regexp.MustCompile(string(pattern))
	}

	db.registry.Descend(func(key []byte, pos *storage.ChunkPosition) (bool, error) {
		if reg == nil || reg.Match(key) {
			var invalid bool
			if filterExpr {
				chunk, err := db.dataFiles.Read(pos)
				if err != nil {
					return false, err
				}
				if value := db.checkValue(chunk); value == nil {
					invalid = true
				}
			}
			if invalid {
				return true, nil
			}
			return callback(key)
		}
		return true, nil
	})
}

func (db *DB) checkValue(chunk []byte) []byte {
	record := decodeLogRecord(chunk)
	now := time.Now().UnixNano()
	if record.Type != LogRecordDeleted && !record.IsExpired(now) {
		return record.Value
	}
	return nil
}

func (db *DB) loadIndexFromWAL() error {
	mergeFinSegmentId, err := getMergeFinSegmentId(db.options.DirPath)
	if err != nil {
		return err
	}
	indexRecords := make(map[uint64][]*IndexRecord)
	now := time.Now().UnixNano()
	// get a reader for WAL
	reader := db.dataFiles.NewReader()
	for {
		if reader.CurrentSegmentId() <= mergeFinSegmentId {
			reader.SkipCurrentSegment()
			continue
		}

		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// decode and get log record
		record := decodeLogRecord(chunk)

		if record.Type == LogRecordTransactionFinished {
			txId, err := sysid.ParseBytes(record.Key)
			if err != nil {
				return err
			}
			for _, idxRecord := range indexRecords[uint64(txId)] {
				if idxRecord.recordType == LogRecordNormal {
					db.registry.Save(idxRecord.key, idxRecord.position)
				}
				if idxRecord.recordType == LogRecordDeleted {
					db.registry.Del(idxRecord.key)
				}
			}
			delete(indexRecords, uint64(txId))
		} else if record.Type == LogRecordNormal && record.TxId == mergeFinishedTxID {
			db.registry.Save(record.Key, position)
		} else {
			// expired records should not be indexed
			if record.IsExpired(now) {
				db.registry.Del(record.Key)
				continue
			}
			// put the record into the temporary indexRecords
			indexRecords[record.TxId] = append(indexRecords[record.TxId],
				&IndexRecord{
					key:        record.Key,
					recordType: record.Type,
					position:   position,
				})
		}
	}
	return nil
}

func (db *DB) DeleteExpiredKeys(timeout time.Duration) error {
	// set timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan struct{}, 1)

	var innerErr error
	now := time.Now().UnixNano()
	go func(ctx context.Context) {
		db.mu.Lock()
		defer db.mu.Unlock()
		for {
			// select 100 keys from the db.index
			positions := make([]*storage.ChunkPosition, 0, 100)
			db.registry.AscendGreaterOrEqual(db.expiredCursorKey, func(k []byte, pos *storage.ChunkPosition) (bool, error) {
				positions = append(positions, pos)
				if len(positions) >= 100 {
					return false, nil
				}
				return true, nil
			})

			// If keys in the db.index has been traversed, len(positions) will be 0.
			if len(positions) == 0 {
				db.expiredCursorKey = nil
				done <- struct{}{}
				return
			}

			// delete from index if the key is expired.
			for _, pos := range positions {
				chunk, err := db.dataFiles.Read(pos)
				if err != nil {
					innerErr = err
					done <- struct{}{}
					return
				}
				record := decodeLogRecord(chunk)
				if record.IsExpired(now) {
					db.registry.Del(record.Key)
				}
				db.expiredCursorKey = record.Key
			}
		}
	}(ctx)

	select {
	case <-ctx.Done():
		return innerErr
	case <-done:
		return nil
	}
}

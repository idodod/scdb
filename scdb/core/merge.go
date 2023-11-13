package core

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/sjy-dv/scdb/scdb/registry"
	"github.com/sjy-dv/scdb/scdb/storage"
	bytebufferpool "github.com/sjy-dv/scdb/scdb/storage/byte_buffer_pool"
)

const (
	mergeDirSuffixName = "-merge"
	mergeFinishedTxID  = 0
)

func (db *DB) Merge(reopenAfterDone bool) error {
	if err := db.doMerge(); err != nil {
		return err
	}
	if !reopenAfterDone {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// close current files
	_ = db.closeFiles()

	// replace original file
	err := loadMergeFiles(db.options.DirPath)
	if err != nil {
		return err
	}

	// open data files
	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return err
	}

	// discard the old index first.
	db.registry = registry.NewAccessor()
	// rebuild index
	if err = db.loadIndex(); err != nil {
		return err
	}

	return nil
}

func (db *DB) doMerge() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDBClosed
	}
	if db.dataFiles.IsNull() {
		db.mu.Unlock()
		return nil
	}
	if atomic.LoadUint32(&db.mergeRunning) == 1 {
		db.mu.Unlock()
		return ErrMergeRunning
	}
	atomic.StoreUint32(&db.mergeRunning, 1)
	defer atomic.StoreUint32(&db.mergeRunning, 0)

	prevActiveSegId := db.dataFiles.GetActiveID()
	if err := db.dataFiles.BootingActiveSegment(); err != nil {
		db.mu.Unlock()
		return err
	}

	db.mu.Unlock()

	mergeDB, err := db.openMergeDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeDB.Close()
	}()

	buf := bytebufferpool.Get()
	now := time.Now().UnixNano()
	defer bytebufferpool.Put(buf)

	reader := db.dataFiles.NewReaderWithMax(prevActiveSegId)
	for {
		buf.Reset()
		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := decodeLogRecord(chunk)
		if record.Type == LogRecordNormal && (record.Expire == 0 || record.Expire > now) {
			db.mu.RLock()
			indexPos := db.registry.Get(record.Key)
			db.mu.RUnlock()
			if indexPos != nil && positionEquals(indexPos, position) {
				record.TxId = mergeFinishedTxID
				newPosition, err := mergeDB.dataFiles.Write(encodeLogRecord(record, mergeDB.encodeHeader, buf))
				if err != nil {
					return err
				}
				_, err = mergeDB.hintFile.Write(encodeHintRecord(record.Key, newPosition))
				if err != nil {
					return err
				}
			}
		}
	}

	mergeFinFile, err := mergeDB.openMergeFinishedFile()
	if err != nil {
		return err
	}
	_, err = mergeFinFile.Write(encodeMergeFinRecord(prevActiveSegId))
	if err != nil {
		return err
	}
	if err := mergeFinFile.Close(); err != nil {
		return err
	}

	return nil
}

func (db *DB) openMergeDB() (*DB, error) {
	mergePath := mergeDirPath(db.options.DirPath)
	if err := os.RemoveAll(mergePath); err != nil {
		return nil, err
	}
	options := db.options
	options.Sync, options.BytesPerSync = false, 0
	options.DirPath = mergePath
	mergeDB, err := Open(options)
	if err != nil {
		return nil, err
	}

	hintFile, err := storage.Open(storage.Options{
		DirPath:        options.DirPath,
		SegmentSize:    math.MaxInt64,
		SegmentFileExt: hintFileNameSuffix,
		Sync:           false,
		BytesPerSync:   0,
		BlockCache:     0,
	})
	if err != nil {
		return nil, err
	}
	mergeDB.hintFile = hintFile
	return mergeDB, nil
}

func mergeDirPath(dirPath string) string {
	dir := filepath.Dir(filepath.Clean(dirPath))
	base := filepath.Base(dirPath)
	return filepath.Join(dir, base+mergeDirSuffixName)
}

func (db *DB) openMergeFinishedFile() (*storage.WAL, error) {
	return storage.Open(storage.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    GB,
		SegmentFileExt: mergeFinNameSuffix,
		Sync:           false,
		BytesPerSync:   0,
		BlockCache:     0,
	})
}

func positionEquals(a, b *storage.ChunkPosition) bool {
	return a.SegmentId == b.SegmentId &&
		a.BlockNumber == b.BlockNumber &&
		a.ChunkOffset == b.ChunkOffset
}

func encodeHintRecord(key []byte, pos *storage.ChunkPosition) []byte {
	// SegmentId BlockNumber ChunkOffset ChunkSize
	//    5          5           10          5      =    25
	// see binary.MaxVarintLen64 and binary.MaxVarintLen32
	buf := make([]byte, 25)
	var idx = 0

	// SegmentId
	idx += binary.PutUvarint(buf[idx:], uint64(pos.SegmentId))
	// BlockNumber
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockNumber))
	// ChunkOffset
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkOffset))
	// ChunkSize
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkSize))

	// key
	result := make([]byte, idx+len(key))
	copy(result, buf[:idx])
	copy(result[idx:], key)
	return result
}

func decodeHintRecord(buf []byte) ([]byte, *storage.ChunkPosition) {
	var idx = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[idx:])
	idx += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[idx:])
	idx += n
	// Key
	key := buf[idx:]

	return key, &storage.ChunkPosition{
		SegmentId:   uint32(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}

func encodeMergeFinRecord(segmentId uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segmentId)
	return buf
}

func loadMergeFiles(dirPath string) error {
	mergeDirPath := mergeDirPath(dirPath)
	if _, err := os.Stat(mergeDirPath); err != nil {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergeDirPath)
	}()

	copyFile := func(suffix string, fileId uint32, force bool) {
		srcFile := storage.GetSegmentSource(mergeDirPath, suffix, fileId)
		stat, err := os.Stat(srcFile)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			panic(fmt.Sprintf("loadMergeFiles: failed to get src file stat %v", err))
		}
		if !force && stat.Size() == 0 {
			return
		}
		destFile := storage.GetSegmentSource(dirPath, suffix, fileId)
		_ = os.Rename(srcFile, destFile)
	}

	mergeFinSegmentId, err := getMergeFinSegmentId(mergeDirPath)
	if err != nil {
		return err
	}
	for fileId := uint32(1); fileId <= mergeFinSegmentId; fileId++ {
		destFile := storage.GetSegmentSource(dirPath, dataFileNameSuffix, fileId)

		if _, err = os.Stat(destFile); err == nil {
			if err = os.Remove(destFile); err != nil {
				return err
			}
		}
		copyFile(dataFileNameSuffix, fileId, false)
	}

	copyFile(mergeFinNameSuffix, 1, true)
	copyFile(hintFileNameSuffix, 1, true)

	return nil
}

func getMergeFinSegmentId(mergePath string) (uint32, error) {
	mergeFinFile, err := os.Open(storage.GetSegmentSource(mergePath, mergeFinNameSuffix, 1))
	if err != nil {
		return 0, nil
	}
	defer func() {
		_ = mergeFinFile.Close()
	}()

	mergeFinBuf := make([]byte, 4)
	if _, err := mergeFinFile.ReadAt(mergeFinBuf, 7); err != nil {
		return 0, err
	}
	mergeFinSegmentId := binary.LittleEndian.Uint32(mergeFinBuf)
	return mergeFinSegmentId, nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFile, err := storage.Open(storage.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    math.MaxInt64,
		SegmentFileExt: hintFileNameSuffix,
		BlockCache:     32 * KB * 10,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = hintFile.Close()
	}()

	reader := hintFile.NewReader()
	for {
		chunk, _, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		key, position := decodeHintRecord(chunk)
		db.registry.Save(key, position)
	}
	return nil
}

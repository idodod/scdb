package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	genesisFileID = 1
)

/*
한국어 설명:
WAL 구조체는 들어오는 쓰기 작업에 대한 내구성과 장애 허용(fault-tolerance)을 제공하는 Write-Ahead Log 구조를 나타냅니다. 이 구조체는 새로운 들어오는 쓰기 작업에 사용되는 현재 세그먼트 파일인 activeSegment와 읽기 작업에 사용되는 이전 세그먼트 파일의 맵인 olderSegments로 구성됩니다.

options: WAL의 다양한 구성 옵션을 저장합니다.
mu: 동시에 WAL 데이터 구조에 접근할 때 사용되는 동기화 매커니즘으로, sync.RWMutex를 사용하여 안전한 접근과 수정을 보장합니다.
blockCache: 디스크 I/O를 줄여 읽기 성능을 향상시키기 위해 최근에 접근한 데이터 블록을 저장하는 LRU(Least Recently Used) 캐시입니다. uint64 타입의 키와 []byte 타입의 값을 가지는 lru.Cache 구조체를 사용하여 구현됩니다.

English Explanation:
The WAL struct represents a Write-Ahead Log structure that provides durability and fault tolerance for incoming write operations. It is composed of an activeSegment, which is the current segment file for new incoming writes, and olderSegments, a map of segment files used for reading operations.

options: Stores various configuration options for the WAL.
mu: A sync.RWMutex used for synchronizing concurrent access to the WAL data structure, ensuring safe access and modification.
blockCache: An LRU (Least Recently Used) cache that stores recently accessed data blocks to improve read performance by reducing disk I/O. It is implemented using an lru.Cache structure with keys of type uint64 and values of type []byte.
*/
type WAL struct {
	activeSegment     *segment            // active segment file, used for new incoming writes.
	olderSegments     map[uint32]*segment // older segment files, only used for read.
	options           Options
	mu                sync.RWMutex
	blockCache        *lru.Cache[uint64, []byte]
	bytesWrite        uint32
	renameIds         []uint32
	pendingWrites     [][]byte
	pendingSize       int64
	pendingWritesLock sync.Mutex
}

/*
한국어 설명:
Reader 구조체는 Write-Ahead Log(WAL)에 대한 리더를 나타냅니다. 이 구조체는 세그먼트 ID에 따라 정렬된 segmentReader 구조체의 슬라이스인 segmentReaders와 현재 슬라이스 내에서 활성화된 segmentReader의 인덱스인 currentReader로 구성됩니다.

segmentReaders: 각각의 segmentReader는 WAL의 특정 세그먼트 파일에서 데이터를 읽는 역할을 합니다. 이 슬라이스는 모든 세그먼트 리더를 포함하고 있으며, 세그먼트 ID 순으로 정렬되어 있습니다.
currentReader: 현재 읽고 있는 세그먼트 리더의 위치를 가리키는 인덱스입니다. 이 필드를 사용하여 segmentReaders 슬라이스를 순회할 수 있습니다.
Reader 구조체는 WAL의 다양한 세그먼트 파일들을 순차적으로 읽기 위해 사용되며, 로그의 데이터를 순차적으로 처리할 수 있도록 도와줍니다.

English Explanation:
The Reader struct represents a reader for the Write-Ahead Log (WAL). It is composed of segmentReaders, which is a slice of segmentReader structures sorted by their segment id, and currentReader, which indicates the index of the active segmentReader within the slice.

segmentReaders: Each segmentReader is responsible for reading data from a specific segment file within the WAL. This slice contains all the segment readers, organized in order of their segment IDs.
currentReader: This is the index that points to the currently active segmentReader in the slice. It is used to iterate over the segmentReaders slice.
The Reader struct is used to sequentially read through the different segment files in the WAL, facilitating the orderly processing of the logged data.
*/
type Reader struct {
	segmentReaders []*segmentReader
	currentReader  int
}

func Open(options Options) (*WAL, error) {
	if !strings.HasPrefix(options.SegmentFileExt, ".") {
		return nil, ErrMustStartWith
	}
	if options.BlockCache > uint32(options.SegmentSize) {
		return nil, ErrLargeBlockCache
	}
	wal := &WAL{
		options:       options,
		olderSegments: make(map[uint32]*segment),
		pendingWrites: make([][]byte, 0),
	}

	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	//create block cache when needed
	if options.BlockCache > 0 {
		lruSize := options.BlockCache / blockSize
		if options.BlockCache%blockSize != 0 {
			lruSize += 1
		}
		cache, err := lru.New[uint64, []byte](int(lruSize))
		if err != nil {
			return nil, err
		}
		wal.blockCache = cache
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	var sIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		_, err := fmt.Sscanf(entry.Name(), "%d"+options.SegmentFileExt, &id)
		if err != nil {
			continue
		}
		sIDs = append(sIDs, id)
	}
	if len(sIDs) == 0 {
		segment, err := loadSegment(options.DirPath, options.SegmentFileExt,
			genesisFileID, wal.blockCache)
		if err != nil {
			return nil, err
		}
		wal.activeSegment = segment
		return wal, nil
	}
	sort.Ints(sIDs)

	for i, segId := range sIDs {
		segment, err := loadSegment(options.DirPath, options.SegmentFileExt,
			uint32(segId), wal.blockCache)
		if err != nil {
			return nil, err
		}
		if i == len(sIDs)-1 {
			wal.activeSegment = segment
		} else {
			wal.olderSegments[segment.id] = segment
		}
	}
	return wal, nil
}

func GetSegmentSource(dir, ext string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%09d"+ext, id))
}

/*
한국어 설명:
BootingActiveSegment 함수는 WAL(Write-Ahead Log)의 현재 활성 세그먼트를 동기화하고 새로운 활성 세그먼트를 준비하는 역할을 합니다. 이 함수는 다음과 같은 작업을 순차적으로 수행합니다:

wal 구조체의 mu 락을 획득하여 동시성 문제를 방지합니다.
현재 활성 세그먼트의 데이터를 디스크에 동기화합니다.
새로운 세그먼트를 로드하기 위해 loadSegment 함수를 호출합니다. 이 때, 새 세그먼트의 ID는 현재 활성 세그먼트의 ID에 1을 더한 값입니다.
새로 로드된 세그먼트를 wal 구조체의 activeSegment로 설정합니다.
기존의 활성 세그먼트를 olderSegments 맵에 추가하여, 이후 읽기 연산에 사용할 수 있도록 합니다.
모든 작업이 성공적으로 완료되면 nil을 반환하여 오류가 없음을 나타냅니다. 만약 동기화하거나 새 세그먼트를 로드하는 과정에서 오류가 발생하면, 해당 오류를 반환합니다.
English Explanation:
The BootingActiveSegment function is responsible for synchronizing the current active segment of the WAL (Write-Ahead Log) and preparing a new active segment. The function performs the following steps in sequence:

It acquires the mu lock of the wal struct to prevent concurrency issues.
It synchronizes the data of the current active segment to disk.
It calls the loadSegment function to load a new segment, with the new segment's ID being one greater than the current active segment's ID.
It sets the newly loaded segment as the activeSegment of the wal struct.
It adds the former active segment to the olderSegments map for later read operations.
If all operations complete successfully, it returns nil to indicate no errors. If an error occurs during synchronization or segment loading, the function returns the error.
*/
func (wal *WAL) BootingActiveSegment() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}
	segment, err := loadSegment(wal.options.DirPath,
		wal.options.SegmentFileExt, wal.activeSegment.id+1, wal.blockCache)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil
}

func (wal *WAL) GetActiveID() uint32 {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return wal.activeSegment.id
}

func (wal *WAL) IsNull() bool {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return len(wal.olderSegments) == 0 && wal.activeSegment.Size() == 0
}

func (wal *WAL) NewReaderWithMax(segId uint32) *Reader {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// get all segment readers.
	var segmentReaders []*segmentReader
	for _, segment := range wal.olderSegments {
		if segId == 0 || segment.id <= segId {
			reader := segment.NewReader()
			segmentReaders = append(segmentReaders, reader)
		}
	}
	if segId == 0 || wal.activeSegment.id <= segId {
		reader := wal.activeSegment.NewReader()
		segmentReaders = append(segmentReaders, reader)
	}

	// sort the segment readers by segment id.
	sort.Slice(segmentReaders, func(i, j int) bool {
		return segmentReaders[i].segment.id < segmentReaders[j].segment.id
	})

	return &Reader{
		segmentReaders: segmentReaders,
		currentReader:  0,
	}
}

func (wal *WAL) NewReaderWithStart(startPos *ChunkPosition) (*Reader, error) {
	if startPos == nil {
		return nil, errors.New("start position is nil")
	}
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	reader := wal.NewReader()
	for {
		// skip the segment readers whose id is less than the given position's segment id.
		if reader.CurrentSegmentId() < startPos.SegmentId {
			reader.SkipCurrentSegment()
			continue
		}
		// skip the chunk whose position is less than the given position.
		currentPos := reader.CurrentChunkPosition()
		if currentPos.BlockNumber >= startPos.BlockNumber &&
			currentPos.ChunkOffset >= startPos.ChunkOffset {
			break
		}
		// call Next to find again.
		if _, _, err := reader.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return reader, nil
}

func (wal *WAL) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
}

func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	if r.currentReader >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}

	data, position, err := r.segmentReaders[r.currentReader].Next()
	if err == io.EOF {
		r.currentReader++
		return r.Next()
	}
	return data, position, err
}

func (r *Reader) SkipCurrentSegment() {
	r.currentReader++
}

func (r *Reader) CurrentSegmentId() uint32 {
	return r.segmentReaders[r.currentReader].segment.id
}

func (r *Reader) CurrentChunkPosition() *ChunkPosition {
	reader := r.segmentReaders[r.currentReader]
	return &ChunkPosition{
		SegmentId:   reader.segment.id,
		BlockNumber: reader.blockNumber,
		ChunkOffset: reader.chunkOffset,
	}
}

func (wal *WAL) ClearPendingWrites() {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()

	wal.pendingSize = 0
	wal.pendingWrites = wal.pendingWrites[:0]
}

func (wal *WAL) PendingWrites(data []byte) {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()

	size := wal.maxDataWriteSize(int64(len(data)))
	wal.pendingSize += size
	wal.pendingWrites = append(wal.pendingWrites, data)
}

func (wal *WAL) rotateActiveSegment() error {
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}
	wal.bytesWrite = 0
	segment, err := loadSegment(wal.options.DirPath, wal.options.SegmentFileExt,
		wal.activeSegment.id+1, wal.blockCache)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil
}

func (wal *WAL) WriteAll() ([]*ChunkPosition, error) {
	if len(wal.pendingWrites) == 0 {
		return make([]*ChunkPosition, 0), nil
	}

	wal.mu.Lock()
	defer func() {
		wal.ClearPendingWrites()
		wal.mu.Unlock()
	}()

	// if the pending size is still larger than segment size, return error
	if wal.pendingSize > wal.options.SegmentSize {
		return nil, ErrPendingSizeTooLarge
	}

	// if the active segment file is full, sync it and create a new one.
	if wal.activeSegment.Size()+wal.pendingSize > wal.options.SegmentSize {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}

	// write all data to the active segment file.
	positions, err := wal.activeSegment.writeAll(wal.pendingWrites)
	if err != nil {
		return nil, err
	}

	return positions, nil
}

func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if int64(len(data))+chunkHeaderSize > wal.options.SegmentSize {
		return nil, ErrValueTooLarge
	}
	// if the active segment file is full, sync it and create a new one.
	if wal.isFull(int64(len(data))) {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}

	// write the data to the active segment file.
	position, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	// update the bytesWrite field.
	wal.bytesWrite += position.ChunkSize

	// sync the active segment file if needed.
	var needSync = wal.options.Sync
	if !needSync && wal.options.BytesPerSync > 0 {
		needSync = wal.bytesWrite >= wal.options.BytesPerSync
	}
	if needSync {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
	}

	return position, nil
}

func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// find the segment file according to the position.
	var segment *segment
	if pos.SegmentId == wal.activeSegment.id {
		segment = wal.activeSegment
	} else {
		segment = wal.olderSegments[pos.SegmentId]
	}

	if segment == nil {
		return nil, fmt.Errorf("segment file %d%s not found", pos.SegmentId, wal.options.SegmentFileExt)
	}

	// read the data from the segment file.
	return segment.Read(pos.BlockNumber, pos.ChunkOffset)
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// purge the block cache.
	if wal.blockCache != nil {
		wal.blockCache.Purge()
	}

	// close all segment files.
	for _, segment := range wal.olderSegments {
		if err := segment.Close(); err != nil {
			return err
		}
		wal.renameIds = append(wal.renameIds, segment.id)
	}
	wal.olderSegments = nil

	wal.renameIds = append(wal.renameIds, wal.activeSegment.id)
	// close the active segment file.
	return wal.activeSegment.Close()
}

func (wal *WAL) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// purge the block cache.
	if wal.blockCache != nil {
		wal.blockCache.Purge()
	}

	// delete all segment files.
	for _, segment := range wal.olderSegments {
		if err := segment.Remove(); err != nil {
			return err
		}
	}
	wal.olderSegments = nil

	// delete the active segment file.
	return wal.activeSegment.Remove()
}

func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

func (wal *WAL) RenameFileExt(ext string) error {
	if !strings.HasPrefix(ext, ".") {
		return fmt.Errorf("segment file extension must start with '.'")
	}
	wal.mu.Lock()
	defer wal.mu.Unlock()

	renameFile := func(id uint32) error {
		oldName := GetSegmentSource(wal.options.DirPath, wal.options.SegmentFileExt, id)
		newName := GetSegmentSource(wal.options.DirPath, ext, id)
		return os.Rename(oldName, newName)
	}

	for _, id := range wal.renameIds {
		if err := renameFile(id); err != nil {
			return err
		}
	}

	wal.options.SegmentFileExt = ext
	return nil
}

func (wal *WAL) isFull(delta int64) bool {
	return wal.activeSegment.Size()+wal.maxDataWriteSize(delta) > wal.options.SegmentSize
}

func (wal *WAL) maxDataWriteSize(size int64) int64 {
	return chunkHeaderSize + size + (size/blockSize+1)*chunkHeaderSize
}

package storage

import (
	"fmt"
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

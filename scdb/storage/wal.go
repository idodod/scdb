package storage

import (
	"errors"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	genesisFileID = 1
)

var (
	ErrValueTooLarge       = errors.New("the data size can't larger than segment size")
	ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")
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

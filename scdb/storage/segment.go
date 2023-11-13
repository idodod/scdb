package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

const (
	// 7 Bytes
	// Checksum Length Type
	//    4      2     1
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32 * KB

	fileModePerm = 0644

	// uin32 + uint32 + int64 + uin32
	// segmentId + BlockNumber + ChunkOffset + ChunkSize
	maxLen = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

/*
----------------------------------------------------------------
-KOR
이 segment 타입은 Write-Ahead Log(WAL)에서 단일 세그먼트 파일을 나타냅니다. WAL 시스템에서 세그먼트 파일은 데이터를 순차적으로 추가하는 데 사용되며, 파일은 블록 단위로 데이터를 기록합니다. 여기서 각각의 블록은 32KB 크기이며, 데이터는 더 작은 단위인 청크로 기록됩니다. segment 구조체의 각 필드는 다음과 같은 목적을 가지고 있습니다:

id: 세그먼트 파일의 고유 식별자입니다.
fd: *os.File은 파일 디스크립터로, 세그먼트 파일에 대한 참조를 가집니다. 이를 통해 파일 읽기 및 쓰기 작업을 할 수 있습니다.
currentBlockNumber: 현재 쓰고 있는 블록의 순번입니다. 각 블록은 32KB 크기를 가지며, 이 숫자는 현재 어떤 블록에 데이터를 추가하고 있는지를 추적합니다.
currentBlockSize: 현재 블록에 쓰여진 데이터의 크기입니다. 이 크기가 32KB에 도달하면 새로운 블록으로 이동해야 합니다.
closed: 세그먼트 파일이 닫혔는지 여부를 나타내는 불리언 값입니다. 파일이 닫혔으면 더 이상 데이터를 쓸 수 없습니다.
cache: 이 *lru.Cache[uint64, []byte]는 최근 사용된 데이터 블록을 캐싱하여 성능을 향상시킵니다. LRU(Least Recently Used) 캐시는 가장 오랫동안 사용되지 않은 항목을 제거함으로써 제한된 메모리 공간을 관리합니다.
header: 세그먼트 파일의 헤더 정보를 담고 있는 바이트 슬라이스입니다. 헤더에는 파일의 메타데이터가 포함될 수 있습니다.
blockPool: 이 sync.Pool은 성능을 향상시키기 위해 블록 데이터의 메모리를 재사용합니다. 블록 데이터를 할당하고 해제하는 오버헤드를 줄여줍니다.
segment 구조체는 파일 시스템 상의 WAL 세그먼트 파일을 효율적으로 관리하고 데이터를 안정적으로 저장하는 데 필요한 상태와 동작을 캡슐화합니다.

----------------------------------------------------------------
-ENG
The segment type represents a single segment file within a Write-Ahead Log (WAL) system. A segment file in WAL is append-only, meaning data is added sequentially, and it is organized in blocks. Here's a breakdown of the structure's fields for clarity:

id: uniquely identifies the segment file. It's used to keep track of different segment files within the WAL system.

fd: This *os.File is a file descriptor, which is a reference to the segment file. It allows the system to perform read and write operations on the file.

currentBlockNumber: This is the number of the block currently being written to. Each block is 32KB in size, and this number helps track which block is currently in use for appending data.

currentBlockSize: This represents the size of the data that has already been written to the current block. Once this size reaches the limit of 32KB, the system must move on to a new block.

closed: A boolean indicating whether the segment file is closed. If it's closed, no further data can be written to it.

cache: This *lru.Cache[uint64, []byte] is used to cache recently accessed data blocks to improve performance. An LRU (Least Recently Used) cache manages a limited memory space by evicting the least recently used items.

header: A byte slice that holds the header information of the segment file. The header can include metadata about the file.

blockPool: This sync.Pool is used to reuse the memory for block data to enhance performance. It reduces the overhead of allocating and deallocating block data.
*/
type segment struct {
	id                 uint32
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
	cache              *lru.Cache[uint64, []byte]
	header             []byte
	blockPool          sync.Pool
}

/*
----------------------------------------------------------------
-KOR
segmentReader 구조체는 세그먼트 파일로부터 모든 데이터를 순회(iterate)하는 데 사용됩니다. Next 메서드를 호출하여 다음 청크 데이터를 가져올 수 있으며, 데이터가 더 이상 없을 때 io.EOF가 반환됩니다. 구조체의 필드는 다음과 같습니다:

segment: 읽고 있는 세그먼트 파일을 가리키는 포인터입니다.
blockNumber: 현재 읽고 있는 블록의 번호입니다.
chunkOffset: 현재 블록 내에서의 청크 데이터의 오프셋(시작 위치)을 나타냅니다.
**이 구조체를 사용하여 파일의 데이터를 순차적으로 읽어 나갈 수 있으며, 각 청크의 시작점을 알 수 있어 효율적인 데이터 처리가 가능합니다.**

----------------------------------------------------------------
-ENG
The segmentReader struct is used to iterate through all the data from a segment file. You can invoke the Next method to retrieve the next chunk of data, and io.EOF will be returned when there is no more data to read. The fields within the struct are:

segment: A pointer to the segment file being read.
blockNumber: The number of the block that is currently being read.
chunkOffset: The offset, or starting position, of the chunk data within the current block.

**This struct allows for the sequential reading of the file's data, enabling efficient processing by knowing the starting point of each chunk.**
*/
type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
}

/*
----------------------------------------------------------
-KOR
blockAndHeader 구조체는 풀(pool)에 저장되는 블록과 청크 헤더를 포함합니다. 이 구조체는 데이터의 블록과 해당 블록의 청크 헤더 정보를 함께 관리하는 데 사용됩니다. 구조체의 필드는 다음과 같습니다:

block: 실제 데이터가 저장되는 바이트 배열입니다.
header: 해당 데이터 블록에 대한 메타데이터를 포함하는 바이트 배열입니다. 예를 들어, 데이터의 체크섬, 길이 또는 타입 등의 정보를 담을 수 있습니다.
이 구조체는 데이터와 그 데이터에 대한 메타정보를 함께 저장하고 관리함으로써, 데이터 처리 시 필요한 추가적인 정보를 빠르게 액세스할 수 있도록 도와줍니다.

--------------------------------------------------------------
-ENG
The blockAndHeader struct holds a block and its corresponding chunk header, which are saved in a pool. This struct is utilized to manage a chunk of data alongside its metadata header. The fields within the struct are:

block: An array of bytes where the actual data is stored.
header: An array of bytes that contains metadata for the data block. This could include information such as checksums, length, or data type.
This struct facilitates the storage and management of data along with its associated metadata, enabling quick access to essential information required for data processing.
*/
type blockAndHeader struct {
	block  []byte
	header []byte
}

/*
한국어 설명:
ChunkPosition 구조체는 세그먼트 파일 내에서 청크의 위치를 나타냅니다. 이 구조체는 세그먼트 파일로부터 데이터를 읽어올 때 사용됩니다. 구조체의 각 필드는 다음과 같은 정보를 담고 있습니다:

SegmentId: 청크가 속한 세그먼트 파일의 고유 식별자입니다.
BlockNumber: 세그먼트 파일 내에서 청크가 위치한 블록 번호입니다.
ChunkOffset: 세그먼트 파일 내에서 청크의 시작 오프셋(위치)을 나타냅니다.
ChunkSize: 세그먼트 파일 내에서 청크 데이터가 차지하는 바이트 수입니다.
이 구조체는 특정 청크의 정확한 위치와 크기 정보를 제공함으로써, 파일 내에서 데이터를 효과적으로 찾아서 읽을 수 있도록 도와줍니다.

English Explanation:
The ChunkPosition struct represents the position of a chunk within a segment file. It is used to read data from the segment file. Each field in the struct provides specific information:

SegmentId: The unique identifier for the segment file to which the chunk belongs.
BlockNumber: The number of the block within the segment file where the chunk is located.
ChunkOffset: The starting offset of the chunk within the segment file.
ChunkSize: The number of bytes that the chunk data occupies within the segment file.
This struct facilitates efficient data retrieval by providing precise location and size information for a given chunk within a file.
*/
type ChunkPosition struct {
	SegmentId uint32
	// BlockNumber The block number of the chunk in the segment file.
	BlockNumber uint32
	// ChunkOffset The start offset of the chunk in the segment file.
	ChunkOffset int64
	// ChunkSize How many bytes the chunk data takes up in the segment file.
	ChunkSize uint32
}

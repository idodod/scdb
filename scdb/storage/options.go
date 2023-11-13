package storage

import "os"

/*
----------------------------------------------------------------
-KOR
이 Options 구조체는 Write-Ahead Log (WAL)의 설정을 위한 구성 옵션을 나타냅니다. WAL은 데이터베이스 시스템에서 데이터를 안전하게 보존하기 위해 사용되는 기술로, 실제 데이터를 디스크에 쓰기 전에 모든 변경 사항을 로그 파일에 먼저 기록합니다.

구조체 내의 각 필드는 다음과 같은 역할을 합니다:

DirPath: WAL 세그먼트 파일이 저장될 디렉토리 경로를 지정합니다. 여기서 세그먼트 파일은 WAL 데이터가 저장되는 개별 파일을 의미합니다.

SegmentSize: 각 세그먼트 파일의 최대 크기를 바이트 단위로 지정합니다. 이 크기에 도달하면 새로운 세그먼트 파일이 생성됩니다.

SegmentFileExt: 세그먼트 파일의 파일 확장자를 지정합니다. 파일 확장자는 점(".")으로 시작해야 하며, 기본값은 ".SEG"입니다. 이는 디렉토리 내에서 세그먼트 파일과 힌트 파일을 식별하는 데 사용됩니다. 대부분의 사용자에게는 일반적이지 않은 사용 방법일 수 있습니다.

BlockCache: 블록 캐시의 크기를 바이트 단위로 지정합니다. 블록 캐시는 최근에 접근한 데이터 블록을 저장하는 데 사용되며, 읽기 성능을 향상시키는 역할을 합니다. 만약 BlockCache가 0으로 설정되면, 블록 캐시는 사용되지 않습니다.

Sync: 쓰기 작업을 OS 버퍼 캐시를 통해 동기화하고 실제 디스크에 기록할지 여부를 결정합니다. Sync를 설정하면 단일 쓰기 작업의 지속성이 보장되지만, 쓰기 작업이 느려질 수 있습니다.

BytesPerSync: fsync를 호출하기 전에 쓸 바이트 수를 지정합니다. 이 값은 쓰기 작업의 성능과 데이터의 지속성 사이의 균형을 맞추는 데 사용됩니다. 더 큰 값은 성능을 향상시킬 수 있지만, 시스템이 충돌할 경우 데이터 손실의 가능성을 높일 수 있습니다.

--------------------------------
-ENG
The Options struct represents the configuration options for a Write-Ahead Log (WAL). A WAL is a technique used in database systems to ensure data persistence by recording all changes to a log file before they are written to the disk.

Each field in the struct serves the following purpose:

DirPath: Specifies the directory path where the WAL segment files will be stored. Segment files are individual files where the WAL data is kept.

SegmentSize: Defines the maximum size for each segment file in bytes. When this size is reached, a new segment file is created.

SegmentFileExt: Sets the file extension for segment files. The file extension must start with a period ".", with the default value being ".SEG". This is used to differentiate between segment files and hint files within the directory. It's not commonly used by most users.

BlockCache: Specifies the size of the block cache in bytes. A block cache is used to store recently accessed data blocks and improves read performance. If BlockCache is set to 0, no block cache will be utilized.

Sync: Determines whether to synchronize writes through the OS buffer cache and onto the actual disk. Enabling Sync is necessary for the durability of a single write operation, but it can also result in slower writes.

BytesPerSync: Indicates the number of bytes to write before calling fsync. This setting balances performance with data persistence. A larger value can improve performance but may increase the risk of data loss in case of a system crash.
*/

type Options struct {
	DirPath        string
	SegmentSize    int64
	SegmentFileExt string
	BlockCache     uint32
	Sync           bool
	BytesPerSync   uint32
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:        os.TempDir(),
	SegmentSize:    GB,
	SegmentFileExt: ".SEG",
	BlockCache:     32 * KB * 10,
	Sync:           false,
	BytesPerSync:   0,
}

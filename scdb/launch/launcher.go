package launch

import (
	"fmt"
	"os"
	"runtime"
	"strconv"

	"github.com/sjy-dv/scdb/scdb/core"
	"github.com/sjy-dv/scdb/scdb/pkg/log"
)

type ScLauncher struct {
	SCDB        *core.DB
	ErrLogCh    chan error
	RunningPort string
}

type dbConfig struct {
	VolumePath      string
	PartitionSize   int64
	BufferCacheSize uint32
	Sync            bool
	SafeAsyncBuffer uint32
}

var scLauncher *ScLauncher

func GetScLauncher() *ScLauncher {
	return scLauncher
}

func LoadEnv() *ScLauncher {
	var err error
	sc := &ScLauncher{}
	sc.setupLogger()
	sc.ErrLogCh = make(chan error)
	config := &dbConfig{}
	if os.Getenv("VOLUME") == "" {
		config.VolumePath = "/tmp/scdb"
	} else {
		config.VolumePath = os.Getenv("VOLUME")
	}
	log.Info(fmt.Sprintf("SCDB mounted %s", config.VolumePath))
	if os.Getenv("PARTITION_SIZE") == "" {
		config.PartitionSize = core.DefaultOptions.SegmentSize
	} else {
		ps, err := strconv.ParseInt(os.Getenv("PARTITION_SIZE"), 10, 64)
		if err != nil {
			log.Info("Failed to Cofigure Partition Size. Set Default Size : 1073741824")
			config.PartitionSize = core.DefaultOptions.SegmentSize
		} else {
			config.PartitionSize = ps
		}
	}
	log.Info(fmt.Sprintf("SCDB Configure Partition Size %d", config.PartitionSize))
	if os.Getenv("BUFFER_CACHE_SIZE") == "" {
		config.BufferCacheSize = core.DefaultOptions.BlockCache
	} else {
		bcs, err := strconv.ParseInt(os.Getenv("BUFFER_CACHE_SIZE"), 10, 64)
		if err != nil {
			log.Info("Failed to Cofigure Buffer Cache Size. Set Default Size 0")
			config.BufferCacheSize = core.DefaultOptions.BlockCache
		} else {
			config.BufferCacheSize = uint32(bcs)
		}
	}
	log.Info(fmt.Sprintf("SCDB Buffer Cache Size %d", config.BufferCacheSize))
	if os.Getenv("sync") == "" {
		config.Sync = false
	} else {
		if os.Getenv("sync") == "1" {
			config.Sync = true
		} else {
			config.Sync = false
		}
	}
	if config.Sync {
		log.Info("SCDB Sync Jobs Starting...")
	} else {
		log.Info("SCDB Jobs Single Starting...")
	}
	if os.Getenv("SAFE_ASYNC_BUFFER") == "" {
		config.SafeAsyncBuffer = core.DefaultOptions.BytesPerSync
	} else {
		sab, err := strconv.ParseInt(os.Getenv("SAFE_ASYNC_BUFFER"), 10, 64)
		if err != nil {
			log.Info("Failed to SAFE ASYNC BUFFER. Set Default Size 0")
			config.BufferCacheSize = core.DefaultOptions.BytesPerSync
		} else {
			config.BufferCacheSize = uint32(sab)
		}
	}
	log.Info(fmt.Sprintf("SCDB SAFE_ASYNC_BUFFER %d", config.BufferCacheSize))
	sc.SCDB, err = core.Open(core.Options{
		DirPath:        config.VolumePath,
		SegmentSize:    config.PartitionSize,
		BlockCache:     config.BufferCacheSize,
		Sync:           config.Sync,
		BytesPerSync:   config.SafeAsyncBuffer,
		WatchQueueSize: 0,
	})
	if err != nil {
		log.Warn(fmt.Sprintf("CRITICAL SystemCrashError: %v", err))
		os.Exit(0)
	}
	if os.Getenv("PORT") == "" {
		sc.RunningPort = ":50051"
	} else {
		sc.RunningPort = fmt.Sprintf(":%s", os.Getenv("PORT"))
	}
	sc.ascii(config.PartitionSize)
	return sc
}

func (sc *ScLauncher) LaunchSolidCoreSystem() {
	log.Info("This System is dependent on ", runtime.Version(), "version.")
	scLauncher = sc
}

func (sc *ScLauncher) setupLogger() {
	log.SetLevel("debug")
}

func (sc *ScLauncher) activeErrorLog() {
	for {
		select {
		case err := <-sc.ErrLogCh:
			if err != nil {
				log.Error(err)
			}
		}
	}
}

func (sc *ScLauncher) ascii(size int64) {
	asciiart := `
		⢀⣀⣀⣀⣤⣤⣤⣤⣀⣀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
	⠀⠀⠀⠀⠀⣠⣴⣶⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⣦⣄⠀⠀⠀⠀⠀
	⠀⠀⠀⠀⠀⠙⠻⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠋⠀⠀⠀⠀⠀
	⠀⠀⠀⠀⠀⣿⣶⣤⣄⣉⣉⠙⠛⠛⠛⠛⠛⠛⠋⣉⣉⣠⣤⣶⣿⠀⠀⠀⠀⠀SCDB (SolidCoreDataBase) v1.0.0
	⠀⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀
	⠀⠀⠀⠀⠀⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠀⠀⠀⠀⠀Running in %s
	⠀⠀⠀⠀⠀⣄⡉⠛⠻⠿⢿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠿⠟⠛⢉⣠⠀⠀⠀⠀⠀BasedStorageEngine: ScBitcask
	⠀⠀⠀⠀⠀⣿⣿⣿⣶⣶⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⣶⣶⣿⣿⣿⠀⠀⠀⠀⠀"ScBitcask" is a database engine written in Golang 
	⠀⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀that follows the Bitcask architecture, which is a key-value storage engine
	⠀⠀⠀⠀⠀⠻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠟⠀⠀⠀⠀⠀
	⠀⠀⠀⠀⠀⣶⣤⣈⡉⠛⠛⠻⠿⠿⠿⠿⠿⠿⠟⠛⠛⢉⣁⣤⣶⠀⠀⠀⠀⠀PartitionDiskSize (%d)
	⠀⠀⠀⠀⠀⣿⣿⣿⣿⣿⣷⣶⣶⣶⣶⣶⣶⣶⣶⣾⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀
	⠀⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀
	⠀⠀⠀⠀⠀⠙⠻⠿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠿⠟⠋⠀⠀⠀⠀⠀
	⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠉⠉⠉⠛⠛⠛⠛⠉⠉⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀`
	fmt.Println(fmt.Sprintf(asciiart, sc.RunningPort, size))
}

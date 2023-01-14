package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/dhstore"
)

var (
	log = logging.Logger("cmd/dhstore")
)

func main() {
	storePath := flag.String("storePath", "./dhstore/store", "The path at which the dhstore data persisted.")
	listenAddr := flag.String("listenAddr", "0.0.0.0:40080", "The dhstore HTTP server listen address.")
	dwal := flag.Bool("disableWAL", false, "Weather to disable WAL in Pebble dhstore.")
	llvl := flag.String("logLevel", "info", "The logging level. Only applied if GOLOG_LOG_LEVEL environment variable is unset.")
	flag.Parse()

	if _, set := os.LookupEnv("GOLOG_LOG_LEVEL"); !set {
		_ = logging.SetLogLevel("*", *llvl)
	}

	// Default options copied from cockroachdb with the addition of 1GiB cache.
	// See:
	// - https://github.com/cockroachdb/cockroach/blob/v22.1.6/pkg/storage/pebble.go#L479
	opts := &pebble.Options{
		BytesPerSync:                10 << 20, // 10 MiB
		WALBytesPerSync:             10 << 20, // 10 MiB
		MaxConcurrentCompactions:    10,
		MemTableSize:                64 << 20, // 64 MiB
		MemTableStopWritesThreshold: 4,
		LBaseMaxBytes:               64 << 20, // 64 MiB
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		DisableWAL:                  *dwal,
		WALMinSyncInterval:          func() time.Duration { return 30 * time.Second },
	}

	opts.Experimental.ReadCompactionRate = 10 << 20 // 20 MiB
	opts.Experimental.MinDeletionRate = 128 << 20   // 128 MiB

	const numLevels = 7
	opts.Levels = make([]pebble.LevelOptions, numLevels)
	for i := 0; i < numLevels; i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KiB
		l.IndexBlockSize = 256 << 10 // 256 KiB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	opts.Levels[numLevels-1].FilterPolicy = nil
	opts.Cache = pebble.NewCache(1 << 30) // 1 GiB

	path := filepath.Clean(*storePath)
	store, err := dhstore.NewPebbleDHStore(path, opts)
	if err != nil {
		panic(err)
	}
	log.Infow("Store opened.", "path", path)

	server, err := dhstore.NewHttpServer(store, *listenAddr)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		panic(err)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Info("Terminating...")
	if err := server.Shutdown(ctx); err != nil {
		log.Warnw("Failure occurred while shutting down server.", "err", err)
	} else {
		log.Info("Shut down server successfully.")
	}
	if err := store.Close(); err != nil {
		log.Warnw("Failure occurred while closing store.", "err", err)
	} else {
		log.Info("Closed store successfully.")
	}
}

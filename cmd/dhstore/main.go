package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/dhstore"
	"github.com/ipni/dhstore/fdb"
	"github.com/ipni/dhstore/metrics"
	dhpebble "github.com/ipni/dhstore/pebble"
	"github.com/ipni/dhstore/server"
)

var (
	log = logging.Logger("cmd/dhstore")
)

func main() {
	storePath := flag.String("storePath", "./dhstore/store", "The path at which the dhstore data persisted.")
	listenAddr := flag.String("listenAddr", "0.0.0.0:40080", "The dhstore HTTP server listen address.")
	metrcisAddr := flag.String("metricsAddr", "0.0.0.0:40081", "The dhstore metrcis HTTP server listen address.")
	provURL := flag.String("providersURL", "", "Providers URL to enable dhfind.")
	dwal := flag.Bool("disableWAL", false, "Weather to disable WAL in Pebble dhstore.")
	maxConcurrentCompactions := flag.Int("maxConcurrentCompactions", 10, "Specifies the maximum number of concurrent Pebble compactions. As a rule of thumb set it to the number of the CPU cores.")
	l0StopWritesThreshold := flag.Int("l0StopWritesThreshold", 12, "Hard limit on Pebble L0 read-amplification. Writes are stopped when this threshold is reached.")
	blockCacheSize := flag.String("blockCacheSize", "1Gi", "Size of pebble block cache. Can be set in Mi or Gi.")
	llvl := flag.String("logLevel", "info", "The logging level. Only applied if GOLOG_LOG_LEVEL environment variable is unset.")
	storeType := flag.String("storeType", "pebble", "The store type to use. only `pebble` and `fdb` is supported. Defaults to `pebble`. When `fdb` is selected, all `fdb*` args must be set.")
	fdbApiVersion := flag.Int("fdbApiVersion", 0, "Required. The FoundationDB API version as a numeric value")
	fdbClusterFile := flag.String("fdbClusterFile", "", "Required. Path to ")

	flag.Parse()

	if _, set := os.LookupEnv("GOLOG_LOG_LEVEL"); !set {
		_ = logging.SetLogLevel("*", *llvl)
	}

	var store dhstore.DHStore
	var pebbleMetricsProvider func() *pebble.Metrics
	switch *storeType {
	case "pebble":
		parsedBlockCacheSize, err := parseBlockCacheSize(*blockCacheSize)
		if err != nil {
			panic(err)
		}

		// Default options copied from cockroachdb with the addition of a custom sized block cache and configurable compaction options.
		// See:
		// - https://github.com/cockroachdb/cockroach/blob/v22.1.6/pkg/storage/pebble.go#L479
		opts := &pebble.Options{
			BytesPerSync:                10 << 20, // 10 MiB
			WALBytesPerSync:             10 << 20, // 10 MiB
			MaxConcurrentCompactions:    *maxConcurrentCompactions,
			MemTableSize:                64 << 20, // 64 MiB
			MemTableStopWritesThreshold: 4,
			LBaseMaxBytes:               64 << 20, // 64 MiB
			L0CompactionThreshold:       2,
			L0StopWritesThreshold:       *l0StopWritesThreshold,
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
		opts.Cache = pebble.NewCache(int64(parsedBlockCacheSize))

		path := filepath.Clean(*storePath)
		pbstore, err := dhpebble.NewPebbleDHStore(path, opts)
		if err != nil {
			panic(err)
		}
		store = pbstore
		pebbleMetricsProvider = pbstore.Metrics
		log.Infow("Store opened.", "path", path)
	case "fdb":
		var err error
		store, err = fdb.NewFDBDHStore(fdb.WithApiVersion(*fdbApiVersion), fdb.WithClusterFile(*fdbClusterFile))
		if err != nil {
			panic(err)
		}
		log.Infow("Using FoundationDB backing store.")
	default:
		panic("unknown storeType: " + *storeType)
	}

	m, err := metrics.New(*metrcisAddr, pebbleMetricsProvider)
	if err != nil {
		panic(err)
	}

	svr, err := server.New(store, *listenAddr, server.WithMetrics(m), server.WithDHFind(*provURL))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := svr.Start(ctx); err != nil {
		panic(err)
	}
	if err := m.Start(ctx); err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Info("Terminating...")
	if err := svr.Shutdown(ctx); err != nil {
		log.Warnw("Failure occurred while shutting down server.", "err", err)
	} else {
		log.Info("Shut down server successfully.")
	}
	if err := m.Shutdown(ctx); err != nil {
		log.Warnw("Failure occurred while shutting down metrics server.", "err", err)
	} else {
		log.Info("Shut down metrics server successfully.")
	}

	if err := store.Close(); err != nil {
		log.Warnw("Failure occurred while closing store.", "err", err)
	} else {
		log.Info("Closed store successfully.")
	}
}

func parseBlockCacheSize(str string) (uint64, error) {
	// If the value is empty - defaulting to zero
	if len(str) == 0 {
		return 0, nil
	}
	// If there is less than two bytes - treating it as a number
	if len(str) <= 2 {
		n, err := strconv.Atoi(str)
		if err != nil {
			return 0, err
		}
		return uint64(n), err
	}
	suffix := strings.ToLower(str[len(str)-2:])
	multiplier := 1
	var n int
	var err error
	switch suffix {
	case "mi":
		n, err = strconv.Atoi(str[:len(str)-2])
		multiplier = 1 << 20
	case "gi":
		n, err = strconv.Atoi(str[:len(str)-2])
		multiplier = 1 << 30
	default:
		n, err = strconv.Atoi(str)
	}
	if err != nil {
		return 0, err
	}
	return uint64(n * multiplier), nil
}

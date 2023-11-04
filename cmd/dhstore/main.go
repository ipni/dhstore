// To build with FDB support, run the command:
//
//	go build -tags fdb ./cmd/dhstore
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/dhstore"
	"github.com/ipni/dhstore/metrics"
	dhpebble "github.com/ipni/dhstore/pebble"
	"github.com/ipni/dhstore/server"
)

var (
	log = logging.Logger("cmd/dhstore")
)

type arrayFlags []string

func (a *arrayFlags) String() string {
	return strings.Join(*a, ", ")
}

func (a *arrayFlags) Set(value string) error {
	*a = append(*a, value)
	return nil
}

func main() {
	if v, found := os.LookupEnv("GO_DEBUG_MAX_THREADS"); found {
		maxThreads, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.Fatalw("Invalid Go debug max threads value", "value", v, "err", err)
		}
		previousMaxThreads := debug.SetMaxThreads(int(maxThreads))
		log.Infof("Go debug max threads is changed from %d to %d", previousMaxThreads, maxThreads)
	}

	var providersURLs arrayFlags
	storePath := flag.String("storePath", "./dhstore/store", "The path at which the dhstore data persisted.")
	listenAddr := flag.String("listenAddr", "0.0.0.0:40080", "The dhstore HTTP server listen address.")
	metrcisAddr := flag.String("metricsAddr", "0.0.0.0:40081", "The dhstore metrics HTTP server listen address.")
	flag.Var(&providersURLs, "providersURL", "Providers URL to enable dhfind. Multiple OK")
	dwal := flag.Bool("disableWAL", false, "Weather to disable WAL in Pebble dhstore.")
	maxConcurrentCompactions := flag.Int("maxConcurrentCompactions", 10, "Specifies the maximum number of concurrent Pebble compactions. As a rule of thumb set it to the number of the CPU cores.")
	l0StopWritesThreshold := flag.Int("l0StopWritesThreshold", 12, "Hard limit on Pebble L0 read-amplification. Writes are stopped when this threshold is reached.")
	l0CompactionThreshold := flag.Int("l0CompactionThreshold", 2, "The amount of L0 read-amplification necessary to trigger an L0 compaction.")
	l0CompactionFileThreshold := flag.Int("l0CompactionFileThreshold", 500, "The count of L0 files necessary to trigger an L0 compaction.")
	experimentalL0CompactionConcurrency := flag.Int("experimentalL0CompactionConcurrency", 10, "The threshold of L0 read-amplification at which compaction concurrency is enabled (if CompactionDebtConcurrency was not already exceeded). Every multiple of this value enables another concurrent compaction up to MaxConcurrentCompactions.")
	blockCacheSize := flag.String("blockCacheSize", "1Gi", "Size of pebble block cache. Can be set in Mi or Gi.")
	experimentalCompactionDebtConcurrency := flag.String("experimentalCompactionDebtConcurrency", "1Gi", "CompactionDebtConcurrency controls the threshold of compaction debt at which additional compaction concurrency slots are added. For every multiple of this value in compaction debt bytes, an additional concurrent compaction is added. This works \"on top\" of L0CompactionConcurrency, so the higher of the count of compaction concurrency slots as determined by the two options is chosen. Can be set in Mi or Gi.")

	llvl := flag.String("logLevel", "info", "The logging level. Only applied if GOLOG_LOG_LEVEL environment variable is unset.")
	storeType := flag.String("storeType", "pebble", "The store type to use. only `pebble` and `fdb` is supported. Defaults to `pebble`. When `fdb` is selected, all `fdb*` args must be set.")
	version := flag.Bool("version", false, "Show version information,")

	flag.Parse()

	if *version {
		fmt.Println(dhstore.Version)
		return
	}

	if _, set := os.LookupEnv("GOLOG_LOG_LEVEL"); !set {
		_ = logging.SetLogLevel("*", *llvl)
	}

	var store dhstore.DHStore
	var pebbleMetricsProvider func() *pebble.Metrics
	switch *storeType {
	case "pebble":
		parsedBlockCacheSize, err := parseBytesIEC(*blockCacheSize)
		if err != nil {
			log.Fatalw("Failed to parse block cache size", "err", err)
		}
		parsedExperimentalCompactionDebtConcurrency, err := parseBytesIEC(*experimentalCompactionDebtConcurrency)
		if err != nil {
			log.Fatalw("Failed to parse experimental compaction debt concurrency", "err", err)
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
			L0CompactionThreshold:       *l0CompactionThreshold,
			L0StopWritesThreshold:       *l0StopWritesThreshold,
			L0CompactionFileThreshold:   *l0CompactionFileThreshold,
			DisableWAL:                  *dwal,
			WALMinSyncInterval:          func() time.Duration { return 30 * time.Second },
		}

		opts.Experimental.ReadCompactionRate = 10 << 20 // 20 MiB
		opts.Experimental.MinDeletionRate = 128 << 20   // 128 MiB
		opts.Experimental.CompactionDebtConcurrency = int(parsedExperimentalCompactionDebtConcurrency)
		opts.Experimental.L0CompactionConcurrency = *experimentalL0CompactionConcurrency

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
		store, err = newFDBDHStore()
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

	svr, err := server.New(store, *listenAddr, server.WithMetrics(m), server.WithDHFind(providersURLs...))
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

func parseBytesIEC(str string) (uint64, error) {
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

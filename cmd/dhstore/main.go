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
	"github.com/ipni/dhstore/metrics"
)

var (
	log = logging.Logger("cmd/dhstore")
)

func main() {
	storePath := flag.String("storePath", "./dhstore/store", "The path at which the dhstore data persisted.")
	listenAddr := flag.String("listenAddr", "0.0.0.0:40080", "The dhstore HTTP server listen address.")
	metrcisAddr := flag.String("metricsAddr", "0.0.0.0:40081", "The dhstore metrcis HTTP server listen address.")
	dwal := flag.Bool("disableWAL", false, "Weather to disable WAL in Pebble dhstore.")
	blockCacheSize := flag.String("blockCacheSize", "1Gi", "Size of pebble block cache. Can be set in Mi or Gi.")
	yugaHost := flag.String("yugaHost", "127.0.0.1", "Host of the yugabyte database.")
	yugaPort := flag.Int("yugaHost", 5433, "Port of the yugabyte database.")
	yugaDBName := flag.String("yugaDBName", "yugabyte", "YugabyteDB name.")
	yugaDBUser := flag.String("yugaDBUser", "yugabyte", "YugabyteDB user.")
	yugaDBPassword := flag.String("yugaDBPassword", "yugabyte", "YugabyteDB user password.")
	yugaSSLMode := flag.String("yugaSSLMode", "disable", "YugabyteDB SSL Mode.")
	yugaSSLRootCert := flag.String("yugaSSLRootCert", "", "YugabyteDB SSL Root Cert.")
	isPebble := flag.Bool("isPebble", true, "Use pebble or yugabyte store")
	llvl := flag.String("logLevel", "info", "The logging level. Only applied if GOLOG_LOG_LEVEL environment variable is unset.")

	flag.Parse()

	if _, set := os.LookupEnv("GOLOG_LOG_LEVEL"); !set {
		_ = logging.SetLogLevel("*", *llvl)
	}

	var store dhstore.DHStore
	var m *metrics.Metrics
	var err error

	if *isPebble {
		var parsedBlockCacheSize uint64
		parsedBlockCacheSize, err = parseBlockCacheSize(*blockCacheSize)
		if err != nil {
			panic(err)
		}
		store, err = newPebbleDHStore(*dwal, int64(parsedBlockCacheSize), *storePath)
		if err != nil {
			panic(err)
		}
		log.Infow("Pebble store opened.", "path", storePath)
		m, err = metrics.New(*metrcisAddr, store.(*dhstore.PebbleDHStore).Metrics)
		if err != nil {
			panic(err)
		}
	} else {
		store, err = newYugabyteDHStore(*yugaHost, *yugaPort, *yugaDBName, *yugaDBUser, *yugaDBPassword, *yugaSSLMode, *yugaSSLRootCert)
		if err != nil {
			panic(err)
		}
		m, err = metrics.New(*metrcisAddr, nil)
		if err != nil {
			panic(err)
		}
		log.Info("YugabyteDB client initialised.")
	}

	server, err := dhstore.NewHttpServer(store, m, *listenAddr)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		panic(err)
	}
	if err := m.Start(ctx); err != nil {
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

func newPebbleDHStore(dwal bool, parsedBlockCacheSize int64, storePath string) (dhstore.DHStore, error) {
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
		DisableWAL:                  dwal,
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

	path := filepath.Clean(storePath)
	return dhstore.NewPebbleDHStore(path, opts)
}

func newYugabyteDHStore(yugaHost string, yugaPort int, yugaDBName, yugaDBUser, yugaDBPassword, yugaSSLMode, yugaSSLRootCert string) (dhstore.DHStore, error) {
	c := dhstore.NewYugabyteConfig()
	if len(yugaHost) > 0 {
		c.Host = yugaHost
	}

	if yugaPort != 0 {
		c.Port = yugaPort
	}

	if len(yugaDBName) > 0 {
		c.DBName = yugaDBName
	}
	if len(yugaDBUser) > 0 {
		c.DBUser = yugaDBUser
	}
	if len(yugaDBPassword) > 0 {
		c.DBPassword = yugaDBPassword
	}
	if len(yugaSSLMode) > 0 {
		c.SSLMode = yugaSSLMode
	}
	if len(yugaSSLRootCert) > 0 {
		c.SSLRootCert = yugaSSLRootCert
	}

	return dhstore.NewYugabyteDHStore(c)
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

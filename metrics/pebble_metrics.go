package metrics

import (
	"context"

	"github.com/cockroachdb/pebble/v2"
	"go.opentelemetry.io/otel/attribute"
	cmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/asyncint64"
	"go.opentelemetry.io/otel/metric/unit"
)

// pebbleMetrics asynchronously reports metrics of pebble DB
type pebbleMetrics struct {
	metricsProvider func() *pebble.Metrics
	meter           cmetric.Meter

	// flushCount reports the total number of flushes
	flushCount asyncint64.Gauge
	// readAdmp reports current read amplification of the database.
	// It's computed as the number of sublevels in L0 + the number of non-empty
	// levels below L0.
	// Read amplification factor should be in the single digits. A value exceeding 50 for 1 hour
	// strongly suggests that the LSM tree has an unhealthy shape.
	readAmp asyncint64.Gauge

	// NOTE: cache metrics report tagged values for both block and table caches
	// cacheSize reports the number of bytes inuse by the cache
	cacheSize asyncint64.Gauge
	// cacheCount reports the count of objects (blocks or tables) in the cache
	cacheCount asyncint64.Gauge
	// cacheHits reports number of cache hits
	cacheHits asyncint64.Gauge
	// cacheMisses reports number of cache misses.
	cacheMisses asyncint64.Gauge

	// compactCount is the total number of compactions, and per-compaction type counts.
	compactCount asyncint64.Gauge
	// compactEstimatedDebt is an estimate of the number of bytes that need to be compacted for the LSM
	// to reach a stable state.
	compactEstimatedDebt asyncint64.Gauge
	// compactInProgressBytes is a number of bytes present in sstables being written by in-progress
	// compactions. This value will be zero if there are no in-progress
	// compactions.
	compactInProgressBytes asyncint64.Gauge
	// compactNumInProgress is a number of compactions that are in-progress.
	compactNumInProgress asyncint64.Gauge
	// compactMarkedFiles is a count of files that are marked for
	// compaction. Such files are compacted in a rewrite compaction
	// when no other compactions are picked.
	compactMarkedFiles asyncint64.Gauge

	// l0TablesCount is the total count of sstables in L0. The number of L0 sstables should not be in the high thousands
	// High values indicate heavy write load that is causing accumulation of sstables in level 0. These sstables are not
	// being compacted quickly enough to lower levels, resulting in a misshapen LSM.
	l0TablesCount asyncint64.Gauge
}

func (pm *pebbleMetrics) start() error {
	var err error

	if pm.flushCount, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/flush_count",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The total number of flushes."),
	); err != nil {
		return err
	}

	if pm.readAmp, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/read_amp",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("current read amplification of the database. "+
			"It's computed as the number of sublevels in L0 + the number of non-empty"+
			" levels below L0."),
	); err != nil {
		return err
	}

	if pm.cacheSize, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/cache_size",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The number of bytes inuse by the cache."),
	); err != nil {
		return err
	}

	if pm.cacheCount, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/cache_count",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The count of objects (blocks or tables) in the cache."),
	); err != nil {
		return err
	}

	if pm.cacheHits, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/cache_hits",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The number of cache hits."),
	); err != nil {
		return err
	}

	if pm.cacheMisses, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/cache_misses",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The number of cache misses."),
	); err != nil {
		return err
	}

	if pm.compactCount, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/compact_count",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The total number of compactions, and per-compaction type counts."),
	); err != nil {
		return err
	}

	if pm.compactEstimatedDebt, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/compact_estimated_debt",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("An estimate of the number of bytes that need to be compacted for the LSM"+
			" to reach a stable state."),
	); err != nil {
		return err
	}

	if pm.compactInProgressBytes, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/compact_in_progress_bytes",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("A number of bytes present in sstables being written by in-progress"+
			" compactions. This value will be zero if there are no in-progress"+
			" compactions."),
	); err != nil {
		return err
	}

	if pm.compactNumInProgress, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/compact_num_in_progress",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("A number of compactions that are in-progress."),
	); err != nil {
		return err
	}

	if pm.compactMarkedFiles, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/compact_marked_files",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("A count of files that are marked for"+
			" compaction. Such files are compacted in a rewrite compaction"+
			" when no other compactions are picked."),
	); err != nil {
		return err
	}

	if pm.l0TablesCount, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/compact_l0_tables_count",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The total count of sstables in L0. The number of L0 sstables should not be in the high thousands."+
			" High values indicate heavy write load that is causing accumulation of sstables in level 0. These sstables are not"+
			" being compacted quickly enough to lower levels, resulting in a misshapen LSM."),
	); err != nil {
		return err
	}

	return pm.meter.RegisterCallback(
		[]instrument.Asynchronous{
			pm.flushCount,
			pm.readAmp,
			pm.cacheCount,
			pm.cacheSize,
			pm.cacheHits,
			pm.cacheMisses,
			pm.compactCount,
			pm.compactEstimatedDebt,
			pm.compactInProgressBytes,
			pm.compactNumInProgress,
			pm.compactMarkedFiles,
			pm.l0TablesCount,
		},
		pm.reportAsyncMetrics,
	)
}

func (pm *pebbleMetrics) reportAsyncMetrics(ctx context.Context) {
	m := pm.metricsProvider()

	pm.flushCount.Observe(ctx, m.Flush.Count)
	pm.readAmp.Observe(ctx, int64(m.ReadAmp()))
	pm.cacheCount.Observe(ctx, m.BlockCache.Count, attribute.String("cache", "block"))
	pm.cacheSize.Observe(ctx, m.BlockCache.Size, attribute.String("cache", "block"))
	pm.cacheHits.Observe(ctx, m.BlockCache.Hits, attribute.String("cache", "block"))
	pm.cacheMisses.Observe(ctx, m.BlockCache.Misses, attribute.String("cache", "block"))

	pm.cacheCount.Observe(ctx, m.FileCache.TableCount+m.FileCache.BlobFileCount, attribute.String("cache", "file"))
	pm.cacheSize.Observe(ctx, m.FileCache.Size, attribute.String("cache", "file"))
	pm.cacheHits.Observe(ctx, m.FileCache.Hits, attribute.String("cache", "file"))
	pm.cacheMisses.Observe(ctx, m.FileCache.Misses, attribute.String("cache", "file"))

	pm.compactCount.Observe(ctx, int64(m.Compact.Count))
	pm.compactEstimatedDebt.Observe(ctx, int64(m.Compact.EstimatedDebt))
	pm.compactInProgressBytes.Observe(ctx, int64(m.Compact.InProgressBytes))
	pm.compactNumInProgress.Observe(ctx, int64(m.Compact.NumInProgress))
	pm.compactMarkedFiles.Observe(ctx, int64(m.Compact.MarkedFiles))

	pm.l0TablesCount.Observe(ctx, int64(m.Levels[0].TablesCount))
}

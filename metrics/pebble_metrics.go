package metrics

import (
	"context"

	"github.com/cockroachdb/pebble"
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

	// fsyncFlushCount reports the total number of flushes
	fsyncFlushCount asyncint64.Gauge
	// NOTE: cache metrics report tagged values for both block and table caches
	// cacheSize reports the number of bytes inuse by the cache
	cacheSize asyncint64.Gauge
	// cacheCount reports the count of objects (blocks or tables) in the cache
	cacheCount asyncint64.Gauge
	// cacheHits reports number of cache hits
	cacheHits asyncint64.Gauge
	// cacheMisses reports number of cache misses.
	cacheMisses asyncint64.Gauge
}

func (pm *pebbleMetrics) start() error {
	var err error

	if pm.fsyncFlushCount, err = pm.meter.AsyncInt64().Gauge(
		"ipni/dhstore/pebble/flush_count",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The total number of flushes."),
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

	return pm.meter.RegisterCallback(
		[]instrument.Asynchronous{
			pm.fsyncFlushCount,
			pm.cacheCount,
			pm.cacheSize,
			pm.cacheHits,
			pm.cacheMisses,
		},
		pm.reportAsyncMetrics,
	)
}

func (pm *pebbleMetrics) reportAsyncMetrics(ctx context.Context) {
	m := pm.metricsProvider()

	pm.fsyncFlushCount.Observe(ctx, m.Flush.Count)
	pm.cacheCount.Observe(ctx, m.BlockCache.Count, attribute.String("cache", "block"))
	pm.cacheSize.Observe(ctx, m.BlockCache.Size, attribute.String("cache", "block"))
	pm.cacheHits.Observe(ctx, m.BlockCache.Hits, attribute.String("cache", "block"))
	pm.cacheMisses.Observe(ctx, m.BlockCache.Misses, attribute.String("cache", "block"))

	pm.cacheCount.Observe(ctx, m.TableCache.Count, attribute.String("cache", "table"))
	pm.cacheSize.Observe(ctx, m.TableCache.Size, attribute.String("cache", "table"))
	pm.cacheHits.Observe(ctx, m.TableCache.Hits, attribute.String("cache", "table"))
	pm.cacheMisses.Observe(ctx, m.TableCache.Misses, attribute.String("cache", "table"))
}

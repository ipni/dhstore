package metrics

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/pebble"
	logging "github.com/ipfs/go-log/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
	"go.opentelemetry.io/otel/sdk/metric"
)

var (
	log = logging.Logger("metrics")
)

type Metrics struct {
	exporter      *prometheus.Exporter
	httpLatency   syncint64.Histogram
	s             *http.Server
	pebbleMetrics *pebbleMetrics
}

func New(metricsAddr string, pebbleMetricsProvider func() *pebble.Metrics) (*Metrics, error) {
	var m Metrics
	var err error
	if m.exporter, err = prometheus.New(prometheus.WithoutUnits()); err != nil {
		return nil, err
	}

	provider := metric.NewMeterProvider(metric.WithReader(m.exporter))
	meter := provider.Meter("ipni/dhstore")

	if m.httpLatency, err = meter.SyncInt64().Histogram("ipni/dhstore/http_latency",
		instrument.WithUnit(unit.Milliseconds),
		instrument.WithDescription("Latency of DHStore HTTP API")); err != nil {
		return nil, err
	}

	m.s = &http.Server{
		Addr:    metricsAddr,
		Handler: metricsMux(),
	}

	if pebbleMetricsProvider != nil {
		m.pebbleMetrics = &pebbleMetrics{
			metricsProvider: pebbleMetricsProvider,
			meter:           meter,
		}
	}

	return &m, nil
}

func (m *Metrics) RecordHttpLatency(ctx context.Context, t time.Duration, method, path string, status int) {
	m.httpLatency.Record(ctx, t.Milliseconds(),
		attribute.String("method", method), attribute.String("path", path), attribute.Int("status", status))
}

func (m *Metrics) Start(_ context.Context) error {
	mln, err := net.Listen("tcp", m.s.Addr)
	if err != nil {
		return err
	}

	go func() { _ = m.s.Serve(mln) }()

	if m.pebbleMetrics != nil {
		m.pebbleMetrics.start()
	}

	log.Infow("Metrics server started", "addr", mln.Addr())
	return nil
}

func (s *Metrics) Shutdown(ctx context.Context) error {
	return s.s.Shutdown(ctx)
}

func metricsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

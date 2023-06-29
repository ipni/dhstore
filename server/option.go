package server

import (
	"fmt"

	"github.com/ipni/dhstore/metrics"
)

// config contains all options for the server.
type config struct {
	metrics      *metrics.Metrics
	providersURL string
	preferJSON   bool
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		preferJSON: true,
	}
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	return cfg, nil
}

// WithMetrics configures metrics.
func WithMetrics(m *metrics.Metrics) Option {
	return func(c *config) error {
		c.metrics = m
		return nil
	}
}

// WithDHFind enables dhfind functionality.
func WithDHFind(providersURL string) Option {
	return func(c *config) error {
		c.providersURL = providersURL
		return nil
	}
}

// preferJSON specifies weather to prefer JSON over NDJSON response when
// request accepts */*, i.e. any response format, has no `Accept` header at
// all. Default is true.
func WithPreferJSON(on bool) Option {
	return func(c *config) error {
		c.preferJSON = on
		return nil
	}
}

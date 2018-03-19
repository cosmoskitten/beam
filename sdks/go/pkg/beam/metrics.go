package beam

import (
	"context"

	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
)

// Counter is a metric that can be incremented and decremented,
// and is aggregated by the sum.
type Counter struct {
	metrics.Counter
}

// Inc increments the counter within by the given amount.
func (c Counter) Inc(ctx context.Context, v int64) {
	c.Counter.Inc(ctx, v)
}

// Dec decrements the counter within by the given amount.
func (c Counter) Dec(ctx context.Context, v int64) {
	c.Counter.Dec(ctx, v)
}

// GetCounter returns the Counter with the given namespace and name.
func GetCounter(namespace, name string) Counter {
	return Counter{metrics.GetCounter(namespace, name)}
}

// Distribution is a metric that records various statistics about the distribution
// of reported values.
type Distribution struct {
	metrics.Distribution
}

// Update adds an observation to this distribution.
func (c Distribution) Update(ctx context.Context, v int64) {
	c.Distribution.Update(ctx, v)
}

// GetDistribution returns the Distribution with the given namespace and name.
func GetDistribution(namespace, name string) metrics.Distribution {
	return metrics.GetDistribution(namespace, name)
}

// Gauge is a metric that can have its new value set, and is aggregated by taking
// the last reported value.
type Gauge struct {
	metrics.Gauge
}

// Set sets the current value for this gauge.
func (c Gauge) Set(ctx context.Context, v int64) {
	c.Gauge.Set(ctx, v)
}

// GetGauge returns the Gauge with the given namespace and name.
func GetGauge(namespace, name string) Gauge {
	return Gauge{metrics.GetGauge(namespace, name)}
}

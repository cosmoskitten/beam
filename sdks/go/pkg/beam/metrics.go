package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
)

// GetCounter returns the Counter with the given namespace and name.
func GetCounter(namespace, name string) *Counter {
	return metrics.GetCounter(namespace, name)
}

// GetDistribution returns the Distribution with the given namespace and name.
func GetDistribution(namespace, name string) *Distribution {
	return metrics.GetDistribution(namespace, name)
}

// GetGauge returns the Gauge with the given namespace and name.
func GetGauge(namespace, name string) *Gauge {
	return metrics.GetGauge(namespace, name)
}

// Counter is a simple counter for incrementing and decrementing a value.
type Counter = metrics.Counter

// Distribution is a simple distribution of values.
type Distribution = metrics.Distribution

// Gauge is a simple time & value pair.
type Gauge = metrics.Gauge

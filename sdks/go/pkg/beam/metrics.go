package beam

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ptransform"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/ptypes"
)

type userMetricker interface {
	toProto() *fnexecution_v1.Metrics_User
}

type metricName struct {
	namespace, name string
}

func (n *metricName) String() string {
	return fmt.Sprintf("%s.%s", n.namespace, n.name)
}

func validateName(mn metricName, t metricType) {
	if len(mn.name) == 0 || len(mn.namespace) == 0 {
		panic(fmt.Sprintf("namespace and name are required to be non-empty, got %q and %q", mn.namespace, mn.name))
	}
}

var (
	metricMu      sync.Mutex
	metricStorage = make(map[string]map[metricName]userMetricker)
	counters      = make(map[metricName]*Counter)
	distributions = make(map[metricName]*Distribution)
	gauges        = make(map[metricName]*Gauge)
)

type metricType uint8

const (
	counterType metricType = iota
	distributionType
	gaugeType
)

func (t metricType) String() string {
	switch t {
	case counterType:
		return "Counter"
	case distributionType:
		return "Distribution"
	case gaugeType:
		return "Gauge"
	default:
		panic(fmt.Sprintf("Unknown metric type value: %v", int(t)))
	}
}

// Here's the flow for Metrics:
// User declares/gets a metric in their DoFn
//  * Can be done per invocation, or
//  * Can be done in a Setup for Structural DoFns
// The metric holds onto all contexts/PTransforms that the metric is being used for
// The user does whatever increments and updates they desire in their DoFn to the metrics
// * Counters and Distributions both use a distribution as a backing storage
// * Gauges use something different for storing the value and the time
// This backing store is also referenced on a per context/Ptransform basis so they can be
// extracted as protos during ProgressUpdates
//
// Metrics are created on demand by user code and are initialized with their first
// value. This is handy for distributions which never need to worry about the
// initial min/max values. They're never sent if they're never created.
//
// Behaviour is undefined if two counters with the same names are used
// with different types of metric are used with different types.
// Since metrics are only created when they're being used the first time
// with a context, we can't check for duplicates within a PTransform until both
// are used.
//
// Metrics within a given PTransform.Namespace.Name must be unique, and within that context,
// may not change types.

type Clocker interface {
	Now() time.Time
}

type defaultclock struct{}

func (defaultclock) Now() time.Time {
	return time.Now()
}

// TODO(lostluck): 2018/03/05 Use a common internal beam.Clock instead, once that exists.
var clock Clocker = defaultclock{}

// Counter is a simple counter for incrementing and decrementing a value.
type Counter struct {
	name    metricName
	storage map[string]*distribution
	mu      sync.Mutex
}

func (m *Counter) String() string {
	return fmt.Sprintf("%s metric %s", counterType, &m.name)
}

// GetCounter returns the Counter with the given namespace and name.
func GetCounter(namespace, name string) *Counter {
	mn := metricName{namespace: namespace, name: name}
	metricMu.Lock()
	defer metricMu.Unlock()
	if m, ok := counters[mn]; ok {
		return m
	}
	validateName(mn, counterType)
	m := &Counter{
		name:    mn,
		storage: make(map[string]*distribution),
	}
	counters[mn] = m
	return m
}

// Inc increments the counter within the given PTransform context by v.
func (m *Counter) Inc(ctx context.Context, v int64) {
	key := getContextKey(ctx, m)
	m.mu.Lock()
	defer m.mu.Unlock()
	createOrUpdateDistribution(m.storage, key, v, m.name, counterType)
}

// Dec decrements the counter within the given PTransform context by v.
func (m *Counter) Dec(ctx context.Context, v int64) {
	m.Inc(ctx, -v)
}

// Distribution is a simple distribution of values.
type Distribution struct {
	name    metricName
	storage map[string]*distribution
	mu      sync.Mutex
}

func (m *Distribution) String() string {
	return fmt.Sprintf("%s metric %s", distributionType, m.name)
}

// GetDistribution returns the Distribution with the given namespace and name.
func GetDistribution(namespace, name string) *Distribution {
	mn := metricName{namespace: namespace, name: name}
	metricMu.Lock()
	defer metricMu.Unlock()
	if m, ok := distributions[mn]; ok {
		return m
	}
	validateName(mn, distributionType)
	m := &Distribution{
		name:    mn,
		storage: make(map[string]*distribution),
	}
	distributions[mn] = m
	return m
}

// Update updates the distribution within the given PTransform context with v.
func (m *Distribution) Update(ctx context.Context, v int64) {
	key := getContextKey(ctx, m)
	m.mu.Lock()
	defer m.mu.Unlock()
	createOrUpdateDistribution(m.storage, key, v, m.name, distributionType)
}

// createOrUpdateDistribution requires the metric's namespaced storage map to be locked by the caller
func createOrUpdateDistribution(storage map[string]*distribution, key string, v int64, name metricName, t metricType) {
	if d, ok := storage[key]; ok {
		d.update(v)
		return
	}
	// We don't have storage for this counter yet, so we need to create it
	// and store it in both for the user namespaced storage, and the metricStorage.
	d := &distribution{
		count: 1,
		sum:   v,
		min:   v,
		max:   v,
		t:     t,
	}
	storage[key] = d
	storeMetric(key, name, d)
}

type distribution struct {
	count, sum, min, max int64
	mu                   sync.Mutex
	t                    metricType
}

func (m *distribution) update(v int64) {
	m.mu.Lock()
	if v < m.min {
		m.min = v
	}
	if v > m.max {
		m.max = v
	}
	m.count++
	m.sum += v
	m.mu.Unlock()
}

// toProto returns a Metrics_User populated with the Data messages, but not the name. The
// caller needs to populate with the metric's name.
func (m *distribution) toProto() *fnexecution_v1.Metrics_User {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.t {
	case counterType:
		return &fnexecution_v1.Metrics_User{
			Data: &fnexecution_v1.Metrics_User_CounterData_{
				CounterData: &fnexecution_v1.Metrics_User_CounterData{
					Value: m.sum,
				},
			},
		}
	case distributionType:
		return &fnexecution_v1.Metrics_User{
			Data: &fnexecution_v1.Metrics_User_DistributionData_{
				DistributionData: &fnexecution_v1.Metrics_User_DistributionData{
					Count: m.count,
					Sum:   m.sum,
					Min:   m.min,
					Max:   m.max,
				},
			},
		}
	default:
		panic(fmt.Sprintf("unexpected metric type to materialize distribution: got %v", int(m.t)))
	}
}

// Gauge is a time, value pair.
type Gauge struct {
	name    metricName
	storage map[string]*gauge
	mu      sync.Mutex
}

func (m *Gauge) String() string {
	return fmt.Sprintf("%s metric %s", gaugeType, m.name)
}

// GetGauge returns the Gauge with the given namespace and name.
func GetGauge(namespace, name string) *Gauge {
	mn := metricName{namespace: namespace, name: name}
	metricMu.Lock()
	defer metricMu.Unlock()
	if m, ok := gauges[mn]; ok {
		return m
	}
	validateName(mn, gaugeType)
	m := &Gauge{
		name:    mn,
		storage: make(map[string]*gauge),
	}
	gauges[mn] = m
	return m
}

// Set sets the guage to the given value, and associates it with the current time on the clock.
func (m *Gauge) Set(ctx context.Context, v int64) {
	key := getContextKey(ctx, m)
	m.mu.Lock()
	defer m.mu.Unlock()

	if g, ok := m.storage[key]; ok {
		g.set(v)
		return
	}
	// We don't have storage for this guage yet, so we need to create it
	// and store it in both for the user namespaced storage, and the metricStorage.
	g := &gauge{
		t: clock.Now(),
		v: v,
	}
	m.storage[key] = g
	storeMetric(key, m.name, g)
}

func getContextKey(ctx context.Context, m interface{}) string {
	if key, ok := ptransform.TryGetID(ctx); ok {
		return key
	}
	return "(ptransform id unset)"
}

func storeMetric(key string, name metricName, m userMetricker) {
	metricMu.Lock()
	defer metricMu.Unlock()
	if _, ok := metricStorage[key]; !ok {
		metricStorage[key] = make(map[metricName]userMetricker)
	}
	if _, ok := metricStorage[key][name]; ok {
		panic(fmt.Sprintf("metric name %s.%s being reused for a second metric in a single PTransform", name.namespace, name.name))
	}
	metricStorage[key][name] = m
}

type gauge struct {
	mu sync.Mutex
	t  time.Time
	v  int64
}

func (m *gauge) set(v int64) {
	m.mu.Lock()
	m.t = clock.Now()
	m.v = v
	m.mu.Unlock()
}

func (m *gauge) toProto() *fnexecution_v1.Metrics_User {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts, err := ptypes.TimestampProto(m.t)
	if err != nil {
		panic(err)
	}
	return &fnexecution_v1.Metrics_User{
		Data: &fnexecution_v1.Metrics_User_GaugeData_{
			GaugeData: &fnexecution_v1.Metrics_User_GaugeData{
				Value:     m.v,
				Timestamp: ts,
			},
		},
	}
}

// MetricsToProto exports all metrics for the given PTransform context key.
func MetricsToProto(contextKey string) []*fnexecution_v1.Metrics_User {
	metricMu.Lock()
	defer metricMu.Unlock()
	s := metricStorage[contextKey]
	var ret []*fnexecution_v1.Metrics_User
	for _, d := range s {
		ret = append(ret, d.toProto())
	}
	return ret
}

// DumpMetricsToLog is a debugging function that outputs all metrics available to
// the local worker to beam.Log.
// Useful for sanity checking with the d
func DumpMetricsToLog(ctx context.Context) {
	metricMu.Lock()
	defer metricMu.Unlock()
	for pt, s := range metricStorage {
		log.Errorf(ctx, "PTransformID: %q", pt)
		for n := range s {
			if m, ok := counters[n]; ok {
				log.Errorf(ctx, "\t%s - %#v", m, m.storage[pt])
			}
			if m, ok := distributions[n]; ok {
				log.Errorf(ctx, "\t%s - %#v", m, m.storage[pt])
			}
			if m, ok := gauges[n]; ok {
				log.Errorf(ctx, "\t%s - %#v", m, m.storage[pt])
			}
		}
	}
}

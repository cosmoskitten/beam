// Package metrics handles both system metrics, and user metrics.
package metrics

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ptransform"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/ptypes"
)

// userMetricker knows how to convert it's value to a Metrics_User proto.
type userMetricker interface {
	toProto() *fnexecution_v1.Metrics_User
}

type metricName struct {
	namespace, name string
}

func (n metricName) String() string {
	return fmt.Sprintf("%s.%s", n.namespace, n.name)
}

func validateName(mn metricName) {
	if len(mn.name) == 0 || len(mn.namespace) == 0 {
		panic(fmt.Sprintf("namespace and name are required to be non-empty, got %q and %q", mn.namespace, mn.name))
	}
}

type metricKey struct {
	name       metricName
	ptransform string
}

var (
	metricMu      sync.RWMutex
	metricStorage = make(map[string]map[metricName]userMetricker)
	counters      = sync.Map{}
	distributions = sync.Map{}
	gauges        = sync.Map{}
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
		panic(fmt.Sprintf("Unknown metric type value: %v", uint8(t)))
	}
}

// TODO(lostluck): 2018/03/05 Use a common internal beam clock instead, once that exists.
type Clocker interface {
	Now() time.Time
}

type defaultclock struct{}

func (defaultclock) Now() time.Time {
	return time.Now()
}

var clock Clocker = defaultclock{}

// Counter is a simple counter for incrementing and decrementing a value.
type Counter struct {
	name metricName
}

func (m Counter) String() string {
	return fmt.Sprintf("%s metric %s", counterType, m.name)
}

// GetCounter returns the Counter with the given namespace and name.
func GetCounter(namespace, name string) Counter {
	mn := metricName{namespace: namespace, name: name}
	validateName(mn)
	return Counter{
		name: mn,
	}
}

// Inc increments the counter within the given PTransform context by v.
func (m Counter) Inc(ctx context.Context, v int64) {
	key := getContextKey(ctx, m)
	createOrUpdateDistribution(&counters, v, metricKey{m.name, key}, counterType)
}

// Dec decrements the counter within the given PTransform context by v.
func (m Counter) Dec(ctx context.Context, v int64) {
	m.Inc(ctx, -v)
}

// Distribution is a simple distribution of values.
type Distribution struct {
	name metricName
}

func (m Distribution) String() string {
	return fmt.Sprintf("%s metric %s", distributionType, m.name)
}

// GetDistribution returns the Distribution with the given namespace and name.
func GetDistribution(namespace, name string) Distribution {
	mn := metricName{namespace: namespace, name: name}
	validateName(mn)
	return Distribution{
		name: mn,
	}
}

// Update updates the distribution within the given PTransform context with v.
func (m Distribution) Update(ctx context.Context, v int64) {
	key := getContextKey(ctx, m)
	createOrUpdateDistribution(&distributions, v, metricKey{m.name, key}, distributionType)
}

func createOrUpdateDistribution(storage *sync.Map, v int64, key metricKey, t metricType) {
	if m, loaded := storage.LoadOrStore(key, &distribution{
		count: 1,
		sum:   v,
		min:   v,
		max:   v,
		t:     t,
	}); loaded {
		d := m.(*distribution)
		d.update(v)
	} else {
		d := m.(*distribution)
		storeMetric(key, d)
	}
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

func (m *distribution) String() string {
	switch m.t {
	case distributionType:
		return fmt.Sprintf("count: %d sum: %d min: %d max: %d", m.count, m.sum, m.min, m.max)
	case counterType:
		return fmt.Sprintf("value: %d", m.sum)
	default:
		return fmt.Sprintf("unknown counter type: %v", m.t)
	}
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

// Gauge is a time, value pair metric.
type Gauge struct {
	name metricName
}

func (m Gauge) String() string {
	return fmt.Sprintf("%s metric %s", gaugeType, m.name)
}

// GetGauge returns the Gauge with the given namespace and name.
func GetGauge(namespace, name string) Gauge {
	mn := metricName{namespace: namespace, name: name}
	validateName(mn)
	return Gauge{
		name: mn,
	}
}

// Set sets the gauge to the given value, and associates it with the current time on the clock.
func (m Gauge) Set(ctx context.Context, v int64) {
	ptransformKey := getContextKey(ctx, m)
	key := metricKey{m.name, ptransformKey}
	if m, loaded := gauges.LoadOrStore(key, &gauge{
		t: clock.Now(),
		v: v,
	}); loaded {
		g := m.(*gauge)
		g.set(v)
	} else {
		d := m.(*gauge)
		// We don't have storage for this counter yet, so we need to create it
		// and store it in both for the user namespaced storage, and the metricStorage.
		storeMetric(key, d)
	}
}

func getContextKey(ctx context.Context, m interface{}) string {
	if key, ok := ptransform.TryGetID(ctx); ok {
		return key
	}
	return "(ptransform id unset)"
}

func storeMetric(key metricKey, m userMetricker) {
	metricMu.Lock()
	defer metricMu.Unlock()
	if _, ok := metricStorage[key.ptransform]; !ok {
		metricStorage[key.ptransform] = make(map[metricName]userMetricker)
	}
	if _, ok := metricStorage[key.ptransform][key.name]; ok {
		panic(fmt.Sprintf("metric name %s being reused for a second metric in a single PTransform", key.name))
	}
	metricStorage[key.ptransform][key.name] = m
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

func (m *gauge) String() string {
	return fmt.Sprintf("time: %s value: %d", m.t, m.v)
}

// ToProto exports all metrics for the given PTransform context key.
func ToProto(contextKey string) []*fnexecution_v1.Metrics_User {
	metricMu.RLock()
	defer metricMu.RUnlock()
	s := metricStorage[contextKey]
	var ret []*fnexecution_v1.Metrics_User
	for _, d := range s {
		ret = append(ret, d.toProto())
	}
	return ret
}

// DumpToLog is a debugging function that outputs all metrics available locally to beam.Log.
func DumpToLog(ctx context.Context) {
	dumpTo(func(format string, args ...interface{}) {
		log.Errorf(ctx, format, args...)
	})
}

// DumpToOut is a debugging function that outputs all metrics available locally to std out.
func DumpToOut() {
	dumpTo(func(format string, args ...interface{}) {
		fmt.Printf(format+"\n", args...)
	})
}

func dumpTo(p func(format string, args ...interface{})) {
	metricMu.RLock()
	defer metricMu.RUnlock()
	var pts []string
	for pt := range metricStorage {
		pts = append(pts, pt)
	}
	sort.Strings(pts)
	for _, pt := range pts {
		p("PTransformID: %q", pt)
		for n := range metricStorage[pt] {
			key := metricKey{n, pt}
			if m, ok := counters.Load(key); ok {
				c := m.(*distribution)
				p("\t%s - %s", key.name, c)
			}
			if m, ok := distributions.Load(key); ok {
				d := m.(*distribution)
				p("\t%s - %s", key.name, d)
			}
			if m, ok := gauges.Load(key); ok {
				g := m.(*gauge)
				p("\t%s - %s", key.name, g)
			}
		}
	}

}

// Clear resets all storage associated with metrics for tests.
func Clear() {
	metricStorage = make(map[string]map[metricName]userMetricker)
	counters = sync.Map{}
	distributions = sync.Map{}
	gauges = sync.Map{}
}

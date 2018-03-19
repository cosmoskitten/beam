// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package metrics handles Beam metrics.
//
// Metrics in the Beam model are uniquely identified by a namespace, a name,
// and the PTransform context in which they are used. Further, they are
// reported as a delta against the bundle being processed, so that overcounting
// doesn't occur if a bundle needs to be retried.
//
// Metric containers aren't initialized until their first mutation, which
// follows from the Beam model design, where metrics are only sent for a bundle
// if they have changed. This is particularly convenient for distributions which
// means their min and max fields can be set to the first value on creation
// rather than have some marker of uninitialized state, which would otherwise
// need to be checked for on every update.
//
// Metric values are implemented as lightweight proxies of the user provided
// namespace and name. This allows them to be declared globally, and used in
// any ParDo. To handle reporting deltas on the metrics by bundle, metrics
// are keyed by bundleID,PTransformID,namespace, and name, so metrics that
// are identical except for bundles are treated as distinct, effectively
// providing per bundle deltas, since a new value container is used per bundle.
package metrics

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/bundle"
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
	name               metricName
	bundle, ptransform string
}

var (
	metricMu sync.RWMutex
	// metricStorage is a map of BundleIds to PtransformIds to metrics.
	metricStorage = make(map[string]map[string]map[metricName]userMetricker)
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

// Clocker is an interface to access the current time.
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
	key := getContextKey(ctx, m.name)
	createOrUpdateDistribution(&counters, v, key, counterType)
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
	key := getContextKey(ctx, m.name)
	createOrUpdateDistribution(&distributions, v, key, distributionType)
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
	key := getContextKey(ctx, m.name)
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

func getContextKey(ctx context.Context, n metricName) metricKey {
	key := metricKey{name: n, bundle: "(bundle id unset)", ptransform: "(ptransform id unset)"}
	if k, ok := ptransform.TryGetID(ctx); ok {
		key.ptransform = k
	}
	if k, ok := bundle.TryGetID(ctx); ok {
		key.bundle = k
	}
	return key
}

// storeMetric stores a metric away on its first use so it may be retrieved later on.
func storeMetric(key metricKey, m userMetricker) {
	metricMu.Lock()
	defer metricMu.Unlock()
	if _, ok := metricStorage[key.bundle]; !ok {
		metricStorage[key.bundle] = make(map[string]map[metricName]userMetricker)
	}
	if _, ok := metricStorage[key.bundle][key.ptransform]; !ok {
		metricStorage[key.bundle][key.ptransform] = make(map[metricName]userMetricker)
	}
	if _, ok := metricStorage[key.bundle][key.ptransform][key.name]; ok {
		panic(fmt.Sprintf("metric name %s being reused for a second metric in a single PTransform", key.name))
	}
	metricStorage[key.bundle][key.ptransform][key.name] = m
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

// ToProto exports all collected metrics for the given BundleID and PTransform ID pair.
func ToProto(b, pt string) []*fnexecution_v1.Metrics_User {
	metricMu.RLock()
	defer metricMu.RUnlock()
	ps := metricStorage[b]
	s := ps[pt]
	var ret []*fnexecution_v1.Metrics_User
	for n, m := range s {
		p := m.toProto()
		p.MetricName = &fnexecution_v1.Metrics_User_MetricName{
			Name:      n.name,
			Namespace: n.namespace,
		}
		ret = append(ret, p)
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
	var bs []string
	for b := range metricStorage {
		bs = append(bs, b)
	}
	sort.Strings(bs)
	for _, b := range bs {
		var pts []string
		for pt := range metricStorage[b] {
			pts = append(pts, pt)
		}
		sort.Strings(pts)
		for _, pt := range pts {
			var ns []metricName
			for n := range metricStorage[b][pt] {
				ns = append(ns, n)
			}
			sort.Slice(ns, func(i, j int) bool {
				if ns[i].namespace < ns[j].namespace {
					return true
				}
				if ns[i].namespace == ns[j].namespace && ns[i].name < ns[j].name {
					return true
				}
				return false
			})
			p("Bundle: %q - PTransformID: %q", b, pt)
			for _, n := range ns {
				key := metricKey{name: n, bundle: b, ptransform: pt}
				if m, ok := counters.Load(key); ok {
					p("\t%s - %s", key.name, m)
				}
				if m, ok := distributions.Load(key); ok {
					p("\t%s - %s", key.name, m)
				}
				if m, ok := gauges.Load(key); ok {
					p("\t%s - %s", key.name, m)
				}
			}
		}
	}
}

// Clear resets all storage associated with metrics for tests.
func Clear() {
	metricMu.Lock()
	metricStorage = make(map[string]map[string]map[metricName]userMetricker)
	counters = sync.Map{}
	distributions = sync.Map{}
	gauges = sync.Map{}
	metricMu.Unlock()
}

// ClearBundleData removes stored references associated with a given bundle,
// so it can be garbage collected.
func ClearBundleData(b string) {
	metricMu.Lock()
	pts := metricStorage[b]
	for pt, m := range pts {
		for n := range m {
			key := metricKey{name: n, bundle: b, ptransform: pt}
			counters.Delete(key)
			distributions.Delete(key)
			gauges.Delete(key)
		}
	}
	delete(metricStorage, b)
	metricMu.Unlock()
}

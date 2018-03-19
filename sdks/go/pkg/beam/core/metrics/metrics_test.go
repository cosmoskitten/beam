package metrics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/bundle"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ptransform"
)

const bID = "bID"

func ctxWith(b, pt string) context.Context {
	ctx := context.Background()
	ctx = ptransform.SetID(ctx, pt)
	ctx = bundle.SetID(ctx, b)
	return ctx
}

func TestCounter_Inc(t *testing.T) {
	tests := []struct {
		ns, n, key string // Counter name and PTransform context
		inc        int64
		count, sum int64 // Internal variables to check
	}{
		{ns: "inc1", n: "count", key: "A", inc: 1, count: 1, sum: 1},
		{ns: "inc1", n: "count", key: "A", inc: 1, count: 2, sum: 2},
		{ns: "inc1", n: "ticker", key: "A", inc: 1, count: 1, sum: 1},
		{ns: "inc1", n: "ticker", key: "A", inc: 2, count: 2, sum: 3},
		{ns: "inc1", n: "count", key: "B", inc: 1, count: 1, sum: 1},
		{ns: "inc1", n: "count", key: "B", inc: 1, count: 2, sum: 2},
		{ns: "inc1", n: "ticker", key: "B", inc: 1, count: 1, sum: 1},
		{ns: "inc1", n: "ticker", key: "B", inc: 2, count: 2, sum: 3},
		{ns: "inc2", n: "count", key: "A", inc: 1, count: 1, sum: 1},
		{ns: "inc2", n: "count", key: "A", inc: 1, count: 2, sum: 2},
		{ns: "inc2", n: "ticker", key: "A", inc: 1, count: 1, sum: 1},
		{ns: "inc2", n: "ticker", key: "A", inc: 2, count: 2, sum: 3},
		{ns: "inc2", n: "count", key: "B", inc: 1, count: 1, sum: 1},
		{ns: "inc2", n: "count", key: "B", inc: 1, count: 2, sum: 2},
		{ns: "inc2", n: "ticker", key: "B", inc: 1, count: 1, sum: 1},
		{ns: "inc2", n: "ticker", key: "B", inc: 2, count: 2, sum: 3},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("add %d to %s.%s[%q] count: %d sum: %d", test.inc, test.ns, test.n, test.key, test.count, test.sum),
			func(t *testing.T) {
				m := GetCounter(test.ns, test.n)
				ctx := ctxWith(bID, test.key)
				m.Inc(ctx, test.inc)

				key := metricKey{name: metricName{namespace: test.ns, name: test.n}, bundle: bID, ptransform: test.key}
				q, ok := counters.Load(key)
				if !ok {
					t.Fatalf("Unable to find Counter for key %v", key)
				}
				d := q.(*distribution)
				if got, want := d.t, counterType; got != want {
					t.Errorf("GetCounter(%q,%q) type is %s, want %s", test.ns, test.n, got, want)
				}
				if got, want := d.count, test.count; got != want {
					t.Errorf("GetCounter(%q,%q).Inc(%s, %d) d.count got %v, want %v", test.ns, test.n, test.key, test.inc, got, want)
				}
				if got, want := d.sum, test.sum; got != want {
					t.Errorf("GetCounter(%q,%q).Inc(%s, %d) d.sum got %v, want %v", test.ns, test.n, test.key, test.inc, got, want)
				}
			})
	}
}

func TestCounter_Dec(t *testing.T) {
	tests := []struct {
		ns, n, key string // Counter name and PTransform context
		dec        int64
		count, sum int64 // Internal variables to check
	}{
		{ns: "dec1", n: "count", key: "A", dec: 1, count: 1, sum: -1},
		{ns: "dec1", n: "count", key: "A", dec: 1, count: 2, sum: -2},
		{ns: "dec1", n: "ticker", key: "A", dec: 1, count: 1, sum: -1},
		{ns: "dec1", n: "ticker", key: "A", dec: 2, count: 2, sum: -3},
		{ns: "dec1", n: "count", key: "B", dec: 1, count: 1, sum: -1},
		{ns: "dec1", n: "count", key: "B", dec: 1, count: 2, sum: -2},
		{ns: "dec1", n: "ticker", key: "B", dec: 1, count: 1, sum: -1},
		{ns: "dec1", n: "ticker", key: "B", dec: 2, count: 2, sum: -3},
		{ns: "dec2", n: "count", key: "A", dec: 1, count: 1, sum: -1},
		{ns: "dec2", n: "count", key: "A", dec: 1, count: 2, sum: -2},
		{ns: "dec2", n: "ticker", key: "A", dec: 1, count: 1, sum: -1},
		{ns: "dec2", n: "ticker", key: "A", dec: 2, count: 2, sum: -3},
		{ns: "dec2", n: "count", key: "B", dec: 1, count: 1, sum: -1},
		{ns: "dec2", n: "count", key: "B", dec: 1, count: 2, sum: -2},
		{ns: "dec2", n: "ticker", key: "B", dec: 1, count: 1, sum: -1},
		{ns: "dec2", n: "ticker", key: "B", dec: 2, count: 2, sum: -3},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("subtract %d to %s.%s[%q] count: %d sum: %d", test.dec, test.ns, test.n, test.key, test.count, test.sum),
			func(t *testing.T) {
				m := GetCounter(test.ns, test.n)
				ctx := ctxWith(bID, test.key)
				m.Dec(ctx, test.dec)

				key := metricKey{name: metricName{namespace: test.ns, name: test.n}, bundle: bID, ptransform: test.key}
				q, ok := counters.Load(key)
				if !ok {
					t.Fatalf("Unable to find Counter for key %v", key)
				}
				d := q.(*distribution)
				if got, want := d.t, counterType; got != want {
					t.Errorf("GetCounter(%q,%q) type is %s, want %s", test.ns, test.n, got, want)
				}
				if got, want := d.count, test.count; got != want {
					t.Errorf("GetCounter(%q,%q).Dec(%s, %d) d.count got %v, want %v", test.ns, test.n, test.key, test.dec, got, want)
				}
				if got, want := d.sum, test.sum; got != want {
					t.Errorf("GetCounter(%q,%q).Dec(%s, %d) d.sum got %v, want %v", test.ns, test.n, test.key, test.dec, got, want)
				}
			})
	}
}

func TestDistribution_Update(t *testing.T) {
	tests := []struct {
		ns, n, key           string // Gauge name and PTransform context
		v                    int64
		count, sum, min, max int64 // Internal variables to check
	}{
		{ns: "update1", n: "latency", key: "A", v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update1", n: "latency", key: "A", v: 1, count: 2, sum: 2, min: 1, max: 1},
		{ns: "update1", n: "latency", key: "A", v: 1, count: 3, sum: 3, min: 1, max: 1},
		{ns: "update1", n: "size", key: "A", v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update1", n: "size", key: "A", v: 2, count: 2, sum: 3, min: 1, max: 2},
		{ns: "update1", n: "size", key: "A", v: 3, count: 3, sum: 6, min: 1, max: 3},
		{ns: "update1", n: "size", key: "A", v: -4, count: 4, sum: 2, min: -4, max: 3},
		{ns: "update1", n: "size", key: "A", v: 1, count: 5, sum: 3, min: -4, max: 3},
		{ns: "update1", n: "latency", key: "B", v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update1", n: "latency", key: "B", v: 1, count: 2, sum: 2, min: 1, max: 1},
		{ns: "update1", n: "size", key: "B", v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update1", n: "size", key: "B", v: 2, count: 2, sum: 3, min: 1, max: 2},
		{ns: "update2", n: "latency", key: "A", v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update2", n: "latency", key: "A", v: 1, count: 2, sum: 2, min: 1, max: 1},
		{ns: "update2", n: "size", key: "A", v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update2", n: "size", key: "A", v: 2, count: 2, sum: 3, min: 1, max: 2},
		{ns: "update2", n: "latency", key: "B", v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update2", n: "latency", key: "B", v: 1, count: 2, sum: 2, min: 1, max: 1},
		{ns: "update2", n: "size", key: "B", v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update2", n: "size", key: "B", v: 2, count: 2, sum: 3, min: 1, max: 2},
		{ns: "update1", n: "size", key: "A", v: 1, count: 6, sum: 4, min: -4, max: 3},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("add %d to %s.%s[%q] count: %d sum: %d", test.v, test.ns, test.n, test.key, test.count, test.sum),
			func(t *testing.T) {
				m := GetDistribution(test.ns, test.n)
				ctx := ctxWith(bID, test.key)
				m.Update(ctx, test.v)

				key := metricKey{name: metricName{namespace: test.ns, name: test.n}, bundle: bID, ptransform: test.key}
				q, ok := distributions.Load(key)
				if !ok {
					t.Fatalf("Unable to find Distribution for key %v", key)
				}
				d := q.(*distribution)
				if got, want := d.t, distributionType; got != want {
					t.Errorf("GetDistribution(%q,%q) type is %s, want %s", test.ns, test.n, got, want)
				}
				if got, want := d.count, test.count; got != want {
					t.Errorf("GetDistribution(%q,%q).Update(%s, %d) d.count got %v, want %v", test.ns, test.n, test.key, test.v, got, want)
				}
				if got, want := d.sum, test.sum; got != want {
					t.Errorf("GetDistribution(%q,%q).Update(%s, %d) d.sum got %v, want %v", test.ns, test.n, test.key, test.v, got, want)
				}
				if got, want := d.min, test.min; got != want {
					t.Errorf("GetDistribution(%q,%q).Update(%s, %d) d.min got %v, want %v", test.ns, test.n, test.key, test.v, got, want)
				}
				if got, want := d.max, test.max; got != want {
					t.Errorf("GetDistribution(%q,%q).Update(%s, %d) d.max got %v, want %v", test.ns, test.n, test.key, test.v, got, want)
				}
			})
	}
}

type testclock time.Time

func (t testclock) Now() time.Time {
	return time.Time(t)
}

func TestGauge_Set(t *testing.T) {
	tests := []struct {
		ns, n, key string // Gauge name and PTransform context
		v          int64
		t          time.Time
	}{
		{ns: "set1", n: "load", key: "A", v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "load", key: "A", v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "speed", key: "A", v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "speed", key: "A", v: 2, t: time.Unix(0, 0)},
		{ns: "set1", n: "load", key: "B", v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "load", key: "B", v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "speed", key: "B", v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "speed", key: "B", v: 2, t: time.Unix(0, 0)},
		{ns: "set2", n: "load", key: "A", v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "load", key: "A", v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "speed", key: "A", v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "speed", key: "A", v: 2, t: time.Unix(0, 0)},
		{ns: "set2", n: "load", key: "B", v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "load", key: "B", v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "speed", key: "B", v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "speed", key: "B", v: 2, t: time.Unix(0, 0)},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("set %s.%s[%q] to %d at %v", test.ns, test.n, test.key, test.v, test.t),
			func(t *testing.T) {
				m := GetGauge(test.ns, test.n)
				ctx := ctxWith(bID, test.key)
				clock = testclock(test.t)
				m.Set(ctx, test.v)

				key := metricKey{name: metricName{namespace: test.ns, name: test.n}, bundle: bID, ptransform: test.key}
				q, ok := gauges.Load(key)
				if !ok {
					t.Fatalf("Unable to find Gauge for key %v", key)
				}
				g := q.(*gauge)
				if got, want := g.v, test.v; got != want {
					t.Errorf("GetGauge(%q,%q).Set(%s, %d) g.v got %v, want %v", test.ns, test.n, test.key, test.v, got, want)
				}
				if got, want := g.t, test.t; got != want {
					t.Errorf("GetGauge(%q,%q).Set(%s, %d) t.t got %v, want %v", test.ns, test.n, test.key, test.v, got, want)
				}
			})
	}
}

func TestNameCollisions(t *testing.T) {
	ns, c, d, g := "collisions", "counter", "distribution", "gauge"
	// Checks that user code panics if a counter attempts to be defined in the same PTransform
	// Collisions are unfortunately only detectable at runtime, and only if both the initial
	// metric, and the new metric are actually used, since we don't know the context until
	// then.
	// Pre-create and use so that we have existing metrics to collide with.
	GetCounter(ns, c).Inc(ctxWith(bID, c), 1)
	GetDistribution(ns, d).Update(ctxWith(bID, d), 1)
	GetGauge(ns, g).Set(ctxWith(bID, g), 1)
	tests := []struct {
		existing, new metricType
	}{
		{existing: counterType, new: counterType},
		{existing: counterType, new: distributionType},
		{existing: counterType, new: gaugeType},
		{existing: distributionType, new: counterType},
		{existing: distributionType, new: distributionType},
		{existing: distributionType, new: gaugeType},
		{existing: gaugeType, new: counterType},
		{existing: gaugeType, new: distributionType},
		{existing: gaugeType, new: gaugeType},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%s name collides with %s", test.existing, test.new),
			func(t *testing.T) {
				defer func() {
					if test.existing != test.new {
						if e := recover(); e != nil {
							t.Logf("panic caught re-using a name between a %s, and a %s", test.existing, test.new)
							return
						}
						t.Error("panic expected")
					} else {
						t.Log("reusing names is fine when the metrics the same type:", test.existing, test.new)
					}
				}()
				var name string
				switch test.existing {
				case counterType:
					name = c
				case distributionType:
					name = d
				case gaugeType:
					name = g
				default:
					t.Fatalf("unknown existing metricType with value: %v", int(test.existing))
				}
				switch test.new {
				case counterType:
					GetCounter(ns, name).Inc(ctxWith(bID, name), 1)
				case distributionType:
					GetDistribution(ns, name).Update(ctxWith(bID, name), 1)
				case gaugeType:
					GetGauge(ns, name).Set(ctxWith(bID, name), 1)
				default:
					t.Fatalf("unknown new metricType with value: %v", int(test.new))
				}

			})
	}
}

func TestClearBundleData(t *testing.T) {
	Clear()
	dump := func(t *testing.T) {
		dumpTo(func(format string, args ...interface{}) {
			t.Logf(format, args...)
		})
	}
	pt, c, d, g := "clear.bundle.data", "counter", "distribution", "gauge"
	aBundleID := "aBID"
	otherBundleID := "otherBID"
	GetCounter(pt, c).Inc(ctxWith(aBundleID, pt), 1)
	GetDistribution(pt, d).Update(ctxWith(aBundleID, pt), 1)
	GetGauge(pt, g).Set(ctxWith(aBundleID, pt), 1)

	GetCounter(pt, c).Inc(ctxWith(otherBundleID, pt), 1)
	GetDistribution(pt, d).Update(ctxWith(otherBundleID, pt), 1)
	GetGauge(pt, g).Set(ctxWith(otherBundleID, pt), 1)

	initialAP := ToProto(aBundleID, pt)
	if got, want := len(initialAP), 3; got != want {
		dump(t)
		t.Fatalf("len(ToProto(%q, %q)) = %v, want %v - initialAP: %v", aBundleID, pt, got, want, initialAP)
	}
	initialOP := ToProto(otherBundleID, pt)
	if got, want := len(initialOP), 3; got != want {
		dump(t)
		t.Fatalf("len(ToProto(%q, %q)) = %v, want %v - initialOP: %v", otherBundleID, pt, got, want, initialOP)
	}

	ClearBundleData(aBundleID)

	newAP := ToProto(aBundleID, pt)
	if got, want := len(newAP), 0; got != want {
		dump(t)
		t.Fatalf("len(ToProto(%q, %q)) = %v, want %v - newAP: %v", aBundleID, pt, got, want, newAP)
	}

	newOP := ToProto(otherBundleID, pt)
	if got, want := len(newOP), 3; got != want {
		dump(t)
		t.Fatalf("len(ToProto(%q, %q)) = %v, want %v - newOP: %v", otherBundleID, pt, got, want, newOP)
	}
}

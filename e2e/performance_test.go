package e2e

import (
	"context"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Budget thresholds for the load test, parsed from the environment.
type perfBudgets struct {
	maxP99  time.Duration
	minRate float64
}

func perfBudgetsFromEnv(t *testing.T) perfBudgets {
	t.Helper()
	var b perfBudgets
	if raw := os.Getenv("ETCD_PERF_MAX_P99"); raw != "" {
		var err error
		b.maxP99, err = time.ParseDuration(raw)
		if err != nil || b.maxP99 <= 0 {
			t.Fatalf("ETCD_PERF_MAX_P99 must be a positive duration, got %q", raw)
		}
	}
	if raw := os.Getenv("ETCD_PERF_MIN_OPS_PER_SEC"); raw != "" {
		var err error
		b.minRate, err = strconv.ParseFloat(raw, 64)
		if err != nil || b.minRate <= 0 || math.IsNaN(b.minRate) || math.IsInf(b.minRate, 0) {
			t.Fatalf("ETCD_PERF_MIN_OPS_PER_SEC must be a positive finite number, got %q", raw)
		}
	}
	return b
}

// Shared load-test fixture: 1000 keys, one 1KiB value, and a helper that
// runs one op over all keys and checks the budgets.
type perfFixture struct {
	t        *testing.T
	c        *clientv3.Client
	p        string
	budgets  perfBudgets
	requests int
	workers  int
	value    string
	keys     []string
}

func newPerfFixture(t *testing.T, c *clientv3.Client, p string, budgets perfBudgets) *perfFixture {
	t.Helper()
	const requests, workers = 1000, 8
	value := strings.Repeat("x", 1024)
	keys := make([]string, requests)
	for i := range keys {
		keys[i] = p + strconv.Itoa(i)
	}
	return &perfFixture{t: t, c: c, p: p, budgets: budgets, requests: requests, workers: workers, value: value, keys: keys}
}

func (f *perfFixture) run(op func(context.Context, string) error) {
	f.t.Helper()
	p50, p95, p99, rate, err := measure(f.t.Context(), f.workers, f.keys, op)
	noError(f.t, err)
	f.t.Logf("%d requests, %d workers: %.1f ops/s, p50=%v p95=%v p99=%v",
		f.requests, f.workers, rate, p50, p95, p99)
	if f.budgets.maxP99 > 0 && p99 > f.budgets.maxP99 {
		f.t.Errorf("p99 %v exceeds budget %v", p99, f.budgets.maxP99)
	}
	if f.budgets.minRate > 0 && rate < f.budgets.minRate {
		f.t.Errorf("throughput %.1f ops/s is below budget %.1f ops/s", rate, f.budgets.minRate)
	}
}

func (f *perfFixture) putPhase() bool {
	f.t.Helper()
	complete := false
	f.t.Run("Put", func(t *testing.T) {
		f.run(func(ctx context.Context, key string) error {
			_, err := f.c.Put(ctx, key, f.value)
			return err
		})
		complete = true
	})
	return complete
}

func (f *perfFixture) txnPhase() {
	f.t.Helper()
	// k8s-style conditional write: compare the key's version, then write
	// the same key. Exercises the per-key revalidation path (compare key
	// == written key), the dominant txn shape of the apiserver.
	f.t.Run("Txn", func(t *testing.T) {
		f.run(func(ctx context.Context, key string) error {
			key = key + "/txn"
			resp, err := f.c.Txn(ctx).If(
				clientv3.Compare(clientv3.Version(key), "=", 0),
			).Then(
				clientv3.OpPut(key, f.value),
			).Else(
				clientv3.OpGet(key),
			).Commit()
			if err != nil {
				return err
			}
			if !resp.Succeeded {
				return fmt.Errorf("txn on fresh key %q did not succeed", key)
			}
			return nil
		})
	})
}

func (f *perfFixture) getPhase() {
	f.t.Helper()
	f.t.Run("Get", func(t *testing.T) {
		f.run(func(ctx context.Context, key string) error {
			r, err := f.c.Get(ctx, key)
			if err != nil {
				return err
			}
			if len(r.Kvs) != 1 || string(r.Kvs[0].Key) != key || string(r.Kvs[0].Value) != f.value {
				return fmt.Errorf("incorrect Get result for %q", key)
			}
			return nil
		})
	})
}

func (f *perfFixture) rangePhase() {
	f.t.Helper()
	f.t.Run("Range", func(t *testing.T) {
		// Self-contained prefix scan: 500 dedicated keys, 50 scans
		// bounded to 100 rows. Exercises the list/live-range pushdown.
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		const rangekeys = 500
		for i := range rangekeys {
			if _, err := f.c.Put(ctx, f.p+"r/"+strconv.Itoa(i), f.value); err != nil {
				t.Fatal(err)
			}
		}
		const scans = 50
		lat := make([]time.Duration, scans)
		for i := range scans {
			start := time.Now()
			r, err := f.c.Get(ctx, f.p+"r/", clientv3.WithPrefix(), clientv3.WithLimit(100))
			lat[i] = time.Since(start)
			noError(t, err)
			if len(r.Kvs) != 100 || r.Count != int64(rangekeys) {
				t.Fatalf("prefix scan: got %d rows count %d, want 100 rows count %d", len(r.Kvs), r.Count, rangekeys)
			}
		}
		slices.Sort(lat)
		t.Logf("%d prefix scans (limit 100 over %d keys): p50=%v p99=%v", scans, rangekeys, lat[len(lat)/2], lat[len(lat)-1])
	})
}

func (f *perfFixture) watchPhase() {
	f.t.Helper()
	f.t.Run("Watch", func(t *testing.T) {
		// Live fan-out: one watcher, 200 sequential puts, count events.
		wctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		wch := f.c.Watch(wctx, f.p+"w/", clientv3.WithPrefix(), clientv3.WithCreatedNotify())
		select {
		case r := <-wch:
			if !r.Created {
				t.Fatalf("expected watch creation response, got %+v", r)
			}
		case <-wctx.Done():
			t.Fatalf("waiting for watch creation: %v", wctx.Err())
		}
		const events = 200
		start := time.Now()
		for i := range events {
			if _, err := f.c.Put(wctx, f.p+"w/"+strconv.Itoa(i), f.value); err != nil {
				t.Fatal(err)
			}
		}
		got := 0
		for got < events {
			select {
			case r, ok := <-wch:
				if !ok {
					t.Fatalf("watch closed after %d/%d events", got, events)
				}
				if err := r.Err(); err != nil {
					t.Fatal(err)
				}
				got += len(r.Events)
			case <-wctx.Done():
				t.Fatalf("waiting for watch events: %v (got %d/%d)", wctx.Err(), got, events)
			}
		}
		elapsed := time.Since(start)
		t.Logf("%d puts + %d events: %.1f events/s, elapsed %v", events, got, float64(got)/elapsed.Seconds(), elapsed)
	})
}

func (f *perfFixture) leaseExpiryPhase() {
	f.t.Helper()
	f.t.Run("LeaseExpiry", func(t *testing.T) {
		// Mass expiry: 300 leased keys, one short TTL. Measures drain
		// time from deadline to zero keys (tick delay + delete cost).
		ctx := t.Context()
		grant, err := f.c.Grant(ctx, 2)
		noError(t, err)
		deadline := time.Now().Add(2 * time.Second)
		const leased = 300
		for i := range leased {
			if _, err := f.c.Put(ctx, f.p+"l/"+strconv.Itoa(i), f.value, clientv3.WithLease(grant.ID)); err != nil {
				t.Fatal(err)
			}
		}
		if d := time.Until(deadline); d > 0 {
			time.Sleep(d)
		}
		start := time.Now()
		for {
			r, err := f.c.Get(ctx, f.p+"l/", clientv3.WithPrefix(), clientv3.WithCountOnly())
			noError(t, err)
			if r.Count == 0 {
				break
			}
			if time.Since(start) > time.Minute {
				t.Fatalf("lease expiry did not drain 300 keys within a minute (%d remain)", r.Count)
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Logf("300-key expiry drained in %v", time.Since(start))
	})
}

func TestPerformance(t *testing.T) {
	if os.Getenv("ETCD_PERF") != "1" {
		t.Skip("set ETCD_PERF=1 to run the load test")
	}
	budgets := perfBudgetsFromEnv(t)

	targets(t, func(t *testing.T, c *clientv3.Client, p string) {
		f := newPerfFixture(t, c, p, budgets)
		if !f.putPhase() {
			return
		}
		f.txnPhase()
		f.getPhase()
		f.rangePhase()
		f.watchPhase()
		f.leaseExpiryPhase()
	})
}

func measure(parent context.Context, workers int, keys []string, op func(context.Context, string) error) (p50, p95, p99 time.Duration, rate float64, err error) {
	ctx, cancel := context.WithTimeoutCause(parent, time.Minute, fmt.Errorf("load phase exceeded one minute"))
	defer cancel()
	run, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	latencies := make([]time.Duration, len(keys))
	var wg sync.WaitGroup
	start := time.Now()
	for worker := range workers {
		wg.Go(func() {
			for i := worker; i < len(keys) && run.Err() == nil; i += workers {
				rpc, done := context.WithTimeout(run, 5*time.Second)
				sent := time.Now()
				err := op(rpc, keys[i])
				latencies[i] = time.Since(sent)
				done()
				if err != nil {
					stop(fmt.Errorf("request %d failed: %w", i, err))
					return
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	if err := context.Cause(run); err != nil {
		return 0, 0, 0, 0, err
	}
	slices.Sort(latencies)
	percentile := func(p int) time.Duration { return latencies[(len(latencies)*p+99)/100-1] }
	return percentile(50), percentile(95), percentile(99), float64(len(keys)) / elapsed.Seconds(), nil
}

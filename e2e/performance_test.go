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

func TestPerformance(t *testing.T) {
	if os.Getenv("ETCD_PERF") != "1" {
		t.Skip("set ETCD_PERF=1 to run the load test")
	}
	maxP99 := time.Duration(0)
	if raw := os.Getenv("ETCD_PERF_MAX_P99"); raw != "" {
		var err error
		maxP99, err = time.ParseDuration(raw)
		if err != nil || maxP99 <= 0 {
			t.Fatalf("ETCD_PERF_MAX_P99 must be a positive duration, got %q", raw)
		}
	}
	minRate := 0.0
	if raw := os.Getenv("ETCD_PERF_MIN_OPS_PER_SEC"); raw != "" {
		var err error
		minRate, err = strconv.ParseFloat(raw, 64)
		if err != nil || minRate <= 0 || math.IsNaN(minRate) || math.IsInf(minRate, 0) {
			t.Fatalf("ETCD_PERF_MIN_OPS_PER_SEC must be a positive finite number, got %q", raw)
		}
	}

	targets(t, func(t *testing.T, c *clientv3.Client, p string) {
		const requests, workers = 1000, 8
		value := strings.Repeat("x", 1024)
		keys := make([]string, requests)
		for i := range keys {
			keys[i] = p + strconv.Itoa(i)
		}
		run := func(t *testing.T, op func(context.Context, string) error) bool {
			t.Helper()
			p50, p95, p99, rate, err := measure(t.Context(), workers, keys, op)
			noError(t, err)
			t.Logf("%d requests, %d workers: %.1f ops/s, p50=%v p95=%v p99=%v",
				requests, workers, rate, p50, p95, p99)
			if maxP99 > 0 && p99 > maxP99 {
				t.Errorf("p99 %v exceeds budget %v", p99, maxP99)
			}
			if minRate > 0 && rate < minRate {
				t.Errorf("throughput %.1f ops/s is below budget %.1f ops/s", rate, minRate)
			}
			return true
		}
		writesComplete := false
		t.Run("Put", func(t *testing.T) {
			writesComplete = run(t, func(ctx context.Context, key string) error {
				_, err := c.Put(ctx, key, value)
				return err
			})
		})
		if !writesComplete {
			return
		}
		t.Run("Get", func(t *testing.T) {
			run(t, func(ctx context.Context, key string) error {
				r, err := c.Get(ctx, key)
				if err != nil {
					return err
				}
				if len(r.Kvs) != 1 || string(r.Kvs[0].Key) != key || string(r.Kvs[0].Value) != value {
					return fmt.Errorf("incorrect Get result for %q", key)
				}
				return nil
			})
		})
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

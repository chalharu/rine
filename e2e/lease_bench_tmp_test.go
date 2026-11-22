package e2e

// Temporary lease micro-benchmark (deleted after measurement).
// Mirrors the 2026-11-21 lease bench: Grant 500x8, PutWithLease 500x8 on
// one lease, Keepalive 200 sequential round trips on one raw stream,
// TimeToLive 500x8 with attached keys, Revoke 200 (2 keys each) x8,
// Expiry drain of 300 keys at TTL 2s.

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func mustCli(t *testing.T) (*clientv3.Client, string) {
	t.Helper()
	ep := os.Getenv("ETCD_ENDPOINT")
	if ep == "" {
		t.Skip("set ETCD_ENDPOINT to an isolated test server")
	}
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: 5 * time.Second})
	noError(t, err)
	t.Cleanup(func() { noError(t, c.Close()) })
	return c, fmt.Sprintf("/lb-%d/", time.Now().UnixNano())
}

func lbMeasure(t *testing.T, workers int, n int, op func(context.Context, int) error) (rate float64, p50, p95, p99 time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeoutCause(t.Context(), time.Minute, fmt.Errorf("lease bench phase exceeded one minute"))
	defer cancel()
	lats := make([]time.Duration, n)
	var failed atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < n; i += workers {
				rpc, done := context.WithTimeout(ctx, 5*time.Second)
				sent := time.Now()
				if err := op(rpc, i); err != nil {
					failed.Add(1)
				} else {
					lats[i] = time.Since(sent)
				}
				done()
			}
		}(w)
	}
	wg.Wait()
	if failed.Load() > 0 {
		t.Fatalf("%d ops failed", failed.Load())
	}
	elapsed := time.Since(start)
	slices.Sort(lats)
	pct := func(p int) time.Duration { return lats[(len(lats)*p+99)/100-1] }
	return float64(n) / elapsed.Seconds(), pct(50), pct(95), pct(99)
}

func TestLeaseBench(t *testing.T) {
	c, p := mustCli(t)
	ctx := t.Context()
	value := strings.Repeat("x", 1024)

	t.Run("Grant", func(t *testing.T) {
		rate, p50, p95, p99 := lbMeasure(t, 8, 500, func(ctx context.Context, i int) error {
			_, err := c.Grant(ctx, 100)
			return err
		})
		t.Logf("500 grants, 8 workers: %.1f ops/s, p50=%v p95=%v p99=%v", rate, p50, p95, p99)
	})

	var lease clientv3.LeaseID
	t.Run("PutWithLease", func(t *testing.T) {
		g, err := c.Grant(ctx, 3600)
		noError(t, err)
		lease = g.ID
		rate, p50, p95, p99 := lbMeasure(t, 8, 500, func(ctx context.Context, i int) error {
			_, err := c.Put(ctx, p+"a/"+strconv.Itoa(i), value, clientv3.WithLease(lease))
			return err
		})
		t.Logf("500 leased puts, 8 workers: %.1f ops/s, p50=%v p95=%v p99=%v", rate, p50, p95, p99)
	})

	t.Run("Keepalive", func(t *testing.T) {
		// Sequential round trips on one raw stream.
		stream, err := pb.NewLeaseClient(c.ActiveConnection()).LeaseKeepAlive(ctx)
		noError(t, err)
		lats := make([]time.Duration, 200)
		for i := range 200 {
			sent := time.Now()
			err = stream.Send(&pb.LeaseKeepAliveRequest{ID: int64(lease)})
			noError(t, err)
			_, err = stream.Recv()
			noError(t, err)
			lats[i] = time.Since(sent)
		}
		slices.Sort(lats)
		t.Logf("200 keepalive round trips, sequential single stream: p50=%v", lats[100])
	})

	t.Run("TimeToLive", func(t *testing.T) {
		r, err := c.TimeToLive(ctx, lease, clientv3.WithAttachedKeys())
		noError(t, err)
		if int64(len(r.Keys)) != 500 {
			t.Fatalf("attached keys: got %d, want 500", len(r.Keys))
		}
		if r.GrantedTTL != 3600 {
			t.Fatalf("granted TTL: got %d, want 3600", r.GrantedTTL)
		}
		rate, p50, p95, p99 := lbMeasure(t, 8, 500, func(ctx context.Context, i int) error {
			_, err := c.TimeToLive(ctx, lease, clientv3.WithAttachedKeys())
			return err
		})
		t.Logf("500 timetolive (attached keys), 8 workers: %.1f ops/s, p50=%v p95=%v p99=%v", rate, p50, p95, p99)
	})

	t.Run("Revoke", func(t *testing.T) {
		ids := make([]clientv3.LeaseID, 200)
		for i := range ids {
			g, err := c.Grant(ctx, 3600)
			noError(t, err)
			ids[i] = g.ID
			for k := range 2 {
				_, err = c.Put(ctx, fmt.Sprintf("%sr/%d/%d", p, i, k), value, clientv3.WithLease(g.ID))
				noError(t, err)
			}
		}
		rate, p50, p95, p99 := lbMeasure(t, 8, 200, func(ctx context.Context, i int) error {
			_, err := c.Revoke(ctx, ids[i])
			return err
		})
		t.Logf("200 revokes x2 keys, 8 workers: %.1f ops/s, p50=%v p95=%v p99=%v", rate, p50, p95, p99)
	})

	t.Run("Expiry", func(t *testing.T) {
		g, err := c.Grant(ctx, 2)
		noError(t, err)
		stop := keepAliveDuring(t, ctx, c, g.ID)
		for i := range 300 {
			_, err = c.Put(ctx, p+"e/"+strconv.Itoa(i), value, clientv3.WithLease(g.ID))
			noError(t, err)
		}
		stop()
		deadline := time.Now().Add(2 * time.Second)
		if d := time.Until(deadline); d > 0 {
			time.Sleep(d)
		}
		start := time.Now()
		for {
			r, err := c.Get(ctx, p+"e/", clientv3.WithPrefix(), clientv3.WithCountOnly())
			noError(t, err)
			if r.Count == 0 {
				break
			}
			if time.Since(start) > time.Minute {
				t.Fatalf("expiry did not drain (%d remain)", r.Count)
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Logf("300-key expiry drained in %v", time.Since(start))
	})
}

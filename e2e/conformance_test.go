package e2e

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func targets(t *testing.T, run func(*testing.T, *clientv3.Client, string)) {
	t.Helper()
	endpoint := os.Getenv("ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set ETCD_ENDPOINT to an isolated test server")
	}
	reference := os.Getenv("ETCD_REFERENCE_ENDPOINT")
	if endpoint == reference {
		t.Fatal("ETCD_REFERENCE_ENDPOINT must be a separate server")
	}
	t.Run("target", func(t *testing.T) { runTarget(t, endpoint, run) })
	if reference != "" {
		t.Run("reference", func(t *testing.T) { runTarget(t, reference, run) })
	}
}

func runTarget(t *testing.T, endpoint string, run func(*testing.T, *clientv3.Client, string)) {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second,
	})
	noError(t, err)
	t.Cleanup(func() { noError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	_, err = client.Get(ctx, "/rine-e2e/readiness")
	cancel()
	noError(t, err)

	prefix := "/rine-e2e/" + rand.Text() + "/"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if _, err := client.Delete(ctx, prefix, clientv3.WithPrefix()); err != nil {
			t.Errorf("cleanup %q: %v", prefix, err)
		}
	})
	run(t, client, prefix)
}

func noError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func equal[T comparable](t *testing.T, field string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", field, got, want)
	}
}

func put(t *testing.T, ctx context.Context, c *clientv3.Client, key, value string, opts ...clientv3.OpOption) *clientv3.PutResponse {
	t.Helper()
	r, err := c.Put(ctx, key, value, opts...)
	noError(t, err)
	return r
}

func get(t *testing.T, ctx context.Context, c *clientv3.Client, key string, opts ...clientv3.OpOption) *clientv3.GetResponse {
	t.Helper()
	r, err := c.Get(ctx, key, opts...)
	noError(t, err)
	return r
}

func checkKV(t *testing.T, kv *mvccpb.KeyValue, key, value string, create, mod, version int64) {
	t.Helper()
	if kv == nil {
		t.Fatal("missing KV")
	}
	equal(t, "key", string(kv.Key), key)
	equal(t, "value", string(kv.Value), value)
	equal(t, "CreateRevision", kv.CreateRevision, create)
	equal(t, "ModRevision", kv.ModRevision, mod)
	equal(t, "Version", kv.Version, version)
}

func TestConformance(t *testing.T) {
	targets(t, func(t *testing.T, c *clientv3.Client, prefix string) {
		for _, tc := range []struct {
			name string
			run  func(*testing.T, context.Context, *clientv3.Client, string)
		}{
			{"KV", testKV},
			{"TxnBranches", testTxnBranches},
			{"Watch", testWatch},
			{"Lease", testLease},
			{"Compaction", testCompaction},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
				defer cancel()
				tc.run(t, ctx, c, prefix+tc.name+"/")
			})
		}
	})
}

func testKV(t *testing.T, ctx context.Context, c *clientv3.Client, p string) {
	a, b := p+"a", p+"b"
	r1 := put(t, ctx, c, a, "one").Header.Revision
	r2 := put(t, ctx, c, b, "two").Header.Revision
	update := put(t, ctx, c, a, "three", clientv3.WithPrevKV())
	r3 := update.Header.Revision
	equal(t, "second put revision", r2, r1+1)
	equal(t, "update revision", r3, r2+1)
	checkKV(t, update.PrevKv, a, "one", r1, r1, 1)

	all := get(t, ctx, c, p, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	equal(t, "prefix count", all.Count, int64(2))
	equal(t, "prefix size", len(all.Kvs), 2)
	checkKV(t, all.Kvs[0], a, "three", r1, r3, 2)
	checkKV(t, all.Kvs[1], b, "two", r2, r2, 1)

	bounded := get(t, ctx, c, a, clientv3.WithRange(b))
	equal(t, "half-open range size", len(bounded.Kvs), 1)
	checkKV(t, bounded.Kvs[0], a, "three", r1, r3, 2)

	deleted, err := c.Delete(ctx, a, clientv3.WithRange(b), clientv3.WithPrevKV())
	noError(t, err)
	equal(t, "deleted count", deleted.Deleted, int64(1))
	equal(t, "delete revision", deleted.Header.Revision, r3+1)
	equal(t, "delete previous size", len(deleted.PrevKvs), 1)
	checkKV(t, deleted.PrevKvs[0], a, "three", r1, r3, 2)
	equal(t, "deleted key count", get(t, ctx, c, a).Count, int64(0))
	equal(t, "untouched key count", get(t, ctx, c, b).Count, int64(1))
}

func testTxnBranches(t *testing.T, ctx context.Context, c *clientv3.Client, p string) {
	key, marker := p+"key", p+"marker"
	create := put(t, ctx, c, key, "one").Header.Revision
	mod := put(t, ctx, c, key, "two").Header.Revision
	compares := []clientv3.Cmp{
		clientv3.Compare(clientv3.Version(key), "=", 2),
		clientv3.Compare(clientv3.CreateRevision(key), "=", create),
		clientv3.Compare(clientv3.ModRevision(key), "=", mod),
		clientv3.Compare(clientv3.Value(key), "=", "two"),
	}
	wrong := []clientv3.Cmp{
		clientv3.Compare(clientv3.Version(key), "=", 1),
		clientv3.Compare(clientv3.CreateRevision(key), "=", mod),
		clientv3.Compare(clientv3.ModRevision(key), "=", create),
		clientv3.Compare(clientv3.Value(key), "=", "one"),
	}
	for i, cmp := range wrong {
		checks := append([]clientv3.Cmp(nil), compares...)
		checks[i] = cmp
		r, err := c.Txn(ctx).If(checks...).Then(clientv3.OpPut(marker, "wrong branch")).Else(clientv3.OpGet(key)).Commit()
		noError(t, err)
		equal(t, "Succeeded with a false compare", r.Succeeded, false)
		equal(t, "Else response count", len(r.Responses), 1)
		read := r.Responses[0].GetResponseRange()
		if read == nil {
			t.Fatalf("compare %d: missing Else range response", i)
		}
		equal(t, "Else range size", len(read.Kvs), 1)
		checkKV(t, read.Kvs[0], key, "two", create, mod, 2)
	}
	equal(t, "marker absent after Else", get(t, ctx, c, marker).Count, int64(0))

	r, err := c.Txn(ctx).If(compares...).Then(clientv3.OpPut(marker, "ok"), clientv3.OpGet(key)).
		Else(clientv3.OpDelete(key)).Commit()
	noError(t, err)
	equal(t, "Succeeded with all compares true", r.Succeeded, true)
	equal(t, "Then response count", len(r.Responses), 2)
	write, read := r.Responses[0].GetResponsePut(), r.Responses[1].GetResponseRange()
	if write == nil || read == nil {
		t.Fatalf("Then responses: got %v, want Put then Range", r.Responses)
	}
	equal(t, "Then put revision", r.Header.Revision, mod+1)
	equal(t, "Then range size", len(read.Kvs), 1)
	checkKV(t, read.Kvs[0], key, "two", create, mod, 2)
	marked := get(t, ctx, c, marker)
	equal(t, "marker size", len(marked.Kvs), 1)
	checkKV(t, marked.Kvs[0], marker, "ok", r.Header.Revision, r.Header.Revision, 1)
}

func receive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("stream closed before expected response")
		}
		return v
	case <-ctx.Done():
		t.Fatalf("waiting for stream response: %v", ctx.Err())
	}
	panic("unreachable")
}

func watchLive(t *testing.T, ctx context.Context, c *clientv3.Client, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	t.Helper()
	ch := c.Watch(ctx, key, append(opts, clientv3.WithCreatedNotify())...)
	r := receive(t, ctx, ch)
	noError(t, r.Err())
	equal(t, "watch created", r.Created, true)
	return ch
}

func watchEvents(t *testing.T, ctx context.Context, ch clientv3.WatchChan, n int) []*clientv3.Event {
	t.Helper()
	var events []*clientv3.Event
	for len(events) < n {
		r := receive(t, ctx, ch)
		noError(t, r.Err())
		events = append(events, r.Events...)
	}
	equal(t, "event count", len(events), n)
	return events
}

func testWatch(t *testing.T, ctx context.Context, c *clientv3.Client, p string) {
	live := watchLive(t, ctx, c, p+"in/", clientv3.WithPrefix())
	put(t, ctx, c, p+"outside", "ignored")
	a, b := p+"in/a", p+"in/b"
	r1 := put(t, ctx, c, a, "one").Header.Revision
	r2 := put(t, ctx, c, b, "two").Header.Revision
	events := watchEvents(t, ctx, live, 2)
	checkPuts := func(events []*clientv3.Event) {
		t.Helper()
		equal(t, "first event type", events[0].Type, mvccpb.PUT)
		equal(t, "second event type", events[1].Type, mvccpb.PUT)
		checkKV(t, events[0].Kv, a, "one", r1, r1, 1)
		checkKV(t, events[1].Kv, b, "two", r2, r2, 1)
	}
	checkPuts(events)
	replay := c.Watch(ctx, p+"in/", clientv3.WithPrefix(), clientv3.WithRev(r1))
	checkPuts(watchEvents(t, ctx, replay, 2))
}

func testLease(t *testing.T, ctx context.Context, c *clientv3.Client, p string) {
	t.Run("KeepAliveAndRevoke", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		grant, err := c.Grant(ctx, 3)
		noError(t, err)
		until := time.Now().Add(time.Duration(grant.TTL)*time.Second + 250*time.Millisecond)
		key := p + "kept"
		put(t, ctx, c, key, "value", clientv3.WithLease(grant.ID))
		ch := watchLive(t, ctx, c, key, clientv3.WithPrevKV())
		keep, err := c.KeepAlive(ctx, grant.ID)
		noError(t, err)
		responses := 0
		for responses < 2 || time.Now().Before(until) {
			r := receive(t, ctx, keep)
			equal(t, "keepalive lease ID", r.ID, grant.ID)
			if r.TTL <= 0 {
				t.Fatalf("non-positive keepalive TTL: %d", r.TTL)
			}
			responses++
		}
		ttl, err := c.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
		noError(t, err)
		equal(t, "attached key count", len(ttl.Keys), 1)
		equal(t, "attached key", string(ttl.Keys[0]), key)
		equal(t, "key present beyond original TTL", get(t, ctx, c, key).Count, int64(1))
		_, err = c.Revoke(ctx, grant.ID)
		noError(t, err)
		checkLeaseDeletion(t, ctx, c, ch, key, grant.ID)
	})
	t.Run("Expiry", func(t *testing.T) {
		key := p + "expired"
		ch := watchLive(t, ctx, c, key, clientv3.WithPrevKV(), clientv3.WithFilterPut())
		grant, err := c.Grant(ctx, 2)
		noError(t, err)
		put(t, ctx, c, key, "value", clientv3.WithLease(grant.ID))
		checkLeaseDeletion(t, ctx, c, ch, key, grant.ID)
	})
}

func checkLeaseDeletion(t *testing.T, ctx context.Context, c *clientv3.Client, ch clientv3.WatchChan, key string, id clientv3.LeaseID) {
	t.Helper()
	ev := watchEvents(t, ctx, ch, 1)[0]
	equal(t, "lease event type", ev.Type, mvccpb.DELETE)
	equal(t, "deleted key", string(ev.Kv.Key), key)
	if ev.PrevKv == nil {
		t.Fatal("missing previous leased KV")
	}
	equal(t, "previous lease ID", ev.PrevKv.Lease, int64(id))
	equal(t, "key gone", get(t, ctx, c, key).Count, int64(0))
}

func testCompaction(t *testing.T, ctx context.Context, c *clientv3.Client, p string) {
	key := p + "key"
	old := put(t, ctx, c, key, "old").Header.Revision
	point := put(t, ctx, c, key, "point").Header.Revision
	newer := put(t, ctx, c, key, "new").Header.Revision
	_, err := c.Compact(ctx, point)
	noError(t, err)
	latest := get(t, ctx, c, key).Header.Revision
	rawKV := etcdserverpb.NewKVClient(c.ActiveConnection())
	_, err = rawKV.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(key), Revision: old})
	equal(t, "compacted Range gRPC code", status.Code(err), codes.OutOfRange)
	equal(t, "compacted Range etcd error", rpctypes.Error(err), rpctypes.ErrCompacted)
	_, err = rawKV.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: latest + 1})
	equal(t, "future Compact gRPC code", status.Code(err), codes.OutOfRange)
	equal(t, "future Compact etcd error", rpctypes.Error(err), rpctypes.ErrFutureRev)

	ch := c.Watch(ctx, key, clientv3.WithRev(old))
	r := receive(t, ctx, ch)
	equal(t, "compacted watch canceled", r.Canceled, true)
	equal(t, "watch compact revision", r.CompactRevision, point)
	if !errors.Is(r.Err(), rpctypes.ErrCompacted) {
		t.Fatalf("compacted watch error: got %v, want %v", r.Err(), rpctypes.ErrCompacted)
	}
	rng := get(t, ctx, c, key, clientv3.WithRev(newer))
	equal(t, "post-compaction range size", len(rng.Kvs), 1)
	checkKV(t, rng.Kvs[0], key, "new", old, newer, 3)
}

func TestExtendedConformance(t *testing.T) {
	if os.Getenv("ETCD_EXTENDED") != "1" {
		t.Skip("set ETCD_EXTENDED=1 to check additional upstream semantics")
	}
	targets(t, func(t *testing.T, c *clientv3.Client, p string) {
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		t.Run("RecreateKey", func(t *testing.T) {
			key := p + "recreate"
			put(t, ctx, c, key, "old")
			deleted, err := c.Delete(ctx, key)
			noError(t, err)
			recreated := put(t, ctx, c, key, "new").Header.Revision
			equal(t, "recreate revision", recreated, deleted.Header.Revision+1)
			r := get(t, ctx, c, key)
			equal(t, "recreated key count", len(r.Kvs), 1)
			checkKV(t, r.Kvs[0], key, "new", recreated, recreated, 1)
		})
		t.Run("HistoricalRange", func(t *testing.T) {
			key := p + "history"
			first := put(t, ctx, c, key, "old").Header.Revision
			put(t, ctx, c, key, "new")
			r := get(t, ctx, c, key, clientv3.WithRev(first))
			equal(t, "historical range size", len(r.Kvs), 1)
			checkKV(t, r.Kvs[0], key, "old", first, first, 1)
			marker := p + "history-marker"
			point := put(t, ctx, c, marker, "point").Header.Revision
			put(t, ctx, c, marker, "later")
			_, err := c.Compact(ctx, point)
			noError(t, err)
			r = get(t, ctx, c, key, clientv3.WithRev(point))
			equal(t, "historical range at compaction point", len(r.Kvs), 1)
			equal(t, "unchanged key survives compaction", string(r.Kvs[0].Value), "new")
		})
		t.Run("WatchReplayAfterOverwrite", func(t *testing.T) {
			key := p + "replay"
			first := put(t, ctx, c, key, "old").Header.Revision
			second := put(t, ctx, c, key, "new").Header.Revision
			ch := c.Watch(ctx, key, clientv3.WithRev(first), clientv3.WithPrevKV())
			events := watchEvents(t, ctx, ch, 2)
			equal(t, "first event type", events[0].Type, mvccpb.PUT)
			checkKV(t, events[0].Kv, key, "old", first, first, 1)
			equal(t, "second event type", events[1].Type, mvccpb.PUT)
			checkKV(t, events[1].Kv, key, "new", first, second, 2)
			checkKV(t, events[1].PrevKv, key, "old", first, first, 1)
		})
		t.Run("TxnSingleRevision", func(t *testing.T) {
			a, b := p+"tx/a", p+"tx/b"
			ch := watchLive(t, ctx, c, p+"tx/", clientv3.WithPrefix())
			before := get(t, ctx, c, a).Header.Revision
			r, err := c.Txn(ctx).If(clientv3.Compare(clientv3.Version(a), "=", 0)).
				Then(clientv3.OpPut(a, "one"), clientv3.OpPut(b, "two")).
				Else(clientv3.OpGet(a)).Commit()
			noError(t, err)
			equal(t, "Succeeded", r.Succeeded, true)
			equal(t, "response count", len(r.Responses), 2)
			equal(t, "transaction revision increment", r.Header.Revision, before+1)
			for i, key := range []string{a, b} {
				if r.Responses[i].GetResponsePut() == nil {
					t.Fatalf("response %d is not a Put", i)
				}
				got := get(t, ctx, c, key)
				equal(t, "key count", len(got.Kvs), 1)
				checkKV(t, got.Kvs[0], key, []string{"one", "two"}[i], r.Header.Revision, r.Header.Revision, 1)
			}
			events := watchEvents(t, ctx, ch, 2)
			for i, key := range []string{a, b} {
				checkKV(t, events[i].Kv, key, []string{"one", "two"}[i], r.Header.Revision, r.Header.Revision, 1)
			}
		})
		t.Run("TxnRollback", func(t *testing.T) {
			key := p + "rollback"
			before := get(t, ctx, c, key).Header.Revision
			_, err := c.Txn(ctx).Then(
				clientv3.OpPut(key, "must not commit"),
				clientv3.OpPut(key+"-leased", "value", clientv3.WithLease(123456)),
			).Commit()
			if err == nil {
				t.Fatal("transaction unexpectedly succeeded with an unknown lease")
			}
			got := get(t, ctx, c, key)
			equal(t, "failed transaction revision", got.Header.Revision, before)
			equal(t, "first write was not committed", got.Count, int64(0))
		})
		t.Run("CompactionDoesNotAdvanceRevision", func(t *testing.T) {
			key := p + "compact"
			point := put(t, ctx, c, key, "one").Header.Revision
			latest := put(t, ctx, c, key, "two").Header.Revision
			_, err := c.Compact(ctx, point)
			noError(t, err)
			equal(t, "latest revision after compaction", get(t, ctx, c, key).Header.Revision, latest)
		})
		t.Run("FutureRange", func(t *testing.T) {
			key := p + "future"
			latest := put(t, ctx, c, key, "one").Header.Revision
			rawKV := etcdserverpb.NewKVClient(c.ActiveConnection())
			_, err := rawKV.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(key), Revision: latest + 1})
			equal(t, "future Range gRPC code", status.Code(err), codes.OutOfRange)
			equal(t, "future Range etcd error", rpctypes.Error(err), rpctypes.ErrFutureRev)
		})
	})
}

# rine

An etcd-API-compatible key-value store backed by Oracle Database.

- `crates/rine-etcd`: etcd API (KV, Watch, Lease, Maintenance, Cluster) over a
  pluggable `Backend` trait object (`Arc<dyn Backend>`); includes an in-memory
  `MemoryBackend`.
- `crates/rine-store-executor`: shared `SqlBackend` over a SQL executor
  and dialect abstraction.
- `crates/rine-store-oracle`: `OracleDialect`, the live `OracleDb` executor
  (official `oracledb` driver), and a live XE integration test.
- `crates/rine-store-sqlite`: same backend on SQLite (`SqliteDialect` +
  `SqliteDb` over rusqlite) — runs locally without any database server.
- `crates/rine`: server binary (`memory`, `sqlite` or `oracle` backend,
  selected with `--backend`).
- Storage drivers: the official [`oracledb`](https://crates.io/crates/oracledb)
  crate (thin mode, no Oracle Client libraries needed) and
  [`rusqlite`](https://crates.io/crates/rusqlite) (bundled SQLite).

## Run

```sh
# in-memory backend
cargo run -p rine

# SQLite backend (file is created on demand, schema auto-created)
RINE_BACKEND=sqlite RINE_SQLITE_PATH=/tmp/rine.db cargo run -p rine

# Oracle backend (Easy Connect, full descriptor, or TNS alias)
RINE_BACKEND=oracle \
RINE_ORACLE_CONNECT=127.0.0.1:1521/XEPDB1 \
RINE_ORACLE_USER=rine \
RINE_ORACLE_PASSWORD=secret \
cargo run -p rine
```

File-backed SQLite uses WAL with `synchronous=FULL`: acknowledged writes
remain durable, and the database directory must support SQLite's `-wal`
and `-shm` sidecar files. Use SQLite's backup API (or stop the server
before copying the database) rather than copying the main file alone
while the server is running.

On Linux/musl, the `rine` binary uses mimalloc to reduce allocation overhead
under concurrent gRPC traffic. Other targets keep their system allocator.

Oracle serves latest range reads as rows plus count in one read
transaction (rows in index order with LIMIT, count from a covering index).
Single-key writes of 1–2,000 byte values can validate the predecessor and
commit in one Oracle call. Writes still serialize across nodes and wait for
durable commit; deploy Rine close to Oracle to reduce the remaining latency.

With an Oracle wallet (`tnsnames.ora` + `ewallet.pem`) and a TNS alias:

```sh
RINE_BACKEND=oracle \
RINE_ORACLE_CONNECT=adb_high \
RINE_WALLET_DIR=/path/to/wallet \
RINE_ORACLE_USER=admin \
RINE_ORACLE_PASSWORD=secret \
cargo run -p rine
```

## TLS / mTLS

Give `--cert-file` and `--key-file` (PEM) to serve gRPC over TLS.
Add `--client-cert-auth` with `--trusted-ca-file` (PEM) to require
clients to present a certificate signed by that CA (mTLS, etcd's
`--client-cert-auth` + `--trusted-ca-file` equivalent):

```sh
cargo run -p rine -- \
  --cert-file server.pem --key-file server-key.pem \
  --client-cert-auth --trusted-ca-file ca.pem
```

The official client then needs the CA and (for mTLS) a client
certificate, e.g. `etcdctl --cacert=ca.pem --cert=client.pem
--key=client-key.pem --endpoints=https://127.0.0.1:2379 ...`, or set
`TLS_CA`/`TLS_CERT`/`TLS_KEY` when running `e2e/etcdctl.sh` with an
`https://` `ENDPOINT`.

Mixed plaintext + TLS on one server via `--listen-client-urls`
(etcd `--listen-client-urls` style, `RINE_LISTEN_CLIENT_URLS`).
`https://` entries serve TLS, `http://` entries stay plaintext, all
sharing one backend:

```sh
cargo run -p rine -- \
  --listen-client-urls "http://127.0.0.1:2379,https://127.0.0.1:2380" \
  --cert-file server.pem --key-file server-key.pem
```

When `--listen-client-urls` is absent, `--listen` is used as a single
plaintext listener as before.

## Test

```sh
cargo test --workspace          # no database needed
```

Live integration tests (gated by `RINE_LIVE_ORACLE=1`) run in CI against
Oracle XE, see `.github/workflows/ci.yml`:

```sh
RINE_LIVE_ORACLE=1 \
ORACLE_HOST=127.0.0.1 ORACLE_PORT=1521 ORACLE_SERVICE=XEPDB1 \
ORACLE_USER=rine ORACLE_PASSWORD=secret \
cargo test -p rine-store-oracle --test live_oracle
```

### Official-client E2E tests

Start `rine` on a fresh backend (memory, SQLite, or an isolated Oracle
schema). These tests use real gRPC/TCP via the official etcd v3.5.35
Go client and require Go 1.26 or newer. **Use only dedicated test
instances:** compaction affects the entire instance even though test
keys use unique prefixes. Do not run these suites concurrently against
the same instance.

```sh
cd e2e
ETCD_ENDPOINT=127.0.0.1:2379 \
  go test -v -count=1 -timeout=3m -run '^TestConformance$' .
```

Set `ETCD_REFERENCE_ENDPOINT` to a separate, disposable upstream etcd
instance (e.g. `127.0.0.1:12379`) to run the same assertions against both
servers. Revision checks use revisions returned by each server rather
than comparing absolute revision numbers, and Watch assertions are
independent of how the server batches events. Coverage includes KV metadata
and range operations, all four Txn comparison targets and both branches,
live and historical Watch delivery, streaming lease renewal, revocation
and expiry, and compaction errors. The `etcdctl` smoke test is still
available as `ENDPOINT=127.0.0.1:2379 ETCDCTL=/path/to/etcdctl bash e2e/etcdctl.sh`
from the repository root.

Run the stricter probes with `ETCD_EXTENDED=1` and
`-run '^TestExtendedConformance$'` (use the same endpoint configuration).
They check key recreation, historical Range and Watch replay after an
overwrite, the single-revision guarantee of a multi-write Txn, compaction
without a revision increment, rejection of future Range revisions, and
rollback on a rejected Txn.
CI runs the extended suite against SQLite, Oracle, and upstream etcd.
The suite covers the API subset listed above, not full etcd compatibility:
Auth, cluster membership changes, and other Maintenance RPCs are not
implemented. It does not certify Kubernetes integration or TLS.

### Performance tests

On an isolated test instance, preferably running a release build, run
from `e2e/`:

```sh
ETCD_ENDPOINT=127.0.0.1:2379 ETCD_PERF=1 \
  go test -v -count=1 -timeout=5m -run '^TestPerformance$' .
```

The test uses eight concurrent workers for 1,000 Put requests with
1 KiB values, followed by 1,000 Get requests that verify the returned
keys and values. It reports throughput and p50/p95/p99 end-to-end
latency per phase and fails on request errors or unexpected results.
Set `ETCD_PERF_MAX_P99` (a Go duration, e.g. `50ms`) and/or
`ETCD_PERF_MIN_OPS_PER_SEC` (e.g. `200`) to enforce budgets for each
phase. Set budgets from a baseline on comparable hardware and backend
configuration; CI runs a load smoke test without machine-dependent
thresholds. With `ETCD_REFERENCE_ENDPOINT` the same workload and
budgets apply to both servers.

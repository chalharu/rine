# rine

An etcd-API-compatible key-value store backed by Oracle Database, aimed at
running Kubernetes on Oracle Cloud Infrastructure without etcd.

- `crates/rine-etcd`: etcd API (KV, Watch, Lease, Maintenance, Cluster) over a
  pluggable `Backend` trait object (`Arc<dyn Backend>`); includes an in-memory
  `MemoryBackend`.
- `crates/rine-store-oracle`: shared kine-style `SqlBackend` over a `Dialect`
  plus the Oracle `OracleDialect`, the live `OracleDb` executor (official
  `oracledb` driver), and a `MockExec` for unit tests.
- `crates/rine-store-sqlite`: same backend on SQLite (`SqliteDialect` +
  `SqliteDb` over rusqlite) — runs locally without any database server.
- `crates/rine`: server binary (`memory`, `sqlite` or `oracle` backend,
  selected with `--backend` like kine).
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

With an Oracle wallet (`tnsnames.ora` + `ewallet.pem`) and a TNS alias:

```sh
RINE_BACKEND=oracle \
RINE_ORACLE_CONNECT=adb_high \
RINE_WALLET_DIR=/path/to/wallet \
RINE_ORACLE_USER=admin \
RINE_ORACLE_PASSWORD=secret \
cargo run -p rine
```

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

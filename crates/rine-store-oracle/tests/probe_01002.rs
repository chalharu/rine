//! Temporary probe for ORA-01002 on multi-row BLOB fetches.
//!
//! Runs only with `RINE_LIVE_ORACLE=1`. Prints OK/ERR per case so one CI run
//! identifies the trigger. Uses `oracledb` directly (not `OracleDb`).

use oracledb::Connection;
use std::sync::Arc;

fn live_enabled() -> bool {
    std::env::var("RINE_LIVE_ORACLE").as_deref() == Ok("1")
}

fn connect() -> Connection {
    let host = std::env::var("ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("ORACLE_PORT").unwrap_or_else(|_| "1521".into());
    let service = std::env::var("ORACLE_SERVICE").unwrap_or_else(|_| "XEPDB1".into());
    let user = std::env::var("ORACLE_USER").unwrap_or_else(|_| "rine".into());
    let password = std::env::var("ORACLE_PASSWORD").unwrap_or_else(|_| "RinePass123_".into());
    let config = oracledb::Config::default()
        .set_credentials(&user, &password)
        .set_connect_string(&format!("{host}:{port}/{service}"))
        .unwrap();
    oracledb::connect(config).unwrap()
}

fn setup(conn: &Connection) {
    for ddl in [
        "DROP TABLE probe_nolob",
        "DROP TABLE probe_lob",
        "DROP TABLE probe_kine",
        "CREATE TABLE probe_nolob (id NUMBER PRIMARY KEY, name VARCHAR2(100))",
        "CREATE TABLE probe_lob (id NUMBER PRIMARY KEY, v BLOB)",
        "CREATE TABLE probe_kine (id NUMBER PRIMARY KEY, name VARCHAR2(630), created NUMBER DEFAULT 0, deleted NUMBER DEFAULT 0, create_revision NUMBER DEFAULT 0, prev_revision NUMBER DEFAULT 0, lease NUMBER DEFAULT 0, version NUMBER DEFAULT 1, value BLOB)",
    ] {
        let _ = conn.execute(ddl, &[]);
    }
    for i in 1..=3i64 {
        conn.execute(
            "INSERT INTO probe_nolob (id, name) VALUES (:1, :2)",
            &[&i, &"n"],
        )
        .unwrap();
        let data = vec![i as u8; 10];
        conn.execute(
            "INSERT INTO probe_lob (id, v) VALUES (:1, :2)",
            &[&i, &data],
        )
        .unwrap();
        let name = format!("s/{i}");
        conn.execute(
            "INSERT INTO probe_kine (id, name, created, deleted, create_revision, prev_revision, lease, version, value) VALUES (:1, :2, 0, 0, 0, 0, 0, 1, :3)",
            &[&i, &name, &data],
        )
        .unwrap();
    }
    conn.commit().unwrap();
}

fn count_all(cursor: oracledb::Cursor) -> Result<(usize, usize), oracledb::Error> {
    let mut rows = 0usize;
    let mut bytes = 0usize;
    for row in cursor {
        let row = row?;
        let v: Option<Vec<u8>> = row.get(1)?;
        rows += 1;
        bytes += v.map(|b| b.len()).unwrap_or(0);
    }
    Ok((rows, bytes))
}

#[test]
fn probe_fetch_01002() {
    if !live_enabled() {
        eprintln!("skipping probe (RINE_LIVE_ORACLE != 1)");
        return;
    }
    let conn = connect();
    setup(&conn);

    // A. multi-row, no LOB
    let r: Result<usize, oracledb::Error> = (|| {
        let c = conn.query("SELECT id, name FROM probe_nolob ORDER BY id", &[])?;
        let mut n = 0;
        for row in c {
            let _: i64 = row?.get(0)?;
            n += 1;
        }
        Ok(n)
    })();
    println!("A multi-row no-LOB: {r:?}");

    // B. single-row BLOB via query_row
    let r: Result<Option<usize>, oracledb::Error> = (|| {
        let row = conn.query_row("SELECT id, v FROM probe_lob WHERE id = 1", &[])?;
        let v: Option<Vec<u8>> = row.get(1)?;
        Ok(v.map(|b| b.len()))
    })();
    println!("B single-row BLOB: {r:?}");

    // C. multi-row BLOB via query()
    let r = (|| {
        let c = conn.query("SELECT id, v FROM probe_lob ORDER BY id", &[])?;
        count_all(c)
    })();
    println!("C multi-row BLOB: {r:?}");

    // G. multi-row, BLOB column not selected
    let r: Result<usize, oracledb::Error> = (|| {
        let c = conn.query("SELECT id FROM probe_lob ORDER BY id", &[])?;
        let mut n = 0;
        for row in c {
            let _: i64 = row?.get(0)?;
            n += 1;
        }
        Ok(n)
    })();
    println!("G multi-row id-only: {r:?}");

    // D. multi-row BLOB with fetch_array_size(1)
    let r = (|| {
        let c = conn
            .statement("SELECT id, v FROM probe_lob ORDER BY id")?
            .fetch_array_size(1)
            .build()?
            .query(&[])?;
        count_all(c)
    })();
    println!("D multi-row BLOB array-size-1: {r:?}");

    // D2. multi-row BLOB with prefetch_rows(1)
    let r = (|| {
        let c = conn
            .statement("SELECT id, v FROM probe_lob ORDER BY id")?
            .prefetch_rows(1)
            .build()?
            .query(&[])?;
        count_all(c)
    })();
    println!("D2 multi-row BLOB prefetch-1: {r:?}");

    // E. multi-row BLOB with LOB locators
    let r: Result<(usize, usize), String> = (|| {
        let c = conn
            .statement("SELECT id, v FROM probe_lob ORDER BY id")
            .map_err(|e| e.to_string())?
            .fetch_lobs()
            .build()
            .map_err(|e| e.to_string())?
            .query(&[])
            .map_err(|e| e.to_string())?;
        let mut n = 0;
        let mut bytes = 0;
        for row in c {
            let mut row = row.map_err(|e| e.to_string())?;
            let mut lob: oracledb::Lob = row.take(1).map_err(|e| e.to_string())?;
            let mut buf = Vec::new();
            use std::io::Read;
            lob.read_to_end(&mut buf).map_err(|e| e.to_string())?;
            n += 1;
            bytes += buf.len();
        }
        Ok((n, bytes))
    })();
    println!("E multi-row BLOB locators: {r:?}");

    // H. kine-shaped 9-col query with binds (the failing etcdctl shape).
    let r: Result<usize, oracledb::Error> = (|| {
        let c = conn.query(
            "SELECT id, name, created, deleted, create_revision, prev_revision, lease, version, value FROM probe_kine WHERE name >= :1 AND name < :2 AND deleted = 0 ORDER BY name, id",
            &[&"s/" as &dyn oracledb::ToDbValue, &"s0"],
        )?;
        let mut n = 0;
        for row in c {
            let row = row?;
            let v: Option<Vec<u8>> = row.get(8)?;
            n += v.map(|b| b.len()).unwrap_or(0);
        }
        Ok(n)
    })();
    println!("H kine-shaped 9col binds: {r:?}");

    // I. same shape, literal predicates (no binds).
    let r: Result<usize, oracledb::Error> = (|| {
        let c = conn.query(
            "SELECT id, name, created, deleted, create_revision, prev_revision, lease, version, value FROM probe_kine WHERE name >= 's/' AND name < 's0' AND deleted = 0 ORDER BY name, id",
            &[],
        )?;
        let mut n = 0;
        for row in c {
            let row = row?;
            let v: Option<Vec<u8>> = row.get(8)?;
            n += v.map(|b| b.len()).unwrap_or(0);
        }
        Ok(n)
    })();
    println!("I kine-shaped 9col literals: {r:?}");

    // J. 8 columns (no BLOB) with binds.
    let r: Result<usize, oracledb::Error> = (|| {
        let c = conn.query(
            "SELECT id, name, created, deleted, create_revision, prev_revision, lease, version FROM probe_kine WHERE name >= :1 AND name < :2 ORDER BY name, id",
            &[&"s/" as &dyn oracledb::ToDbValue, &"s0"],
        )?;
        let mut n = 0;
        for row in c {
            let _: i64 = row?.get(0)?;
            n += 1;
        }
        Ok(n)
    })();
    println!("J 8col binds no-LOB: {r:?}");

    for ddl in [
        "DROP TABLE probe_nolob",
        "DROP TABLE probe_lob",
        "DROP TABLE probe_kine",
    ] {
        let _ = conn.execute(ddl, &[]);
    }
    conn.commit().unwrap();
}

/// Bisects the live_by_name blind spot on an isolated table (no `kine`
/// clashes with the other probe running in parallel).
#[test]
fn probe_select_shapes() {
    if !live_enabled() {
        eprintln!("skipping probe (RINE_LIVE_ORACLE != 1)");
        return;
    }
    let host = std::env::var("ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("ORACLE_PORT").unwrap_or_else(|_| "1521".into());
    let service = std::env::var("ORACLE_SERVICE").unwrap_or_else(|_| "XEPDB1".into());
    let user = std::env::var("ORACLE_USER").unwrap_or_else(|_| "rine".into());
    let password = std::env::var("ORACLE_PASSWORD").unwrap_or_else(|_| "RinePass123_".into());
    let conn = oracledb::connect(
        oracledb::Config::default()
            .set_credentials(&user, &password)
            .set_connect_string(&format!("{host}:{port}/{service}"))
            .unwrap(),
    )
    .unwrap();
    for ddl in [
        "DROP TABLE kineq",
        "CREATE TABLE kineq (id NUMBER PRIMARY KEY, name VARCHAR2(630), created NUMBER DEFAULT 0, deleted NUMBER DEFAULT 0, create_revision NUMBER DEFAULT 0, prev_revision NUMBER DEFAULT 0, lease NUMBER DEFAULT 0, version NUMBER DEFAULT 1, value BLOB)",
    ] {
        let _ = conn.execute(ddl, &[]);
    }
    for (i, (k, v)) in [("foo", "bar"), ("s/a", "1"), ("s/b", "2"), ("s/c", "3")]
        .into_iter()
        .enumerate()
    {
        let id = i as i64 + 1;
        let data = v.as_bytes().to_vec();
        conn.execute(
            "INSERT INTO kineq (id, name, created, deleted, create_revision, prev_revision, lease, version, value) VALUES (:1, :2, 0, 0, 0, 0, 0, 1, :3)",
            &[&id as &dyn oracledb::ToDbValue, &k, &data],
        )
        .unwrap();
    }
    conn.commit().unwrap();

    fn read_all(row: &oracledb::Row) -> Result<Vec<String>, oracledb::Error> {
        let mut out = Vec::new();
        for i in [0usize, 1, 2, 3, 4, 5, 6, 7] {
            let v: Option<i64> = if i == 1 {
                let s: Option<String> = row.get(i)?;
                out.push(format!("{s:?}"));
                continue;
            } else {
                row.get(i)?
            };
            out.push(format!("{v:?}"));
        }
        let b: Option<Vec<u8>> = row.get(8)?;
        out.push(format!("{:?}", b.map(|v| v.len())));
        Ok(out)
    }

    // Q1: exact live_by_name shape with bind.
    let r: Result<Vec<Vec<String>>, oracledb::Error> = (|| {
        let c = conn.query(
            "SELECT id, name, created, deleted, create_revision, prev_revision, lease, version, value FROM kineq WHERE name = :1 AND deleted = 0",
            &[&"foo" as &dyn oracledb::ToDbValue],
        )?;
        let mut out = Vec::new();
        for row in c {
            out.push(read_all(&row?)?);
        }
        Ok(out)
    })();
    println!("Q1 live_by_name bind: {r:?}");

    // Q2: same with literal.
    let r: Result<Vec<Vec<String>>, oracledb::Error> = (|| {
        let c = conn.query(
            "SELECT id, name, created, deleted, create_revision, prev_revision, lease, version, value FROM kineq WHERE name = 'foo' AND deleted = 0",
            &[],
        )?;
        let mut out = Vec::new();
        for row in c {
            out.push(read_all(&row?)?);
        }
        Ok(out)
    })();
    println!("Q2 live_by_name literal: {r:?}");

    // Q3: same shape, different key.
    let r: Result<Vec<Vec<String>>, oracledb::Error> = (|| {
        let c = conn.query(
            "SELECT id, name, created, deleted, create_revision, prev_revision, lease, version, value FROM kineq WHERE name = :1 AND deleted = 0",
            &[&"s/a" as &dyn oracledb::ToDbValue],
        )?;
        let mut out = Vec::new();
        for row in c {
            out.push(read_all(&row?)?);
        }
        Ok(out)
    })();
    println!("Q3 live_by_name other key: {r:?}");

    // Q4: resolve_id shape (control).
    let r: Result<Option<i64>, oracledb::Error> = (|| {
        let row = conn.query_row(
            "SELECT id FROM kineq WHERE name = :1 AND prev_revision = :2",
            &[&"foo" as &dyn oracledb::ToDbValue, &0i64],
        )?;
        Ok(row.get(0)?)
    })();
    println!("Q4 resolve_id control: {r:?}");

    // Q6: live_by_name shape + ORDER BY.
    let r: Result<Vec<Vec<String>>, oracledb::Error> = (|| {
        let c = conn.query(
            "SELECT id, name, created, deleted, create_revision, prev_revision, lease, version, value FROM kineq WHERE name = :1 AND deleted = 0 ORDER BY id",
            &[&"foo" as &dyn oracledb::ToDbValue],
        )?;
        let mut out = Vec::new();
        for row in c {
            out.push(read_all(&row?)?);
        }
        Ok(out)
    })();
    println!("Q6 live_by_name + ORDER BY: {r:?}");

    let _ = conn.execute("DROP TABLE kineq", &[]);
    conn.commit().unwrap();
}

/// Replicates the failing etcdctl sequence through the real backend stack
/// (OracleDb + OracleBackend): create foo, get foo, create s/*, prefix list.
#[tokio::test]
async fn probe_backend_prefix_list() {
    use rine_etcd::backend::{Backend, ListOptions};
    use rine_store_oracle::{OracleBackend, OracleDb, SqlExecutor};

    if !live_enabled() {
        eprintln!("skipping probe (RINE_LIVE_ORACLE != 1)");
        return;
    }
    let host = std::env::var("ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("ORACLE_PORT").unwrap_or_else(|_| "1521".into());
    let service = std::env::var("ORACLE_SERVICE").unwrap_or_else(|_| "XEPDB1".into());
    let user = std::env::var("ORACLE_USER").unwrap_or_else(|_| "rine".into());
    let password = std::env::var("ORACLE_PASSWORD").unwrap_or_else(|_| "RinePass123_".into());
    let db = OracleDb::connect(&user, &password, &format!("{host}:{port}/{service}")).unwrap();
    let raw = oracledb::connect(
        oracledb::Config::default()
            .set_credentials(&user, &password)
            .set_connect_string(&format!("{host}:{port}/{service}"))
            .unwrap(),
    )
    .unwrap();
    let dump = |label: &str| {
        let c = raw
            .query(
                "SELECT id, name, deleted, LENGTH(value) FROM kine ORDER BY id",
                &[],
            )
            .unwrap();
        let mut rows = Vec::new();
        for row in c {
            let row = row.unwrap();
            let id: Option<i64> = row.get(0).unwrap();
            let name: Option<String> = row.get(1).unwrap();
            let deleted: Option<i64> = row.get(2).unwrap();
            let vlen: Option<i64> = row.get(3).unwrap();
            rows.push(format!("{id:?}/{name:?}/{deleted:?}/{vlen:?}"));
        }
        println!("DUMP {label}: [{}]", rows.join(", "));
    };
    let _ = db.execute("DROP TABLE kine", vec![]).await;
    let backend = OracleBackend::new(Arc::new(db));
    backend.ensure_schema().await.unwrap();
    dump("fresh");

    backend
        .create(bytes::Bytes::from("foo"), bytes::Bytes::from("bar"), 0)
        .await
        .unwrap();
    dump("after create foo");
    let (_, kv) = backend.get(b"foo", 0).await.unwrap();
    println!("GET foo -> {kv:?}");
    assert_eq!(kv.unwrap().value, bytes::Bytes::from("bar"));
    for (k, v) in [("s/b", "2"), ("s/a", "1"), ("s/c", "3")] {
        backend
            .create(bytes::Bytes::from(k), bytes::Bytes::from(v), 0)
            .await
            .unwrap();
    }
    let (rev, kvs, count) = backend
        .list(
            b"s",
            Some(b"t".as_slice()),
            ListOptions {
                keys_only: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!(
        "L backend prefix list: rev={rev} count={count} rows={}",
        kvs.len()
    );
    assert_eq!(count, 3);
}

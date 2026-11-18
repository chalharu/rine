//! Temporary probe for ORA-01002 on multi-row BLOB fetches.
//!
//! Runs only with `RINE_LIVE_ORACLE=1`. Prints OK/ERR per case so one CI run
//! identifies the trigger. Uses `oracledb` directly (not `OracleDb`).

use oracledb::Connection;

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
        for mut row in c {
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

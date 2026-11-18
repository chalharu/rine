//! Decisive bisect for the live_by_name blind spot: DROP history, indexes,
//! and row count on isolated tables (no `kine` clashes).

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

const DDL: &str = "CREATE TABLE <T> (id NUMBER PRIMARY KEY, name VARCHAR2(630), created NUMBER DEFAULT 0, deleted NUMBER DEFAULT 0, create_revision NUMBER DEFAULT 0, prev_revision NUMBER DEFAULT 0, lease NUMBER DEFAULT 0, version NUMBER DEFAULT 1, value BLOB)";
const IDX: &[&str] = &[
    "CREATE INDEX <T>_name_index ON <T>(name)",
    "CREATE INDEX <T>_name_id_index ON <T>(name, id)",
    "CREATE INDEX <T>_id_deleted_index ON <T>(id, deleted)",
    "CREATE INDEX <T>_prev_revision_index ON <T>(prev_revision)",
    "CREATE UNIQUE INDEX <T>_name_prev_revision_uindex ON <T>(name, prev_revision)",
];
const LIVE_BY_NAME: &str = "SELECT id, name, created, deleted, create_revision, prev_revision, lease, version, value FROM <T> WHERE name = :1 AND deleted = 0";

fn setup_table(conn: &Connection, name: &str, with_indexes: bool, pre_create_rows: bool) {
    let _ = conn.execute(&format!("DROP TABLE {name}"), &[]);
    if pre_create_rows {
        // Create, fill, then drop: reproduces DROP-existing history.
        let _ = conn.execute(&DDL.replace("<T>", name), &[]);
        for i in 1..=3i64 {
            let n = format!("old{i}");
            let v = vec![i as u8; 3];
            let _ = conn.execute(
                &format!("INSERT INTO {name} (id, name, value) VALUES (:1, :2, :3)"),
                &[&i as &dyn oracledb::ToDbValue, &n, &v],
            );
        }
        conn.commit().unwrap();
        conn.execute(&format!("DROP TABLE {name}"), &[]).unwrap();
    }
    conn.execute(&DDL.replace("<T>", name), &[]).unwrap();
    if with_indexes {
        for idx in IDX {
            conn.execute(&idx.replace("<T>", name), &[]).unwrap();
        }
    }
    conn.commit().unwrap();
}

fn insert_foo(conn: &Connection, table: &str) {
    let data = b"bar".to_vec();
    conn.execute(
        &format!("INSERT INTO {table} (id, name, value) VALUES (1, 'foo', :1)"),
        &[&data as &dyn oracledb::ToDbValue],
    )
    .unwrap();
    conn.commit().unwrap();
}

fn live_by_name(conn: &Connection, table: &str) -> Result<usize, oracledb::Error> {
    let sql = LIVE_BY_NAME.replace("<T>", table);
    let c = conn.query(&sql, &[&"foo" as &dyn oracledb::ToDbValue])?;
    let mut n = 0;
    for row in c {
        let row = row?;
        let id: Option<i64> = row.get(0)?;
        n += id.map(|_| 1).unwrap_or(0);
    }
    Ok(n)
}

#[test]
fn probe_table_state_matrix() {
    if !live_enabled() {
        eprintln!("skipping probe (RINE_LIVE_ORACLE != 1)");
        return;
    }
    let conn = connect();

    // T1: plain CREATE, no indexes, no DROP history, 1 row.
    setup_table(&conn, "q1", false, false);
    insert_foo(&conn, "q1");
    println!("T1 plain 1 row: {:?}", live_by_name(&conn, "q1"));

    // T2: DROP-missing history, no indexes, 1 row.
    setup_table(&conn, "q2", false, false);
    insert_foo(&conn, "q2");
    println!("T2 drop-missing 1 row: {:?}", live_by_name(&conn, "q2"));

    // T3: DROP-existing history (pre-created then dropped), no indexes.
    setup_table(&conn, "q3", false, true);
    insert_foo(&conn, "q3");
    println!("T3 drop-existing 1 row: {:?}", live_by_name(&conn, "q3"));

    // T4: indexes, no DROP history, 1 row.
    setup_table(&conn, "q4", true, false);
    insert_foo(&conn, "q4");
    println!("T4 indexes 1 row: {:?}", live_by_name(&conn, "q4"));

    // T5: DROP-existing history + indexes (the probe-L shape).
    setup_table(&conn, "q5", true, true);
    insert_foo(&conn, "q5");
    println!(
        "T5 drop-existing + indexes 1 row: {:?}",
        live_by_name(&conn, "q5")
    );

    // T6: plain CREATE, no indexes, 4 rows.
    setup_table(&conn, "q6", false, false);
    for (i, (k, v)) in [("foo", "bar"), ("s/a", "1"), ("s/b", "2"), ("s/c", "3")]
        .into_iter()
        .enumerate()
    {
        let id = i as i64 + 1;
        let data = v.as_bytes().to_vec();
        conn.execute(
            "INSERT INTO q6 (id, name, value) VALUES (:1, :2, :3)",
            &[&id as &dyn oracledb::ToDbValue, &k, &data],
        )
        .unwrap();
    }
    conn.commit().unwrap();
    println!("T6 plain 4 rows: {:?}", live_by_name(&conn, "q6"));

    for t in ["q1", "q2", "q3", "q4", "q5", "q6"] {
        let _ = conn.execute(&format!("DROP TABLE {t}"), &[]);
    }
    conn.commit().unwrap();

    // Dictionary state: recyclebin leftovers, kine indexes status.
    let dump_q = |label: &str, sql: &str| {
        let c = conn.query(sql, &[]).unwrap();
        let mut out = Vec::new();
        for row in c {
            let row = row.unwrap();
            let a: Option<String> = row.get(0).unwrap();
            let b: Option<String> = row.get(1).unwrap();
            out.push(format!("{a:?}/{b:?}"));
        }
        println!("DICT {label}: [{}]", out.join(", "));
    };
    dump_q(
        "recyclebin",
        "SELECT object_name, original_name FROM user_recyclebin ORDER BY 1",
    );
    dump_q(
        "kine tables",
        "SELECT table_name, status FROM user_tables WHERE table_name LIKE 'KINE%' ORDER BY 1",
    );
    dump_q(
        "kine indexes",
        "SELECT index_name, status FROM user_indexes WHERE table_name = 'KINE' ORDER BY 1",
    );
}

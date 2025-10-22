use std::collections::HashSet;
use clickhouse::Row as ChRow;
use clickhouse::Client;
use tempfile::TempDir;
use std::time::Duration;
use serde::Deserialize;
use rocks_wal_tailer::{run_single_src, Src, TailArgs};

#[derive(ChRow, Deserialize, Debug)]
struct LiveCnt { n: u64 }
#[derive(ChRow, Deserialize, Debug)]
struct Cnt { n: u64 }
#[derive(ChRow, Deserialize, Debug)]
struct DistinctKeys { dk: u64 }
#[derive(ChRow, serde::Deserialize, Debug)]
struct WalAgg {
    total: u64,
    puts: u64,
    deletes: u64,
}

#[derive(ChRow, Deserialize, Debug)]
struct AggRow {
    src_db: String,
    src_table: String,
    total: u64,
    puts: u64,
    deletes: u64,
}
#[derive(ChRow, Deserialize)]
struct Dist { total: u64, distinct_seq: u64 }
#[derive(ChRow, Deserialize)]
struct MaxSeq { mx: u64 }
#[derive(ChRow, Deserialize)]
struct LastVal { value_b64: Option<String> }
async fn put_tuple_in(
    client: &Client,
    tbl: &str,
    a: i64,
    b: i64,
    c: &str,
    v1: &str,
    v2: i64,
) -> clickhouse::error::Result<()> {
    client
        .query(&format!(
            "INSERT INTO srcdb.{tbl} (key, v1, v2) VALUES (tuple(?,?,?), ?, ?)"
        ))
        .bind(a)
        .bind(b)
        .bind(c)
        .bind(v1)
        .bind(v2)
        .execute()
        .await
}

async fn del_tuple_in(client: &Client, tbl: &str, a: i64, b: i64, c: &str) -> clickhouse::error::Result<()> {
    // target the tuple elements to avoid string building
    client
        .query(&format!(
            "DELETE FROM srcdb.{tbl} WHERE key.1 = ? AND key.2 = ? AND key.3 = ?"
        ))
        .bind(a)
        .bind(b)
        .bind(c)
        .execute()
        .await
}
async fn put_in(client: &Client, tbl: &str, k: &str, v: &str) -> clickhouse::error::Result<()> {
    client
        .query(&format!("INSERT INTO srcdb.{tbl} (key, value) VALUES (?, ?)"))
        .bind(k)
        .bind(v)
        .execute()
        .await
}

async fn del_in(client: &Client, tbl: &str, k: &str) -> clickhouse::error::Result<()> {
    client
        .query(&format!("DELETE FROM srcdb.{tbl} WHERE key = ?"))
        .bind(k)
        .execute()
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn wal_to_clickhouse_discovery_end_to_end_with_op_counts() {
    let ch_url = "http://127.0.0.1:8123".to_string();
    let client = Client::default()
        .with_url(&ch_url)
        .with_user("default")
        .with_password("default123");

    client.query("CREATE DATABASE IF NOT EXISTS srcdb").execute().await.unwrap();
    client.query("CREATE DATABASE IF NOT EXISTS wal").execute().await.unwrap();

    client.query("DROP TABLE IF EXISTS srcdb.kv").execute().await.unwrap();
    client.query(r#"
        CREATE TABLE srcdb.kv
        (
            key   String,
            value String
        )
        ENGINE = EmbeddedRocksDB
        PRIMARY KEY key
        SETTINGS optimize_for_bulk_insert = 0
    "#).execute().await.unwrap();

    client.query("DROP TABLE IF EXISTS wal.rdb_changelog").execute().await.unwrap();
    client.query(r#"
        CREATE TABLE wal.rdb_changelog
        (
          src_db     String,
          src_table  String,
          ts         DateTime64(3) DEFAULT now64(3),
          seq        UInt64,
          key        String,
          is_deleted UInt8,
          value_b64  Nullable(String),
          value_json Nullable(String)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(ts)
        ORDER BY (src_db, src_table, key, seq)
    "#).execute().await.unwrap();

    let ckpt_dir = TempDir::new().unwrap();
    let args = TailArgs {
        src: vec![Src { db: "srcdb".into(), table: "kv".into() }],
        checkpoint_dir: ckpt_dir.path().to_path_buf(),
        ch_url: ch_url.clone(),
        ch_database: "wal".into(),
        ch_table: "rdb_changelog".into(),
    };

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let tail = tokio::spawn(async move {
        let src = Src { db: "srcdb".into(), table: "kv".into() };
        let _ = run_single_src(args, src, Some(rx)).await;
    });

    tokio::time::sleep(Duration::from_millis(5000)).await;
    let mut produced_puts: u64 = 0;
    let mut produced_deletes: u64 = 0;

    put_in(&client,"kv", "user:1", "A").await.unwrap(); produced_puts += 1;
    put_in(&client,"kv", "user:1", "B").await.unwrap(); produced_puts += 1;
    put_in(&client,"kv", "user:2", "X").await.unwrap(); produced_puts += 1;

    for i in 0..50u32 {
        let k = format!("k{:03}", i);
        put_in(&client,"kv", &k, "v").await.unwrap(); produced_puts += 1;
    }

    del_in(&client,"kv", "k001").await.unwrap(); produced_deletes += 1;
    del_in(&client,"kv", "user:2").await.unwrap(); produced_deletes += 1;

    tokio::time::sleep(Duration::from_millis(5_000)).await;
    put_in(&client,"kv",
        "user:3", "Z").await.unwrap(); produced_puts += 1;

    del_in(&client,"kv", "k010").await.unwrap(); produced_deletes += 1;

    let mut captured_total = 0u64;
    let mut captured_puts = 0u64;
    let mut captured_deletes = 0u64;
    tokio::time::sleep(Duration::from_millis(15_000)).await;
    for _ in 0..80 {
        let WalAgg { total, puts, deletes } = client
            .query(r#"
                SELECT
                    count() AS total,
                    countIf(is_deleted = 0) AS puts,
                    countIf(is_deleted = 1) AS deletes
                FROM wal.rdb_changelog
                WHERE src_db = 'srcdb' AND src_table = 'kv'
            "#)
            .fetch_one::<WalAgg>()
            .await
            .unwrap();

        captured_total = total;
        captured_puts = puts;
        captured_deletes = deletes;

        // Expect at least the number of produced put and delete events
        if total >= produced_puts + produced_deletes { break; }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    eprintln!(
        "[summary] produced_puts={}, produced_deletes={}, captured_puts={}, captured_deletes={}, captured_total={}",
        produced_puts, produced_deletes, captured_puts, captured_deletes, captured_total
    );

    assert_eq!(captured_puts, produced_puts, "PUTs mismatch: produced {}, captured {}", produced_puts, captured_puts);
    assert_eq!(captured_deletes, produced_deletes, "DELETEs mismatch: produced {}, captured {}", produced_deletes, captured_deletes);

    let cnt_user1: u64 = client
        .query("SELECT count() FROM wal.rdb_changelog WHERE src_db='srcdb' AND src_table='kv' AND key='user:1'")
        .fetch_one()
        .await
        .unwrap();
    assert!(cnt_user1 >= 2, "user:1 should have at least 2 rows, got {}", cnt_user1);

    // Sanity check: 50 distinct k*** keys touched PUT and DELETE
    let k_distinct: u64 = client
        .query("SELECT countDistinctIf(key, startsWith(key, 'k')) FROM wal.rdb_changelog WHERE src_db='srcdb' AND src_table='kv'")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(k_distinct, 50, "expected 50 distinct k*** keys, got {}", k_distinct);

    // Latest for user:1 is 'B' (base64 'AUI=')
    let results: Vec<String> = client
        .query(
            "SELECT value_b64
         FROM wal.rdb_changelog
         WHERE src_db='srcdb' AND src_table='kv' AND key='user:1'
         ORDER BY seq DESC
         LIMIT 1"
        )
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(results.get(1), Some("AUI=".to_string()).as_ref());
    
    // Shutdown tailer
    let _ = tx.send(());
    let _ = tail.await;
}
#[tokio::test(flavor = "multi_thread")]
async fn wal_to_clickhouse_multi_tables_stream_ok() {
    // ClickHouse client
    let ch_url = "http://127.0.0.1:8123".to_string();
    let client = Client::default()
        .with_url(&ch_url)
        .with_user("default")
        .with_password("default123");

    // Schema
    client.query("CREATE DATABASE IF NOT EXISTS srcdb").execute().await.unwrap();
    client.query("CREATE DATABASE IF NOT EXISTS wal").execute().await.unwrap();

    client.query("DROP TABLE IF EXISTS srcdb.kv_a").execute().await.unwrap();
    client.query("DROP TABLE IF EXISTS srcdb.kv_b").execute().await.unwrap();

    client.query(r#"
        CREATE TABLE srcdb.kv_a
        (
            key   String,
            value String
        )
        ENGINE = EmbeddedRocksDB
        PRIMARY KEY key
        SETTINGS optimize_for_bulk_insert = 0
    "#).execute().await.unwrap();

    client.query(r#"
        CREATE TABLE srcdb.kv_b
        (
            key   String,
            value String
        )
        ENGINE = EmbeddedRocksDB
        PRIMARY KEY key
        SETTINGS optimize_for_bulk_insert = 0
    "#).execute().await.unwrap();

    client.query("DROP TABLE IF EXISTS wal.rdb_changelog").execute().await.unwrap();
    client.query(r#"
        CREATE TABLE wal.rdb_changelog
        (
          src_db     String,
          src_table  String,
          ts         DateTime64(3) DEFAULT now64(3),
          seq        UInt64,
          key        String,
          is_deleted UInt8,
          value_b64  Nullable(String),
          value_json Nullable(String)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(ts)
        ORDER BY (src_db, src_table, key, seq)
    "#).execute().await.unwrap();

    // Tailer args
    let ckpt_dir = TempDir::new().unwrap();
    // deterministic start from 0 for both sources
    tokio::fs::create_dir_all(ckpt_dir.path()).await.unwrap();
    tokio::fs::write(ckpt_dir.path().join("srcdb__kv_a.seq"), b"0\n").await.unwrap();
    tokio::fs::write(ckpt_dir.path().join("srcdb__kv_b.seq"), b"0\n").await.unwrap();

    let args = TailArgs {
        src: vec![],
        checkpoint_dir: ckpt_dir.path().to_path_buf(),
        ch_url: ch_url.clone(),
        ch_database: "wal".into(),
        ch_table: "rdb_changelog".into(),
    };

    // Spawn two tailers
    let (tx_a, rx_a) = tokio::sync::oneshot::channel::<()>();
    let (tx_b, rx_b) = tokio::sync::oneshot::channel::<()>();

    let args_a = args.clone();
    let args_b = args.clone();

    let t_a = tokio::spawn(async move {
        let src = Src { db: "srcdb".into(), table: "kv_a".into() };
        let _ = run_single_src(args_a, src, Some(rx_a)).await;
    });
    let t_b = tokio::spawn(async move {
        let src = Src { db: "srcdb".into(), table: "kv_b".into() };
        let _ = run_single_src(args_b, src, Some(rx_b)).await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut a_puts: u64 = 0;
    let mut a_dels: u64 = 0;
    put_in(&client, "kv_a", "user:1", "A").await.unwrap(); a_puts += 1;
    put_in(&client, "kv_a", "user:1", "B").await.unwrap(); a_puts += 1;
    put_in(&client, "kv_a", "user:2", "X").await.unwrap(); a_puts += 1;
    for i in 0..50u32 {
        let k = format!("a{:03}", i);
        put_in(&client, "kv_a", &k, "v").await.unwrap(); a_puts += 1;
    }
    del_in(&client, "kv_a", "a001").await.unwrap(); a_dels += 1;
    del_in(&client, "kv_a", "user:2").await.unwrap(); a_dels += 1;

    let mut b_puts: u64 = 0;
    let mut b_dels: u64 = 0;
    put_in(&client, "kv_b", "user:10", "U").await.unwrap(); b_puts += 1;
    for i in 0..30u32 {
        let k = format!("b{:03}", i);
        put_in(&client, "kv_b", &k, "w").await.unwrap(); b_puts += 1;
    }
    del_in(&client, "kv_b", "b005").await.unwrap(); b_dels += 1;
    put_in(&client, "kv_b", "user:10", "V").await.unwrap(); b_puts += 1;
    put_in(&client, "kv_b", "user:11", "W").await.unwrap(); b_puts += 1;

    tokio::time::sleep(Duration::from_millis(1500)).await;
    put_in(&client, "kv_a", "user:3", "Z").await.unwrap(); a_puts += 1;
    del_in(&client, "kv_a", "a010").await.unwrap(); a_dels += 1;

    let expect_a_total = a_puts + a_dels;
    let expect_b_total = b_puts + b_dels;

    let mut got_a = (0u64, 0u64, 0u64);
    let mut got_b = (0u64, 0u64, 0u64);

    for _ in 0..80 {
        let rows: Vec<AggRow> = client
            .query(r#"
                SELECT
                  src_db, src_table,
                  count() AS total,
                  countIf(is_deleted = 0) AS puts,
                  countIf(is_deleted = 1) AS deletes
                FROM wal.rdb_changelog
                WHERE (src_db, src_table) IN (('srcdb','kv_a'), ('srcdb','kv_b'))
                GROUP BY src_db, src_table
            "#)
            .fetch_all()
            .await
            .unwrap();

        for r in &rows {
            if r.src_table == "kv_a" { got_a = (r.total, r.puts, r.deletes); }
            if r.src_table == "kv_b" { got_b = (r.total, r.puts, r.deletes); }
        }

        if got_a.0 >= expect_a_total && got_b.0 >= expect_b_total { break; }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert_eq!(got_a.1, a_puts,   "kv_a PUTs mismatch");
    assert_eq!(got_a.2, a_dels,   "kv_a DELETEs mismatch");
    assert_eq!(got_a.0, expect_a_total, "kv_a total mismatch");

    assert_eq!(got_b.1, b_puts,   "kv_b PUTs mismatch");
    assert_eq!(got_b.2, b_dels,   "kv_b DELETEs mismatch");
    assert_eq!(got_b.0, expect_b_total, "kv_b total mismatch");
    #[derive(ChRow, Deserialize)]
    struct LastVal { value_b64: Option<String> }
    let LastVal { value_b64 } = client
        .query(
            "SELECT value_b64
             FROM wal.rdb_changelog
             WHERE src_db='srcdb' AND src_table='kv_a' AND key='user:1' AND is_deleted=0
             ORDER BY seq DESC LIMIT 1"
        )
        .fetch_one::<LastVal>()
        .await
        .unwrap();
    assert_eq!(value_b64.as_deref(), Some("AUI="));

    // Shutdown
    let _ = tx_a.send(());
    let _ = tx_b.send(());
    let _ = t_a.await;
    let _ = t_b.await;
}
#[tokio::test(flavor = "multi_thread")]
async fn wal_resume_from_checkpoint_using_helpers() {
    // ClickHouse client
    let ch_url = "http://127.0.0.1:8123".to_string();
    let client = Client::default()
        .with_url(&ch_url)
        .with_user("default")
        .with_password("default123");

    // Schema
    client.query("CREATE DATABASE IF NOT EXISTS srcdb").execute().await.unwrap();
    client.query("CREATE DATABASE IF NOT EXISTS wal").execute().await.unwrap();

    client.query("DROP TABLE IF EXISTS srcdb.kv").execute().await.unwrap();
    client.query(r#"
        CREATE TABLE srcdb.kv
        (
            key   String,
            value String
        )
        ENGINE = EmbeddedRocksDB
        PRIMARY KEY key
        SETTINGS optimize_for_bulk_insert = 0
    "#).execute().await.unwrap();

    client.query("DROP TABLE IF EXISTS wal.rdb_changelog").execute().await.unwrap();
    client.query(r#"
        CREATE TABLE wal.rdb_changelog
        (
          src_db     String,
          src_table  String,
          ts         DateTime64(3) DEFAULT now64(3),
          seq        UInt64,
          key        String,
          is_deleted UInt8,
          value_b64  Nullable(String),
          value_json Nullable(String)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(ts)
        ORDER BY (src_db, src_table, key, seq)
    "#).execute().await.unwrap();

    // Tailer args + deterministic checkpoint
    let ckpt_dir = TempDir::new().unwrap();
    let ckpt_file = ckpt_dir.path().join("srcdb__kv.seq");
    tokio::fs::create_dir_all(ckpt_dir.path()).await.unwrap();
    tokio::fs::write(&ckpt_file, b"0\n").await.unwrap();

    let args = TailArgs {
        src: vec![],
        checkpoint_dir: ckpt_dir.path().to_path_buf(),
        ch_url: ch_url.clone(),
        ch_database: "wal".into(),
        ch_table: "rdb_changelog".into(),
    };

    // Phase 1: start tailer, ingest first batch
    let (tx1, rx1) = tokio::sync::oneshot::channel::<()>();
    let args1 = args.clone();
    let t1 = tokio::spawn(async move {
        let src = Src { db: "srcdb".into(), table: "kv".into() };
        let _ = run_single_src(args1, src, Some(rx1)).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Produce first batch
    let mut p1_puts = 0u64;
    let mut p1_dels = 0u64;

    put_in(&client, "kv", "user:1", "A").await.unwrap(); p1_puts += 1;
    put_in(&client, "kv", "user:1", "B").await.unwrap(); p1_puts += 1;
    for i in 0..10u32 {
        let k = format!("p{:03}", i);
        put_in(&client, "kv", &k, "v").await.unwrap(); p1_puts += 1;
    }
    del_in(&client, "kv", "p003").await.unwrap(); p1_dels += 1;

    // Wait until fist batch totals visible
    let expect1 = p1_puts + p1_dels;
    let (mut tot1, mut puts1, mut dels1) = (0u64, 0u64, 0u64);
    for _ in 0..60 {
        let WalAgg { total, puts, deletes } = client
            .query("SELECT count() AS total,
                           countIf(is_deleted=0) AS puts,
                           countIf(is_deleted=1) AS deletes
                    FROM wal.rdb_changelog
                    WHERE src_db='srcdb' AND src_table='kv'")
            .fetch_one::<WalAgg>().await.unwrap();
        tot1 = total; puts1 = puts; dels1 = deletes;
        if total >= expect1 { break; }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(puts1, p1_puts, "phase1 puts");
    assert_eq!(dels1, p1_dels, "phase1 deletes");
    assert_eq!(tot1,  expect1, "phase1 total");

    // Max seq after first batch
    let MaxSeq { mx: max_seq1 } = client
        .query("SELECT max(seq) AS mx
                FROM wal.rdb_changelog
                WHERE src_db='srcdb' AND src_table='kv'")
        .fetch_one::<MaxSeq>().await.unwrap();

    // Stop tailer
    let _ = tx1.send(());
    let _ = t1.await;

    // Checkpoint == last applied seq
    let ckpt_seq1: u64 = tokio::fs::read_to_string(&ckpt_file).await.unwrap().trim().parse().unwrap();
    assert_eq!(ckpt_seq1, max_seq1, "checkpoint after stop");

    // produce more while tailer is down
    let mut p2_puts = 0u64;
    let mut p2_dels = 0u64;

    for i in 10..20u32 {
        let k = format!("p{:03}", i);
        put_in(&client, "kv", &k, "v2").await.unwrap(); p2_puts += 1;
    }
    del_in(&client, "kv", "p011").await.unwrap(); p2_dels += 1;
    put_in(&client, "kv", "user:2", "X").await.unwrap(); p2_puts += 1;
    del_in(&client, "kv", "user:2").await.unwrap(); p2_dels += 1;

    tokio::time::sleep(Duration::from_millis(25000)).await;
    // Sink unchanged while tailer down
    let total:u64 = client
        .query("SELECT count() AS total
                FROM wal.rdb_changelog
                WHERE src_db='srcdb' AND src_table='kv'")
        .fetch_one().await.unwrap();

    assert_eq!(total, tot1, "sink must not change while stopped");

    // Phase 3: restart tailer, must resume from checkpoint
    let (tx2, rx2) = tokio::sync::oneshot::channel::<()>();
    let args2 = args.clone();
    let t2 = tokio::spawn(async move {
        let src = Src { db: "srcdb".into(), table: "kv".into() };
        let _ = run_single_src(args2, src, Some(rx2)).await;
    });

    // Wait for totals == phase1 + phase2
    let expect_all = p1_puts + p1_dels + p2_puts + p2_dels;
    let mut tot_all = 0u64;
    for _ in 0..80 {
        let total_agg: u64 = client
            .query("SELECT count() AS total
                    FROM wal.rdb_changelog
                    WHERE src_db='srcdb' AND src_table='kv'")
            .fetch_one().await.unwrap();
        tot_all = total_agg;
        if total_agg >= expect_all { break; }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(tot_all, expect_all, "resume must ingest delta exactly once");

    // No duplicates by seq after resume
    let Dist { total, distinct_seq } = client
        .query("SELECT count() AS total, countDistinct(seq) AS distinct_seq
                FROM wal.rdb_changelog
                WHERE src_db='srcdb' AND src_table='kv'")
        .fetch_one::<Dist>().await.unwrap();
    assert_eq!(total, distinct_seq, "seq uniqueness violated (duplicate reprocessing)");

    // Checkpoint advanced after resume
    let MaxSeq { mx: max_seq2 } = client
        .query("SELECT max(seq) AS mx
                FROM wal.rdb_changelog
                WHERE src_db='srcdb' AND src_table='kv'")
        .fetch_one::<MaxSeq>().await.unwrap();
    let ckpt_seq2: u64 = tokio::fs::read_to_string(&ckpt_file).await.unwrap().trim().parse().unwrap();
    assert_eq!(ckpt_seq2, max_seq2, "checkpoint after resume");

    // Sanity: latest user:1 is PUT ('B' -> base64 'AUI=')
    let LastVal { value_b64 } = client
        .query("SELECT value_b64
                FROM wal.rdb_changelog
                WHERE src_db='srcdb' AND src_table='kv' AND key='user:1' AND is_deleted=0
                ORDER BY seq DESC LIMIT 1")
        .fetch_one::<LastVal>().await.unwrap();
    assert_eq!(value_b64.as_deref(), Some("AUI="));

    // Stop
    let _ = tx2.send(());
    let _ = t2.await;
}
#[tokio::test(flavor = "multi_thread")]
async fn wal_embedded_rocksdb_tuple_pk_end_to_end() {
    let ch_url = "http://127.0.0.1:8123".to_string();
    let client = Client::default().with_url(&ch_url).with_user("default").with_password("default123");

    client.query("CREATE DATABASE IF NOT EXISTS srcdb").execute().await.unwrap();
    client.query("CREATE DATABASE IF NOT EXISTS wal").execute().await.unwrap();

    client.query("DROP TABLE IF EXISTS srcdb.kv_tuple").execute().await.unwrap();
    client.query(r#"
        CREATE TABLE srcdb.kv_tuple
        (
            key Tuple(Int64, Int64, String),
            v1  String,
            v2  Int64
        )
        ENGINE = EmbeddedRocksDB
        PRIMARY KEY key
        SETTINGS optimize_for_bulk_insert = 0
    "#).execute().await.unwrap();

    client.query("DROP TABLE IF EXISTS wal.rdb_changelog").execute().await.unwrap();
    client.query(r#"
        CREATE TABLE wal.rdb_changelog
        (
          src_db     String,
          src_table  String,
          ts         DateTime64(3) DEFAULT now64(3),
          seq        UInt64,
          key        String,
          is_deleted UInt8,
          value_b64  Nullable(String),
          value_json Nullable(String)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(ts)
        ORDER BY (src_db, src_table, key, seq)
    "#).execute().await.unwrap();
    
    let ckpt_dir = TempDir::new().unwrap();
    let ckpt_file = ckpt_dir.path().join("srcdb__kv_tuple.seq");
    tokio::fs::create_dir_all(ckpt_dir.path()).await.unwrap();
    tokio::fs::write(&ckpt_file, b"0\n").await.unwrap();

    let args = TailArgs {
        src: vec![],
        checkpoint_dir: ckpt_dir.path().to_path_buf(),
        ch_url: ch_url.clone(),
        ch_database: "wal".into(),
        ch_table: "rdb_changelog".into(),
    };

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let t = tokio::spawn({
        let args1 = args.clone();
        async move {
            let src = Src { db: "srcdb".into(), table: "kv_tuple".into() };
            let _ = run_single_src(args1, src, Some(rx)).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(400)).await;

    // Produce mutations
    let mut produced_puts: u64 = 0;
    let mut produced_deletes: u64 = 0;
    let mut touched: HashSet<(i64, i64, String)> = HashSet::new();

    // base 5
    put_tuple_in(&client, "kv_tuple", 1, 1, "A1", "init", 10).await.unwrap(); produced_puts += 1; touched.insert((1,1,"A1".into()));
    put_tuple_in(&client, "kv_tuple", 1, 2, "A2", "init", 11).await.unwrap(); produced_puts += 1; touched.insert((1,2,"A2".into()));
    put_tuple_in(&client, "kv_tuple", 2, 1, "B1", "init", 12).await.unwrap(); produced_puts += 1; touched.insert((2,1,"B1".into()));
    put_tuple_in(&client, "kv_tuple", 3, 3, "C1", "init", 13).await.unwrap(); produced_puts += 1; touched.insert((3,3,"C1".into()));
    put_tuple_in(&client, "kv_tuple", 4, 4, "D1", "init", 14).await.unwrap(); produced_puts += 1; touched.insert((4,4,"D1".into()));

    // updates on same PK
    put_tuple_in(&client, "kv_tuple", 1, 1, "A1", "u1", 100).await.unwrap(); produced_puts += 1;
    put_tuple_in(&client, "kv_tuple", 1, 1, "A1", "u2", 101).await.unwrap(); produced_puts += 1;

    // delete one PK
    del_tuple_in(&client, "kv_tuple", 2, 1, "B1").await.unwrap(); produced_deletes += 1;

    // bulk distinct PKs
    for i in 0..20i64 {
        let k = format!("S{i:02}");
        put_tuple_in(&client, "kv_tuple", 100, i, &k, "v", i * 2).await.unwrap();
        produced_puts += 1;
        touched.insert((100, i, k));
    }

    // update one, delete two
    put_tuple_in(&client, "kv_tuple", 100, 7, "S07", "v2", 777).await.unwrap(); produced_puts += 1;
    del_tuple_in(&client, "kv_tuple", 100, 5, "S05").await.unwrap(); produced_deletes += 1;
    del_tuple_in(&client, "kv_tuple", 100, 10, "S10").await.unwrap(); produced_deletes += 1;

    // late insert
    tokio::time::sleep(Duration::from_millis(400)).await;
    put_tuple_in(&client, "kv_tuple", 9, 9, "Z9", "zz", 9).await.unwrap(); produced_puts += 1; touched.insert((9,9,"Z9".into()));

    // Poll sink
    let expect_total = produced_puts + produced_deletes;
    let mut got = WalAgg { total: 0, puts: 0, deletes: 0 };
    for _ in 0..100 {
        got = client
            .query(r#"
                SELECT
                  count() AS total,
                  countIf(is_deleted = 0) AS puts,
                  countIf(is_deleted = 1) AS deletes
                FROM wal.rdb_changelog
                WHERE src_db='srcdb' AND src_table='kv_tuple'
            "#)
            .fetch_one::<WalAgg>()
            .await
            .unwrap();
        if got.total >= expect_total { break; }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert_eq!(got.puts, produced_puts, "PUTs mismatch");
    assert_eq!(got.deletes, produced_deletes, "DELETEs mismatch");
    assert_eq!(got.total, expect_total, "TOTAL mismatch");

    // seq uniqueness
    let Dist { total, distinct_seq } = client
        .query("SELECT count() AS total, countDistinct(seq) AS distinct_seq
                FROM wal.rdb_changelog
                WHERE src_db='srcdb' AND src_table='kv_tuple'")
        .fetch_one::<Dist>()
        .await
        .unwrap();
    assert_eq!(total, distinct_seq, "seq duplicates detected");

    // live from WAL vs source
    let LiveCnt { n: live_from_wal } = client
        .query(r#"
            SELECT count() AS n
            FROM (
              SELECT argMax(is_deleted, seq) AS last_del
              FROM wal.rdb_changelog
              WHERE src_db='srcdb' AND src_table='kv_tuple'
              GROUP BY key
            ) WHERE last_del = 0
        "#)
        .fetch_one::<LiveCnt>()
        .await
        .unwrap();

    let LiveCnt { n: live_in_src } = client
        .query("SELECT count() AS n FROM srcdb.kv_tuple")
        .fetch_one::<LiveCnt>()
        .await
        .unwrap();

    assert_eq!(live_from_wal, live_in_src, "final state mismatch");

    // distinct keys touched ~= number of unique tuple PKs we addressed
    let expected_distinct = touched.len() as u64;
    let LiveCnt { n: distinct_in_wal } = client
        .query("SELECT countDistinct(key) AS n
                FROM wal.rdb_changelog
                WHERE src_db='srcdb' AND src_table='kv_tuple'")
        .fetch_one::<LiveCnt>()
        .await
        .unwrap();
    assert_eq!(distinct_in_wal, expected_distinct, "distinct key count mismatch");

    // stop tailer
    let _ = tx.send(());
    let _ = t.await;
}
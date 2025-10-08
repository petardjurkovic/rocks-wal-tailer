use clickhouse::Client;
use tempfile::TempDir;
use std::time::Duration;

use rocks_wal_tailer::{run_single_src, Src, TailArgs};
#[tokio::test(flavor = "multi_thread")]
async fn wal_to_clickhouse_discovery_end_to_end() {
    let ch_url = "http://127.0.0.1:8123".to_string();
    let client = Client::default().with_url(&ch_url).with_user("default").with_password("default123");

    // --- Schema: source (EmbeddedRocksDB) and destination (MergeTree) ---
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
        SETTINGS optimize_for_bulk_insert = 0;
    "#).execute().await.unwrap();

    // Destination with src_db/src_table columns
    client.query(r#"
        DROP TABLE IF EXISTS wal.rdb_changelog
    "#).execute().await.unwrap();
    client.query(r#"
        CREATE TABLE wal.rdb_changelog
        (
          src_db     String,
          src_table  String,
          ts         DateTime64(3),
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

    // --- Tailer args: multi-source shape (we pass one Src here) ---
    let ckpt_dir = TempDir::new().unwrap();
    let args = TailArgs {
        src: vec![Src { db: "srcdb".into(), table: "kv".into() }],
        checkpoint_dir: ckpt_dir.path().to_path_buf(),
        ch_url: ch_url.clone(),
        ch_database: "wal".into(),
        ch_table: "rdb_changelog".into(),
    };

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    // spawn exactly one tail loop
    let tail = tokio::spawn(async move {
        let src = Src { db: "srcdb".into(), table: "kv".into() };
        let _ = run_single_src(args, src, Some(rx)).await;
    });

    // --- Produce WAL via CH INSERTs into EmbeddedRocksDB source ---
    client.query("INSERT INTO srcdb.kv (key, value) VALUES ('user:1','A')").execute().await.unwrap();
    client.query("INSERT INTO srcdb.kv (key, value) VALUES ('user:1','B')").execute().await.unwrap();
    client.query("INSERT INTO srcdb.kv (key, value) VALUES ('user:2','X')").execute().await.unwrap();

    for i in 0..50u32 {
        let k = format!("k{:03}", i);
        client.query("INSERT INTO srcdb.kv (key, value) VALUES (?, 'v')")
            .bind(k)
            .execute().await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(10000)).await;
    client.query("INSERT INTO srcdb.kv (key, value) VALUES ('user:3','Z')").execute().await.unwrap();
    tokio::time::sleep(Duration::from_millis(10000)).await;
    let mut total = 0u64;
    for _ in 0..80 {
        let cnt: u64 = client
            .query("SELECT count() FROM wal.rdb_changelog WHERE src_db='srcdb' AND src_table='kv'")
            .fetch_one()
            .await
            .unwrap();
        total = cnt;
        if cnt >= 54 { break; }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(total >= 54, "expected at least 54 rows, got {}", total);

    // latest for user:1 should be value 'B'
    let last_b64: String = client
        .query("SELECT value_b64 FROM wal.rdb_changelog \
                WHERE src_db='srcdb' AND src_table='kv' AND key='user:1' \
                ORDER BY seq DESC LIMIT 1")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(last_b64, "Qg==");

    let _ = tx.send(());
    let _ = tail.await;
}

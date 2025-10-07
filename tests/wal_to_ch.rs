use clickhouse::Client;
use tempfile::TempDir;
use std::time::Duration;
use rocks_wal_tailer::{run_tailer, Src, TailArgs};

#[tokio::test(flavor = "multi_thread")]
async fn wal_to_clickhouse_discovery_end_to_end() {
    let ch_url = "http://127.0.0.1:8123".to_string();
    let client = Client::default().with_url(&ch_url);

    // --- Schema: source (EmbeddedRocksDB) and destination (MergeTree) ---
    client.query("CREATE DATABASE IF NOT EXISTS srcdb").execute().await.unwrap();
    client.query("CREATE DATABASE IF NOT EXISTS wal").execute().await.unwrap();
    client.query("DROP TABLE IF EXISTS srcdb.kv").execute().await.unwrap();
    client.query(
        r#"
        CREATE TABLE srcdb.kv
        (
            key   String,
            value String
        )
        ENGINE = EmbeddedRocksDB
        PRIMARY KEY key
        "#,
    ).execute().await.unwrap();

    client.query(
        r#"
        CREATE TABLE IF NOT EXISTS wal.rdb_changelog
        (
          ts         DateTime64(3),
          seq        UInt64,
          key        String,
          is_deleted UInt8,
          value_b64  Nullable(String),
          value_json Nullable(String)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(ts)
        ORDER BY (key, seq)
        "#,
    ).execute().await.unwrap();

    let ckpt_dir = TempDir::new().unwrap();
    let args = TailArgs {
        src: Src { db: "srcdb".into(), table: "kv".into() },
        checkpoint: ckpt_dir.path().join("ckpt.seq"),
        ch_url: ch_url.clone(),
        ch_database: "wal".into(),
        ch_table: "rdb_changelog".into(),
    };

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let tailer = tokio::spawn(async move {
        let _ = run_tailer(args, Some(rx)).await;
    });

    client.query("INSERT INTO srcdb.kv (key, value) VALUES ('user:1','A')").execute().await.unwrap();
    client.query("INSERT INTO srcdb.kv (key, value) VALUES ('user:1','B')").execute().await.unwrap();
    client.query("INSERT INTO srcdb.kv (key, value) VALUES ('user:2','X')").execute().await.unwrap();

    for i in 0..50u32 {
        let k = format!("k{:03}", i);
        client.query("INSERT INTO srcdb.kv (key, value) VALUES (?, 'v')")
            .bind(k)
            .execute().await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    client.query("INSERT INTO srcdb.kv (key, value) VALUES ('user:3','Z')").execute().await.unwrap();

    let mut total = 0u64;
    for _ in 0..80 { // up to ~16s
        let cnt: u64 = client
            .query("SELECT count() FROM wal.rdb_changelog")
            .fetch_one()
            .await
            .unwrap();
        total = cnt;
        if cnt >= 54 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(total >= 54, "expected at least 54 rows, got {}", total);

    let last_b64: String = client
        .query("SELECT value_b64 FROM wal.rdb_changelog WHERE key='user:1' ORDER BY seq DESC LIMIT 1")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(last_b64, "Qg==");

    let _ = tx.send(());
    let _ = tailer.await;
}

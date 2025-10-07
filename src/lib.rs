use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use clap::Parser;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use rocksdb::{Options, DB};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::time::sleep;
const BATCH_ROWS: usize = 10_000; // flush by size
const BATCH_MS: u64 = 500;        // flush by time (ms)
const IDLE_MS: u64 = 200;         // sleep when idle (ms)

#[derive(Parser, Debug, Clone)]
#[command(
    name = "ch-rocks-wal-tailer",
    about = "Tail RocksDB WAL and insert into ClickHouse using the clickhouse driver (discovery-only)"
)]
pub struct TailArgs {
    /// Source table to read from, format: db.table (must be EmbeddedRocksDB)
    #[arg(long = "src", env = "SRC", value_parser = parse_src)]
    pub src: Src,

    /// Checkpoint file storing the NEXT sequence to read
    #[arg(long, env = "CHECKPOINT", default_value = "wal.checkpoint")]
    pub checkpoint: PathBuf,

    /// ClickHouse connection
    #[arg(long, env = "CH_URL")]
    pub ch_url: String,
    /// Destination database & table
    #[arg(long, env = "CH_DATABASE", default_value = "wal")]
    pub ch_database: String,
    #[arg(long, env = "CH_TABLE", default_value = "rdb_changelog")]
    pub ch_table: String,
}

#[derive(Debug, Clone)]
pub struct Src { pub db: String, pub table: String }

fn parse_src(s: &str) -> std::result::Result<Src, String> {
    let (db, table) = s.split_once('.').ok_or("use format db.table")?;
    Ok(Src { db: db.to_string(), table: table.to_string() })
}

#[derive(Row, Serialize, Clone, Debug)]
pub struct ChangelogRow {
    ts: DateTime<Utc>,
    seq: u64,
    key: String,
    is_deleted: u8,
    value_b64: Option<String>,
    value_json: Option<String>,
}

pub async fn run_tailer_until_ctrlc(args: TailArgs) -> Result<()> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let ctrlc = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = tx.send(());
    });
    run_tailer(args, Some(rx)).await?;
    let _ = ctrlc.await;
    Ok(())
}

pub async fn run_tailer(
    mut args: TailArgs,
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
) -> Result<()> {
    // ClickHouse client
    let mut client = Client::default()
        .with_url(&args.ch_url)
        .with_database(&args.ch_database);
    if let (Ok(u), Ok(p)) = (std::env::var("CH_USER"), std::env::var("CH_PASSWORD")) {
        client = client.with_user(u).with_password(p);
    }

    // Discover rocksdb_dir for the given src
    let rocks_path = discover_rocksdb_dir(&client, &args.src.db, &args.src.table).await
        .with_context(|| format!("discover rocksdb_dir for {}.{}", args.src.db, args.src.table))?;
    eprintln!("Discovered rocksdb_dir: {}", rocks_path.display());

    // RocksDB open (read-only)
    let cf_names = list_or_default_cfs(&rocks_path);
    let db = DB::open_cf_for_read_only(&Options::default(), &rocks_path, &cf_names, false)
        .with_context(|| format!("open_cf_for_read_only at {:?}", rocks_path))?;

    // Resume from checkpoint if exists else start from latest
    let mut next_seq = read_checkpoint(&args.checkpoint)
        .unwrap_or_else(|| db.latest_sequence_number());
    eprintln!("CFs={:?}. Starting seq={}", cf_names, next_seq);

    let mut buf: Vec<ChangelogRow> = Vec::with_capacity(BATCH_ROWS);
    let mut last_flush = Instant::now();
    let mut cancel = cancel;

    loop {
        if let Some(rx) = cancel.as_mut() {
            if rx.try_recv().is_ok() { break; }
        }

        let mut advanced = false;
        let mut need_flush_now = false;

        match db.get_updates_since(next_seq) {
            Ok(mut it) => {
                while let Some(Ok((batch_first_seq, wb))) = it.next() {
                    let batch_len = wb.len() as u64;
                    let mut h = WalEmitter {
                        batch_first_seq,
                        idx: 0,
                        buf: &mut buf,
                    };
                    wb.iterate_cf(&mut h);

                    next_seq = batch_first_seq.saturating_add(batch_len);
                    advanced = true;

                    if buf.len() >= BATCH_ROWS {
                        need_flush_now = true;
                        break;
                    }
                }
                drop(it);
            }
            Err(e) => {
                anyhow::bail!(
                    "get_updates_since({}) failed: {} (increase WAL retention or reset checkpoint)",
                    next_seq,
                    e
                );
            }
        }

        if need_flush_now {
            flush(&client, &args.ch_table, &mut buf, &args.checkpoint, next_seq).await?;
            last_flush = Instant::now();
        }

        if !buf.is_empty() && last_flush.elapsed() >= Duration::from_millis(BATCH_MS) {
            flush(&client, &args.ch_table, &mut buf, &args.checkpoint, next_seq).await?;
            last_flush = Instant::now();
        }

        if !advanced {
            sleep(Duration::from_millis(IDLE_MS)).await;
        }
    }

    if !buf.is_empty() {
        flush(&client, &args.ch_table, &mut buf, &args.checkpoint, next_seq).await?;
    }
    Ok(())
}

async fn flush(
    client: &Client,
    table: &str,
    buf: &mut Vec<ChangelogRow>,
    ckpt_path: &Path,
    next_seq: u64,
) -> Result<()> {
    if buf.is_empty() { return Ok(()); }
    let rows = std::mem::take(buf);

    let mut insert = client.insert(table)?;
    for r in &rows { insert.write(r).await?; }
    insert.end().await?; // commit

    write_checkpoint(ckpt_path, next_seq)?;
    Ok(())
}
#[derive(Row, Deserialize)]
struct SysRow {
    engine: String,
    data_paths: Vec<String>,
    engine_full: String,
}

async fn discover_rocksdb_dir(client: &Client, db: &str, table: &str) -> Result<PathBuf> {
    let row: SysRow = client
        .query("SELECT engine, data_paths, engine_full FROM system.tables WHERE database = ? AND name = ?")
        .bind(db)
        .bind(table)
        .fetch_one()
        .await
        .with_context(|| "query system.tables")?;

    if row.engine != "EmbeddedRocksDB" {
        anyhow::bail!("{}.{} is not EmbeddedRocksDB (engine={})", db, table, row.engine);
    }
    if let Some(first) = row.data_paths.first() {
        return Ok(PathBuf::from(first));
    }

    if let Some(idx) = row.engine_full.find("rocksdb_dir") {
        let s = &row.engine_full[idx..];
        if let Some(path) = s.split(&['\'','"'][..]).nth(1) {
            return Ok(PathBuf::from(path));
        }
    }
    anyhow::bail!("No data_paths and no rocksdb_dir in engine_full for {}.{}", db, table);
}

fn encode_key(key: &[u8]) -> String {
    match std::str::from_utf8(key) {
        Ok(s) if s.chars().all(|c| !c.is_control() || matches!(c, '\n' | '\r' | '\t')) => s.to_string(),
        _ => format!("base64:{}", B64.encode(key)),
    }
}
fn read_checkpoint(path: &Path) -> Option<u64> { fs::read_to_string(path).ok()?.trim().parse::<u64>().ok() }
fn write_checkpoint(path: &Path, seq: u64) -> Result<()> {
    if let Some(p) = path.parent() { let _ = fs::create_dir_all(p); }
    fs::write(path, format!("{}\n", seq)).context("write checkpoint")
}
fn list_or_default_cfs(path: &Path) -> Vec<String> {
    match DB::list_cf(&Options::default(), path) {
        Ok(mut names) => { if names.is_empty() { names.push("default".into()); } names }
        Err(_) => vec!["default".into()],
    }
}

struct WalEmitter<'a> {
    batch_first_seq: u64,
    idx: u64,
    buf: &'a mut Vec<ChangelogRow>,
}
impl<'a> rocksdb::WriteBatchIteratorCf for WalEmitter<'a> {
    fn put_cf(&mut self, _cf_id: u32, key: &[u8], value: &[u8]) {
        self.buf.push(ChangelogRow {
            ts: Utc::now(),
            seq: self.batch_first_seq + self.idx,
            key: encode_key(key),
            is_deleted: 0,
            value_b64: Some(B64.encode(value)),
            value_json: None,
        });
        self.idx += 1;
    }
    fn delete_cf(&mut self, _cf_id: u32, key: &[u8]) {
        self.buf.push(ChangelogRow {
            ts: Utc::now(),
            seq: self.batch_first_seq + self.idx,
            key: encode_key(key),
            is_deleted: 1,
            value_b64: None,
            value_json: None,
        });
        self.idx += 1;
    }
    fn merge_cf(&mut self, _cf_id: u32, _key: &[u8], _value: &[u8]) { /* ignore merges */ }
}

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

/// Flush thresholds
const BATCH_ROWS: usize = 10_000;
const BATCH_MS: u64 = 500;
const IDLE_MS: u64 = 200;

#[derive(Debug, Clone)]
pub struct Src {
    pub db: String,
    pub table: String,
}

fn parse_src(s: &str) -> std::result::Result<Src, String> {
    let (db, table) = s.split_once('.').ok_or("use format db.table")?;
    Ok(Src { db: db.to_string(), table: table.to_string() })
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "ch-rocks-wal-tailer",
    about = "Tail multiple EmbeddedRocksDB tables and stream WAL into ClickHouse"
)]
pub struct TailArgs {
    /// One or more sources in form db.table | --src db1.t1,db2.t2
    #[arg(long = "src", env = "SRC", value_parser = parse_src, num_args = 1.., value_delimiter = ','
    )]
    pub src: Vec<Src>,

    /// Directory where per-source checkpoints will be stored (db__table.seq)
    #[arg(long, env = "CHECKPOINT_DIR", default_value = "wal_ckpts")]
    pub checkpoint_dir: PathBuf,

    /// ClickHouse connection
    #[arg(long, env = "CH_URL")]
    pub ch_url: String,

    /// Destination database & table
    #[arg(long, env = "CH_DATABASE", default_value = "wal")]
    pub ch_database: String,

    #[arg(long, env = "CH_TABLE", default_value = "rdb_changelog")]
    pub ch_table: String,
}

#[derive(Row, Serialize, Clone, Debug)]
pub struct ChangelogRow {
    src_db: String,
    src_table: String,
    ts: DateTime<Utc>,
    seq: u64,
    key: String,
    is_deleted: u8,
    value_b64: Option<String>,
    value_json: Option<String>,
}

pub async fn run_many_until_ctrlc(args: TailArgs) -> Result<()> {
    // spawn one task per source; keep a sender to cancel each
    let mut handles = Vec::with_capacity(args.src.len());
    let mut cancels = Vec::with_capacity(args.src.len());

    for src in args.src.clone() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        cancels.push(tx);

        let args_clone = args.clone();
        let h = tokio::spawn(async move {
            let _ = run_single_src(args_clone, src, Some(rx)).await;
        });
        handles.push(h);
    }

    tokio::signal::ctrl_c().await?;
    for tx in cancels {
        let _ = tx.send(());
    }

    // wait for all tasks
    for h in handles {
        let _ = h.await;
    }
    Ok(())
}

pub async fn run_single_src(
    args: TailArgs,
    src: Src,
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
) -> Result<()> {
    let mut writer = clickhouse_client_for(&args)?;

    let rocks_path = discover_rocksdb_dir(&writer, &src.db, &src.table).await
        .with_context(|| format!("discover rocksdb_dir for {}.{}", src.db, src.table))?;
    eprintln!("[{}.{}] rocksdb_dir: {}", src.db, src.table, rocks_path.display());

    let cf_names = list_or_default_cfs(&rocks_path);
    let db = DB::open_cf_for_read_only(&Options::default(), &rocks_path, &cf_names, false)
        .with_context(|| format!("open_cf_for_read_only at {:?}", rocks_path))?;

    let ckpt_path = checkpoint_path(&args.checkpoint_dir, &src);
    let mut next_seq = read_checkpoint(&ckpt_path).unwrap_or_else(|| db.latest_sequence_number());
    eprintln!("[{}.{}] CFs={:?}, start_seq={}", src.db, src.table, cf_names, next_seq);

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
                        src_db: &src.db,
                        src_table: &src.table,
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
                    "[{}.{}] get_updates_since({}) failed: {} (increase WAL retention or reset checkpoint)",
                    src.db, src.table, next_seq, e
                );
            }
        }

        if need_flush_now {
            flush(&mut writer, &args.ch_table, &mut buf, &ckpt_path, next_seq).await?;
            last_flush = Instant::now();
        }

        if !buf.is_empty() && last_flush.elapsed() >= Duration::from_millis(BATCH_MS) {
            flush(&mut writer, &args.ch_table, &mut buf, &ckpt_path, next_seq).await?;
            last_flush = Instant::now();
        }

        if !advanced {
            sleep(Duration::from_millis(IDLE_MS)).await;
        }
    }

    if !buf.is_empty() {
        flush(&mut writer, &args.ch_table, &mut buf, &ckpt_path, next_seq).await?;
    }
    eprintln!("[{}.{}] stopped", src.db, src.table);
    Ok(())
}
fn clickhouse_client_for(args: &TailArgs) -> Result<Client> {
    let mut c = Client::default()
        .with_url(&args.ch_url)
        .with_database(&args.ch_database);
    if let (Ok(u), Ok(p)) = (std::env::var("CH_USER"), std::env::var("CH_PASSWORD")) {
        c = c.with_user(u).with_password(p);
    }
    Ok(c)
}
async fn flush(
    client: &mut Client,
    table: &str,
    buf: &mut Vec<ChangelogRow>,
    ckpt_path: &Path,
    next_seq: u64,
) -> Result<()> {
    if buf.is_empty() { return Ok(()); }
    let rows = std::mem::take(buf);

    let mut insert = client.insert(table)?;
    for r in &rows { insert.write(r).await?; }
    insert.end().await?;

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
        if let Some(path) = s.split(&['\'', '"'][..]).nth(1) {
            return Ok(PathBuf::from(path));
        }
    }
    anyhow::bail!("No data_paths and no rocksdb_dir in engine_full for {}.{}", db, table);
}
fn checkpoint_path(dir: &Path, src: &Src) -> PathBuf {
    let safe_db = src.db.replace('.', "_");
    let safe_tbl = src.table.replace('.', "_");
    dir.join(format!("{safe_db}__{safe_tbl}.seq"))
}

fn encode_key(key: &[u8]) -> String {
    match std::str::from_utf8(key) {
        Ok(s) if s.chars().all(|c| !c.is_control() || matches!(c, '\n' | '\r' | '\t')) => s.to_string(),
        _ => format!("base64:{}", B64.encode(key)),
    }
}
fn read_checkpoint(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse::<u64>().ok()
}

fn write_checkpoint(path: &Path, seq: u64) -> Result<()> {
    if let Some(p) = path.parent() { let _ = fs::create_dir_all(p); }
    fs::write(path, format!("{}\n", seq)).context("write checkpoint")
}

fn list_or_default_cfs(path: &Path) -> Vec<String> {
    match DB::list_cf(&Options::default(), path) {
        Ok(mut names) => {
            if names.is_empty() { names.push("default".into()); }
            names
        }
        Err(_) => vec!["default".into()],
    }
}
struct WalEmitter<'a> {
    src_db: &'a str,
    src_table: &'a str,
    batch_first_seq: u64,
    idx: u64,
    buf: &'a mut Vec<ChangelogRow>,
}
impl<'a> rocksdb::WriteBatchIteratorCf for WalEmitter<'a> {
    fn put_cf(&mut self, _cf_id: u32, key: &[u8], value: &[u8]) {
        self.buf.push(ChangelogRow {
            src_db: self.src_db.to_string(),
            src_table: self.src_table.to_string(),
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
            src_db: self.src_db.to_string(),
            src_table: self.src_table.to_string(),
            ts: Utc::now(),
            seq: self.batch_first_seq + self.idx,
            key: encode_key(key),
            is_deleted: 1,
            value_b64: None,
            value_json: None,
        });
        self.idx += 1;
    }
    fn merge_cf(&mut self, _cf_id: u32, _key: &[u8], _value: &[u8]) {}
}

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use clap::Parser;
use clickhouse::Row as ChRow;
use clickhouse::{Client, Row};
use rocksdb::WriteBatchIteratorCf;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::io;
/// Flush thresholds
const BATCH_ROWS: usize = 10_000;
const BATCH_MS: u64 = 500;
const IDLE_MS: u64 = 200;

const SMALL_BATCH_ROWS: usize = 32;
const MAX_APPEND_DELAY_MS: u64 = 150;

#[derive(Debug, Clone)]
pub struct Src {
    pub db: String,
    pub table: String,
}

fn parse_src(s: &str) -> std::result::Result<Src, String> {
    let (db, table) = s.split_once('.').ok_or("use format db.table")?;
    Ok(Src {
        db: db.to_string(),
        table: table.to_string(),
    })
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

#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize)]
pub enum Op { Put=1, Merge=2, Delete=3, DeleteRange=4, LogData=5 }

#[derive(Debug, Serialize, clickhouse::Row)]
struct WalOut {
    db: String,
    table: String,
    cf: String,
    seq: u64,
    op: u8,
    key: Vec<u8>,                 // maps to CH String (arbitrary bytes)
    value: Option<Vec<u8>>,       // maps to Nullable(String)
}

impl From<ChangelogRow> for WalOut {
    fn from(r: ChangelogRow) -> Self {
        Self {
            db: r.db,
            table: r.table,
            cf: r.cf,
            seq: r.seq,
            op: r.op as u8,
            key: r.key,
            value: r.value,
        }
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct ChangelogRow {
    pub db: String,
    pub table: String,
    pub cf: String,
    pub seq: u64,
    pub op: Op,
    pub key: Vec<u8>,              // empty for LogData
    pub value: Option<Vec<u8>>,    // None for Delete
}

pub struct WalEmitter<'a> {
    pub src_db: &'a str,
    pub src_table: &'a str,
    pub batch_first_seq: u64,
    pub idx: u64,
    pub buf: &'a mut Vec<ChangelogRow>,
}

impl<'a> WalEmitter<'a> {
    #[inline]
    fn push(&mut self, cf: u32, op: Op, key: Vec<u8>, value: Option<Vec<u8>>) {
        let seq = self.batch_first_seq.saturating_add(self.idx);
        self.buf.push(ChangelogRow {
            db: self.src_db.to_string(),
            table: self.src_table.to_string(),
            cf: cf.to_string(),
            seq,
            op,
            key,
            value,
        });
        self.idx = self.idx.saturating_add(1);
    }
}

impl<'a> WriteBatchIteratorCf for WalEmitter<'a> {
    fn put_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) {
        self.push(cf, Op::Put, key.into(), Some(value.into()));
    }
    fn merge_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) {
        self.push(cf, Op::Merge, key.into(), Some(value.into()));
    }
    fn delete_cf(&mut self, cf: u32, key: &[u8]) {
        self.push(cf, Op::Delete, key.into(), None);
    }
}

#[derive(Debug, Serialize, ChRow)]
struct WalRow {
    src_db: String,
    src_table: String,
    seq: u64,
    key: String,
    is_deleted: u8,
    value_b64: Option<String>,
    value_json: Option<String>,
}

impl From<ChangelogRow> for WalRow {
    fn from(r: ChangelogRow) -> Self {
        let is_del = matches!(r.op, Op::Delete | Op::DeleteRange) as u8;
        let key = String::from_utf8_lossy(&r.key).to_string();
        let value_b64 = r.value.as_ref().map(|v| B64.encode(v));
        Self {
            src_db: r.db,
            src_table: r.table,
            seq: r.seq,
            key,
            is_deleted: is_del,
            value_b64,
            value_json: None,
        }
    }
}
pub async fn run_many_until_ctrlc(args: TailArgs) -> Result<()> {
    let mut handles = Vec::with_capacity(args.src.len());
    let mut cancels = Vec::with_capacity(args.src.len());

    for src in args.src.clone() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        cancels.push(tx);

        let args_clone = args.clone();
        let src_id = format!("{}.{}", src.db, src.table);
        let h = tokio::spawn(async move {
            match run_single_src(args_clone, src, Some(rx)).await {
                Ok(()) => eprintln!("[{src_id}] exited cleanly"),
                Err(e) => {
                    eprintln!("[{src_id}] ERROR: {:#}", e);
                    for (i, cause) in e.chain().skip(1).enumerate() {
                        eprintln!("    caused by [{i}]: {}", cause);
                    }
                }
            }
        });
        handles.push(h);
    }

    tokio::signal::ctrl_c().await?;
    for tx in cancels {
        let _ = tx.send(());
    }
    for h in handles {
        let _ = h.await;
    }
    Ok(())
}
fn load_checkpoint(path: &Path) -> Option<u64> {
    let s = fs::read_to_string(path).ok()?;
    s.trim().parse::<u64>().ok()
}

fn save_checkpoint(path: &Path, seq: u64) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    // tmp path in the same dir to keep rename atomic
    let mut tmp_os = path.as_os_str().to_owned();
    tmp_os.push(".tmp");
    let tmp: PathBuf = tmp_os.into();

    {
        let mut f = File::create(&tmp)?;
        f.write_all(seq.to_string().as_bytes())?;
        f.sync_all()?;
    }

    // atomic replace
    fs::rename(&tmp, path)?;

    // fsync directory to persist the rename on crash
    if let Some(parent) = path.parent() {
        let dir = File::open(parent)?;
        dir.sync_all()?;
    }

    Ok(())
}

pub async fn run_single_src(
    args: TailArgs,
    src: Src,
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
) -> Result<()> {
    const SMALL_BATCH_ROWS: usize = 32;
    const MAX_APPEND_DELAY_MS: u64 = 150;

    let mut writer = clickhouse_client_for(&args)?;

    let rocks_path = discover_rocksdb_dir(&writer, &src.db, &src.table)
        .await
        .with_context(|| format!("discover rocksdb_dir for {}.{}", src.db, src.table))?;
    let cf_names = list_or_default_cfs(&rocks_path);

    // Open as secondary no locks, with generous WAL retention
    let mut secondary_dir: PathBuf = args.checkpoint_dir.join("secondary");
    secondary_dir.push(format!("{}_{}", &src.db, &src.table));
    std::fs::create_dir_all(&secondary_dir).ok();

    let mut opts = Options::default();
    opts.set_wal_ttl_seconds(6 * 3600);
    opts.set_wal_size_limit_mb(4096);

    let db = DB::open_cf_as_secondary(&opts, &rocks_path, &secondary_dir, &cf_names)
        .with_context(|| format!("open_cf_as_secondary at {:?}", rocks_path))?;
    db.try_catch_up_with_primary().ok();

    // Start sequence from checkpoint or current head
    let ckpt_path = checkpoint_path(&args.checkpoint_dir, &src);
    let mut next_seq = load_checkpoint(&ckpt_path).unwrap_or_else(|| db.latest_sequence_number());

    // Buffers and timers
    let mut buf: Vec<ChangelogRow> = Vec::with_capacity(BATCH_ROWS);
    let mut last_append = std::time::Instant::now();
    let mut cancel = cancel;

    loop {
        if let Some(rx) = cancel.as_mut() {
            if rx.try_recv().is_ok() { break; }
        }

        db.try_catch_up_with_primary().ok();
        let mut appended = false;
        let mut request_small_flush = false;

        if next_seq < db.latest_sequence_number() {
            {
                let mut it = match db.get_updates_since(next_seq) {
                    Ok(it) => it,
                    Err(e) => {
                        // if is_wal_gap(&e) { next_seq = db.latest_sequence_number(); }
                        // else { anyhow::bail!("[{}.{}] get_updates_since({}) failed: {e}", src.db, src.table, next_seq); }
                        anyhow::bail!("[{}.{}] get_updates_since({}) failed: {e}", src.db, src.table, next_seq);
                    }
                };

                while let Some(item) = it.next() {
                    let (batch_first_seq, wb) = match item {
                        Ok(ok) => ok,
                        Err(e) => {
                            if is_wal_gap(&e) { next_seq = db.latest_sequence_number(); }
                            else { anyhow::bail!("[{}.{}] WAL iterator error at seq {}: {e}", src.db, src.table, next_seq); }
                            break;
                        }
                    };

                    let before = buf.len();
                    let mut h = WalEmitter {
                        src_db: &src.db,
                        src_table: &src.table,
                        batch_first_seq,
                        idx: 0,
                        buf: &mut buf,
                    };
                    wb.iterate_cf(&mut h);

                    if buf.len() > before {
                        appended = true;
                    }

                    next_seq = batch_first_seq.saturating_add(wb.len() as u64);

                    if buf.len() >= SMALL_BATCH_ROWS {
                        request_small_flush = true;
                        break;
                    }
                }
            }
        }

        db.try_catch_up_with_primary().ok();
        let drained = next_seq >= db.latest_sequence_number();

        // immediate flush decision
        if request_small_flush
            || buf.len() >= BATCH_ROWS
            || (!buf.is_empty() && drained)
            || (!buf.is_empty()
            && last_append.elapsed() >= std::time::Duration::from_millis(MAX_APPEND_DELAY_MS))
        {
            flush(&mut writer, &args.ch_table, &mut buf, &ckpt_path, next_seq).await?;
            last_append = std::time::Instant::now();
        }

        if !appended {
            tokio::time::sleep(std::time::Duration::from_millis(IDLE_MS)).await;
        }
    }
    println!("Buffer empty: {:?}", buf.is_empty());
    if !buf.is_empty() {
        flush(&mut writer, &args.ch_table, &mut buf, &ckpt_path, next_seq).await?;
        save_checkpoint(&ckpt_path, next_seq).ok();
    }

    Ok(())
}
fn is_wal_gap<E: std::fmt::Display>(e: &E) -> bool {
    let s = e.to_string();
    s.contains("NotFound")
        || s.contains("No such file")
        || s.contains("missing")
        || s.contains("gap")
        || s.contains("corrupt")
        || s.contains("recovered")
}

fn clickhouse_client_for(args: &TailArgs) -> Result<Client> {
    let mut c = Client::default()
        .with_url(&args.ch_url)
        .with_database(&args.ch_database)
        .with_user("default")
        .with_password("default123");
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
) -> anyhow::Result<()> {
    if buf.is_empty() { return Ok(()); }

    let rows: Vec<WalRow> = std::mem::take(buf).into_iter().map(WalRow::from).collect();
    eprintln!("[flush] inserting {} rows up to seq {}", rows.len(), next_seq);

    let mut insert = client.insert::<WalRow>(table)?;
    for r in &rows {
        insert.write(r).await?;
    }
    insert.end().await?;

    save_checkpoint(ckpt_path, next_seq)?;
    Ok(())
}
#[derive(Row, Deserialize)]
struct SysRowNew {
    engine: String,
    data_paths: Vec<String>,
    engine_full: String,
}

#[derive(Row, Deserialize)]
struct SysRowOld {
    engine: String,
    data_path: String,
    #[serde(default)]
    engine_full: String,
}

fn is_rocks_engine(engine: &str) -> bool {
    engine == "EmbeddedRocksDB" || engine == "RocksDB"
}

fn extract_rocksdb_dir(engine_full: &str) -> Option<PathBuf> {
    if let Some(idx) = engine_full.find("rocksdb_dir") {
        let rest = &engine_full[idx..];
        for quote in ['\'', '"'] {
            if let Some(start) = rest.find(quote) {
                let tail = &rest[start + 1..];
                if let Some(end) = tail.find(quote) {
                    return Some(PathBuf::from(&tail[..end]));
                }
            }
        }
    }
    None
}
pub async fn discover_rocksdb_dir(
    client: &clickhouse::Client,
    db: &str,
    table: &str,
) -> Result<PathBuf> {
    let q_new = "SELECT engine, data_paths, engine_full \
                 FROM system.tables WHERE database = ? AND name = ?";
    if let Some(row) = client
        .query(q_new)
        .bind(db)
        .bind(table)
        .fetch_optional::<SysRowNew>()
        .await
        .with_context(|| "query system.tables (data_paths)")?
    {
        if !is_rocks_engine(&row.engine) {
            return Err(anyhow!(
                "{}.{} is not EmbeddedRocksDB (engine={})",
                db,
                table,
                row.engine
            ));
        }
        if let Some(first) = row.data_paths.first() {
            return Ok(PathBuf::from(first));
        }
        if let Some(p) = extract_rocksdb_dir(&row.engine_full) {
            return Ok(p);
        }
        return Err(anyhow!(
            "{}.{}: data_paths empty and no rocksdb_dir in engine_full",
            db,
            table
        ));
    }

    let q_old = "SELECT engine, data_path, engine_full \
                 FROM system.tables WHERE database = ? AND name = ?";
    if let Some(row) = client
        .query(q_old)
        .bind(db)
        .bind(table)
        .fetch_optional::<SysRowOld>() // <-- and here
        .await
        .with_context(|| "query system.tables (data_path)")?
    {
        if !is_rocks_engine(&row.engine) {
            return Err(anyhow!(
                "{}.{} is not EmbeddedRocksDB (engine={})",
                db,
                table,
                row.engine
            ));
        }
        if !row.data_path.is_empty() {
            return Ok(PathBuf::from(row.data_path));
        }
        if let Some(p) = extract_rocksdb_dir(&row.engine_full) {
            return Ok(p);
        }
        return Err(anyhow!(
            "{}.{}: data_path empty and no rocksdb_dir in engine_full",
            db,
            table
        ));
    }

    Err(anyhow!(
        "Table not found in system.tables: {}.{}",
        db,
        table
    ))
}
fn checkpoint_path(dir: &Path, src: &Src) -> PathBuf {
    let safe_db = src.db.replace('.', "_");
    let safe_tbl = src.table.replace('.', "_");
    dir.join(format!("{safe_db}__{safe_tbl}.seq"))
}

fn encode_key(key: &[u8]) -> String {
    match std::str::from_utf8(key) {
        Ok(s)
            if s.chars()
                .all(|c| !c.is_control() || matches!(c, '\n' | '\r' | '\t')) =>
        {
            s.to_string()
        }
        _ => format!("base64:{}", B64.encode(key)),
    }
}
fn read_checkpoint(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse::<u64>().ok()
}

fn write_checkpoint(path: &Path, seq: u64) -> Result<()> {
    if let Some(p) = path.parent() {
        let _ = fs::create_dir_all(p);
    }
    fs::write(path, format!("{}\n", seq)).context("write checkpoint")
}

fn list_or_default_cfs(path: &Path) -> Vec<String> {
    match DB::list_cf(&Options::default(), path) {
        Ok(mut names) => {
            if names.is_empty() {
                names.push("default".into());
            }
            names
        }
        Err(_) => vec!["default".into()],
    }
}

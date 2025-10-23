use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use clap::Parser;
use clickhouse::Row as ChRow;
use clickhouse::{Client, Row};
use rocksdb::{Options, DB};
use rocksdb::{WriteBatchIterator, WriteBatchIteratorCf};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::io;
use serde_json::json;
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

#[derive(Debug, Clone)]
enum Ty { I64, U64, I32, U32, Str, Tuple(Vec<Ty>), Unknown }

#[derive(Debug, Clone)]
struct TableSchema {
    pk_name: String,
    pk_ty: Ty,
    vals: Vec<(String, Ty)>, // non-PK columns: (name, type)
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct ColRow { name: String, r#type: String, position: u64 }
fn parse_ty(s: &str) -> Ty {
    let t = s.trim();
    match t {
        "Int64" => Ty::I64,
        "UInt64" => Ty::U64,
        "Int32" => Ty::I32,
        "UInt32" => Ty::U32,
        "String" => Ty::Str,
        _ if t.starts_with("Tuple(") && t.ends_with(')') => {
            let inner = &t[6..t.len()-1];
            // split on commas at depth 0 (no nesting beyond one level for our tests)
            let mut parts = Vec::new();
            let mut cur = String::new();
            let mut depth = 0i32;
            for ch in inner.chars() {
                match ch {
                    '(' => { depth += 1; cur.push(ch); }
                    ')' => { depth -= 1; cur.push(ch); }
                    ',' if depth == 0 => { parts.push(cur.trim().to_string()); cur.clear(); }
                    _ => cur.push(ch),
                }
            }
            if !cur.trim().is_empty() { parts.push(cur.trim().to_string()); }
            Ty::Tuple(parts.into_iter().map(|p| parse_ty(&p)).collect())
        }
        _ => Ty::Unknown,
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> anyhow::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    // SIGINT (Ctrl-C)
    let ctrl_c = tokio::signal::ctrl_c();

    // SIGTERM (systemd stop)
    let mut term = signal(SignalKind::terminate())?;

    tokio::select! {
        _ = ctrl_c => {}
        _ = term.recv() => {}
    }
    Ok(())
}

async fn load_schema(client: &Client, db: &str, tbl: &str) -> Result<TableSchema> {
    let cols: Vec<ColRow> = client
        .query("SELECT name, type, position FROM system.columns WHERE database=? AND table=? ORDER BY position")
        .bind(db).bind(tbl)
        .fetch_all().await
        .context("load system.columns")?;

    if cols.is_empty() { return Err(anyhow!("no columns for {}.{}", db, tbl)); }

    // EmbeddedRocksDB uses a single column as PK
    let pk_idx = cols.iter().position(|c| c.name == "key").unwrap_or(0);
    let pk_col = &cols[pk_idx];
    let pk_ty = parse_ty(&pk_col.r#type);

    let mut vals = Vec::new();
    for (i, c) in cols.iter().enumerate() {
        if i == pk_idx { continue; }
        vals.push((c.name.clone(), parse_ty(&c.r#type)));
    }
    Ok(TableSchema { pk_name: pk_col.name.clone(), pk_ty, vals })
}
fn take<'a>(buf: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    if buf.len() < n { None } else { let (h,t)=buf.split_at(n); *buf=t; Some(h) }
}
fn read_u64(buf: &mut &[u8]) -> Option<u64> { Some(u64::from_le_bytes(take(buf,8)?.try_into().ok()?)) }
fn read_i64(buf: &mut &[u8]) -> Option<i64> { Some(i64::from_le_bytes(take(buf,8)?.try_into().ok()?)) }
fn read_u32(buf: &mut &[u8]) -> Option<u32> { Some(u32::from_le_bytes(take(buf,4)?.try_into().ok()?)) }
fn read_i32(buf: &mut &[u8]) -> Option<i32> { Some(i32::from_le_bytes(take(buf,4)?.try_into().ok()?)) }
fn read_varu64(buf: &mut &[u8]) -> Option<u64> {
    let mut x = 0u64;
    let mut s = 0u32;
    for _ in 0..10 {
        let b = *take(buf, 1)?.first().unwrap();
        if (b & 0x80) == 0 {
            x |= (b as u64) << s;
            return Some(x);
        }
        x |= ((b & 0x7F) as u64) << s;
        s += 7;
    }
    None
}
fn read_str(buf: &mut &[u8]) -> Option<String> {
    let len = read_varu64(buf)? as usize;
    let s = take(buf, len)?;
    Some(String::from_utf8_lossy(s).to_string())
}
fn decode_by_ty(buf: &mut &[u8], ty: &Ty) -> Option<serde_json::Value> {
    match ty {
        Ty::I64 => read_i64(buf).map(|v| json!(v)),
        Ty::U64 => read_u64(buf).map(|v| json!(v)),
        Ty::I32 => read_i32(buf).map(|v| json!(v)),
        Ty::U32 => read_u32(buf).map(|v| json!(v)),
        Ty::Str => read_str(buf).map(|v| json!(v)),
        Ty::Tuple(inner) => {
            let mut arr = Vec::with_capacity(inner.len());
            for t in inner {
                arr.push(decode_by_ty(buf, t)?);
            }
            Some(json!(arr))
        }
        Ty::Unknown => None,
    }
}
fn decode_key_human(raw: &[u8], ty: &Ty) -> Option<String> {
    let mut s = raw;
    let v = decode_by_ty(&mut s, ty)?;
    Some(match v {
        serde_json::Value::Array(a) => format!("{}", serde_json::Value::Array(a)),
        _ => v.to_string().trim_matches('"').to_string(),
    })
}
fn decode_values_json(raw: &[u8], cols: &[(String, Ty)]) -> Option<String> {
    let mut s = raw;
    let mut obj = serde_json::Map::with_capacity(cols.len());
    for (name, ty) in cols {
        let val = decode_by_ty(&mut s, ty)?;
        obj.insert(name.clone(), val);
    }
    Some(serde_json::Value::Object(obj).to_string())
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
    #[arg(long = "src", env = "SRC", value_parser = parse_src, num_args = 1.., value_delimiter = ',')]
    pub src: Vec<Src>,

    #[arg(long, env = "CHECKPOINT_DIR", default_value = "wal_ckpts")]
    pub checkpoint_dir: PathBuf,

    #[arg(long, env = "CH_URL")]
    pub ch_url: String,

    #[arg(long, env = "CH_DATABASE", default_value = "wal")]
    pub ch_database: String,

    #[arg(long, env = "CH_TABLE", default_value = "rdb_changelog")]
    pub ch_table: String,

    #[arg(long = "node-id", env = "NODE_ID")]
    pub node_id: Option<String>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize)]
pub enum Op {
    Put = 1,
    Merge = 2,
    Delete = 3,
    DeleteRange = 4,
    LogData = 5,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChangelogRow {
    pub db: String,
    pub table: String,
    pub cf: String,
    pub seq: u64,
    pub op: Op,
    pub key: Vec<u8>,           // empty for LogData
    pub value: Option<Vec<u8>>, // None for Delete
}

pub struct WalEmitterCf<'a> {
    pub src_db: &'a str,
    pub src_table: &'a str,
    pub batch_first_seq: u64,
    pub idx: u64,
    pub buf: &'a mut Vec<ChangelogRow>,
    pub count: usize,
}

impl<'a> WalEmitterCf<'a> {
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
        self.count += 1;
    }
}
impl<'a> WriteBatchIteratorCf for WalEmitterCf<'a> {
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
pub struct WalEmitterNoCf<'a> {
    pub src_db: &'a str,
    pub src_table: &'a str,
    pub batch_first_seq: u64,
    pub idx: u64,
    pub buf: &'a mut Vec<ChangelogRow>,
    pub count: usize,
}
impl<'a> WalEmitterNoCf<'a> {
    #[inline]
    fn push(&mut self, op: Op, key: Vec<u8>, value: Option<Vec<u8>>) {
        let seq = self.batch_first_seq.saturating_add(self.idx);

        self.buf.push(ChangelogRow {
            db: self.src_db.to_string(),
            table: self.src_table.to_string(),
            cf: "0".to_string(), // default CF
            seq,
            op,
            key,
            value,
        });
        self.idx = self.idx.saturating_add(1);
        self.count += 1;
    }
}
impl<'a> WriteBatchIterator for WalEmitterNoCf<'a> {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.push(Op::Put, key.into(), Some(value.into()));
    }
    fn delete(&mut self, key: &[u8]) {
        self.push(Op::Delete, key.into(), None);
    }
}

#[derive(Debug, Serialize, ChRow)]
struct WalRow {
    node_id: String,
    src_db: String,
    src_table: String,
    seq: u64,
    key: String,
    is_deleted: u8,
    value_b64: Option<String>,
    value_json: Option<String>,
}
fn fallback_key(raw: &[u8]) -> String {
    match std::str::from_utf8(raw) {
        Ok(s) if s.chars().all(|c| !c.is_control() || matches!(c, '\n'|'\r'|'\t')) => s.to_string(),
        _ => format!("base64:{}", B64.encode(raw)),
    }
}
fn to_wal_row(r: ChangelogRow, schema: &TableSchema, node_id: &str) -> WalRow {
    let key_human = decode_key_human(&r.key, &schema.pk_ty).unwrap_or_else(|| fallback_key(&r.key));
    let is_del = matches!(r.op, Op::Delete | Op::DeleteRange) as u8;
    let value_b64 = r.value.as_ref().map(|v| B64.encode(v));
    let value_json = r.value.as_ref().and_then(|v| decode_values_json(v, &schema.vals));
    WalRow {
        node_id: node_id.to_string(),
        src_db: r.db,
        src_table: r.table,
        seq: r.seq,
        key: key_human,
        is_deleted: is_del,
        value_b64,
        value_json,
    }
}
fn resolve_node_id(args: &TailArgs) -> String {
    if let Some(s) = args.node_id.clone() { return s; }
    if let Ok(h) = std::env::var("HOSTNAME") {
        if !h.is_empty() { return h; }
    }
    "unknown".to_string()
}
pub async fn run_many_until_ctrlc(args: TailArgs) -> Result<()> {
    if args.src.is_empty() {
        eprintln!("[wal-tailer] no --src provided; nothing to do");
        return Ok(());
    }

    let mut handles = Vec::with_capacity(args.src.len());
    let mut cancels = Vec::with_capacity(args.src.len());

    for src in args.src.clone() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        cancels.push(tx);

        eprintln!("Sync starting for table: [{}.{}]", src.db, src.table);

        let args_clone = args.clone();
        let src_id = format!("{}.{}", src.db, src.table);

        let h = tokio::spawn(async move {
            match run_single_src(args_clone, src, Some(rx)).await {
                Ok(()) => eprintln!("[{src_id}] exited cleanly"),
                Err(e) => {
                    eprintln!("[{src_id}] ERROR: {:#}", e);
                    for (i, cause) in e.chain().skip(1).enumerate() {
                        eprintln!("    caused by [{i}]: {cause}");
                    }
                }
            }
        });

        handles.push(h);
    }

    shutdown_signal().await?;

    for tx in cancels {
        let _ = tx.send(());
    }

    for h in handles {
        let _ = h.await;
    }

    eprintln!("[wal-tailer] shutdown complete");
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
    let node_id = resolve_node_id(&args);
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

    let opts = Options::default();
    let db = DB::open_cf_as_secondary(&opts, &rocks_path, &secondary_dir, &cf_names)
        .with_context(|| format!("open_cf_as_secondary at {:?}", rocks_path))?;
    db.try_catch_up_with_primary().ok();

    // Start sequence from checkpoint or current head
    let ckpt_path = checkpoint_path(&args.checkpoint_dir, &src);
    let mut last_applied_seq =
        load_checkpoint(&ckpt_path).unwrap_or_else(|| db.latest_sequence_number());

    // Buffers and timers
    let mut buf: Vec<ChangelogRow> = Vec::with_capacity(BATCH_ROWS);
    let mut last_append = std::time::Instant::now();
    let mut cancel = cancel;

    let mut total_expected: u64 = 0;
    let mut total_emitted: u64 = 0;
    loop {
        if let Some(rx) = cancel.as_mut() {
            if rx.try_recv().is_ok() {
                break;
            }
        }

        db.try_catch_up_with_primary().ok();
        let mut appended = false;
        let mut request_small_flush = false;

        if last_applied_seq < db.latest_sequence_number() {
            {
                let mut it = match db.get_updates_since(last_applied_seq) {
                    Ok(it) => it,
                    Err(e) => {
                        // if is_wal_gap(&e) { next_seq = db.latest_sequence_number(); }
                        // else { anyhow::bail!("[{}.{}] get_updates_since({}) failed: {e}", src.db, src.table, next_seq); }
                        anyhow::bail!(
                            "[{}.{}] get_updates_since({}) failed: {e}",
                            src.db,
                            src.table,
                            last_applied_seq
                        );
                    }
                };

                while let Some(item) = it.next() {
                    let (batch_first_seq, wb) = match item {
                        Ok(ok) => ok,
                        Err(e) => {
                            if is_wal_gap(&e) {
                                last_applied_seq = db.latest_sequence_number();
                            } else {
                                anyhow::bail!(
                                    "[{}.{}] WAL iterator error at seq {}: {e}",
                                    src.db,
                                    src.table,
                                    last_applied_seq
                                );
                            }
                            break;
                        }
                    };

                    let before = buf.len();

                    let mut hcf = WalEmitterCf {
                        src_db: &src.db,
                        src_table: &src.table,
                        batch_first_seq,
                        idx: 0,
                        buf: &mut buf,
                        count: 0,
                    };
                    wb.iterate_cf(&mut hcf);

                    // fallback to non-CF iteration if CF produced nothing
                    if hcf.count == 0 {
                        let mut h = WalEmitterNoCf {
                            src_db: &src.db,
                            src_table: &src.table,
                            batch_first_seq,
                            idx: 0,
                            buf: &mut buf,
                            count: 0,
                        };
                        wb.iterate(&mut h);
                        // debug non-CF detection
                        if h.count > 0 {
                            eprintln!(
                                "[{}.{}] non-CF batch at seq {}: {} ops",
                                src.db, src.table, batch_first_seq, h.count
                            );
                        }
                    }

                    let emitted: usize = buf.len() - before;
                    let expected: usize = wb.len();
                    let batch_len = expected as u64;
                    let last_seq_in_batch = batch_first_seq.saturating_add(batch_len - 1);

                    total_expected += expected as u64;
                    total_emitted += emitted as u64;

                    if emitted != expected {
                        eprintln!(
                            "[warn][{}.{}] batch first_seq={} had {} WAL ops, emitted {} rows (some ops unhandled?)",
                            src.db, src.table, batch_first_seq, expected, emitted
                        );
                    }

                    if emitted > 0 { appended = true; }
                    last_applied_seq = last_seq_in_batch;
                }
            }
        }

        db.try_catch_up_with_primary().ok();
        let drained = last_applied_seq >= db.latest_sequence_number();

        // immediate flush decision
        if request_small_flush
            || buf.len() >= BATCH_ROWS
            || (!buf.is_empty() && drained)
            || (!buf.is_empty()
                && last_append.elapsed() >= std::time::Duration::from_millis(MAX_APPEND_DELAY_MS))
        {
            let schema = load_schema(&writer, &src.db, &src.table).await?;
            flush(&mut writer, &args.ch_table, &mut buf, &ckpt_path, last_applied_seq, &schema, &node_id).await?;
            last_append = std::time::Instant::now();
        }

        if !appended {
            tokio::time::sleep(std::time::Duration::from_millis(IDLE_MS)).await;
        }
    }
    println!("Buffer empty: {:?}", buf.is_empty());
    if !buf.is_empty() {
        let schema = load_schema(&writer, &src.db, &src.table).await?;
        flush(&mut writer, &args.ch_table, &mut buf, &ckpt_path, last_applied_seq, &schema, &node_id).await?;
        save_checkpoint(&ckpt_path, last_applied_seq).ok();
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
    last_seq: u64,
    schema: &TableSchema,
    node_id: &str,
) -> anyhow::Result<()> {
    if buf.is_empty() { return Ok(()); }
    let rows: Vec<WalRow> = std::mem::take(buf).into_iter().map(|r| to_wal_row(r, schema, node_id )).collect();
    eprintln!("[flush] inserting {} rows up to seq {}", rows.len(), last_seq);
    let mut insert = client.insert::<WalRow>(table)?;
    for r in &rows { insert.write(r).await?; }
    insert.end().await?;
    save_checkpoint(ckpt_path, last_seq)?;
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

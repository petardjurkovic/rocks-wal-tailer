#!/usr/bin/env bash
# Usage examples:
#   SRC="srcdb.kv" ./rocks-wal-tailer.sh
#   SRC="db1.t1,db2.t2" CH_URL="http://ch:8123" CHECKPOINT_DIR="/var/lib/wal_ckpts" ./rocks-wal-tailer.sh
#   INIT=1 CH_USER=default CH_PASSWORD=default123 SRC="srcdb.kv" ./rocks-wal-tailer.sh

set -euo pipefail

# ---- config via env  ----
SRC="${SRC:-}" # REQUIRED: "db.table" or "db1.t1,db2.t2"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-wal_ckpts}"
CH_URL="${CH_URL:-http://127.0.0.1:8123}"
CH_DATABASE="${CH_DATABASE:-wal}"
CH_TABLE="${CH_TABLE:-rdb_changelog}"
CH_USER="${CH_USER:-}"
CH_PASSWORD="${CH_PASSWORD:-}"
INIT="${INIT:-1}" # INIT=1 to create dest DB/table if missing
TAIL_BIN="${TAIL_BIN:-rocks-wal-tailer}"
NODE_ID="$(hostname -f 2>/dev/null || hostname)"
# ---- flag parsing ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --src) SRC="$2"; shift 2 ;;
    --checkpoint-dir|--checkpoint_dir) CHECKPOINT_DIR="$2"; shift 2 ;;
    --ch-url|--ch_url) CH_URL="$2"; shift 2 ;;
    --ch-database|--ch_database) CH_DATABASE="$2"; shift 2 ;;
    --ch-table|--ch_table) CH_TABLE="$2"; shift 2 ;;
    --init) INIT=1; shift ;;
    --bin) TAIL_BIN="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: SRC='db1.t1,db2.t2' [CH_URL=..] [CH_DATABASE=..] [CH_TABLE=..] [CHECKPOINT_DIR=..] [INIT=1] $0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2; exit 64 ;;
  esac
done

# ---- checks ----
[[ -n "$SRC" ]] || { echo "ERR: set SRC='db.table[,db2.t2...]'" >&2; exit 64; }
[[ -x "$TAIL_BIN" ]] || { echo "ERR: binary not found/executable: $TAIL_BIN" >&2; exit 66; }

mkdir -p "$CHECKPOINT_DIR"

# ---- ClickHouse reachability + schema init ----
AUTH=()
[[ -n "$CH_USER" ]] && AUTH=(-u "${CH_USER}:${CH_PASSWORD}")

# ping
if ! curl -fsS "${AUTH[@]}" "$CH_URL/ping" >/dev/null; then
  echo "ERR: cannot reach ClickHouse at $CH_URL" >&2
  exit 69
fi

if [[ "$INIT" == "1" ]]; then

  db_sql="CREATE DATABASE IF NOT EXISTS ${CH_DATABASE}"
  tbl_sql="
      CREATE TABLE IF NOT EXISTS wal.rdb_changelog
      (
          node_id    String,
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
          ORDER BY (seq, src_db, src_table, node_id);
  "
  echo "Initializing ${CH_DATABASE}.${CH_TABLE}..."
  curl -fsS "${AUTH[@]}" -H 'Content-Type: text/plain' --data-binary "$db_sql" "$CH_URL"
  curl -fsS "${AUTH[@]}" -H 'Content-Type: text/plain' --data-binary "$tbl_sql" "$CH_URL"
fi

# ---- run tailer ----
cmd=(
  "$TAIL_BIN"
  --node-id "$NODE_ID"
  --src "$SRC"
  --checkpoint-dir "$CHECKPOINT_DIR"
  --ch-url "$CH_URL"
  --ch-database "$CH_DATABASE"
  --ch-table "$CH_TABLE"
)

# forward signals and wait
"${cmd[@]}" &
child_pid=$!

trap 'kill -TERM $child_pid 2>/dev/null || true; wait $child_pid 2>/dev/null || true' INT TERM
wait $child_pid

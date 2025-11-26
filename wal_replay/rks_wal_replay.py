import os, json, time, signal, logging, random, re
from typing import List, Tuple, Dict, Optional, Any
from urllib.parse import urlparse
import clickhouse_connect

WAL_DB = os.getenv('WAL_DB', 'wal')
WAL_TABLE = os.getenv('WAL_TABLE', 'rdb_changelog')

CH_URL = os.getenv('CH_URL', 'http://127.0.0.1:8124')
CH_USER = os.getenv('CH_USER', 'default')
CH_PASS = os.getenv('CH_PASS', 'default123')

REPLICATED_SPECS = json.loads(os.getenv('REPLICATED_SPECS', '[]') or '[]')
SHARDED_SPECS = json.loads(os.getenv('SHARDED_SPECS', '[]') or '[]')

BATCH = int(os.getenv('BATCH', '5000'))
DELETE_BATCH = int(os.getenv('DELETE_BATCH', '2000'))
POLL_INTERVAL_SEC = float(os.getenv('POLL_INTERVAL_SEC', '1.0'))
MAX_BACKOFF_SEC = float(os.getenv('MAX_BACKOFF_SEC', '30'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

# Setup logging
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("wal_replayer")

def ch_client(url: str, user: str, password: str, port: int):
    u = urlparse(url)
    host = u.hostname or '127.0.0.1'

    secure = (u.scheme == 'https')
    logger.info(f"Connecting to ClickHouse at {host}:{port} user={user}")
    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password, secure=secure)

cli = ch_client(CH_URL, CH_USER, CH_PASS, 8124)

def split_comma_top(s: str) -> List[str]:
    parts, cur, depth = [], [], 0
    for ch in s:
        if ch == '(': depth += 1; cur.append(ch)
        elif ch == ')': depth -= 1; cur.append(ch)
        elif ch == ',' and depth == 0: parts.append(''.join(cur).strip()); cur = []
        else: cur.append(ch)
    if cur: parts.append(''.join(cur).strip())
    return parts

def ensure_ckpt_table():
    cli.command(f"""
        CREATE TABLE IF NOT EXISTS {WAL_DB}.applier_ckpt
        (
          src_db     String,
          src_table  String,
          node_id    String,
          last_seq   UInt64,
          updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree
        ORDER BY (src_db, src_table, node_id)
    """)

def get_scalar(val: Any) -> Any:
    if isinstance(val, dict):
        return list(val.values())[0]
    return val

def last_seq_for(db: str, tbl: str, node: str) -> int:
    q = f"""
      SELECT argMax(last_seq, updated_at)
      FROM {WAL_DB}.applier_ckpt
      WHERE src_db=%(db)s AND src_table=%(tbl)s AND node_id = %(node)s
    """
    val = cli.query(q, {'db': db, 'tbl': tbl, 'node': node}).first_item
    return int(get_scalar(val) or 0)

def save_seq(db: str, tbl: str, node: str, seq: int):
    cli.command(f"""
      INSERT INTO {WAL_DB}.applier_ckpt (src_db, src_table, node_id, last_seq)
      VALUES (%(db)s, %(tbl)s, %(node)s, %(seq)s)
    """, {'db': db, 'tbl': tbl, 'node': node, 'seq': seq})

def target_schema(dst_db: str, dst_tbl: str) -> Tuple[str, str, List[Tuple[str, str]]]:
    rows = cli.query("""
                     SELECT name, type, position
                     FROM system.columns
                     WHERE database = %(db)s
                       AND table = %(tbl)s
                     ORDER BY position
                  """, {'db': dst_db, 'tbl': dst_tbl}).result_rows
    if not rows:
        raise RuntimeError(f"Target table not found: {dst_db}.{dst_tbl}")

    if isinstance(rows[0], dict):
        rows = [(r['name'], r['type'], r['position']) for r in rows]

    pk_name, pk_type, _ = rows[0]
    values = [(n, t) for (n, t, _) in rows[1:]]
    return pk_name, pk_type, values

def get_distributed_details(db: str, table: str) -> Tuple[str, str, str]:
    row = cli.query("SELECT engine_full FROM system.tables WHERE database=%(db)s AND name=%(table)s",
                    {'db': db, 'table': table}).first_item
    engine_full = str(get_scalar(row) or '')
    m = re.search(r"Distributed\s*\(\s*'?([^',]+)'?\s*,\s*'?([^',]+)'?\s*,\s*'?([^',]+)'?", engine_full)
    if not m:
        raise ValueError(f"Could not parse Distributed engine details for {db}.{table}: {engine_full}")
    return m.group(1), m.group(2), m.group(3)

def get_shard_address(cluster: str, shard_num: int) -> str:
    """Resolves the host:port for a specific shard index in a cluster."""
    q = f"""
        SELECT host_name, port 
        FROM system.clusters 
        WHERE cluster = '{cluster}' AND shard_num = {shard_num} AND replica_num = 1
    """
    res = cli.query(q).result_rows
    if not res:
        raise ValueError(f"Shard {shard_num} not found in cluster {cluster}")

    row = res[0]
    if isinstance(row, dict):
        return f"{row['host_name']}:{row['port']}"
    return f"{row[0]}:{row[1]}"

def json_extract_field_expr(col_type: str, json_ref: str, field: str) -> str:
    t = col_type.strip()
    if t.startswith('Nullable(') and t.endswith(')'):
        return f"CAST({json_extract_field_expr(t[9:-1], json_ref, field)}, '{t}')"

    mapping = {
        'String': "JSONExtractString",
        'Int64': "JSONExtractInt", 'UInt64': "JSONExtractUInt",
        'Int32': "JSONExtractInt", 'UInt32': "JSONExtractUInt",
        'Float64': "JSONExtractFloat", 'Float32': "JSONExtractFloat"
    }
    if t in mapping:
        return f"{mapping[t]}({json_ref}, '{field}')"
    return f"CAST(JSONExtractString({json_ref}, '{field}'), '{t}')"

def key_expr(pk_type: str) -> str:
    t = pk_type.strip()
    if t.startswith('Tuple('):
        comps = [c.strip() for c in split_comma_top(t[6:-1])]
        items = []
        for i, ct in enumerate(comps):
            idx = i + 1
            if 'Int' in ct: items.append(f"JSONExtractInt(key, {idx})")
            elif 'Float' in ct: items.append(f"JSONExtractFloat(key, {idx})")
            else: items.append(f"JSONExtractString(key, {idx})")
            items[-1] = f"CAST({items[-1]}, '{ct}')"
        return f"({', '.join(items)})"
    if t == 'String': return "if(startsWith(key,'base64:'), base64Decode(substring(key,9)), key)"
    if 'Int' in t: return "JSONExtractInt(key, '$')"
    return f"CAST(JSONExtractString(key, '$'), '{t}')"

def wal_where(sdb: str, stbl: str, since: int, node: str) -> Tuple[str, Dict[str, object]]:
    pred = ["src_db=%(sdb)s", "src_table=%(stbl)s", "seq>%(since)s", "node_id=%(node)s"]
    params = {'sdb': sdb, 'stbl': stbl, 'since': since, 'node': node}
    return ' AND '.join(pred), params

def build_select(dst_db: str, dst_tbl: str, pk_name: str, pk_type: str,
                 value_cols: List[Tuple[str, str]], sdb: str, stbl: str,
                 since: int, node: str) -> Tuple[str, Dict[str, object]]:
    kexpr = key_expr(pk_type)
    sels = [f"{kexpr} AS {pk_name}"] + [f"{json_extract_field_expr(t, 'value_json', n)} AS {n}" for n, t in value_cols]
    where, params = wal_where(sdb, stbl, since, node)
    sql = f"SELECT {', '.join(sels)} FROM {WAL_DB}.{WAL_TABLE} WHERE {where} AND is_deleted=0 ORDER BY seq LIMIT {BATCH}"
    return sql, params

def build_fetch_deletes(pk_name: str, pk_type: str, sdb: str, stbl: str,
                        since: int, node: str) -> Tuple[str, Dict[str, object]]:
    kexpr = key_expr(pk_type)
    where, params = wal_where(sdb, stbl, since, node)
    sql = f"SELECT {kexpr} FROM {WAL_DB}.{WAL_TABLE} WHERE {where} AND is_deleted=1 ORDER BY seq LIMIT {DELETE_BATCH}"
    return sql, params

def max_seq_since(sdb: str, stbl: str, since: int, node: str) -> int:
    where, params = wal_where(sdb, stbl, since, node)
    q = f"SELECT max(seq) FROM {WAL_DB}.{WAL_TABLE} WHERE {where}"
    m = cli.query(q, params).first_item
    return int(get_scalar(m) or 0)

def format_val_for_sql(val: Any) -> str:
    if isinstance(val, str):
        safe_val = val.replace("'", "''")
        return f"'{safe_val}'"
    if isinstance(val, (list, tuple)):
        return f"({', '.join(format_val_for_sql(v) for v in val)})"
    return str(val)

def get_tuple_arity(pk_type: str) -> int:
    if not pk_type.startswith('Tuple('): return 1
    inner = pk_type[6:-1]
    parts = split_comma_top(inner)
    return len(parts)

# --- Appliers ---

def apply_replicated_spec(spec: Dict):
    sdb = spec['src_db']
    stbl = spec['src_table']
    dst_db = spec['dst_db']
    dst_tbl = spec['dst_table']

    source_node = spec.get('source_node')
    if not source_node:
        q = f"SELECT min(node_id) FROM {WAL_DB}.{WAL_TABLE} WHERE src_db=%(db)s AND src_table=%(tbl)s"
        source_node = get_scalar(cli.query(q, {'db': sdb, 'tbl': stbl}).first_item)
        if not source_node: return

    pk, pk_ty, vals = target_schema(dst_db, dst_tbl)
    cols = [pk] + [n for n, _ in vals]
    since = last_seq_for(sdb, stbl, source_node)
    logger.info(f"[Replicated] Start {sdb}.{stbl}->{dst_db}.{dst_tbl} Node={source_node} Seq={since}")

    while True:
        new_since = max_seq_since(sdb, stbl, since, source_node)
        if new_since == 0 or new_since == since: break

        sel_sql, sel_params = build_select(dst_db, dst_tbl, pk, pk_ty, vals, sdb, stbl, since, source_node)

        ins = f"INSERT INTO {dst_db}.{dst_tbl} ({', '.join(cols)}) {sel_sql}"
        cli.query(ins, sel_params)

        del_sql, del_params = build_fetch_deletes(pk, pk_ty, sdb, stbl, since, source_node)
        del_keys = cli.query(del_sql, del_params).result_rows

        if del_keys:
            keys_formatted = []
            for row in del_keys:
                val = row[0] if isinstance(row, (list, tuple)) else row['key']
                keys_formatted.append(format_val_for_sql(val))

            if keys_formatted:
                in_list = ", ".join(keys_formatted)
                lhs = pk
                if pk_ty.startswith('Tuple'):
                    arity = get_tuple_arity(pk_ty)
                    lhs = f"({', '.join([f'{pk}.{i+1}' for i in range(arity)])})"

                cli.command(f"ALTER TABLE {dst_db}.{dst_tbl} DELETE WHERE {lhs} IN ({in_list}) SETTINGS mutations_sync=1")

        since = new_since
        save_seq(sdb, stbl, source_node, since)
        logger.info(f"  -> Committed seq {since}")

def apply_sharded_spec(spec: Dict):
    sdb = spec['src_db']
    stbl = spec['src_table']
    distributed_table = spec.get('distributed') or spec.get('distributed_table')
    node_to_shard: Dict[str, int] = spec['node_to_shard']

    dst_db, dst_tbl_dist = distributed_table.split('.', 1)
    pk, pk_ty, vals = target_schema(dst_db, dst_tbl_dist)
    cols = [pk] + [n for n, _ in vals]

    try:
        cluster, local_db, local_tbl = get_distributed_details(dst_db, dst_tbl_dist)
    except Exception as e:
        logger.error(f"Failed to resolve distributed details: {e}")
        cluster, local_db, local_tbl = None, None, None

    for node_id, target_shard_id in node_to_shard.items():
        since = last_seq_for(sdb, stbl, node_id)

        try:
            target_address = get_shard_address(cluster, int(target_shard_id))
        except Exception as e:
            logger.error(f"Skipping node {node_id}: {e}")
            continue

        logger.info(f"[Sharded] {sdb}.{stbl}({node_id}) -> Cluster '{cluster}' Shard {target_shard_id} ({target_address})")

        while True:
            new_since = max_seq_since(sdb, stbl, since, node_id)
            if new_since == 0 or new_since == since: break

            sel_sql, sel_params = build_select(dst_db, dst_tbl_dist, pk, pk_ty, vals, sdb, stbl, since, node_id)

            ins = (f"INSERT INTO FUNCTION remote('{target_address}', '{local_db}', '{local_tbl}', '{CH_USER}', '{CH_PASS}') "
                   f"({', '.join(cols)}) {sel_sql}")

            logger.info(f"  -> Executing INSERT to {target_address}")
            cli.query(ins, sel_params)

            del_sql, del_params = build_fetch_deletes(pk, pk_ty, sdb, stbl, since, node_id)
            del_keys = cli.query(del_sql, del_params).result_rows

            if del_keys:
                keys_formatted = []
                for row in del_keys:
                    val = row[0] if isinstance(row, (list, tuple)) else row['key']
                    keys_formatted.append(format_val_for_sql(val))

                if keys_formatted:
                    in_list = ", ".join(keys_formatted)
                    lhs = pk
                    if pk_ty.startswith('Tuple'):
                        arity = get_tuple_arity(pk_ty)
                        lhs = f"({', '.join([f'{pk}.{i+1}' for i in range(arity)])})"

                    # Use ON CLUSTER for deletes as it simplifies targeting (all replicas get it anyway)
                    cmd = f"ALTER TABLE {local_db}.{local_tbl} ON CLUSTER '{cluster}' DELETE WHERE {lhs} IN ({in_list}) SETTINGS mutations_sync=1"
                    cli.command(cmd)

            since = new_since
            save_seq(sdb, stbl, node_id, since)
            logger.info(f"  [{node_id}] Checkpoint saved: {since}")

_shutdown = False
def _sigterm(_s, _f): global _shutdown; _shutdown = True

def run_once() -> bool:
    ensure_ckpt_table()
    try:
        did_any = False
        for spec in REPLICATED_SPECS:
            apply_replicated_spec(spec)
            did_any = True
        for spec in SHARDED_SPECS:
            apply_sharded_spec(spec)
            did_any = True
        return did_any
    except Exception as e:
        logging.exception("Error in run_once")
        return False

def serve_forever():
    signal.signal(signal.SIGINT, _sigterm)
    signal.signal(signal.SIGTERM, _sigterm)

    ensure_ckpt_table()
    backoff = POLL_INTERVAL_SEC
    while not _shutdown:
        start = time.time()
        did_work = run_once()
        duration = time.time() - start
        if did_work and duration > 0.1: backoff = POLL_INTERVAL_SEC
        else: backoff = min(MAX_BACKOFF_SEC, max(POLL_INTERVAL_SEC, backoff * 2))
        time.sleep(backoff + random.uniform(0, min(0.25, backoff * 0.1)))

if __name__ == '__main__':
    serve_forever()
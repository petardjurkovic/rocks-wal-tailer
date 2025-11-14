import os, json, time, signal, logging, random
from typing import List, Tuple, Dict, Optional
from urllib.parse import urlparse
import clickhouse_connect

WAL_DB = os.getenv('WAL_DB', 'wal')
WAL_TABLE = os.getenv('WAL_TABLE', 'rdb_changelog')

FILTER_SRC_DB = os.getenv('FILTER_SRC_DB')
FILTER_SRC_TABLE = os.getenv('FILTER_SRC_TABLE')

CH_URL = os.getenv('CH_URL', 'http://127.0.0.1:8124')
CH_USER = os.getenv('CH_USER', 'default')
CH_PASS = os.getenv('CH_PASS', '')

REPLICATED_SPECS = json.loads(os.getenv('REPLICATED_SPECS', '[]') or '[]')
SHARDED_SPECS = json.loads(os.getenv('SHARDED_SPECS', '[]') or '[]')

# preferred node: "db.tbl=nodeA,db2.tbl2=nodeB"
PREFER_NODE: Dict[str, str] = {}
for kv in os.getenv('PREFER_NODE', '').split(','):
    if '=' in kv:
        k, v = kv.split('=', 1)
        k = k.strip();
        v = v.strip()
        if k and v: PREFER_NODE[k] = v

BATCH = int(os.getenv('BATCH', '5000'))
DELETE_BATCH = int(os.getenv('DELETE_BATCH', '2000'))

# Loop timings
POLL_INTERVAL_SEC = float(os.getenv('POLL_INTERVAL_SEC', '1.0'))
MAX_BACKOFF_SEC = float(os.getenv('MAX_BACKOFF_SEC', '30'))

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()


def ch_client(url: str, user: str, password: str):
    u = urlparse(url)
    host = u.hostname or '127.0.0.1'
    port = u.port or (8443 if u.scheme == 'https' else 8123)
    secure = (u.scheme == 'https')
    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password, secure=secure)


cli = ch_client(CH_URL, CH_USER, CH_PASS)


def split_comma_top(s: str) -> List[str]:
    parts, cur, depth = [], [], 0
    for ch in s:
        if ch == '(':
            depth += 1;
            cur.append(ch)
        elif ch == ')':
            depth -= 1;
            cur.append(ch)
        elif ch == ',' and depth == 0:
            parts.append(''.join(cur).strip());
            cur = []
        else:
            cur.append(ch)
    if cur: parts.append(''.join(cur).strip())
    return parts


def ensure_ckpt_table():
    cli.command(f"""
        CREATE TABLE IF NOT EXISTS {WAL_DB}.applier_ckpt
        (
          src_db     String,
          src_table  String,
          node_id    Nullable(String),
          last_seq   UInt64,
          updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree
        ORDER BY (src_db, src_table, node_id)
    """)


def choose_replicated_node(db: str, tbl: str) -> str:
    key = f'{db}.{tbl}'
    if key in PREFER_NODE:
        return PREFER_NODE[key]
    q = f"SELECT min(node_id) FROM {WAL_DB}.{WAL_TABLE} WHERE src_db=%(db)s AND src_table=%(tbl)s"
    nid = cli.query(q, {'db': db, 'tbl': tbl}).first_item
    if not nid:
        raise RuntimeError(f"No WAL rows for replicated selection: {key}")
    return str(nid)


def last_seq_for(db: str, tbl: str, node: Optional[str]) -> int:
    q = f"""
      SELECT argMax(last_seq, updated_at)
      FROM {WAL_DB}.applier_ckpt
      WHERE src_db=%(db)s AND src_table=%(tbl)s AND node_id <=> %(node)s
    """
    val = cli.query(q, {'db': db, 'tbl': tbl, 'node': node}).first_item
    return int(val or 0)


def save_seq(db: str, tbl: str, node: Optional[str], seq: int):
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
    pk_name, pk_type, _ = rows[0]
    values = [(n, t) for (n, t, _) in rows[1:]]
    return pk_name, pk_type, values


def json_extract_field_expr(col_type: str, json_ref: str, field: str) -> str:
    path = f"$.{field}"
    t = col_type.strip()
    if t.startswith('Nullable(') and t.endswith(')'):
        inner = t[9:-1]
        return f"CAST({json_extract_field_expr(inner, json_ref, field)}, '{t}')"
    if t.startswith('Tuple('):
        inner = t[6:-1]
        comps = [c.strip() for c in split_comma_top(inner)]
        parts: List[str] = []
        for i, ct in enumerate(comps):
            ipath = f"$.{field}[{i}]"
            if ct.startswith('Nullable(') and ct.endswith(')'):
                inner_ct = ct[9:-1]
                parts.append(f"CAST({json_extract_field_expr(inner_ct, json_ref, field + f'[{i}]')}, '{ct}')")
            elif ct == 'String':
                parts.append(f"JSONExtractString({json_ref}, '{ipath}')")
            elif ct in ('Int64', 'Int32'):
                parts.append(f"JSONExtractInt({json_ref}, '{ipath}')")
            elif ct in ('UInt64', 'UInt32'):
                parts.append(f"JSONExtractUInt({json_ref}, '{ipath}')")
            elif ct in ('Float32', 'Float64'):
                parts.append(f"JSONExtractFloat({json_ref}, '{ipath}')")
            else:
                parts.append(f"CAST(JSONExtractString({json_ref}, '{ipath}'), '{ct}')")
        return f"tuple({', '.join(parts)})"
    mapping = {
        'String': "JSONExtractString",
        'Int64': "JSONExtractInt",
        'UInt64': "JSONExtractUInt",
        'Int32': "JSONExtractInt",
        'UInt32': "JSONExtractUInt",
        'Float32': "JSONExtractFloat",
        'Float64': "JSONExtractFloat",
    }
    if t in mapping:
        return f"{mapping[t]}({json_ref}, '{path}')"
    if t in ('Date', 'Date32'):
        return f"toDate(JSONExtractString({json_ref}, '{path}'))"
    if t.startswith('DateTime'):
        return f"parseDateTimeBestEffort(JSONExtractString({json_ref}, '{path}'))"
    if t == 'UUID':
        return f"toUUID(JSONExtractString({json_ref}, '{path}'))"
    return f"CAST(JSONExtractString({json_ref}, '{path}'), '{t}')"


def key_expr(pk_type: str) -> str:
    t = pk_type.strip()
    if t.startswith('Tuple('):
        inner = t[6:-1]
        comps = [c.strip() for c in split_comma_top(inner)]
        items: List[str] = []
        for i, ct in enumerate(comps):
            ipath = f"$[{i}]"
            if ct.startswith('Nullable(') and ct.endswith(')'):
                inner_ct = ct[9:-1]
                base = (
                    f"JSONExtractString(key, '{ipath}')" if inner_ct == 'String' else
                    f"JSONExtractInt(key, '{ipath}')" if inner_ct in ('Int64', 'Int32') else
                    f"JSONExtractUInt(key, '{ipath}')" if inner_ct in ('UInt64', 'UInt32') else
                    f"JSONExtractFloat(key, '{ipath}')" if inner_ct in ('Float32', 'Float64') else
                    f"JSONExtractString(key, '{ipath}')"
                )
                items.append(f"CAST({base}, 'Nullable({inner_ct})')")
            elif ct == 'String':
                items.append(f"JSONExtractString(key, '{ipath}')")
            elif ct in ('Int64', 'Int32'):
                items.append(f"JSONExtractInt(key, '{ipath}')")
            elif ct in ('UInt64', 'UInt32'):
                items.append(f"JSONExtractUInt(key, '{ipath}')")
            elif ct in ('Float32', 'Float64'):
                items.append(f"JSONExtractFloat(key, '{ipath}')")
            else:
                items.append(f"CAST(JSONExtractString(key, '{ipath}'), '{ct}')")
        return f"tuple({', '.join(items)})"
    if t == 'String':
        return "if(startsWith(key,'base64:'), base64Decode(substring(key,9)), key)"
    if t in ('Int64', 'Int32'):
        return "JSONExtractInt(key, '$')"
    if t in ('UInt64', 'UInt32'):
        return "JSONExtractUInt(key, '$')"
    if t in ('Float32', 'Float64'):
        return "JSONExtractFloat(key, '$')"
    return f"CAST(JSONExtractString(key, '$'), '{t}')"


def wal_where(sdb: str, stbl: str, since: int, node: Optional[str]) -> Tuple[str, Dict[str, object]]:
    pred = ["src_db=%(sdb)s", "src_table=%(stbl)s", "seq>%(since)s"]
    params = {'sdb': sdb, 'stbl': stbl, 'since': since}
    if node is not None:
        pred.append("node_id=%(node)s")
        params['node'] = node
    if FILTER_SRC_DB and FILTER_SRC_TABLE:
        pred.append("src_db=%(fdb)s")
        pred.append("src_table=%(ftbl)s")
        params['fdb'] = FILTER_SRC_DB
        params['ftbl'] = FILTER_SRC_TABLE
    return ' AND '.join(pred), params


def build_select(dst_db: str, dst_tbl: str,
                 pk_name: str, pk_type: str,
                 value_cols: List[Tuple[str, str]],
                 sdb: str, stbl: str, since: int,
                 node: Optional[str]) -> Tuple[str, Dict[str, object]]:
    kexpr = key_expr(pk_type)
    sels = [f"{kexpr} AS {pk_name}"] + [f"{json_extract_field_expr(t, 'value_json', n)} AS {n}" for n, t in value_cols]
    where, params = wal_where(sdb, stbl, since, node)
    sql = f"""
      SELECT {', '.join(sels)}
      FROM {WAL_DB}.{WAL_TABLE}
      WHERE {where} AND is_deleted=0
      ORDER BY seq
      LIMIT {BATCH}
    """
    return sql, params


def build_delete_subselect(pk_name: str, pk_type: str,
                           sdb: str, stbl: str, since: int,
                           node: Optional[str]) -> Tuple[str, Dict[str, object]]:
    kexpr = key_expr(pk_type)
    where, params = wal_where(sdb, stbl, since, node)
    sql = f"""
      SELECT {kexpr}
      FROM {WAL_DB}.{WAL_TABLE}
      WHERE {where} AND is_deleted=1
      ORDER BY seq
      LIMIT {DELETE_BATCH}
    """
    return sql, params


def max_seq_since(sdb: str, stbl: str, since: int, node: Optional[str]) -> int:
    where, params = wal_where(sdb, stbl, since, node)
    q = f"SELECT max(seq) FROM {WAL_DB}.{WAL_TABLE} WHERE {where}"
    m = cli.query(q, params).first_item
    return int(m or 0)


def apply_replicated_spec(spec: Dict):
    sdb = spec['src_db'];
    stbl = spec['src_table']
    cluster = spec['cluster']
    dst_db = spec['dst_db'];
    dst_local_tbl = spec['dst_table_local']
    pk, pk_ty, vals = target_schema(dst_db, dst_local_tbl)
    node = choose_replicated_node(sdb, stbl)
    ensure_ckpt_table()
    since = last_seq_for(sdb, stbl, node)
    cols = [pk] + [n for n, _ in vals]
    while True:
        sel_sql, sel_params = build_select(dst_db, dst_local_tbl, pk, pk_ty, vals, sdb, stbl, since, node)
        ins = f"INSERT INTO cluster('{cluster}', {dst_db}.{dst_local_tbl}) ({', '.join(cols)}) {sel_sql}"
        cli.command(ins, sel_params)
        del_sub, del_params = build_delete_subselect(pk, pk_ty, sdb, stbl, since, node)
        del_sql = f"ALTER TABLE cluster('{cluster}', {dst_db}.{dst_local_tbl}) DELETE WHERE {pk} IN ({del_sub})"
        cli.command(del_sql, del_params)
        new_since = max_seq_since(sdb, stbl, since, node)
        if new_since == 0 or new_since == since: break
        since = new_since;
        save_seq(sdb, stbl, node, since)


def apply_sharded_spec(spec: Dict):
    sdb = spec['src_db'];
    stbl = spec['src_table']
    distributed = spec['distributed']  # e.g. "dst.kv_tuple_dist"
    node_to_shard: Dict[str, int] = spec['node_to_shard']
    dst_db, dst_dist_tbl = distributed.split('.', 1)
    pk, pk_ty, vals = target_schema(dst_db, dst_dist_tbl)
    ensure_ckpt_table()
    cols = [pk] + [n for n, _ in vals]
    for node_id, shard_id in node_to_shard.items():
        since = last_seq_for(sdb, stbl, node_id)
        while True:
            sel_sql, sel_params = build_select(dst_db, dst_dist_tbl, pk, pk_ty, vals, sdb, stbl, since, node_id)
            ins = f"INSERT INTO {distributed} ({', '.join(cols)}) {sel_sql}"
            cli.command(ins, sel_params, settings={'insert_distributed_target_shard': int(shard_id)})
            del_sub, del_params = build_delete_subselect(pk, pk_ty, sdb, stbl, since, node_id)
            del_sql = f"ALTER TABLE {distributed} DELETE WHERE {pk} IN ({del_sub})"
            cli.command(del_sql, del_params, settings={'insert_distributed_target_shard': int(shard_id)})
            new_since = max_seq_since(sdb, stbl, since, node_id)
            if new_since == 0 or new_since == since: break
            since = new_since;
            save_seq(sdb, stbl, node_id, since)


_shutdown = False


def _sigterm(_s, _f):
    global _shutdown
    _shutdown = True


def run_once() -> bool:
    """Returns True if any progress likely occurred (heuristic: max_seq advanced), else False."""
    progressed = False
    for spec in REPLICATED_SPECS:
        before = max_seq_since(spec['src_db'], spec['src_table'], 0,
                               choose_replicated_node(spec['src_db'], spec['src_table']))
        apply_replicated_spec(spec)
        after = max_seq_since(spec['src_db'], spec['src_table'], before,
                              choose_replicated_node(spec['src_db'], spec['src_table']))
        progressed = progressed or (after and after != before)
    for spec in SHARDED_SPECS:
        nid_row = cli.query(
            f"SELECT anyHeavy(node_id) FROM {WAL_DB}.{WAL_TABLE} WHERE src_db=%(db)s AND src_table=%(tbl)s",
            {'db': spec['src_db'], 'tbl': spec['src_table']}
        ).first_item
        before = max_seq_since(spec['src_db'], spec['src_table'], 0, None) if nid_row else 0
        apply_sharded_spec(spec)
        after = max_seq_since(spec['src_db'], spec['src_table'], before, None) if nid_row else before
        progressed = progressed or (after and after != before)
    return progressed


def serve_forever():
    logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                        format='%(asctime)s %(levelname)s %(message)s')
    signal.signal(signal.SIGINT, _sigterm)
    signal.signal(signal.SIGTERM, _sigterm)

    if not REPLICATED_SPECS and not SHARDED_SPECS:
        raise RuntimeError("REPLICATED_SPECS or SHARDED_SPECS must be provided")

    ensure_ckpt_table()
    backoff = POLL_INTERVAL_SEC
    while not _shutdown:
        try:
            did = run_once()
            if did:
                backoff = POLL_INTERVAL_SEC
            else:
                backoff = min(MAX_BACKOFF_SEC, max(POLL_INTERVAL_SEC, backoff * 2))
            time.sleep(backoff + random.uniform(0, min(0.25, backoff * 0.1)))
        except Exception as e:
            logging.exception("apply loop error")
            backoff = min(MAX_BACKOFF_SEC, max(POLL_INTERVAL_SEC, backoff * 2))
            time.sleep(backoff)


def main():
    serve_forever()


if __name__ == '__main__':
    main()

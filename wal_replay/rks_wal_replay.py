import os
import sys
from typing import List, Tuple, Dict
import clickhouse_connect

SRC_WAL_DB = os.getenv('WAL_DB', 'wal')
SRC_WAL_TABLE = os.getenv('WAL_TABLE', 'rdb_changelog')
# Optional filter to replay only one table
FILTER_SRC_DB = os.getenv('FILTER_SRC_DB')
FILTER_SRC_TABLE = os.getenv('FILTER_SRC_TABLE')

# Source (WAL host) and DR (target) clusters; can be the same
SRC_URL  = os.getenv('CH_SRC_URL', 'http://127.0.0.1:8123')
SRC_USER = os.getenv('CH_SRC_USER', 'default')
SRC_PASS = os.getenv('CH_SRC_PASS', '')

DST_URL  = os.getenv('CH_DST_URL', SRC_URL)
DST_USER = os.getenv('CH_DST_USER', SRC_USER)
DST_PASS = os.getenv('CH_DST_PASS', SRC_PASS)

BATCH = int(os.getenv('BATCH', '5000'))
DELETE_BATCH = int(os.getenv('DELETE_BATCH', '2000'))

def ch_client(url, user, password):
    return clickhouse_connect.get_client(host=url.split('://')[-1].split(':')[0],
                                         port=int(url.split(':')[-1]),
                                         username=user, password=password)

src = ch_client(SRC_URL, SRC_USER, SRC_PASS)
dst = ch_client(DST_URL, DST_USER, DST_PASS)

def ensure_ckpt_table():
    dst.command(f"""
        CREATE TABLE IF NOT EXISTS {SRC_WAL_DB}.applier_ckpt
        (
          src_db    String,
          src_table String,
          last_seq  UInt64,
          updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree
        ORDER BY (src_db, src_table)
    """)

def load_pairs() -> List[Tuple[str,str]]:
    q = f"""
        SELECT DISTINCT src_db, src_table
        FROM {SRC_WAL_DB}.{SRC_WAL_TABLE}
        {("WHERE src_db = %(db)s AND src_table = %(tbl)s" if FILTER_SRC_DB and FILTER_SRC_TABLE else "")}
        ORDER BY src_db, src_table
    """
    rows = src.query(q, {'db': FILTER_SRC_DB, 'tbl': FILTER_SRC_TABLE} if FILTER_SRC_DB and FILTER_SRC_TABLE else None).result_rows
    return [(r[0], r[1]) for r in rows]

def last_seq_for(db:str, tbl:str) -> int:
    q = f"""
      SELECT argMax(last_seq, updated_at)
      FROM {SRC_WAL_DB}.applier_ckpt
      WHERE src_db=%(db)s AND src_table=%(tbl)s
    """
    val = dst.query(q, {'db': db, 'tbl': tbl}).first_item
    return int(val or 0)

def save_seq(db:str, tbl:str, seq:int):
    dst.command(f"""
      INSERT INTO {SRC_WAL_DB}.applier_ckpt (src_db, src_table, last_seq)
      VALUES (%(db)s, %(tbl)s, %(seq)s)
    """, {'db': db, 'tbl': tbl, 'seq': seq})

def target_schema(db:str, tbl:str) -> Tuple[str,str,List[Tuple[str,str]]]:
    """
    Returns: (pk_name, pk_type, value_columns[(name,type), ...])
    Assumes the first column is the primary key for EmbeddedRocksDB tables.
    """
    q = """
        SELECT name, type, position
        FROM system.columns
        WHERE database=%(db)s AND table=%(tbl)s
        ORDER BY position \
        """
    rows = dst.query(q, {'db': db, 'tbl': tbl}).result_rows
    if not rows:
        raise RuntimeError(f"Target table not found: {db}.{tbl}")
    pk_name, pk_type, _ = rows[0]
    values = [(n,t) for (n,t,_) in rows[1:]]
    return pk_name, pk_type, values

def json_extract_expr(ch_type: str, json_ref: str) -> str:
    t = ch_type.strip()
    if t.startswith('Nullable(') and t.endswith(')'):
        inner = t[9:-1]
        return f"CAST(JSONExtract({json_ref}, '{inner}'), '{t}')"
    if t.startswith('Tuple('):
        # Tuple components: JSONExtract on indices
        inner = t[6:-1]
        comps = [c.strip() for c in split_comma_top(inner)]
        parts = []
        for i, ct in enumerate(comps):
            parts.append(f"JSONExtract({json_ref}, '{ct}', '$[{i}]')")
        return f"tuple({', '.join(parts)})"
    mapping = {
        'String':     "JSONExtractString",
        'Int64':      "JSONExtractInt",
        'UInt64':     "JSONExtractUInt",
        'Int32':      "JSONExtractInt",
        'UInt32':     "JSONExtractUInt",
        'Float32':    "JSONExtractFloat",
        'Float64':    "JSONExtractFloat",
        'Date':       "toDate(JSONExtractString({json}, '$'))",
        'DateTime':   "parseDateTimeBestEffort(JSONExtractString({json}, '$'))",
        'DateTime64': "parseDateTimeBestEffort(JSONExtractString({json}, '$'))",
        'UUID':       "toUUID(JSONExtractString({json}, '$'))",
    }
    if t in mapping and 'JSONExtract' in mapping[t]:
        fn = mapping[t]
        return f"{fn}({json_ref}, '$')"
    if t in mapping:
        # Date/DateTime forms above
        return mapping[t].format(json=json_ref)
    # Fallback: cast from string
    return f"CAST(JSONExtractString({json_ref}, '$'), '{t}')"

def split_comma_top(s: str) -> List[str]:
    parts, cur, depth = [], [], 0
    for ch in s:
        if ch == '(':
            depth += 1
            cur.append(ch)
        elif ch == ')':
            depth -= 1
            cur.append(ch)
        elif ch == ',' and depth == 0:
            parts.append(''.join(cur).strip()); cur=[]
        else:
            cur.append(ch)
    if cur:
        parts.append(''.join(cur).strip())
    return parts

def key_expr(pk_type: str) -> str:
    """
    Build a ClickHouse SQL expression that reconstructs the PK from WAL.key (String).
    - Tuple PK: parse JSON array positions with JSONExtract* and wrap into tuple(...)
    - String PK: pass through unless prefixed `base64:`, then base64Decode(substring(key,9))
    """
    t = pk_type.strip()
    if t.startswith('Tuple('):
        inner = t[6:-1]
        comps = [c.strip() for c in split_comma_top(inner)]
        items = []
        for i, ct in enumerate(comps):
            if ct == 'String':
                items.append(f"JSONExtractString(key, '$[{i}]')")
            elif ct in ('Int64','Int32'):
                items.append(f"JSONExtractInt(key, '$[{i}]')")
            elif ct in ('UInt64','UInt32'):
                items.append(f"JSONExtractUInt(key, '$[{i}]')")
            elif ct in ('Float32','Float64'):
                items.append(f"JSONExtractFloat(key, '$[{i}]')")
            else:
                items.append(f"CAST(JSONExtractString(key, '$[{i}]'), '{ct}')")
        return f"tuple({', '.join(items)})"
    if t == 'String':
        # substring is 1-based; 'base64:' length = 8 -> start at 9
        return "if(startsWith(key,'base64:'), base64Decode(substring(key,9)), key)"
    # Scalar typed key encoded as JSON string or bare literal; coerce from JSON
    if t in ('Int64','Int32'):
        return "JSONExtractInt(key, '$')"
    if t in ('UInt64','UInt32'):
        return "JSONExtractUInt(key, '$')"
    if t in ('Float32','Float64'):
        return "JSONExtractFloat(key, '$')"
    return f"CAST(JSONExtractString(key, '$'), '{t}')"

def build_insert_sql(dst_db: str, dst_tbl: str,
                     pk_name: str, pk_type: str,
                     value_cols: List[Tuple[str,str]],
                     since_seq: int) -> str:
    key_ex = key_expr(pk_type)
    selects = [f"{key_ex} AS {pk_name}"]
    if value_cols:
        for name, typ in value_cols:
            selects.append(f"{json_extract_expr(typ, 'value_json')} AS {name}")
    sql = f"""
      INSERT INTO {dst_db}.{dst_tbl} ({', '.join([pk_name] + [n for n,_ in value_cols])})
      SELECT {', '.join(selects)}
      FROM {SRC_WAL_DB}.{SRC_WAL_TABLE}
      WHERE src_db=%(sdb)s AND src_table=%(stbl)s
        AND is_deleted=0
        AND seq > %(since)s
      ORDER BY seq
      LIMIT {BATCH}
    """
    return sql

def build_delete_sql(dst_db: str, dst_tbl: str,
                     pk_name: str, pk_type: str,
                     since_seq: int) -> str:
    key_ex = key_expr(pk_type)
    # Use subquery to avoid constructing huge IN-lists in Python
    return f"""
      ALTER TABLE {dst_db}.{dst_tbl}
      DELETE WHERE {pk_name} IN (
        SELECT {key_ex}
        FROM {SRC_WAL_DB}.{SRC_WAL_TABLE}
        WHERE src_db=%(sdb)s AND src_table=%(stbl)s
          AND is_deleted=1
          AND seq > %(since)s
        ORDER BY seq
        LIMIT {DELETE_BATCH}
      )
    """

def max_seq_since(db:str, tbl:str, since:int) -> int:
    q = f"""
      SELECT max(seq) FROM {SRC_WAL_DB}.{SRC_WAL_TABLE}
      WHERE src_db=%(sdb)s AND src_table=%(stbl)s AND seq > %(since)s
    """
    m = src.query(q, {'sdb': db, 'stbl': tbl, 'since': since}).first_item
    return int(m or 0)

def apply_one(db:str, tbl:str):
    pk_name, pk_type, value_cols = target_schema(db, tbl)
    ensure_ckpt_table()
    since = last_seq_for(db, tbl)

    while True:
        # apply PUTs
        ins_sql = build_insert_sql(db, tbl, pk_name, pk_type, value_cols, since)
        dst.command(ins_sql, {'sdb': db, 'stbl': tbl, 'since': since})
        # apply DELETEs
        del_sql = build_delete_sql(db, tbl, pk_name, pk_type, since)
        dst.command(del_sql, {'sdb': db, 'stbl': tbl, 'since': since})

        # 3) move watermark up to highest seq seen so far (not just emitted rows)
        new_since = max_seq_since(db, tbl, since)
        if new_since == 0 or new_since == since:
            break
        since = new_since
        save_seq(db, tbl, since)

def main():
    ensure_ckpt_table()
    pairs = load_pairs()
    for (db, tbl) in pairs:
        apply_one(db, tbl)

if __name__ == '__main__':
    main()

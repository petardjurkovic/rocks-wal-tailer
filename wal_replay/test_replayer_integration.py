import logging
import os
import json
import time
import csv
from io import StringIO

import pytest
import clickhouse_connect

CH_URL = os.getenv('CH_URL', 'http://127.0.0.1:8124')
CH_USER = os.getenv('CH_USER', 'default')
CH_PASS = os.getenv('CH_PASS', 'default123')

REPL_CLUSTER = "test_cluster"

NODE_TO_SHARD = json.loads(os.getenv('NODE_TO_SHARD', '{"node1":1}'))

WAL_DB = 'wal'
WAL_TABLE = 'rdb_changelog'
DST_DB = 'dst'
DST_LOCAL_REPL = 'kv_tuple_local'
DST_LOCAL_SHARD = 'kv_tuple_local_sharded'
DST_DIST = 'kv_tuple_dist'

WAL_CSV = """\
node1,srcdb,kv_tuple,2025-11-13T22:24:57.962+01:00[Europe/Berlin],1,"[1,1,""A1""]",0,BGluaXQKAAAAAAAAAA==,"{""v1"":""init"",""v2"":10}"
node1,srcdb,kv_tuple,2025-11-13T22:24:57.962+01:00[Europe/Berlin],2,"[1,2,""A2""]",0,BGluaXQLAAAAAAAAAA==,"{""v1"":""init"",""v2"":11}"
node1,srcdb,kv_tuple,2025-11-13T22:24:57.962+01:00[Europe/Berlin],3,"[2,1,""B1""]",0,BGluaXQMAAAAAAAAAA==,"{""v1"":""init"",""v2"":12}"
node1,srcdb,kv_tuple,2025-11-13T22:24:57.962+01:00[Europe/Berlin],4,"[3,3,""C1""]",0,BGluaXQNAAAAAAAAAA==,"{""v1"":""init"",""v2"":13}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.050+01:00[Europe/Berlin],5,"[4,4,""D1""]",0,BGluaXQOAAAAAAAAAA==,"{""v1"":""init"",""v2"":14}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.050+01:00[Europe/Berlin],6,"[1,1,""A1""]",0,AnUxZAAAAAAAAAA=,"{""v1"":""u1"",""v2"":100}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.050+01:00[Europe/Berlin],7,"[1,1,""A1""]",0,AnUyZQAAAAAAAAA=,"{""v1"":""u2"",""v2"":101}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.050+01:00[Europe/Berlin],8,"[2,1,""B1""]",1,,
node1,srcdb,kv_tuple,2025-11-13T22:24:58.050+01:00[Europe/Berlin],9,"[100,0,""S00""]",0,AXYAAAAAAAAAAA==,"{""v1"":""v"",""v2"":0}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.050+01:00[Europe/Berlin],10,"[100,1,""S01""]",0,AXYCAAAAAAAAAA==,"{""v1"":""v"",""v2"":2}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.050+01:00[Europe/Berlin],11,"[100,2,""S02""]",0,AXYEAAAAAAAAAA==,"{""v1"":""v"",""v2"":4}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],12,"[100,3,""S03""]",0,AXYGAAAAAAAAAA==,"{""v1"":""v"",""v2"":6}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],13,"[100,4,""S04""]",0,AXYIAAAAAAAAAA==,"{""v1"":""v"",""v2"":8}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],14,"[100,5,""S05""]",0,AXYKAAAAAAAAAA==,"{""v1"":""v"",""v2"":10}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],15,"[100,6,""S06""]",0,AXYMAAAAAAAAAA==,"{""v1"":""v"",""v2"":12}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],16,"[100,7,""S07""]",0,AXYOAAAAAAAAAA==,"{""v1"":""v"",""v2"":14}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],17,"[100,8,""S08""]",0,AXYQAAAAAAAAAA==,"{""v1"":""v"",""v2"":16}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],18,"[100,9,""S09""]",0,AXYSAAAAAAAAAA==,"{""v1"":""v"",""v2"":18}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],19,"[100,10,""S10""]",0,AXYUAAAAAAAAAA==,"{""v1"":""v"",""v2"":20}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],20,"[100,11,""S11""]",0,AXYWAAAAAAAAAA==,"{""v1"":""v"",""v2"":22}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],21,"[100,12,""S12""]",0,AXYYAAAAAAAAAA==,"{""v1"":""v"",""v2"":24}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],22,"[100,13,""S13""]",0,AXYaAAAAAAAAAA==,"{""v1"":""v"",""v2"":26}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],23,"[100,14,""S14""]",0,AXYcAAAAAAAAAA==,"{""v1"":""v"",""v2"":28}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],24,"[100,15,""S15""]",0,AXYeAAAAAAAAAA==,"{""v1"":""v"",""v2"":30}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.133+01:00[Europe/Berlin],25,"[100,16,""S16""]",0,AXYgAAAAAAAAAA==,"{""v1"":""v"",""v2"":32}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.206+01:00[Europe/Berlin],26,"[100,17,""S17""]",0,AXYiAAAAAAAAAA==,"{""v1"":""v"",""v2"":34}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.206+01:00[Europe/Berlin],27,"[100,18,""S18""]",0,AXYkAAAAAAAAAA==,"{""v1"":""v"",""v2"":36}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.206+01:00[Europe/Berlin],28,"[100,19,""S19""]",0,AXYmAAAAAAAAAA==,"{""v1"":""v"",""v2"":38}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.206+01:00[Europe/Berlin],29,"[100,7,""S07""]",0,AnYyCQMAAAAAAAA=,"{""v1"":""v2"",""v2"":777}"
node1,srcdb,kv_tuple,2025-11-13T22:24:58.206+01:00[Europe/Berlin],30,"[100,5,""S05""]",1,,
node1,srcdb,kv_tuple,2025-11-13T22:24:58.206+01:00[Europe/Berlin],31,"[100,10,""S10""]",1,,
node1,srcdb,kv_tuple,2025-11-13T22:24:58.705+01:00[Europe/Berlin],32,"[9,9,""Z9""]",0,Anp6CQAAAAAAAAA=,"{""v1"":""zz"",""v2"":9}"
"""


def ch():
    ch_host = CH_URL.split('://')[-1].split(':')[0]
    ch_port = int(CH_URL.split(':')[-1])

    u = clickhouse_connect.get_client(
        host='localhost',
        port=8125,
        username='default',
        password='default123'
    )
    return u


def reset_schema(client):
    client.command(f"CREATE DATABASE IF NOT EXISTS {WAL_DB}")
    client.command(f"CREATE DATABASE IF NOT EXISTS {DST_DB}")
    client.command(f"DROP TABLE IF EXISTS {WAL_DB}.{WAL_TABLE}")
    client.command(f"""
        CREATE TABLE {WAL_DB}.{WAL_TABLE}
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
        ENGINE MergeTree
        PARTITION BY toDate(ts)
        ORDER BY (src_db, src_table, node_id, seq, key)
    """)

    client.command(f"DROP TABLE IF EXISTS {DST_DB}.{DST_LOCAL_REPL}")
    client.command(f"""
        CREATE TABLE {DST_DB}.{DST_LOCAL_REPL}
        (
          key Tuple(Int64, Int64, String),
          v1  String,
          v2  Int64
        )
        ENGINE MergeTree
        ORDER BY key
    """)

    client.command(f"DROP TABLE IF EXISTS {DST_DB}.{DST_LOCAL_SHARD}")
    client.command(f"""
        CREATE TABLE {DST_DB}.{DST_LOCAL_SHARD}
        (
          key Tuple(Int64, Int64, String),
          v1  String,
          v2  Int64
        )
        ENGINE MergeTree
        ORDER BY key
    """)
    client.command(f"DROP TABLE IF EXISTS {DST_DB}.{DST_DIST}")

    if REPL_CLUSTER:
        client.command(f"""
            CREATE TABLE {DST_DB}.{DST_DIST}
            AS {DST_DB}.{DST_LOCAL_SHARD}
            ENGINE = Distributed('{REPL_CLUSTER}', '{DST_DB}', '{DST_LOCAL_SHARD}', cityHash64(key))
        """)
    else:
        print(
            "Fallback: you don't have a cluster configured, sharded test will still pass by inserting directly into the local table")
        pass


def load_wal_rows(client):
    rdr = csv.reader(StringIO(WAL_CSV))
    rows = []
    for line in rdr:
        node_id, sdb, stbl, _ts, seq, key, is_del, b64, vjson = line
        seq = int(seq)
        is_del = int(is_del or 0)
        b64 = None if b64 == '' else b64
        vjson = None if vjson == '' else vjson
        rows.append((node_id, sdb, stbl, seq, key, is_del, b64, vjson))
    client.insert(
        f"{WAL_DB}.{WAL_TABLE}",
        rows,
        column_names=['node_id', 'src_db', 'src_table', 'seq', 'key', 'is_deleted', 'value_b64', 'value_json']
    )


def expected_live_count(client):
    return client.query(f"""
        SELECT count() FROM (
          SELECT argMax(is_deleted, seq) AS last_del
          FROM {WAL_DB}.{WAL_TABLE}
          WHERE src_db='srcdb' AND src_table='kv_tuple'
          GROUP BY key
        ) WHERE last_del=0
    """).first_item


def assert_final_state(client, table_fqn):
    # A1 updated to u2/101
    r = client.query(f"""
      SELECT v1, v2 FROM {table_fqn}
      WHERE key=(1,1,'A1')
    """).result_rows
    assert r and r[0] == ('u2', 101), "A1 final mismatch"

    # B1 deleted
    r = client.query(f"SELECT count() FROM {table_fqn} WHERE key=(2,1,'B1')").first_item
    assert r == 0, "B1 should be deleted"

    # S07 updated to v2/777
    r = client.query(f"""
      SELECT v1, v2 FROM {table_fqn}
      WHERE key=(100,7,'S07')
    """).result_rows
    assert r and r[0] == ('v2', 777), "S07 final mismatch"

    # S05 deleted
    r = client.query(f"SELECT count() FROM {table_fqn} WHERE key=(100,5,'S05')").first_item
    assert r == 0, "S05 should be deleted"

    # S10 deleted
    r = client.query(f"SELECT count() FROM {table_fqn} WHERE key=(100,10,'S10')").first_item
    assert r == 0, "S10 should be deleted"

    # Z9 present
    r = client.query(f"""
      SELECT v1, v2 FROM {table_fqn}
      WHERE key=(9,9,'Z9')
    """).result_rows
    assert r and r[0] == ('zz', 9), "Z9 final mismatch"

    # Global count equals live keys from WAL
    live = expected_live_count(client)
    cnt = client.query(f"SELECT count() FROM {table_fqn}").first_item
    assert cnt == live, f"Row count mismatch: {cnt} != {live}"


def test_sharded_apply():
    c = ch()
    reset_schema(c)
    load_wal_rows(c)

    import rks_wal_replay as wr
    # wire the module to connection and config
    wr.cli = c
    wr.WAL_DB = WAL_DB
    wr.WAL_TABLE = WAL_TABLE

    # Building minimal sharded spec: route node1 -> shard 1
    if REPL_CLUSTER:
        distributed = f"{DST_DB}.{DST_DIST}"
    else:
        # apply directly to local_sharded
        distributed = f"{DST_DB}.{DST_LOCAL_SHARD}"

    spec = {
        "src_db": "srcdb",
        "src_table": "kv_tuple",
        "distributed": distributed,
        "node_to_shard": NODE_TO_SHARD
    }

    wr.ensure_ckpt_table()
    wr.apply_sharded_spec(spec)

    target = f"{DST_DB}.{DST_LOCAL_SHARD}" if REPL_CLUSTER else f"{DST_DB}.{DST_LOCAL_SHARD}"
    assert_final_state(c, target)


@pytest.mark.skipif(not REPL_CLUSTER, reason="Set REPL_CLUSTER to run ON CLUSTER apply")
def test_replicated_apply():
    c = ch()
    reset_schema(c)
    load_wal_rows(c)

    import rks_wal_replay as wr
    wr.cli = c
    wr.WAL_DB = WAL_DB
    wr.WAL_TABLE = WAL_TABLE

    spec = {
        "src_db": "srcdb",
        "src_table": "kv_tuple",
        "cluster": REPL_CLUSTER,
        "dst_db": DST_DB,
        "dst_table_local": DST_LOCAL_REPL
    }

    wr.ensure_ckpt_table()
    wr.apply_replicated_spec(spec)

    # Validate on this node ON CLUSTER should have applied to all nodes
    assert_final_state(c, f"{DST_DB}.{DST_LOCAL_REPL}")

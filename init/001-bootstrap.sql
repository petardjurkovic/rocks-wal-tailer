CREATE DATABASE IF NOT EXISTS dst ON CLUSTER test_cluster;
CREATE DATABASE IF NOT EXISTS wal ON CLUSTER test_cluster;

CREATE TABLE dst.kv_tuple_local_sharded ON CLUSTER test_cluster
(
    key Tuple(Int64, Int64, String),
    v1  String,
    v2  Int64
)
    ENGINE = EmbeddedRocksDB
        PRIMARY KEY key
        SETTINGS optimize_for_bulk_insert = 0;

CREATE TABLE dst.kv_tuple_dist
    AS dst.kv_tuple_local_sharded
        ENGINE = Distributed('test_cluster', 'dst', 'kv_tuple_local_sharded', cityHash64(key));

CREATE TABLE wal.rdb_changelog ON CLUSTER test_cluster
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
        ORDER BY (src_db, src_table, node_id, seq, key);

# rocks-wal-tailer

#### Tails the RocksDB WAL of ClickHouse EmbeddedRocksDB tables and writes a normalized changelog into a ClickHouse MergeTree table. Stateless across restarts except for a persisted sequence-number checkpoint per source.

# What it does

#### Discovers the on-disk RocksDB directory for a given db.table in ClickHouse.

- Opens RocksDB as a secondary (no locks on primary).

- Reads WAL via DB::get_updates_since(next_seq) in monotonic order.

- Expands WriteBatch entries using WriteBatchIteratorCF.

- Emits rows (src_db, src_table, seq, key, is_deleted, value_b64, value_json, ts) into a destination table.

- Persists next_seq to a file checkpoint. Resumes from checkpoint on restart.

- Flush policy favors “small, timely” in tests and “batched” under load.
![](diagram.svg)
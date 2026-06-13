# Ordinum

A Log-Structured-Merge Tree (LSM) Database Storage Engine

---



### So, What is it?

Ordinum is a database storage engine built from scratch in Rust. It is based off of the log-structured-merge tree architecture popularised by the likes of **Google [(LevelDB)](https://en.wikipedia.org/wiki/LevelDB)**, **Facebook [(RocksDB)](https://rocksdb.org/)** and, **Cockroach Labs [(PebbleDB)](https://www.cockroachlabs.com/blog/pebble-rocksdb-kv-store/)**

The design and architecture itself originally documented in the well known paper - [The Log-Structured-Merge Tree](https://www.cs.umb.edu/~poneil/lsmtree.pdf).

>Before giving an overview, I encourage further reading on this interesting area of database design, specifically the research which has been done around the design principles and decisions behind what makes this architecture so unique and different from your traditional b-tree structures widely used by realational databases.

An LSM tree behaves much like its name suggests. Writes are first recorded sequentially, usually into a write-ahead log, and then inserted into an in-memory structure. Rather than updating records in place on disk, the engine turns many small writes into larger sequential writes over time.

This is one of the main reasons LSM-based engines are known for fast write performance. Sequential writes are friendlier to storage devices than scattered random updates: on spinning disks they avoid costly seeks, and on SSDs they align better with batching, buffering, and flash page/block behavior.

Now, you must be thinking, '__that's great, but won't we end up with loads of garbage data?__' - Yes! and this brings us to the second part of the LSM... **the Tree.**

In any database, we will inevitably build up some form of garbage data. In b-tree based databases, the garbage is less about the data itself and more about the redundant space in pages caused by updates in place, page splits or maintaining multi-version concurrency control (MVCC). Postgres calls this [Vacumming](https://www.snowflake.com/en/blog/engineering/tuning-postgres-vacuum/). For an LSM Tree, we build up

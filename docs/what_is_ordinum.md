# Ordinum

A Log-Structured-Merge Tree (LSM) Database Storage Engine

---



### So, what is it?

Ordinum is a database storage engine built from scratch in Rust. It is based on the log-structured-merge tree architecture popularised by systems such as **Google [(LevelDB)](https://en.wikipedia.org/wiki/LevelDB)**, **Facebook [(RocksDB)](https://rocksdb.org/)**, and **Cockroach Labs [(PebbleDB)](https://www.cockroachlabs.com/blog/pebble-rocksdb-kv-store/)**.

The design itself was originally described in the well-known paper [The Log-Structured-Merge Tree](https://www.cs.umb.edu/~poneil/lsmtree.pdf).

> Before giving an overview, I encourage further reading on this interesting area of database design, specifically the research done around the design principles and decisions that make this architecture so different from the traditional b-tree structures widely used by relational databases.

At a high level, an LSM tree is designed around a simple idea: writes should be cheap, ordered, and durable before the system spends effort reorganising them. Instead of seeking around disk to update records in place, new changes are appended sequentially, usually to a write-ahead log, and then inserted into an in-memory structure. Over time, those in-memory writes are flushed to disk and merged into larger sorted files.

This is one of the main reasons LSM-based engines are known for fast write performance. Sequential writes are friendlier to storage devices than scattered random updates: on spinning disks they avoid costly seeks, and on SSDs they align better with batching, buffering, and flash page/block behavior.

That write path also creates an interesting engineering tradeoff. The engine accepts that data may exist in several places at once: in memory, in the log, and in immutable on-disk files. Reads therefore need to know how to find the newest version of a key, while background work gradually merges and discards older versions. The storage engine becomes a coordination layer between append-only durability, sorted structures, and cleanup.

This is where **Ordinum** fits in. The goal is not just to wrap an existing database API, but to build a storage-engine-first database from first principles. Ordinum follows the LSM model because it gives a clear foundation for exploring the core parts of a modern key-value store: write-ahead logging, memtables, sorted string tables, sequence numbers, tombstones, compaction, and eventually read/write correctness under realistic workloads.

Ordinum, and the storage engine itself, works purely on bytes with little to no semantic understanding of the data being persisted. This is its strength.

> Bytes In -->-- [Engine] -->-- Bytes Out

Rust aligns well with this kind of system. A storage engine spends a lot of time managing bytes, files, indexes, buffers, and ownership boundaries. Rust gives Ordinum low-level control without giving up memory safety, and its type system helps make state transitions explicit: data moves from log to memory, from memory to disk, and from many files into fewer compacted files.

#### The Log Structure

The "log" part of an LSM tree is about how changes enter the system. When a key is written, the engine does not immediately search through an on-disk page and mutate it in place. Instead, it appends a new record to the end of a log.

That append is important. Writing to the end of a file is predictable and efficient: the engine can keep moving forward, adding one entry after another, rather than constantly jumping around storage to update existing records. Each entry can be treated as a fact: at sequence number `N`, this key had this value.

For a simplified write, Ordinum's storage path looks something like this:

1. A write arrives as bytes for a key and value.
2. The engine assigns the write a new sequence number.
3. The record is appended to the write-ahead log so it can be recovered after a crash.
4. The same key/value pair is inserted into an in-memory sorted structure, usually called a memtable.
5. When the memtable grows large enough, it is flushed to disk as an immutable sorted file.

Because each update is appended as a new fact, the latest value is determined by the highest sequence number for that key. Older values remain physically present until the engine has enough information to safely remove them.

**Example:**

| Key | Value | Seq No. | State |
| --- | --- | ---: | --- |
| Bob | bob.bobby@gmail.com | 1 | Old |
| Bob | bob.bobson@gmail.com | 2 | Old |
| Bob | bob.just.bob@gmail.com | 3 | Old |
| Bob | bob_the_blob@yahoo.com | 4 | Newest |

This is clearly a contrived explanation of the log structure, but it illustrates the important point: the engine does not overwrite Bob's previous email addresses in place. It appends newer facts and uses sequence numbers to decide which value is currently visible.

Now, you must be thinking, '__that's great, but won't we end up with loads of garbage data?__' - Yes! and this brings us to the second part of the LSM... **the Tree.**

#### The Tree

In any database, we will inevitably build up some form of garbage data. In b-tree based databases, the garbage is less about the data itself and more about the redundant space in pages caused by updates in place, page splits, or maintaining multi-version concurrency control (MVCC). Postgres calls this [Vacuuming](https://www.snowflake.com/en/blog/engineering/tuning-postgres-vacuum/). For an LSM tree, we build up garbage by appending new changes for previously written data without overwriting the old data.

The tree part of the design is how the engine brings order back to those appended writes. Once in-memory data is flushed to disk, it becomes an immutable sorted file. Over time, the engine merges these files together in a process called compaction.

Compaction is where the LSM tree pays back some of the work it deferred during writes. During a compaction, the engine reads multiple sorted files, merges their key ranges, keeps the newest version of each key, and drops entries that are no longer needed. If a key was deleted, the engine may write a tombstone first, then later remove both the tombstone and the older values once it is safe to do so.

This gives the engine a useful tradeoff: writes stay fast because they are mostly append-only, while cleanup happens later in larger sequential batches. The cost does not disappear, but it is moved into background work that can be scheduled, throttled, and tuned.

#### The Levels

The "tree" in an LSM tree is usually represented as a set of levels. Each level contains sorted immutable files, often called SSTables. New data starts near the top of the tree, and as compaction runs, data is gradually pushed down into lower levels.

A simplified level layout looks like this:

| Level | Contents | Role |
| --- | --- | --- |
| Memtable | Recent writes in memory | Fast reads and writes before flushing |
| Level 0 | Newly flushed sorted files | First durable on-disk level |
| Level 1+ | Larger compacted sorted files | Older, more organised data |

Lower levels are usually larger and more stable. They contain data that has already been compacted, so there should be less duplication and fewer obsolete versions. Reads may need to check multiple places, starting with the memtable and moving down through the levels until the newest matching key is found.

This levelled structure is what keeps the append-only write model practical. Without it, the engine would keep accumulating old versions forever. With levels and compaction, Ordinum can accept writes quickly, preserve durability through the log, and gradually organise the data into sorted files that are efficient to search and merge.

All of this combines to create an elegant system designed for simplicity and built for speed.

These are the principles Ordinum endeavours to capture and harness, which brings us to the next section.

### Why Ordinum?

>The purpose of **Ordinum** is to be a storage-engine-first database: simple in design, reliable in persistence, and fast by construction.

Ordinum exists to make the core mechanics of database storage explicit. It does not start with a query language, a distributed protocol, or a large feature surface. It starts with the foundation: bytes written to disk, recovered after failure, organised into sorted structures, and compacted over time into a durable key-value store.

Once we get that right, building from that strong foundation becomes easier and an entirely better experience. Knowing and owning our internals is crucial and a pillar to this project.

That focus matters. A database is only as strong as its storage engine. If writes are not durable, reads are not correct, deletes are not handled carefully, or compaction corrupts state, every layer above it becomes meaningless. Ordinum treats the storage engine as the product, not an implementation detail.

The project is built around a few clear principles:

- **Storage first**: persistence, recovery, indexing, and compaction are the centre of the system.
- **Correctness before complexity**: the engine should be understandable before it becomes clever.
- **Append first, organise later**: writes should be cheap and sequential, with structure restored through flushing and compaction.
- **Bytes in, bytes out**: the engine stores data without needing to understand application-level meaning.
- **Rust-native reliability**: ownership, types, and explicit state transitions should help enforce the shape of the system.

Ordinum is not trying to hide the hard parts of database engineering. It is trying to expose them, implement them, and make them understandable. Its purpose is to grow into a database engine that stays true to the LSM model: durable writes, ordered data, deliberate compaction, and a design that remains small enough to reason about as it becomes more capable.

There is the sales pitch. In fact, it's much more a mantra than a pitch and even less about sales.

Odinum is a passion project originally started as a means to build a deeper knowledge of database internals, and to explore a love for low-level system design.

Ordinum's storage engine is and always will be open-source. In the spirit of not re-inventing the wheel it is inspired by the likes of **RocksDB**, **LevelDB** and, **PebbleDB** whilst also implementing key research papers. This allows Ordinum to remain at the cutting edge of modern design whilst also saving space for iterating and improving where possible.

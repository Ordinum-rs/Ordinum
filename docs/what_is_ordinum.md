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

Ordinum is a passion project originally started as a means to build a deeper knowledge of database internals, and to explore a love for low-level system design.

Ordinum's storage engine is and always will be open-source. In the spirit of not re-inventing the wheel it is inspired by the likes of **RocksDB**, **LevelDB** and, **PebbleDB** whilst also implementing key research papers. This allows Ordinum to remain at the cutting edge of modern design whilst also saving space for iterating and improving where possible.

You may be asking, 'What makes Ordinum different?' or 'Why another database?' and honestly, I wish I had a better answer than that we hope to take something incredibly complex by design and make it beautifully simple. That the innovation is taking years worth of iteration and many different implementations and condensing that into one focused application with proven efficiency.

And on that, it was worth maybe talking about the architecture of Ordinum and some of the design decisions.

### Architecture and Design

Ordinum stays true to the LSM structure as a whole. That being, the engine is broken up into two distinct stages of memory. `In-memory` and `Persisted memory`.

```mermaid
flowchart LR

    IN([Bytes In])

    subgraph Engine
        MEM(In-Memory Processing)
        DISK(Persistent Storage)

        MEM --> DISK
    end

    OUT([Bytes Out])

    IN --> MEM
    DISK --> OUT

```


This is, of course, a highly simplistic way of reducing the engine. The two stages each have their own complexities and sub-systems, which must ultimately be woven together to create a seamless transition from bytes entering the engine, to bytes being persisted, and finally to bytes being read back again.

Many of these subsystems are what give an LSM database its character. Writes do not immediately become sorted files on disk. They first pass through an in-memory write path, where they may be batched, assigned sequence numbers, written to the WAL, and inserted into mutable in-memory structures. In Ordinum, this means thinking carefully about how batches move through the pipeline, how ordering is preserved, and how multiple writer threads can safely apply their work concurrently.

The memtable is one of the most important pieces of this stage. It acts as the first searchable home for newly written keys, while also buffering writes before they are flushed into immutable on-disk tables. A structure such as a Skiplist is a natural fit here because it maintains sorted order while supporting efficient inserts and lookups. The interesting part is not simply choosing a Skiplist, but making it work safely under concurrency: allocating nodes, publishing links, handling retries, and ensuring readers never observe partially-installed state.

Once the memtable reaches a threshold, the problem changes shape. The in-memory structure must become immutable, handed off for flushing, and eventually encoded into SSTables on disk. At that point, the engine shifts from fast concurrent mutation to careful persistence: block layout, indexes, filters, checksums, compression, and metadata all become part of the story.

Reads then have to stitch these worlds back together. A lookup may need to consult the current memtable, immutable memtables waiting to be flushed, and several levels of SSTables. The engine must make this feel like one coherent view of the database, even though the data is physically spread across memory and disk, and may exist in multiple versions due to updates, deletes, and snapshots.

So while the simplified model is “bytes in, bytes persisted, bytes out,” the real design is about coordinating many smaller systems: the write pipeline, WAL, memtables, flushes, SSTables, version management, snapshots, iterators, and compaction. The strength of an LSM engine comes from how cleanly these pieces are connected.

A slightly more accurate, high level picture therefore represents the two caller paths `Writer` and `Reader` and shows how they flow through the two memory stages.


```mermaid

flowchart LR

    R(Read Path)
    W(Write Path)

    subgraph Memory
        MT(Memtable)
    end

    subgraph Disk
        SST(SSTables)
    end

    W --> MT
    MT -. Flush .-> SST

    R --> MT
    R --> SST

```

Traditionally, the in-memory part of the engine would be made up of a single active memtable and a single frozen memtable. The active memtable, on becoming full, would trigger a rotation and would become frozen and be swapped for the secondary memtable which would then become the active one. The frozen memtable would then be flushed to disk in the background.

Ordinum, similar to Rocks, expands on this by allowing multiple frozen memtables (up to a configured `n` amount) which speeds up the write and read path by not stalling on rotations whilst waiting for the secondary memtable to flush. Particularly on increased write workloads which can cause a lot of memtable churn. By allowing multiple frozen memtables to build up in a queue, writes and reads can continue to hit the in-memory portion of the engine while background threads handle flushing.

```mermaid
flowchart LR
    W[Writes] --> A[Active Memtable]

    subgraph Immutable Memtable Queue
        F1[Frozen 1] --> F2[Frozen 2] --> F3[Frozen N]
    end

    A -- Rotate --> F1
    F3 --> X[Flushing]
    X --> S[(SSTable)]

    R[Reads] --> A
    R --> F1
    R --> F2
    R --> F3
```


Ordinum seeks to compile the latest advancements and implementations of LSM databases as well as leading academic papers in trying to create a robust and simple storage engine. Another key aspect of this which informs the design architecture of the engine is the separation of Keys and Values.

#### Key Value Separation

The paper [WiscKey: Separating Keys from Values
in SSD-conscious Storage](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) details the optimisations that come with separating large Values from Keys when storing bytes.

Typically, in standard storage engines, both the Key and the Value are stored inline together, meaning they are run through the write path through to being persisted on disk as a contiguous block of memory. This may be fine for small Values, the read becomes quite trivial, we use a `LookUpKey` to find the latest visible key in the LSM Tree and the Value is right there next to it.

For large values, the traditional LSM design begins to suffer from significant write amplification. Every time a value moves through the compaction hierarchy, the entire key-value pair must be rewritten. A 32-byte key with a 4 KB value is treated as a 4 KB record, meaning compactions repeatedly read and rewrite large quantities of data even though the expensive portion is rarely used for ordering or searching.

The key observation made by WiscKey is that the LSM tree primarily exists to organise keys. During a lookup, the engine searches for a key and only needs the associated value once the latest visible version has been located. Storing large values inline therefore causes the LSM to perform work that is unrelated to its core purpose.

To address this, WiscKey separates keys from values. Values are written to an append-only Value Log while the LSM tree stores only the key and a small pointer describing where the value resides within the log. This pointer typically contains a file identifier, offset and length.

Pure WiscKey creates the value pointer during the write path and only inserts the pointer into the memtable. Ordinum's design is closer to Pebble/RocksDB in that the memtable remains a complete representation of recently written data. Every write enters the WAL and memtable as a normal key-value pair, allowing reads from active and frozen memtables without ever touching the Value Log.

Only when a memtable is flushed do we decide whether a value should remain inline or be separated. During flush, values larger than a configured threshold are written into the Value Log and replaced with a ValuePointer in the generated SSTable. Smaller values continue to be embedded directly within the SSTable.

This preserves the simplicity and performance of the write path. Memtable inserts remain a single operation, readers can access recent writes directly from memory, and value separation becomes a storage-level optimisation rather than a write-path concern.

The trade-off is that the flush process becomes slightly more expensive, as it must decide how each value should be encoded and potentially append large values to the Value Log. In return, the LSM tree benefits from reduced compaction costs once data reaches disk.

```mermaid
flowchart LR
    A[Write Key + 64KB Value]

    A --> B[WAL]
    B --> C[Memtable<br/>Stores Full Value]

    C --> D[Frozen Memtable]

    D --> E[Flush]

    E --> F[Write 64KB Value<br/>to Value Log]

    F --> G[Create ValuePointer]

    G --> H[SSTable<br/>Key + Pointer]

    H --> I[Future Compactions<br/>Move Only Key + Pointer]
```

>Unlike WiscKey, Ordinum does not separate values during the write path. All writes are stored in the WAL and memtables as complete key-value pairs. Value separation occurs only during memtable flush, where large values are redirected into the Value Log and replaced with compact value pointers in SSTables. This preserves fast in-memory reads while still achieving the reduced write amplification benefits of key-value separation for persisted data.

#### Version Management

Versioning is an important part of any storage engine

#### Topics to Expand

TODO: Durability and recovery - how the WAL protects acknowledged writes, how recovery rebuilds in-memory state, and when old log files can be recycled.

TODO: Read visibility - how sequence numbers, snapshots, and tombstones decide which version of a key is visible.

TODO: SSTables - the high-level role of data blocks, sparse indexes, filters, checksums, compression, and metadata.

TODO: Version management - how readers keep a stable view while writers, flushes, and compactions install new state.

TODO: Compaction policy - how Ordinum will decide what to compact, when to throttle writes, and how to balance write, read, and space amplification.

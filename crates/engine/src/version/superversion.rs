//
//
//
//
//
use std::ptr::NonNull;

use mem::hazard::domain::Global;
use mem::hazard::hazard_ptr::HzdPtr;

use crate::column_family::cf::ColumnFamilyData;
use crate::memtable::memtable::{Immutable, Memtable, Mutable, ReadableMemtable};
use crate::sync::Arc;
use crate::sync::atomic::AtomicPtr;
use crate::utils::tagged_pointer::AtomicTaggedPtr;
use crate::version::memtable_list::MemListVersion;

pub(crate) const DEFAULT_SV_CACHE_SIZE: usize = 4;

pub(crate) struct Superversion {
    // NOTE: Backpointer which should be guranteed to outlive all super versions it must also be a stable heap-allocated object
    cf: NonNull<ColumnFamilyData>, // Circular reference to parent
    // NOTE: We don't need pointer or Arc<> because we create a wrapper over the MemtableInner which is an Arc<> to give us a safe readable struct over the
    // mutable memtable
    mem: ReadableMemtable,
    // NOTE: Immutable published snapshot of the current immutable memtable set.
    // Writers must build a new MemListVersion rather than mutating a published one.
    // Arc ensures old readers keep seeing a stable snapshot because the MemListVersion is shared and multiple SuperVersions can point to the same MemListVersion
    // Even though SuperVersion is protected by HazardPointer that protection is only granted to itself and the objects it owns NOT for shared objects that
    // exist elsewhere
    imm: Arc<MemListVersion>,
    // TO_ADD:
    // version: *version,
    // version_number
    // write_stall_condition
    //
    // From RocksDB:
    // An immutable snapshot of the DB's seqno to time mapping, usually shared
    // between SuperVersions.
    // std::shared_ptr<const SeqnoToTimeMapping> seqno_to_time_mapping{nullptr};
    //
}

// ---- Super Version Cache Entry ---- //

// CacheEntry is a per-(DB, CF) TLS cache slot for a SuperVersion.
//
// Design:
// - Hazard pointers provide lifetime safety.
// - Tagged pointer states provide cache validity and reader/writer coordination.
// - Writers never mutate hazard pointers.
// - Readers never dereference a pointer that is not protected by `hazard`.
//
// State machine:
//
// Empty
//   -> Cached(ptr)          Reader refreshes from global SV and protects ptr.
//
// Cached(ptr)
//   -> InUse(ptr)           Reader successfully acquires the cached SV.
//   -> Invalid(ptr)         Writer invalidates cached SV after installing a newer SV.
//
// InUse(ptr)
//   -> Cached(ptr)          Reader finishes and returns SV to cache.
//   -> Invalid(ptr)         Writer invalidates while reader is actively using SV.
//
// Invalid(ptr)
//   -> Empty               Reader observes invalidation, clears hazard protection
//                          and refreshes from the current global SV.
//
// Safety invariant:
//
// If the tagged pointer contains `ptr` in any non-empty state
// (Cached, InUse, Invalid), then `hazard` must currently protect `ptr`.
//
// Reclamation:
//
// Writers install a new global SuperVersion and retire the old one into the
// hazard domain.
//
// Writers may walk registered SVCache entries and transition
// Cached(ptr) -> Invalid(ptr)
// InUse(ptr)  -> Invalid(ptr)
//
// Writers never clear a reader's hazard pointer.
//
// Old SuperVersions are reclaimed only after:
//
// 1. All cache entries have dropped references to the retired SV.
// 2. No hazard pointer in the domain protects the retired SV.
//
struct CacheEntry {
    // Tagged states:
    // Empty | Cached(ptr) | InUse(ptr) | Invalid(ptr)
    tagged_ptr: AtomicTaggedPtr<Superversion>,

    // Protects the pointer currently stored in `tagged_ptr`.
    //
    // Readers must update `tagged_ptr` and `hazard` together through
    // dedicated APIs to preserve invariants.
    hazard: HzdPtr<'static, Global>,
}

// XXX: To be implemented - just putting something here so tls can store something
pub(crate) struct SVCache<const N: usize> {
    cache: Option<()>,
}

impl SVCache<DEFAULT_SV_CACHE_SIZE> {
    pub(crate) fn new() -> Self {
        Self { cache: None }
    }
}

impl<const N: usize> SVCache<N> {
    pub(crate) fn new_with_size() -> Self {
        Self { cache: None }
    }
}

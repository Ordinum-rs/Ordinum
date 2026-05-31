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

//
// Readers CAS the SV Pointer and tries to install SV_IN_USE
// If successful, we are free to use the SV and then return it once done
// If we fail and instead get back SV_NULL then we must clear() then hazard slot and drop the pointer
//
// Only writers can invalidate a SVCache by walking the array

// TODO: Think about the reclamation and reader/writer relationship and how to expose methods which keep hazard ptrs and raw ptrs in sync
struct CacheEntry {
    protected_ptr: AtomicPtr<Superversion>,
    hazard: HzdPtr<'static, Global>,
}

// SuperVersion Cache to be stored in Thread Local Storage which is effectively static for the lifetime of the programme
pub(crate) struct SVCache<const N: usize> {
    pub(crate) cache: Vec<CacheEntry>,
}

impl SVCache<DEFAULT_SV_CACHE_SIZE> {
    pub(crate) fn new() -> Self {
        Self::new_with_size()
    }
}

impl<const N: usize> SVCache<N> {
    pub(crate) fn new_with_size() -> Self {
        let vec: Vec<CacheEntry>;

        Self {
            cache: Vec::with_capacity(N),
        }
    }
    // Methods operating or deferencing the ptr MUST use a pin()
}

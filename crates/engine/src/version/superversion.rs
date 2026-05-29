//
//
//
//
//
use std::ptr::NonNull;
use std::sync::Arc;

use mem::hazard::domain::Global;
use mem::hazard::hazard_ptr::HzdPtr;

use crate::column_family::cf::ColumnFamilyData;
use crate::memtable::memtable::{Immutable, Memtable, Mutable, ReadableMemtable};
use crate::version::memtable_list::MemListVersion;

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

// SuperVersion Cache to be stored in Thread Local Storage which is effectively static for the lifetime of the programme
pub(crate) struct SVCache {
    pub(crate) hzd: HzdPtr<'static, Global>,
    pub(crate) generation: u64,
    pub(crate) sv: *const Superversion,
}

impl SVCache {
    pub(crate) fn new() -> Self {
        Self {
            hzd: HzdPtr::new(),
            generation: 0,
            sv: std::ptr::null(),
        }
    }

    // TODO: Implement safe methods to load and check generation number, return SV from

    // Methods operating or deferencing the ptr MUST use a pin()
}

//
//
//
//
use std::sync::{Arc, atomic::AtomicPtr};

use crate::{
    column_family::cf_options::CFOptions,
    memtable::memtable::{Memtable, Mutable},
    version::{memtable_list::MemTableList, superversion::Superversion},
};

// Latest view of the LSM Tree
pub(crate) struct ColumnFamilySet {}

pub(crate) struct ColumnFamilyData {
    id: u64,
    name: String,
    options: CFOptions,
    //
    // Write Path
    mem: Memtable<Mutable>,
    imm: MemTableList,
    //
    // Read Path
    // NOTE: Should always be loaded with HzdPtr
    // We do not Arc<SV> on the superversion because we are using HazardPointers and we need to be able to CAS on the hot path
    superversion: AtomicPtr<Superversion>,
    tls_id: u64,
    // --
    // NOTE: *Version
    // NOTE: ThreadLocalID
    //
    // Version_history?
}

// BASIC IMPL
impl ColumnFamilySet {
    pub(crate) fn set_memtable() {
        // Checks
        // mem->MarkImmutable()
    }

    fn assign_memtable_id() {}

    // calculate the oldest log needed for the durability of this column family
    fn oldest_log_to_keep() {}
}

// Direct path handle without going through DBImpl
pub(crate) struct ColumnFamilyHandle {
    // NOTE: Needs to be Arc because if we drop the cf_set then we need to wait for all handles to unref before dropping fully
    inner: Arc<ColumnFamilyData>,
}

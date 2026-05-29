use crate::memtable::memtable::{Flushed, Immutable, Memtable, Mutable};
use std::{ptr::NonNull, sync::Arc};

//------------------
//
//
//
//
// MemtableList holds the immutable state and logic for Immutable Memtables
pub(crate) struct MemTableList {
    imm: Vec<Memtable<Immutable>>,
    current_version: MemListVersion,
    flushed: Vec<Memtable<Flushed>>,
}

// Memtable List Version is a snapshot of the memtable registry at a given point in time
// We centralise the memtable registry access for a particular point in time to give to a database snapshot which will allow readers to
// access memtables without blocking or seeing conflicting states
pub(crate) struct MemListVersion {
    imm_version_list: Vec<Memtable<Immutable>>,
}

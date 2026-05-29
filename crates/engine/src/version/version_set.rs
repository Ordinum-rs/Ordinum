// Version set holds metadata for managing the persistent LSM tree structure and database-wide state.
//
// It is responsible for logging version meta changes to the MANIFEST
// Holding column family sets
// Storing the global sequence numbers such as log_seq_num and visible_seq_num
// ....
// DOCS: Finish the explanation for VersionSet
//

use crate::sync::Arc;
use crate::{column_family::cf::ColumnFamilySet, version::SeqNumState};

pub(crate) struct VersionSet {
    cf_set: ColumnFamilySet,
    global_sn: Arc<SeqNumState>,
    //
    //
    // XXX: Future fields
    //
    // current_version: Arc<Version>,
    // next_file_number: ___,
    // next_manifest_file_number: ___,
    //
    // MANIFEST fields
    // Obselete Tracking fields
    // Database identity?
    // Congig/Options?
    //
    //
}

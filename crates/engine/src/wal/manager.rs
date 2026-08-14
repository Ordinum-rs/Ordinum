// WAL Manager
// Central interface that handles WAL Lifecycle operations including: WAL Creation, Recycling, Obsolescence tracking

pub(crate) trait WALManager {
    //
    //
    fn init(/* */);
    //
    fn create(/* */);
    //
}

// NOTE: Writes and Reads are handled by a log::Reader and log::Writer

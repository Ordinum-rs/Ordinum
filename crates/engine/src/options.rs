// Memtable Options
//

use crate::arena::arena::ArenaPolicy;

const MB: usize = 1024;

pub(crate) const SMALL_16MB: usize = 16 * MB;
pub(crate) const MEDIUM_32MB: usize = 32 * MB;
pub(crate) const DEFAULT_64MB: usize = 64 * MB;
pub(crate) const LARGE_128MB: usize = 128 * MB;

const SMALL_BLOCK: usize = 2 * MB;
const MEDIUM_BLOCK: usize = 4 * MB;
const DEFAULT_BLOCK: usize = 4 * MB;
const LARGE_BLOCK: usize = 8 * MB;

pub(crate) enum WriteBufferSize {
    Small,
    Medium,
    Default,
    Large,
}

impl WriteBufferSize {
    pub const fn as_bytes(self) -> usize {
        match self {
            Self::Small => SMALL_16MB,
            Self::Medium => MEDIUM_32MB,
            Self::Default => DEFAULT_64MB,
            Self::Large => LARGE_128MB,
        }
    }

    pub const fn arena_policy(self) -> ArenaPolicy {
        match self {
            Self::Small => ArenaPolicy {
                block_size: SMALL_BLOCK,
                cap: SMALL_16MB,
            },
            Self::Medium => ArenaPolicy {
                block_size: MEDIUM_BLOCK,
                cap: MEDIUM_32MB,
            },
            Self::Default => ArenaPolicy {
                block_size: DEFAULT_BLOCK,
                cap: DEFAULT_64MB,
            },
            Self::Large => ArenaPolicy {
                block_size: LARGE_BLOCK,
                cap: LARGE_128MB,
            },
        }
    }
}

#[test]
fn const_test() {}

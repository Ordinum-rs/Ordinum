// Memtable Options
//

use crate::arena::arena::ArenaPolicy;

const KIB: usize = 1024;
const MIB: usize = KIB * 1024;

pub(crate) const SMALL_16MB: usize = 16 * MIB;
pub(crate) const MEDIUM_32MB: usize = 32 * MIB;
pub(crate) const DEFAULT_64MB: usize = 64 * MIB;
pub(crate) const LARGE_128MB: usize = 128 * MIB;

const SMALL_BLOCK: usize = 2 * MIB;
const MEDIUM_BLOCK: usize = 4 * MIB;
const DEFAULT_BLOCK: usize = 4 * MIB;
const LARGE_BLOCK: usize = 8 * MIB;

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
fn write_buffer_presets_are_measured_in_mebibytes() {
    assert_eq!(WriteBufferSize::Small.as_bytes(), 16 * 1024 * 1024);
    assert_eq!(WriteBufferSize::Default.as_bytes(), 64 * 1024 * 1024);
    assert_eq!(
        WriteBufferSize::Default.arena_policy().block_size,
        4 * 1024 * 1024
    );
}

//
//
//

// DB - Options which are configured and stored on DB::Open()

use mem::arena::ArenaPolicy;

pub(crate) struct DBOptions {
    //
    write_buffer_size: usize,
    //
    arena_policy: ArenaPolicy,
}

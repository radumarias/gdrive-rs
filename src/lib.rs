use std::sync::LazyLock;

pub(crate) mod app_details;
pub(crate) mod async_util;
pub(crate) mod directories;
pub(crate) mod fs;
mod fs_util;
pub mod mount;
pub(crate) mod remote;
pub(crate) mod stream_util;

pub static UID: LazyLock<u32> = LazyLock::new(|| unsafe {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        libc::getuid()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    0
});
pub static GID: LazyLock<u32> = LazyLock::new(|| unsafe {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        libc::getgid()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    0
});

#[allow(unreachable_code)]
#[must_use]
pub const fn is_debug() -> bool {
    #[cfg(debug_assertions)]
    {
        return true;
    }
    false
}

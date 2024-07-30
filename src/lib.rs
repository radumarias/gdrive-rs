#![cfg_attr(not(debug_assertions), deny(warnings))]
#![doc(html_playground_url = "https://play.rust-lang.org")]
#![deny(clippy::all)]
#![deny(clippy::correctness)]
#![deny(clippy::suspicious)]
#![deny(clippy::complexity)]
#![deny(clippy::perf)]
#![deny(clippy::style)]
#![deny(clippy::pedantic)]
#![deny(clippy::nursery)]
#![deny(clippy::cargo)]
// #![deny(missing_docs)]
#![allow(clippy::similar_names)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::missing_errors_doc)]
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

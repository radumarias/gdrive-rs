use crate::fs::model::FsResult;
use crate::mount::fuse3::{MountHandleInnerImpl, MountPointImpl};
use async_trait::async_trait;
use futures_util::FutureExt;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

mod fuse3;

#[async_trait]
#[allow(clippy::module_name_repetitions)]
#[allow(clippy::struct_excessive_bools)]
pub trait MountPoint {
    #[allow(clippy::fn_params_excessive_bools)]
    fn new(
        mountpoint: PathBuf,
        allow_root: bool,
        allow_other: bool,
        direct_io: bool,
        suid_support: bool,
    ) -> Self
    where
        Self: Sized;
    async fn mount(mut self) -> FsResult<MountHandle>;
}

#[allow(clippy::module_name_repetitions)]
pub struct MountHandle {
    inner: MountHandleInnerImpl,
}
impl MountHandle {
    pub async fn umount(self) -> io::Result<()> {
        self.inner.unmount().await
    }
}

impl Future for MountHandle {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}

#[async_trait]
pub(crate) trait MountHandleInner: Future<Output = io::Result<()>> {
    async fn unmount(mut self) -> io::Result<()>;
}

/// **`mountpoint`** where it wil mount the filesystem
/// **`data_dir`** the directory where the encrypted files will be stored
/// **`password_provider`** the password provider
/// **`cipher`** The encryption algorithm to use.
/// Currently, it supports these ciphers [`Cipher`]
/// **`allow_root`** allow root to access the file system
/// **`allow_other`** allow other users to access the file system
/// **`direct_io`** use direct I/O (bypass page cache for open files)
/// **`suid_support`** if it should allow setting `SUID` and `SGID` when files are created. On `false` it will unset those flags when creating files
///
#[must_use]
#[allow(clippy::fn_params_excessive_bools)]
pub fn create_mount_point(
    mountpoint: &Path,
    allow_root: bool,
    allow_other: bool,
    direct_io: bool,
    suid_support: bool,
) -> impl MountPoint {
    MountPointImpl::new(
        mountpoint.to_path_buf(),
        allow_root,
        allow_other,
        direct_io,
        suid_support,
    )
}

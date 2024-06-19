use async_trait::async_trait;

use crate::fs::model::{
    CreateFileAttr, DirectoryEntryIterator, DirectoryEntryPlusIterator, FileAttr, FsResult,
    SetFileAttr,
};

pub(crate) mod model;

#[async_trait]
pub(crate) trait Filesystem: Send + Sync {
    fn exists(&self, ino: u64) -> bool;

    async fn is_dir(&self, ino: u64) -> bool;

    async fn is_file(&self, ino: u64) -> bool;

    /// Create a new node in the filesystem
    async fn create(
        &self,
        parent: u64,
        name: &str,
        create_attr: CreateFileAttr,
        read: bool,
        write: bool,
    ) -> FsResult<(u64, FileAttr)>;

    async fn find_by_name(&self, parent: u64, name: &str) -> FsResult<Option<FileAttr>>;

    /// Count children of a directory. This **EXCLUDES** "." and "..".
    async fn len(&self, ino: u64) -> FsResult<usize>;

    /// Delete a directory
    async fn remove_dir(&self, parent: u64, name: &str) -> FsResult<()>;

    /// Delete a file
    async fn remove_file(&self, parent: u64, name: &str) -> FsResult<()>;

    async fn exists_by_name(&self, parent: u64, name: &str) -> FsResult<bool>;

    async fn read_dir(&self, ino: u64) -> FsResult<DirectoryEntryIterator>;

    /// Like [`crate::remote::wrapper::RemoteFilesystemImpl::read_dir`] but with [`FileAttr`] so we don't need to query again for those.
    async fn read_dir_plus(&self, ino: u64) -> FsResult<DirectoryEntryPlusIterator>;

    /// Get metadata
    async fn get_attr(&self, ino: u64) -> FsResult<FileAttr>;

    /// Set metadata
    async fn set_attr(&self, ino: u64, set_attr: SetFileAttr) -> FsResult<()>;

    /// Read the contents from an 'offset'. If we try to read outside of file size, we return 0 bytes.
    /// If the file is not opened for read, it will return an error of type ['FsError::InvalidFileHandle'].
    async fn read(&self, ino: u64, offset: u64, buf: &mut [u8], handle: u64) -> FsResult<usize>;

    async fn release(&self, handle: u64) -> FsResult<()>;

    /// Check if a file is opened for read with this handle.
    async fn is_read_handle(&self, fh: u64) -> bool;

    /// Check if a file is opened for write with this handle.
    async fn is_write_handle(&self, fh: u64) -> bool;

    /// Writes the contents of `buf` to the file at `ino` starting at `offset`.
    /// If we write outside of file size, we fill up with zeros until offset.
    /// If the file is not opened for writing, it will return an error of type ['FsError::InvalidFileHandle'].
    async fn write(&self, ino: u64, offset: u64, buf: &[u8], handle: u64) -> FsResult<usize>;

    /// Flush the data to the underlying storage.
    async fn flush(&self, handle: u64) -> FsResult<()>;

    /// Helpful when we want to copy just some portions of the file.
    async fn copy_file_range(
        &self,
        src_ino: u64,
        src_offset: u64,
        dest_ino: u64,
        dest_offset: u64,
        size: usize,
        src_fh: u64,
        dest_fh: u64,
    ) -> FsResult<usize>;

    /// Open a file. We can open multiple times for read but only one to write at a time.
    async fn open(&self, ino: u64, read: bool, write: bool) -> FsResult<u64>;

    /// Truncates or extends the underlying file, updating the size of this file to become size.
    async fn set_len(&self, ino: u64, size: u64) -> FsResult<()>;

    async fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
    ) -> FsResult<()>;
}

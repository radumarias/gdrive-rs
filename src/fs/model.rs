use crate::{GID, UID};
use atomic_write_file::AtomicWriteFile;
use google_drive3::api::File as RemoteFile;
use google_drive3::hyper::http;
use mime::FromStrError;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::fs::File;
use std::io;
use std::io::IntoInnerError;
use std::num::ParseIntError;
use std::time::SystemTime;
use thiserror::Error;
use tokio::task::JoinError;
use tracing::instrument;

/// File attributes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FileAttr {
    /// Inode number
    pub ino: u64,
    /// Size in bytes
    pub size: u64,
    /// Size in blocks
    pub blocks: u64,
    /// Time of last access
    pub atime: SystemTime,
    /// Time of last modification
    pub mtime: SystemTime,
    /// Time of last change
    pub ctime: SystemTime,
    /// Time of creation (macOS only)
    pub crtime: SystemTime,
    /// Kind of file (directory, file, pipe, etc.)
    pub kind: FileType,
    /// Permissions
    pub perm: u16,
    /// Number of hard links
    pub nlink: u32,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
    /// Rdev
    pub rdev: u32,
    /// Block size
    pub blksize: u32,
    /// Flags (macOS only, see chflags(2))
    pub flags: u32,
}

/// File types.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum FileType {
    // /// Named pipe (S_IFIFO)
    // NamedPipe,
    // /// Character device (S_IFCHR)
    // CharDevice,
    // /// Block device (S_IFBLK)
    // BlockDevice,
    /// Directory (`S_IFDIR`)
    Directory,
    /// Regular file (`S_IFREG`)
    RegularFile,
    /// Symbolic link (S_IFLNK)
    Symlink,
    // /// Unix domain socket (S_IFSOCK)
    // Socket,
}

impl Default for FileAttr {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            ino: 0,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: *UID,
            gid: *GID,
            rdev: 0,
            blksize: 0,
            flags: 0,
        }
    }
}

impl From<RemoteFile> for FileAttr {
    fn from(file: RemoteFile) -> Self {
        let kind = file.kind.unwrap();
        let mime_type = file.mime_type.unwrap();
        let size = file.size.unwrap_or(0) as u64;
        let created_time = file.created_time.unwrap();
        let modified_time = file.modified_time.unwrap();
        let kind = to_file_type(&kind, &mime_type).unwrap();
        let perm = if matches!(kind, FileType::Directory) {
            0o755
        } else {
            0o644
        };
        let uid = *UID;
        let gid = *GID;
        FileAttr {
            ino: 0,
            size,
            blocks: 0,
            atime: created_time.into(),
            mtime: modified_time.into(),
            ctime: created_time.into(),
            crtime: created_time.into(),
            kind,
            perm,
            nlink: 1,
            uid,
            gid,
            ..Default::default()
        }
    }
}

impl Into<RemoteFile> for FileAttr {
    fn into(self) -> RemoteFile {
        // let kind = "drive#file";
        let mime_type = match self.kind {
            FileType::Directory => Some("application/vnd.google-apps.folder".to_string()),
            FileType::RegularFile => None,
            FileType::Symlink => Some("application/vnd.google-apps.shortcut".to_string()),
        };
        // let size = self.size as i64;
        // let created_time = self.ctime.into();
        // let modified_time = self.mtime.into();
        let file = RemoteFile {
            name: None,
            // kind: Some(kind.to_string()),
            mime_type,
            // size: Some(size),
            // created_time: Some(created_time),
            // modified_time: Som   e(modified_time),
            ..Default::default()
        };
        file
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SetFileAttr {
    /// Size in bytes
    pub size: Option<u64>,
    /// Time of last access
    pub atime: Option<SystemTime>,
    /// Time of last modification
    pub mtime: Option<SystemTime>,
    /// Time of last change
    pub ctime: Option<SystemTime>,
    /// Time of creation (macOS only)
    pub crtime: Option<SystemTime>,
    /// Permissions
    pub perm: Option<u16>,
    /// User id
    pub uid: Option<u32>,
    /// Group id
    pub gid: Option<u32>,
    /// Rdev
    pub rdev: Option<u32>,
    /// Flags (macOS only, see chflags(2))
    pub flags: Option<u32>,
}

impl SetFileAttr {
    #[must_use]
    pub const fn with_size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    #[must_use]
    pub const fn with_atime(mut self, atime: SystemTime) -> Self {
        self.atime = Some(atime);
        self
    }

    #[must_use]
    pub const fn with_mtime(mut self, mtime: SystemTime) -> Self {
        self.mtime = Some(mtime);
        self
    }

    #[must_use]
    pub const fn with_ctime(mut self, ctime: SystemTime) -> Self {
        self.ctime = Some(ctime);
        self
    }

    #[must_use]
    pub const fn with_crtime(mut self, crtime: SystemTime) -> Self {
        self.crtime = Some(crtime);
        self
    }

    #[must_use]
    pub const fn with_perm(mut self, perm: u16) -> Self {
        self.perm = Some(perm);
        self
    }

    #[must_use]
    pub const fn with_uid(mut self, uid: u32) -> Self {
        self.uid = Some(uid);
        self
    }

    #[must_use]
    pub const fn with_gid(mut self, gid: u32) -> Self {
        self.gid = Some(gid);
        self
    }

    #[must_use]
    #[allow(dead_code)]
    pub const fn with_rdev(mut self, rdev: u32) -> Self {
        self.rdev = Some(rdev);
        self
    }

    #[must_use]
    pub const fn with_flags(mut self, flags: u32) -> Self {
        self.rdev = Some(flags);
        self
    }
}

#[derive(Debug, Clone)]
pub struct CreateFileAttr {
    /// Kind of file (directory, file, pipe, etc.)
    pub kind: FileType,
    /// Permissions
    pub perm: u16,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
    /// Rdev
    pub rdev: u32,
    /// Flags (macOS only, see chflags(2))
    pub flags: u32,
}

impl From<CreateFileAttr> for FileAttr {
    fn from(value: CreateFileAttr) -> Self {
        let now = SystemTime::now();
        Self {
            ino: 0,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: value.kind,
            perm: value.perm,
            nlink: if value.kind == FileType::Directory {
                2
            } else {
                1
            },
            uid: value.uid,
            gid: value.gid,
            rdev: value.rdev,
            blksize: 0,
            flags: value.flags,
        }
    }
}

#[derive(Debug, Clone)]
struct TimeAndSizeFileAttr {
    atime: SystemTime,
    mtime: SystemTime,
    ctime: SystemTime,
    crtime: SystemTime,
    size: u64,
}

impl TimeAndSizeFileAttr {
    #[allow(dead_code)]
    const fn new(
        atime: SystemTime,
        mtime: SystemTime,
        ctime: SystemTime,
        crtime: SystemTime,
        size: u64,
    ) -> Self {
        Self {
            atime,
            mtime,
            ctime,
            crtime,
            size,
        }
    }
}

impl From<FileAttr> for TimeAndSizeFileAttr {
    fn from(value: FileAttr) -> Self {
        Self {
            atime: value.atime,
            mtime: value.mtime,
            ctime: value.ctime,
            crtime: value.crtime,
            size: value.size,
        }
    }
}

impl From<TimeAndSizeFileAttr> for SetFileAttr {
    fn from(value: TimeAndSizeFileAttr) -> Self {
        Self::default()
            .with_atime(value.atime)
            .with_mtime(value.mtime)
            .with_ctime(value.ctime)
            .with_crtime(value.crtime)
            .with_size(value.size)
    }
}

#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    pub ino: u64,
    pub name: String,
    pub kind: FileType,
}

impl From<DirectoryEntryPlus> for DirectoryEntry {
    fn from(value: DirectoryEntryPlus) -> Self {
        Self {
            ino: value.ino,
            name: value.name,
            kind: value.kind,
        }
    }
}

impl PartialEq for DirectoryEntry {
    fn eq(&self, other: &Self) -> bool {
        self.ino == other.ino && self.name == other.name && self.kind == other.kind
    }
}

/// Like [`DirectoryEntry`] but with [`FileAttr`].
#[derive(Debug, Clone)]
pub struct DirectoryEntryPlus {
    pub ino: u64,
    pub name: String,
    pub kind: FileType,
    pub attr: FileAttr,
}

impl From<DirectoryEntry> for DirectoryEntryPlus {
    fn from(value: DirectoryEntry) -> Self {
        Self {
            ino: value.ino,
            name: value.name,
            kind: value.kind,
            attr: FileAttr::default(),
        }
    }
}

impl PartialEq for DirectoryEntryPlus {
    fn eq(&self, other: &Self) -> bool {
        self.ino == other.ino
            && self.name == other.name
            && self.kind == other.kind
            && self.attr == other.attr
    }
}

pub struct DirectoryEntryIterator(pub VecDeque<FsResult<DirectoryEntry>>);

impl Iterator for DirectoryEntryIterator {
    type Item = FsResult<DirectoryEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

pub struct DirectoryEntryPlusIterator(pub VecDeque<FsResult<DirectoryEntryPlus>>);

impl Iterator for DirectoryEntryPlusIterator {
    type Item = FsResult<DirectoryEntryPlus>;

    #[instrument(name = "DirectoryEntryPlusIterator::next", skip(self))]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

pub type FsResult<T> = Result<T, FsError>;

#[derive(Error, Debug)]
pub enum FsError {
    #[error("IO error: {source}")]
    Io {
        #[from]
        source: io::Error,
        // backtrace: Backtrace,
    },

    #[error("serialize error: {source}")]
    SerializeError {
        #[from]
        source: bincode::Error,
        // backtrace: Backtrace,
    },

    #[error("infallible error: {source}")]
    InfallibleError {
        #[from]
        source: Infallible,
        // backtrace: Backtrace,
    },

    #[error("infallible error: {source}")]
    GoogleDriveError {
        #[from]
        source: google_drive3::Error,
        // backtrace: Backtrace,
    },

    #[error("from str error: {source}")]
    FromStrError {
        #[from]
        source: FromStrError,
        // backtrace: Backtrace,
    },

    #[error("json error: {source}")]
    SerdeJsonError {
        #[from]
        source: serde_json::Error,
        // backtrace: Backtrace,
    },

    #[error("http error: {source}")]
    HttpError {
        #[from]
        source: http::Error,
        // backtrace: Backtrace,
    },

    #[error("into inner error: {source}")]
    IntoInnerError {
        #[from]
        source: IntoInnerError<io::BufWriter<File>>,
        // backtrace: Backtrace,
    },

    #[error("into inner error: {source}")]
    IntoInnerError2 {
        #[from]
        source: IntoInnerError<io::BufWriter<AtomicWriteFile>>,
        // backtrace: Backtrace,
    },

    #[error("item not found: {0}")]
    NotFound(&'static str),

    #[error("inode not found")]
    InodeNotFound,

    #[error("invalid input")]
    InvalidInput(&'static str),

    #[error("invalid node type")]
    InvalidInodeType,

    #[error("invalid file handle")]
    InvalidFileHandle,

    #[error("already exists")]
    AlreadyExists,

    #[error("already open for write")]
    AlreadyOpenForWrite,

    #[error("not empty")]
    NotEmpty,

    #[error("other: {0}")]
    Other(&'static str),

    #[error("invalid password")]
    InvalidPassword,

    #[error("invalid structure of data directory")]
    InvalidDataDirStructure,

    #[error("parse int error: {source}")]
    ParseIntError {
        #[from]
        source: ParseIntError,
        // backtrace: Backtrace,
    },

    #[error("tokio join error: {source}")]
    JoinError {
        #[from]
        source: JoinError,
        // backtrace: Backtrace,
    },

    #[error("max filesize exceeded, max allowed {0}")]
    MaxFilesizeExceeded(usize),
}

pub fn to_file_type(kind: &str, mime_type: &str) -> FsResult<FileType> {
    match kind {
        "drive#file" => match mime_type {
            "application/vnd.google-apps.folder" => Ok(FileType::Directory),
            "application/vnd.google-apps.shortcut" => Ok(FileType::Symlink),
            _ => Ok(FileType::RegularFile),
        },
        _ => Err(FsError::InvalidInput("invalid kind")),
    }
}

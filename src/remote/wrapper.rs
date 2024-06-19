use crate::async_util::call_async;
use crate::directories::get_data_dir;
use crate::fs::model::{
    CreateFileAttr, DirectoryEntry, DirectoryEntryIterator, DirectoryEntryPlus,
    DirectoryEntryPlusIterator, FileAttr, FileType, FsError, FsResult, SetFileAttr,
};
use crate::fs::Filesystem;
use crate::{fs, fs_util, stream_util, GID, UID};
use async_trait::async_trait;
use bimap::BiMap;
use google_drive3::api::{File as RemoteFile, Scope};
use google_drive3::chrono::{DateTime, Utc};
use google_drive3::hyper::body::HttpBody;
use google_drive3::hyper::client::HttpConnector;
use google_drive3::hyper_rustls::HttpsConnector;
use google_drive3::{hyper, hyper_rustls, oauth2, DriveHub};
use mime::Mime;
use num_format::{Locale, ToFormattedString};
use rand::thread_rng;
use rand_core::RngCore;
use std::cmp::{max, min};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument};

pub static HUB: OnceLock<DriveHub<HttpsConnector<HttpConnector>>> = OnceLock::new();

pub(crate) const ROOT_INODE: u64 = 1;

const FILES_DIR: &str = "files";

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
struct LocalFile {
    ino: u64,
    parent: u64,
    name: String,
    mime_type: Mime,
    remote_md5_checksum: String,
}

impl From<RemoteFile> for LocalFile {
    fn from(fr: RemoteFile) -> Self {
        LocalFile {
            ino: 0,
            parent: 0,
            name: fr.name.as_ref().unwrap().clone(),
            mime_type: Mime::from_str(fr.mime_type.unwrap_or_default().as_str()).unwrap(),
            remote_md5_checksum: fr.md5_checksum.unwrap_or_default(),
        }
    }
}

/// Encrypted FS that stores encrypted files in a dedicated directory with a specific structure based on `inode`.
pub(crate) struct RemoteFilesystemImpl {
    data_dir: PathBuf,
    direct_io: bool,
    suid_support: bool,
    ids: std::sync::RwLock<BiMap<u64, String>>,
    meta: RwLock<HashMap<u64, FileAttr>>,
    // files: RwLock<HashMap<u64, Vec<DirectoryEntryPlus>>>,
    files: RwLock<HashMap<u64, HashMap<u64, Arc<RwLock<LocalFile>>>>>,
    ino_to_file: RwLock<HashMap<u64, Arc<RwLock<LocalFile>>>>,
    read_fhs: RwLock<HashMap<u64, u64>>,
    write_fhs: RwLock<HashMap<u64, u64>>,
}

impl RemoteFilesystemImpl {
    pub async fn new(direct_io: bool, suid_support: bool) -> FsResult<Arc<Self>> {
        let fs = Self {
            data_dir: get_data_dir(),
            direct_io,
            suid_support,
            ids: Default::default(),
            meta: Default::default(),
            // files: Default::default(),
            files: Default::default(),
            ino_to_file: Default::default(),
            read_fhs: Default::default(),
            write_fhs: Default::default(),
        };
        fs.ensure_root_exists().await?;
        fs.ensure_structure()?;
        setup_api().await?;
        let arc = Arc::new(fs);
        Ok(arc)
    }

    async fn ensure_root_exists(&self) -> FsResult<()> {
        let now = SystemTime::now();
        let root = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm: 0x755,
            nlink: 1,
            uid: *UID,
            gid: *GID,
            rdev: 0,
            blksize: 0,
            flags: 0,
        };
        self.ids
            .write()
            .unwrap()
            .insert(ROOT_INODE, String::from_str("root")?);
        self.meta.write().await.insert(ROOT_INODE, root.clone());
        let file: Arc<RwLock<LocalFile>> = Arc::new(RwLock::new(LocalFile {
            ino: ROOT_INODE,
            parent: 0,
            name: String::from_str("root").unwrap(),
            mime_type: Mime::from_str("*/*")?,
            remote_md5_checksum: "".to_string(),
        }));
        self.files
            .write()
            .await
            .insert(ROOT_INODE, Default::default());
        self.ino_to_file.write().await.insert(ROOT_INODE, file);
        Ok(())
    }

    fn ensure_structure(&self) -> FsResult<()> {
        if !self.data_dir.join(FILES_DIR).exists() {
            std::fs::create_dir_all(self.data_dir.join(FILES_DIR))?;
        }
        Ok(())
    }

    async fn into_attr(&self, ino: u64, file: RemoteFile) -> FileAttr {
        let mut attr: FileAttr = file.into();
        attr.ino = ino;
        self.meta.write().await.insert(ino, attr.clone());
        attr
    }

    fn next_ino(&self) -> u64 {
        loop {
            let ino = thread_rng().next_u64();
            if !self.exists(ino) {
                return ino;
            }
        }
    }

    fn next_fh(&self) -> u64 {
        loop {
            let ino = thread_rng().next_u64();
            // todo: check in opened fns
            if !self.exists(ino) {
                return ino;
            }
        }
    }

    pub(crate) async fn download_file(&self, ino: u64) -> FsResult<()> {
        let file_path = self.get_file_path(ino);
        if !file_path.exists() {
            // create the file locally
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(file_path.clone())?
                .sync_all()
                .unwrap();
            File::open(self.data_dir.join(FILES_DIR))?.sync_all()?;
        }

        // update metadata
        let rid = self.ids.read().unwrap().get_by_left(&ino).unwrap().clone();
        let (_, remote_file) = HUB
            .get()
            .as_ref()
            .unwrap()
            .files()
            .get(&rid)
            .param(
                "fields",
                "id,name,kind,size,parents,createdTime,modifiedTime,mimeType,md5Checksum",
            )
            .doit()
            .await?;
        {
            let guard = self.ino_to_file.read().await;
            let mut local_file_entry = guard.get(&ino).unwrap().write().await;

            // update metadata
            merge_file(&mut *local_file_entry, &remote_file);
            merge_file_attr(
                self.meta.write().await.get_mut(&ino).unwrap(),
                &remote_file.into(),
            );
        }
        let (mut res, _) = HUB
            .get()
            .as_ref()
            .unwrap()
            .files()
            .get(&rid)
            .param("alt", "media")
            .doit()
            .await?;

        // update content
        let fout = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(file_path)?;
        let mut fout = BufWriter::new(fout);
        while !res.body().is_end_stream() {
            let data = res
                .data()
                .await
                .ok_or(FsError::Other("No more data"))?
                .map_err(|e| FsError::Io {
                    source: io::Error::other(e.to_string()),
                })?;
            fout.write_all(data.as_ref())?;
        }
        fout.flush()?;
        fout.into_inner()?.sync_all()?;
        File::open(self.data_dir.join(FILES_DIR))?.sync_all()?;
        Ok(())
    }

    pub(crate) async fn is_file_outdated(&self, ino: u64) -> FsResult<bool> {
        let rid = self.ids.read().unwrap().get_by_left(&ino).unwrap().clone();
        let (_, file) = HUB
            .get()
            .as_ref()
            .unwrap()
            .files()
            .get(&rid)
            .param("fields", "modifiedTime")
            .doit()
            .await?;
        Ok(self.meta.read().await.get(&ino).unwrap().mtime
            != <DateTime<Utc> as Into<SystemTime>>::into(
            file.modified_time.as_ref().unwrap().clone(),
        )
            || self.meta.read().await.get(&ino).unwrap().ctime
            != <DateTime<Utc> as Into<SystemTime>>::into(
            file.modified_time.as_ref().unwrap().clone(),
        )
            || self.meta.read().await.get(&ino).unwrap().atime
            != <DateTime<Utc> as Into<SystemTime>>::into(
            file.modified_time.as_ref().unwrap().clone(),
        ))
    }

    async fn read_files(&self, ino: u64) -> FsResult<VecDeque<FsResult<DirectoryEntryPlus>>> {
        let mut vec = VecDeque::new();
        let result = HUB
            .get()
            .as_ref()
            .unwrap()
            .files()
            .list()
            .q(format!(
                "'{}' in parents and trashed = false",
                self.ids.read().unwrap().get_by_left(&ino).unwrap()
            )
                .as_str())
            .param(
                "fields",
                "files(id,name,kind,size,parents,createdTime,modifiedTime,mimeType,md5Checksum)",
            )
            .add_scope(Scope::Full)
            .add_scope(Scope::Readonly)
            .add_scope(Scope::Metadata)
            .add_scope(Scope::MetadataReadonly)
            .include_items_from_all_drives(false)
            .doit()
            .await?;
        if let Some(files) = result.1.files {
            for f in files {
                let rid = f.id.as_ref().unwrap().clone();
                if !self.ids.read().unwrap().contains_right(&rid) {
                    let next_ino = self.next_ino();
                    self.ids.write().unwrap().insert(next_ino, rid.clone());
                }
                let file_ino = *self.ids.read().unwrap().get_by_right(&rid).unwrap();
                let attr = self.into_attr(file_ino, f.clone()).await;
                let file_ino = *self.ids.read().unwrap().get_by_right(&rid).unwrap();
                vec.push_back(Ok(DirectoryEntryPlus {
                    ino: self.ids.read().unwrap().get_by_right(&rid).unwrap().clone(),
                    name: f.name.as_ref().unwrap().clone(),
                    kind: fs::model::to_file_type(
                        &f.kind.as_ref().unwrap(),
                        &f.mime_type.as_ref().unwrap(),
                    )?,
                    attr,
                }));
                let mut guard = self.files.write().await;
                let file = guard
                    .entry(ino)
                    .or_default()
                    .get_mut(self.ids.read().unwrap().get_by_right(&rid).unwrap());
                if let Some(file) = file {
                    merge_file(&mut *file.write().await, &f);
                } else {
                    let file: Arc<RwLock<LocalFile>> = Arc::new(RwLock::new(f.into()));
                    file.write().await.ino = file_ino;
                    file.write().await.parent = ino;
                    guard.get_mut(&ino).unwrap().insert(file_ino, file.clone());
                    self.ino_to_file.write().await.insert(file_ino, file);
                };
            }
        }
        Ok(vec)
    }

    fn get_file_path(&self, ino: u64) -> PathBuf {
        self.data_dir.join(FILES_DIR).join(ino.to_string())
    }
}

#[async_trait]
impl Filesystem for RemoteFilesystemImpl {
    fn exists(&self, ino: u64) -> bool {
        self.ids.read().unwrap().contains_left(&ino)
    }

    async fn is_dir(&self, ino: u64) -> bool {
        if let Some(attr) = self.meta.read().await.get(&ino) {
            matches!(attr.kind, FileType::Directory)
        } else {
            false
        }
    }

    async fn is_file(&self, ino: u64) -> bool {
        if let Some(attr) = self.meta.read().await.get(&ino) {
            !matches!(attr.kind, FileType::Directory)
        } else {
            false
        }
    }

    async fn create(
        &self,
        parent: u64,
        name: &str,
        create_attr: CreateFileAttr,
        read: bool,
        write: bool,
    ) -> FsResult<(u64, FileAttr)> {
        if name == "." || name == ".." {
            return Err(FsError::InvalidInput("name cannot be '.' or '..'"));
        }
        if !self.exists(parent) {
            return Err(FsError::InodeNotFound);
        }
        if self.exists_by_name(parent, name).await? {
            return Err(FsError::AlreadyExists);
        }

        let mut attr: FileAttr = create_attr.into();
        attr.ino = self.next_ino();
        let mut f: RemoteFile = attr.into();
        // f.size = Some(1);
        f.name = Some(name.to_string());
        f.parents = Some(vec![self
            .ids
            .read()
            .unwrap()
            .get_by_left(&parent)
            .unwrap()
            .clone()]);
        let (_, f) = HUB
            .get()
            .as_ref()
            .unwrap()
            .files()
            .create(f)
            .param(
                "fields",
                "id,name,kind,size,parents,createdTime,modifiedTime,ownedByMe,mimeType",
            )
            .upload(
                Cursor::new(vec![]),
                if attr.kind == FileType::Directory {
                    mime::Mime::from_str("application/vnd.google-apps.folder")?
                } else {
                    mime::Mime::from_str("*/*")?
                },
            )
            .await?;
        self.ids
            .write()
            .unwrap()
            .insert(attr.ino, f.id.as_ref().unwrap().clone());
        let set_attr = SetFileAttr::default()
            .with_mtime((*f.modified_time.as_ref().unwrap()).into())
            .with_ctime((*f.created_time.as_ref().unwrap()).into())
            .with_atime((*f.modified_time.as_ref().unwrap()).into());
        merge_attr(&mut attr, &set_attr);
        self.meta.write().await.insert(attr.ino, attr.clone());
        Ok((self.open(attr.ino, read, write).await?, attr))
    }

    async fn find_by_name(&self, parent: u64, name: &str) -> FsResult<Option<FileAttr>> {
        if !self.exists(parent) {
            return Err(FsError::InodeNotFound);
        }
        if !self.is_dir(parent).await {
            return Err(FsError::InvalidInodeType);
        }
        let attr = self
            .files
            .read()
            .await
            .get(&parent)
            . unwrap()
            .iter()
            .filter_map(|(_, r)| {
                Some(r).filter(|s| call_async(async { s.read().await.name == name }))
            })
            .map(|d| {
                call_async(async {
                    self.meta
                        .read()
                        .await
                        .get(&d.read().await.ino)
                        .cloned()
                        .unwrap()
                })
            })
            .take(1)
            .next();
        if let Some(attr) = attr {
            Ok(Some(attr))
        } else {
            // fetch files
            let vec = self.read_files(parent).await?;
            Ok(vec
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .filter(|d| d.name == name)
                .map(|d| d.attr.clone())
                .take(1)
                .next())
        }
    }

    async fn len(&self, ino: u64) -> FsResult<usize> {
        if !self.is_dir(ino).await {
            return Err(FsError::InvalidInodeType);
        }
        Ok(self.files.read().await.get(&ino).unwrap().len())
    }

    async fn remove_dir(&self, parent: u64, name: &str) -> FsResult<()> {
        if !self.is_dir(parent).await {
            return Err(FsError::InvalidInodeType);
        }

        if !self.exists_by_name(parent, name).await? {
            return Err(FsError::NotFound("name not found"));
        }

        let attr = self
            .find_by_name(parent, name)
            .await?
            .ok_or(FsError::NotFound("name not found"))?;
        if !matches!(attr.kind, FileType::Directory) {
            return Err(FsError::InvalidInodeType);
        }
        // check if it's empty
        if self.len(attr.ino).await? > 0 {
            return Err(FsError::NotEmpty);
        }

        let fid = {
            let guard = self.ids.read().unwrap();
            guard.get_by_left(&attr.ino).unwrap().clone()
        };
        HUB.get()
            .as_ref()
            .unwrap()
            .files()
            .delete(&fid)
            .doit()
            .await?;
        self.ids.write().unwrap().remove_by_left(&attr.ino);
        self.meta.write().await.remove(&attr.ino);

        let guard = self.ino_to_file.read().await;
        let file = guard.get(&attr.ino).unwrap().read().await;
        let now = SystemTime::now();
        let set_attr = SetFileAttr::default().with_ctime(now).with_atime(now);
        self.set_attr(file.parent, set_attr).await?;

        Ok(())
    }

    async fn remove_file(&self, parent: u64, name: &str) -> FsResult<()> {
        if !self.is_dir(parent).await {
            return Err(FsError::InvalidInodeType);
        }
        if !self.exists_by_name(parent, name).await? {
            return Err(FsError::NotFound("name not found"));
        }

        let attr = self
            .find_by_name(parent, name)
            .await?
            .ok_or(FsError::NotFound("name not found"))?;
        if !matches!(attr.kind, FileType::RegularFile) {
            return Err(FsError::InvalidInodeType);
        }

        let fid = {
            let guard = self.ids.read().unwrap();
            guard.get_by_left(&attr.ino).unwrap().clone()
        };
        HUB.get()
            .as_ref()
            .unwrap()
            .files()
            .delete(&fid)
            .doit()
            .await?;
        self.ids.write().unwrap().remove_by_left(&attr.ino);
        self.meta.write().await.remove(&attr.ino);

        let guard = self.ino_to_file.read().await;
        let file = guard.get(&attr.ino).unwrap().read().await;
        let now = SystemTime::now();
        let set_attr = SetFileAttr::default().with_ctime(now).with_atime(now);
        self.set_attr(file.parent, set_attr).await?;

        Ok(())
    }

    async fn exists_by_name(&self, parent: u64, name: &str) -> FsResult<bool> {
        if !self.exists(parent) {
            return Err(FsError::InodeNotFound);
        }
        if !self.is_dir(parent).await {
            return Err(FsError::InvalidInodeType);
        }
        Ok(self
            .read_dir(parent)
            .await?
            .filter_map(|r| r.ok().map(|d| d.name))
            .any(|n| n == name))
    }

    #[instrument(skip(self))]
    async fn read_dir(&self, ino: u64) -> FsResult<DirectoryEntryIterator> {
        if !self.is_dir(ino).await {
            return Err(FsError::InvalidInodeType);
        }
        if let Some(files) = self.files.read().await.get(&ino) {
            let mut vec = VecDeque::new();
            vec.push_back(Ok(DirectoryEntry {
                ino,
                name: ".".to_string(),
                kind: FileType::Directory,
            }));
            if ino != ROOT_INODE {
                vec.push_back(Ok(DirectoryEntry {
                    ino: self
                        .ino_to_file
                        .read()
                        .await
                        .get(&ino)
                        .unwrap()
                        .read()
                        .await
                        .parent,
                    name: "..".to_string(),
                    kind: FileType::Directory,
                }));
            }
            for (_, file) in files {
                let file = file.read().await;
                vec.push_back(Ok(DirectoryEntry {
                    ino: file.ino,
                    name: file.name.clone(),
                    kind: self.meta.read().await.get(&file.ino).unwrap().kind.clone(),
                }));
            }
            if vec.len() > if ino == ROOT_INODE { 1 } else { 2 } {
                return Ok(DirectoryEntryIterator(vec));
            }
        }
        let vec = self.read_files(ino).await?;
        let mut vec2: VecDeque<FsResult<DirectoryEntry>> = VecDeque::new();
        vec.iter().for_each(|f| {
            vec2.push_back(Ok(f.as_ref().unwrap().clone().into()));
        });
        let now = SystemTime::now();
        let set_attr = SetFileAttr::default().with_atime(now);
        self.set_attr(ino, set_attr).await?;
        Ok(DirectoryEntryIterator(vec2))
    }

    #[instrument(skip(self))]
    async fn read_dir_plus(&self, ino: u64) -> FsResult<DirectoryEntryPlusIterator> {
        if !self.is_dir(ino).await {
            return Err(FsError::InvalidInodeType);
        }
        if let Some(files) = self.files.read().await.get(&ino) {
            let mut vec = VecDeque::new();
            vec.push_back(Ok(DirectoryEntryPlus {
                ino,
                name: ".".to_string(),
                kind: FileType::Directory,
                attr: self.meta.read().await.get(&ino).unwrap().clone(),
            }));
            if ino != ROOT_INODE {
                let parent = self
                    .ino_to_file
                    .read()
                    .await
                    .get(&ino)
                    .unwrap()
                    .read()
                    .await
                    .parent;
                vec.push_back(Ok(DirectoryEntryPlus {
                    ino: self
                        .ino_to_file
                        .read()
                        .await
                        .get(&ino)
                        .unwrap()
                        .read()
                        .await
                        .parent,
                    name: "..".to_string(),
                    kind: FileType::Directory,
                    attr: self.meta.read().await.get(&parent).unwrap().clone(),
                }));
            }
            for (_, file) in files {
                let file = file.read().await;
                vec.push_back(Ok(DirectoryEntryPlus {
                    ino: file.ino,
                    name: file.name.clone(),
                    kind: self.meta.read().await.get(&file.ino).unwrap().kind.clone(),
                    attr: self.meta.read().await.get(&file.ino).unwrap().clone(),
                }));
            }
            if vec.len() > if ino == ROOT_INODE { 1 } else { 2 } {
                return Ok(DirectoryEntryPlusIterator(vec));
            }
        }
        let vec = self.read_files(ino).await?;
        let now = SystemTime::now();
        let set_attr = SetFileAttr::default().with_atime(now);
        self.set_attr(ino, set_attr).await?;
        Ok(DirectoryEntryPlusIterator(vec))
    }

    async fn get_attr(&self, ino: u64) -> FsResult<FileAttr> {
        if !self.exists(ino) {
            return Err(FsError::InodeNotFound);
        }
        self.meta
            .read()
            .await
            .get(&ino)
            .cloned()
            .ok_or(FsError::InodeNotFound)
    }

    async fn set_attr(&self, ino: u64, set_attr: SetFileAttr) -> FsResult<()> {
        if !self.exists(ino) {
            return Err(FsError::InodeNotFound);
        }
        merge_attr(self.meta.write().await.get_mut(&ino).unwrap(), &set_attr);
        Ok(())
    }

    #[instrument(skip(self, buf))]
    async fn read(&self, ino: u64, offset: u64, buf: &mut [u8], handle: u64) -> FsResult<usize> {
        if !self.exists(ino) {
            return Err(FsError::InodeNotFound);
        }
        if !self.is_file(ino).await {
            return Err(FsError::InvalidInodeType);
        }
        let guard = self.meta.read().await;
        let attr = guard.get(&ino).unwrap();
        if offset > attr.size {
            return Ok(0);
        }
        let mut fin = BufReader::new(File::open(self.get_file_path(ino))?);
        fin.seek(SeekFrom::Start(offset))?;
        let len = min(attr.size - offset, buf.len() as u64) as usize;
        fin.read_exact(&mut buf[..len])?;
        let now = SystemTime::now();
        let set_attr = SetFileAttr::default()
            .with_mtime(now)
            .with_ctime(now)
            .with_atime(now);
        self.set_attr(ino, set_attr).await?;
        Ok(len)
    }

    async fn release(&self, handle: u64) -> FsResult<()> {
        if let Some(ino) = self.write_fhs.write().await.remove(&handle) {
            // todo: if remote file has changed from when we opened local file for write, merge changes
            let guard = self.ino_to_file.read().await;
            let file = guard.get(&ino).unwrap().read().await;
            let fin = File::open(self.get_file_path(ino))?;
            let rid = self.ids.read().unwrap().get_by_left(&ino).unwrap().clone();
            HUB.get()
                .as_ref()
                .unwrap()
                .files()
                .update(RemoteFile::default(), &rid)
                .upload_resumable(fin, file.mime_type.clone())
                .await?;
        }
        self.read_fhs.write().await.remove(&handle);
        Ok(())
    }

    async fn is_read_handle(&self, fh: u64) -> bool {
        self.read_fhs.read().await.contains_key(&fh)
    }

    async fn is_write_handle(&self, fh: u64) -> bool {
        self.write_fhs.read().await.contains_key(&fh)
    }

    #[instrument(skip(self, buf))]
    async fn write(&self, ino: u64, offset: u64, buf: &[u8], handle: u64) -> FsResult<usize> {
        if !self.exists(ino) {
            return Err(FsError::InodeNotFound);
        }
        if !self.is_file(ino).await {
            return Err(FsError::InvalidInodeType);
        }
        if buf.is_empty() {
            // no-op
            return Ok(0);
        }
        let guard = self.meta.read().await;
        let attr = guard.get(&ino).unwrap();
        let mut fout = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .open(self.get_file_path(ino))?,
        );
        let len = {
            if offset > attr.size {
                fout.seek(SeekFrom::End(0))?;
                stream_util::fill_zeros(&mut fout, offset - attr.size)?;
                fout.write(buf)?
            } else {
                fout.seek(SeekFrom::Start(offset))?;
                fout.write(buf)?
            }
        };
        fout.flush()?;
        fout.into_inner()?.sync_all()?;
        File::open(self.data_dir.join(FILES_DIR))?.sync_all()?;
        let now = SystemTime::now();
        let guard = self.meta.read().await;
        let set_attr = SetFileAttr::default()
            .with_size(guard.get(&ino).unwrap().size.max(offset + len as u64))
            .with_mtime(now)
            .with_ctime(now)
            .with_atime(now);
        self.set_attr(ino, set_attr).await?;
        Ok(len)
    }

    async fn flush(&self, handle: u64) -> FsResult<()> {
        let ino = *self
            .write_fhs
            .read()
            .await
            .get(&handle)
            .ok_or(FsError::InvalidFileHandle)?;
        let now = SystemTime::now();
        let set_attr = SetFileAttr::default()
            .with_mtime(now)
            .with_ctime(now)
            .with_atime(now);
        self.set_attr(ino, set_attr).await?;
        Ok(())
    }

    async fn copy_file_range(
        &self,
        src_ino: u64,
        src_offset: u64,
        dest_ino: u64,
        dest_offset: u64,
        size: usize,
        src_fh: u64,
        dest_fh: u64,
    ) -> FsResult<usize> {
        if self.is_dir(src_ino).await || self.is_dir(dest_ino).await {
            return Err(FsError::InvalidInodeType);
        }

        let mut buf = vec![0; size];
        let len = self.read(src_ino, src_offset, &mut buf, src_fh).await?;
        if len == 0 {
            return Ok(0);
        }
        let mut copied = 0;
        while copied < size {
            let len = self
                .write(dest_ino, dest_offset, &buf[copied..len], dest_fh)
                .await?;
            if len == 0 && copied < size {
                error!(len, "Failed to copy all read bytes");
                return Err(FsError::Other("Failed to copy all read bytes"));
            }
            copied += len;
        }
        Ok(len)
    }

    async fn open(&self, ino: u64, read: bool, write: bool) -> FsResult<u64> {
        if !read && !write {
            return Err(FsError::InvalidInput(
                "read and write cannot be false at the same time",
            ));
        }
        if self.is_dir(ino).await {
            return Err(FsError::InvalidInodeType);
        }
        if !self.get_file_path(ino).exists() || self.is_file_outdated(ino).await? {
            self.download_file(ino).await?;
        }
        let fh = self.next_fh();
        if read {
            self.read_fhs.write().await.insert(fh, ino);
        }
        if write {
            self.write_fhs.write().await.insert(fh, ino);
        }

        Ok(fh)
    }

    async fn set_len(&self, ino: u64, size: u64) -> FsResult<()> {
        let attr = self.get_attr(ino).await?;
        if matches!(attr.kind, FileType::Directory) {
            return Err(FsError::InvalidInodeType);
        }

        if size == attr.size {
            // no-op
            return Ok(());
        }

        let file_path = self.get_file_path(ino);
        if size == 0 {
            debug!("truncate to zero");
            // truncate to zero
            let file = File::create(&file_path)?;
            file.set_len(0)?;
            file.sync_all()?;
        } else {
            debug!("truncate size to {}", size.to_formatted_string(&Locale::en));

            let mut file = fs_util::open_atomic_write(&file_path)?;
            {
                // have a new scope, so we drop the reader before moving new content files
                let mut reader = BufReader::new(File::open(&file_path)?);

                let mut writer = BufWriter::new(file);

                let len = if size > attr.size {
                    // increase size, copy existing data until existing size
                    attr.size
                } else {
                    // decrease size, copy existing data until new size
                    size
                };
                stream_util::copy_exact(&mut reader, &mut writer, len)?;
                if size > attr.size {
                    // increase size, seek to new size will write zeros
                    stream_util::fill_zeros(&mut writer, size - attr.size)?;
                }
                writer.flush()?;
                file = writer.into_inner()?;
            }
            file.commit()?;
        }
        File::open(file_path.parent().unwrap())?.sync_all()?;
        let file = File::create(&file_path)?;
        let rid = self.ids.read().unwrap().get_by_left(&ino).unwrap().clone();
        let (_, remote_file) = HUB
            .get()
            .as_ref()
            .unwrap()
            .files()
            .update(RemoteFile::default(), &rid)
            .param("fields", "size,modifiedTime,mimeType,md5Checksum)")
            .upload_resumable(
                file,
                self.ino_to_file
                    .read()
                    .await
                    .get(&ino)
                    .unwrap()
                    .read()
                    .await
                    .mime_type
                    .clone(),
            )
            .await?;
        merge_file(
            &mut *self
                .ino_to_file
                .read()
                .await
                .get(&ino)
                .unwrap()
                .write()
                .await,
            &remote_file,
        );
        merge_file_attr(self.meta.write().await.get_mut(&ino).unwrap(), &remote_file);

        let modified_type = remote_file.modified_time.as_ref().unwrap().clone().into();
        let set_attr = SetFileAttr::default()
            .with_size(size)
            .with_mtime(modified_type)
            .with_ctime(modified_type)
            .with_atime(modified_type);
        self.set_attr(ino, set_attr).await?;

        Ok(())
    }

    async fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
    ) -> FsResult<()> {
        if !self.exists(parent) {
            return Err(FsError::InodeNotFound);
        }
        if !self.is_dir(parent).await {
            return Err(FsError::InvalidInodeType);
        }
        if !self.exists(new_parent) {
            return Err(FsError::InodeNotFound);
        }
        if !self.is_dir(new_parent).await {
            return Err(FsError::InvalidInodeType);
        }
        if !self.exists_by_name(parent, name).await? {
            return Err(FsError::NotFound("name not found"));
        }

        if parent == new_parent && name == new_name {
            // no-op
            return Ok(());
        }

        // Only overwrite an existing directory if it's empty
        if let Ok(Some(new_attr)) = self.find_by_name(new_parent, new_name).await {
            if new_attr.kind == FileType::Directory && self.len(new_attr.ino).await? > 0 {
                return Err(FsError::NotEmpty);
            }
        }

        let mut attr = self
            .find_by_name(parent, name)
            .await?
            .ok_or(FsError::NotFound("name not found"))?;
        let mut new_attr = self.find_by_name(new_parent, new_name).await?;

        // delete new file from remote
        if let Some(new_attr) = new_attr {
            let rid = self
                .ids
                .read()
                .unwrap()
                .get_by_left(&new_attr.ino)
                .unwrap()
                .clone();
            let _ = HUB.get()
                .as_ref()
                .unwrap()
                .files()
                .delete(&rid)
                .doit()
                .await.map_err(|err| error!(%err, "Cannot delete new file from remote, maybe it doesn't exists"));
        }
        // move to new parent and save name on remote
        let remote_file = RemoteFile {
            name: Some(new_name.to_string()),
            mime_type: Some(
                self.ino_to_file
                    .read()
                    .await
                    .get(&attr.ino)
                    .unwrap()
                    .read()
                    .await
                    .mime_type
                    .to_string(),
            ),
            ..Default::default()
        };
        let parent_rid = self
            .ids
            .read()
            .unwrap()
            .get_by_left(&parent)
            .unwrap()
            .clone();
        let new_parent_rid = self
            .ids
            .read()
            .unwrap()
            .get_by_left(&new_parent)
            .unwrap()
            .clone();
        let rid = self
            .ids
            .read()
            .unwrap()
            .get_by_left(&attr.ino)
            .unwrap()
            .clone();
        let (_, remote_file) = HUB
            .get()
            .as_ref()
            .unwrap()
            .files()
            .update(remote_file, &rid)
            .remove_parents(&parent_rid)
            .add_parents(&new_parent_rid)
            .enforce_single_parent(true)
            .param("fields", "name,modifiedTime)")
            .doit_without_upload()
            .await?;

        // remove from parent
        let file = self
            .files
            .write()
            .await
            .get_mut(&parent)
            .unwrap()
            .remove(&attr.ino)
            .unwrap();
        // remove from new_parent, if exists
        if let Some(new_attr) = new_attr {
            self.files
                .write()
                .await
                .get_mut(&new_parent)
                .unwrap()
                .remove(&new_attr.ino);
        }
        // add to new parent
        self.files
            .write()
            .await
            .get_mut(&new_parent)
            .unwrap()
            .insert(attr.ino, file);

        let now = SystemTime::now();
        let set_attr = SetFileAttr::default()
            .with_mtime(now)
            .with_ctime(now)
            .with_atime(now);
        self.set_attr(parent, set_attr).await?;

        let set_attr = SetFileAttr::default()
            .with_mtime(now)
            .with_ctime(now)
            .with_atime(now);
        self.set_attr(new_parent, set_attr).await?;

        let modified_time = remote_file.modified_time.as_ref().unwrap().clone().into();
        let set_attr = SetFileAttr::default()
            .with_ctime(modified_time)
            .with_atime(modified_time);
        self.set_attr(attr.ino, set_attr).await?;

        Ok(())
    }
}

fn merge_attr(attr: &mut FileAttr, set_attr: &SetFileAttr) {
    if let Some(size) = set_attr.size {
        attr.size = size;
    }
    if let Some(atime) = set_attr.atime {
        attr.atime = max(atime, attr.atime);
    }
    if let Some(mtime) = set_attr.mtime {
        attr.mtime = max(mtime, attr.mtime);
    }
    if let Some(ctime) = set_attr.ctime {
        attr.ctime = max(ctime, attr.ctime);
    }
    if let Some(crtime) = set_attr.crtime {
        attr.crtime = max(crtime, attr.crtime);
    }
    if let Some(perm) = set_attr.perm {
        attr.perm = perm;
    }
    if let Some(uid) = set_attr.uid {
        attr.uid = uid;
    }
    if let Some(gid) = set_attr.gid {
        attr.gid = gid;
    }
    if let Some(flags) = set_attr.flags {
        attr.flags = flags;
    }
}

async fn setup_api() -> FsResult<()> {
    info!("Setting up API");

    // Get an ApplicationSecret instance by some means. It contains the `client_id` and
    // `client_secret`, among other things.
    let secrets_josn = include_str!("../../client-secret.json");
    let secret: oauth2::ApplicationSecret = serde_json::from_str(secrets_josn)?;
    // Instantiate the authenticator. It will choose a suitable authentication flow for you,
    // unless you replace `None` with the desired Flow.
    // Provide your own `AuthenticatorDelegate` to adjust the way it operates and get feedback about
    // what's going on. You probably want to bring in your own `TokenStorage` to persist tokens and
    // retrieve them from storage.
    let auth = oauth2::InstalledFlowAuthenticator::builder(
        secret,
        oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
        .persist_tokens_to_disk("token_store.json")
        .build()
        .await
        .unwrap();
    HUB.get_or_init(|| {
        DriveHub::new(
            hyper::Client::builder().build(
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_or_http()
                    .enable_http1()
                    .build(),
            ),
            auth,
        )
    });
    info!("API setup done");

    Ok(())
}

fn merge_file(lhs: &mut LocalFile, rhs: &RemoteFile) {
    if let Some(name) = rhs.name.as_ref() {
        lhs.name = name.clone();
    }
    if let Some(mime_type) = rhs.mime_type.as_ref() {
        lhs.mime_type = Mime::from_str(mime_type).unwrap();
    }
    if let Some(md5_checksum) = rhs.md5_checksum.as_ref() {
        lhs.remote_md5_checksum = md5_checksum.clone();
    }
}

fn merge_file_attr(attr: &mut FileAttr, rhs: &RemoteFile) {
    let remote_attr = SetFileAttr {
        size: rhs.size.map(|s| s as u64),
        mtime: rhs
            .modified_time
            .map(|t| <DateTime<Utc> as Into<SystemTime>>::into(t.clone())),
        ctime: rhs
            .modified_time
            .map(|t| <DateTime<Utc> as Into<SystemTime>>::into(t.clone())),
        atime: rhs
            .modified_time
            .map(|t| <DateTime<Utc> as Into<SystemTime>>::into(t.clone())),
        ..Default::default()
    };
    merge_attr(attr, &remote_attr);
}

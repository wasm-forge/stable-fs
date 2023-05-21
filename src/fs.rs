use crate::{
    error::Error,
    runtime::{
        dir::Dir,
        fd::{FdEntry, FdTable},
        file::File,
    },
    storage::{
        types::{DirEntry, DirEntryIndex, FileSize, FileType, Metadata, Node},
        Storage,
    },
};

pub use crate::runtime::fd::Fd;

pub use crate::runtime::types::{
    DstBuf, DstIoVec, FdFlags, FdStat, OpenFlags, SrcBuf, SrcIoVec, Whence,
};

pub struct FileSystem {
    root_fd: Fd,
    fd_table: FdTable,
    storage: Box<dyn Storage>,
}

impl FileSystem {
    pub fn new(storage: Box<dyn Storage>) -> Result<Self, Error> {
        let mut fd_table = FdTable::new();

        let root_node = storage.root_node();
        let root_entry = Dir::new(root_node, FdStat::default(), &*storage)?;
        let root_fd = fd_table.open(FdEntry::Dir(root_entry));

        Ok(Self {
            root_fd,
            fd_table,
            storage,
        })
    }

    pub fn root_fd(&self) -> Fd {
        self.root_fd
    }

    pub fn root_path(&self) -> &str {
        "/"
    }

    pub fn renumber(&mut self, from: Fd, to: Fd) -> Result<(), Error> {
        self.fd_table.renumber(from, to)
    }

    fn get_node(&self, fd: Fd) -> Result<Node, Error> {
        match self.fd_table.get(fd) {
            Some(FdEntry::File(file)) => Ok(file.node),
            Some(FdEntry::Dir(dir)) => Ok(dir.node),
            None => Err(Error::NotFound),
        }
    }

    fn get_file(&self, fd: Fd) -> Result<File, Error> {
        match self.fd_table.get(fd) {
            Some(FdEntry::File(file)) => Ok(file.clone()),
            Some(FdEntry::Dir(_)) => Err(Error::InvalidFileType),
            None => Err(Error::NotFound),
        }
    }

    fn put_file(&mut self, fd: Fd, file: File) {
        self.fd_table.update(fd, FdEntry::File(file))
    }

    fn get_dir(&self, fd: Fd) -> Result<Dir, Error> {
        match self.fd_table.get(fd) {
            Some(FdEntry::Dir(dir)) => Ok(dir.clone()),
            Some(FdEntry::File(_)) => Err(Error::InvalidFileType),
            None => Err(Error::NotFound),
        }
    }

    pub fn get_direntry(&self, fd: Fd, index: DirEntryIndex) -> Result<DirEntry, Error> {
        self.get_dir(fd)?.get_entry(index, self.storage.as_ref())
    }

    fn put_dir(&mut self, fd: Fd, dir: Dir) {
        self.fd_table.update(fd, FdEntry::Dir(dir))
    }

    pub fn read(&mut self, fd: Fd, dst: &mut [u8]) -> Result<FileSize, Error> {
        let mut file = self.get_file(fd)?;
        let read_size = file.read_with_cursor(dst, self.storage.as_mut())?;
        self.put_file(fd, file);
        Ok(read_size)
    }

    pub fn write(&mut self, fd: Fd, src: &[u8]) -> Result<FileSize, Error> {
        let mut file = self.get_file(fd)?;
        let written_size = file.write_with_cursor(src, self.storage.as_mut())?;
        self.put_file(fd, file);
        Ok(written_size)
    }

    pub fn read_vec(&mut self, fd: Fd, dst: DstIoVec) -> Result<FileSize, Error> {
        let mut file = self.get_file(fd)?;
        let mut read_size = 0;
        for buf in dst {
            let buf = unsafe { std::slice::from_raw_parts_mut(buf.buf, buf.len) };
            let size = file.read_with_cursor(buf, self.storage.as_mut())?;
            read_size += size;
        }
        self.put_file(fd, file);
        Ok(read_size)
    }

    pub fn read_vec_with_offset(
        &mut self,
        fd: Fd,
        dst: DstIoVec,
        offset: FileSize,
    ) -> Result<FileSize, Error> {
        let file = self.get_file(fd)?;
        let mut read_size = 0;
        for buf in dst {
            let buf = unsafe { std::slice::from_raw_parts_mut(buf.buf, buf.len) };
            let size = file.read_with_offset(offset, buf, self.storage.as_mut())?;
            read_size += size;
        }
        self.put_file(fd, file);
        Ok(read_size)
    }

    pub fn write_vec(&mut self, fd: Fd, src: SrcIoVec) -> Result<FileSize, Error> {
        let mut file = self.get_file(fd)?;
        let mut written_size = 0;
        for buf in src {
            let buf = unsafe { std::slice::from_raw_parts(buf.buf, buf.len) };
            let size = file.write_with_cursor(buf, self.storage.as_mut())?;
            written_size += size;
        }
        self.put_file(fd, file);
        Ok(written_size)
    }

    pub fn write_vec_with_offset(
        &mut self,
        fd: Fd,
        src: SrcIoVec,
        offset: FileSize,
    ) -> Result<FileSize, Error> {
        let file = self.get_file(fd)?;
        let mut written_size = 0;
        for buf in src {
            let buf = unsafe { std::slice::from_raw_parts(buf.buf, buf.len) };
            let size = file.write_with_offset(offset, buf, self.storage.as_mut())?;
            written_size += size;
        }
        self.put_file(fd, file);
        Ok(written_size)
    }

    pub fn seek(&mut self, fd: Fd, delta: i64, whence: Whence) -> Result<FileSize, Error> {
        let mut file = self.get_file(fd)?;
        let pos = file.seek(delta, whence, self.storage.as_mut())?;
        self.put_file(fd, file);
        Ok(pos)
    }

    pub fn tell(&mut self, fd: Fd) -> Result<FileSize, Error> {
        let file = self.get_file(fd)?;
        let pos = file.tell();
        Ok(pos)
    }

    pub fn close(&mut self, fd: Fd) -> Result<(), Error> {

        self.fd_table.close(fd).ok_or(Error::NotFound)?;

        Ok(())
    }

    pub fn metadata(&self, fd: Fd) -> Result<Metadata, Error> {
        let node = self.get_node(fd)?;
        self.storage.get_metadata(node)
    }

    pub fn metadata_from_node(&self, node: Node) -> Result<Metadata, Error> {
        self.storage.get_metadata(node)
    }

    pub fn set_metadata(&mut self, fd: Fd, metadata: Metadata) -> Result<(), Error> {
        let node = self.get_node(fd)?;
        self.storage.put_metadata(node, metadata);

        Ok(())
    }

    pub fn set_accessed_time(&mut self, fd: Fd, time: u64) -> Result<(), Error> {
        let node = self.get_node(fd)?;
        let mut metadata = self.storage.get_metadata(node)?;

        metadata.times.accessed = time;

        self.storage.put_metadata(node, metadata);

        Ok(())
    }

    pub fn set_modified_time(&mut self, fd: Fd, time: u64) -> Result<(), Error> {
        let node = self.get_node(fd)?;
        let mut metadata = self.storage.get_metadata(node)?;

        metadata.times.modified = time;

        self.storage.put_metadata(node, metadata);

        Ok(())
    }

    pub fn get_stat(&self, fd: Fd) -> Result<(FileType, FdStat), Error> {
        match self.fd_table.get(fd) {
            None => Err(Error::NotFound),
            Some(FdEntry::File(file)) => Ok((FileType::RegularFile, file.stat)),
            Some(FdEntry::Dir(dir)) => Ok((FileType::Directory, dir.stat)),
        }
    }

    pub fn set_stat(&mut self, fd: Fd, stat: FdStat) -> Result<(), Error> {
        match self.fd_table.get(fd) {
            Some(FdEntry::File(file)) => {
                let mut file = file.clone();
                file.stat = stat;
                self.put_file(fd, file);
                Ok(())
            }
            Some(FdEntry::Dir(dir)) => {
                let mut dir = dir.clone();
                dir.stat = stat;
                self.put_dir(fd, dir);
                Ok(())
            }
            None => Err(Error::NotFound),
        }
    }

    pub fn open_metadata(&self, parent: Fd, path: &str) -> Result<Metadata, Error> {
        let dir = self.get_dir(parent)?;
        let node = dir.find_node(path, self.storage.as_ref())?;
        self.storage.get_metadata(node)
    }

    pub fn open_or_create(
        &mut self,
        parent: Fd,
        path: &str,
        stat: FdStat,
        flags: OpenFlags,
        ctime: u64
    ) -> Result<Fd, Error> {
        let dir = self.get_dir(parent)?;

        match dir.find_node(path, self.storage.as_ref()) {
            Ok(node) => self.open(node, stat, flags),
            Err(Error::NotFound) => {
                if !flags.contains(OpenFlags::CREATE) {
                    return Err(Error::NotFound);
                }
                if flags.contains(OpenFlags::DIRECTORY) {
                    return Err(Error::InvalidFileType);
                }
                self.create_file(parent, path, stat, ctime)
            }
            Err(err) => Err(err),
        }
    }

    pub fn open(&mut self, node: Node, stat: FdStat, flags: OpenFlags) -> Result<Fd, Error> {
        if flags.contains(OpenFlags::EXCLUSIVE) {
            return Err(Error::FileAlreadyExists);
        }
        let metadata = self.storage.get_metadata(node)?;
        match metadata.file_type {
            FileType::Directory => {
                let dir = Dir::new(node, stat, self.storage.as_mut())?;
                let fd = self.fd_table.open(FdEntry::Dir(dir));
                Ok(fd)
            }
            FileType::RegularFile => {
                if flags.contains(OpenFlags::DIRECTORY) {
                    return Err(Error::InvalidFileType);
                }
                let file = File::new(node, stat, self.storage.as_mut())?;
                if flags.contains(OpenFlags::TRUNCATE) {
                    file.truncate(self.storage.as_mut())?;
                }
                let fd = self.fd_table.open(FdEntry::File(file));
                Ok(fd)
            }
            FileType::SymbolicLink => unimplemented!("Symbolic links are not supported yet"),
        }
    }

    pub fn create_file(&mut self, parent: Fd, path: &str, stat: FdStat, ctime: u64) -> Result<Fd, Error> {
        let dir = self.get_dir(parent)?;

        let child = dir.create_file(path, stat, self.storage.as_mut(), ctime)?;

        let child_fd = self.fd_table.open(FdEntry::File(child));
        self.put_dir(parent, dir);
        Ok(child_fd)
    }

    pub fn remove_file(&mut self, parent: Fd, path: &str) -> Result<(), Error> {
        let dir = self.get_dir(parent)?;
        dir.remove_file(path, self.fd_table.node_refcount(), self.storage.as_mut())
    }

    pub fn create_dir(&mut self, parent: Fd, path: &str, stat: FdStat, ctime: u64) -> Result<Fd, Error> {
        let dir = self.get_dir(parent)?;
        let child = dir.create_dir(path, stat, self.storage.as_mut(), ctime)?;
        let child_fd = self.fd_table.open(FdEntry::Dir(child));
        self.put_dir(parent, dir);
        Ok(child_fd)
    }

    pub fn remove_dir(&mut self, parent: Fd, path: &str) -> Result<(), Error> {
        let dir = self.get_dir(parent)?;
        dir.remove_dir(path, self.fd_table.node_refcount(), self.storage.as_mut())
    }

    #[cfg(test)]
    pub(crate) fn get_test_storage(&mut self) -> &mut dyn Storage {
        self.storage.as_mut()
    }

    #[cfg(test)]
    pub(crate) fn get_test_file(&self, fd: Fd) -> File {
        self.get_file(fd).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Error,
        runtime::types::{FdStat, OpenFlags},
        test_utils::test_fs,
    };

    #[test]
    fn create_file() {
        let mut fs = test_fs();

        let file = fs
            .create_file(fs.root_fd(), "test.txt", Default::default(), 0)
            .unwrap();

        assert!(file > fs.root_fd());
    }

    #[test]
    fn create_dir() {
        let mut fs = test_fs();

        let dir = fs
            .create_dir(fs.root_fd(), "test", FdStat::default(), 0)
            .unwrap();

        let fd = fs.create_file(dir, "file.txt", FdStat::default(), 0).unwrap();
        fs.write(fd, "Hello, world!".as_bytes()).unwrap();

        let dir = fs
            .open_or_create(fs.root_fd(), "test", FdStat::default(), OpenFlags::empty(), 0)
            .unwrap();

        let fd = fs
            .open_or_create(dir, "file.txt", FdStat::default(), OpenFlags::empty(), 0)
            .unwrap();

        let mut buf = [0; 13];
        fs.read(fd, &mut buf).unwrap();
        assert_eq!(&buf, "Hello, world!".as_bytes());
    }

    #[test]
    fn create_file_creates_a_few_files() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        fs.create_file(dir, "test1.txt", FdStat::default(), 0).unwrap();
        fs.create_file(dir, "test2.txt", FdStat::default(), 0).unwrap();
        fs.create_file(dir, "test3.txt", FdStat::default(), 0).unwrap();

        let meta = fs.metadata(fs.root_fd()).unwrap();

        let entry_index = meta.first_dir_entry.unwrap();

        let entry1 = fs.get_direntry(fs.root_fd(), entry_index).unwrap();
        let entry2 = fs.get_direntry(fs.root_fd(), entry_index + 1).unwrap();
        let entry3 = fs.get_direntry(fs.root_fd(), entry_index + 2).unwrap();

        assert_eq!(entry1.prev_entry, None);
        assert_eq!(entry1.next_entry, Some(entry_index + 1));

        assert_eq!(entry2.prev_entry, Some(entry_index));
        assert_eq!(entry2.next_entry, Some(entry_index + 2));

        assert_eq!(entry3.prev_entry, Some(entry_index + 1));
        assert_eq!(entry3.next_entry, None);
    }

    #[test]
    fn close_file_fd_reused() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        fs.create_file(dir, "test1.txt", FdStat::default(), 0).unwrap();
        let fd2 = fs.create_file(dir, "test2.txt", FdStat::default(), 0).unwrap();
        fs.create_file(dir, "test3.txt", FdStat::default(), 0).unwrap();

        fs.close(fd2).unwrap();

        let fd4 = fs.create_file(dir, "test4.txt", FdStat::default(), 0).unwrap();

        assert_eq!(fd2, fd4);
    }

    #[test]
    fn fd_renumber() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let fd1 = fs.create_file(dir, "test1.txt", FdStat::default(), 0).unwrap();
        let fd2 = fs.create_file(dir, "test2.txt", FdStat::default(), 0).unwrap();

        let entry1 = fs.get_node(fd1);

        fs.renumber(fd1, fd2).unwrap();

        assert!(fs.get_node(fd1).is_err());
        assert_eq!(fs.get_node(fd2), entry1);
    }

    #[test]
    fn seek_and_write() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let fd = fs
            .open_or_create(dir, "test.txt", FdStat::default(), OpenFlags::CREATE, 0)
            .unwrap();

        fs.seek(fd, 24, super::Whence::SET).unwrap();

        fs.write(fd, &[1, 2, 3, 4, 5]).unwrap();

        let meta = fs.metadata(fd).unwrap();

        assert_eq!(meta.size, 29);

        fs.seek(fd, 0, crate::fs::Whence::SET).unwrap();
        let mut buf = [42u8; 29];
        let rr = fs.read(fd, &mut buf).unwrap();
        assert_eq!(rr, 29);
        assert_eq!(
            buf,
            [
                0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3,
                4, 5
            ]
        );

        fs.close(fd).unwrap();

        let fd = fs
            .open_or_create(dir, "test.txt", FdStat::default(), OpenFlags::CREATE, 0)
            .unwrap();

        let mut buf = [42u8; 29];
        let rr = fs.read(fd, &mut buf).unwrap();
        assert_eq!(rr, 29);
        assert_eq!(
            buf,
            [
                0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3,
                4, 5
            ]
        );

        fs.close(fd).unwrap();
    }

    #[test]
    fn create_and_remove_file() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let fd = fs
            .open_or_create(dir, "test.txt", FdStat::default(), OpenFlags::CREATE, 0)
            .unwrap();

        fs.write(fd, &[1, 2, 3, 4, 5]).unwrap();
        fs.close(fd).unwrap();

        fs.remove_file(dir, "test.txt").unwrap();

        let err = fs
            .open_or_create(dir, "test.txt", FdStat::default(), OpenFlags::empty(), 0)
            .unwrap_err();
        assert_eq!(err, Error::NotFound);
    }

    #[test]
    fn cannot_remove_opened_file() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let fd = fs
            .open_or_create(dir, "test.txt", FdStat::default(), OpenFlags::CREATE, 0)
            .unwrap();

        fs.write(fd, &[1, 2, 3, 4, 5]).unwrap();

        let err = fs.remove_file(dir, "test.txt").unwrap_err();
        assert_eq!(err, Error::CannotRemoveOpenedNode);

        fs.close(fd).unwrap();
        fs.remove_file(dir, "test.txt").unwrap();
    }

    #[test]
    fn cannot_remove_directory_as_file() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let fd = fs.create_dir(dir, "test", FdStat::default(), 0).unwrap();
        fs.close(fd).unwrap();

        let err = fs.remove_file(dir, "test").unwrap_err();
        assert_eq!(err, Error::ExpectedToRemoveFile);

        fs.remove_dir(dir, "test").unwrap();
    }

    #[test]
    fn cannot_remove_file_as_directory() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let fd = fs
            .open_or_create(dir, "test.txt", FdStat::default(), OpenFlags::CREATE, 0)
            .unwrap();
        fs.close(fd).unwrap();

        let err = fs.remove_dir(dir, "test.txt").unwrap_err();
        assert_eq!(err, Error::ExpectedToRemoveDirectory);

        fs.remove_file(dir, "test.txt").unwrap();
    }
}

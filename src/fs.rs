use ic_stable_structures::Memory;

use crate::{
    error::Error,
    filename_cache::FilenameCache,
    runtime::{
        dir::Dir,
        fd::{FdEntry, FdTable},
        file::File,
        structure_helpers::{create_hard_link, find_node, rm_dir_entry},
    },
    storage::{
        types::{DirEntry, DirEntryIndex, FileType, Metadata, Node},
        Storage,
    },
};

pub use crate::runtime::fd::Fd;

pub use crate::runtime::types::{
    ChunkSize, ChunkType, DstBuf, DstIoVec, FdFlags, FdStat, OpenFlags, SrcBuf, SrcIoVec, Whence,
};
pub use crate::storage::types::FileSize;

// The main class implementing the API to work with the file system.
pub struct FileSystem {
    root_fd: Fd,
    fd_table: FdTable,
    names_cache: FilenameCache,
    pub storage: Box<dyn Storage>,
}

impl FileSystem {
    // Create a new file system hosted on a given storage implementation.
    pub fn new(storage: Box<dyn Storage>) -> Result<Self, Error> {
        let mut fd_table = FdTable::new();

        if storage.get_version() == 0 {
            return Ok(Self {
                root_fd: 0,
                fd_table,
                names_cache: FilenameCache::new(),
                storage,
            });
        }

        let root_node = storage.root_node();
        let root_entry = Dir::new(root_node, FdStat::default(), &*storage)?;
        let root_fd = fd_table.open(FdEntry::Dir(root_entry));
        let names_cache = FilenameCache::new();

        Ok(Self {
            root_fd,
            fd_table,
            names_cache,
            storage,
        })
    }

    pub fn flush(&mut self, fd: Fd) -> Result<(), Error> {
        let node = self.get_node(fd)?;
        self.storage.flush(node);

        Ok(())
    }

    //
    pub fn get_storage_version(&self) -> u32 {
        self.storage.get_version()
    }

    // Get the file descriptor of the root folder.
    pub fn root_fd(&self) -> Fd {
        self.root_fd
    }

    // Get the path of the root folder.
    pub fn root_path(&self) -> &str {
        "/"
    }

    // Reassign a file descriptor to a new number, the source descriptor is closed in the process.
    // If the destination descriptor is busy, it is closed in the process.
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

    // mount memory on the top of the given host file name, if the file does not exist, it will be created.
    // The method fails if the file system could not open or create the file.
    pub fn mount_memory_file(
        &mut self,
        filename: &str,
        memory: Box<dyn Memory>,
    ) -> Result<(), Error> {
        // create a file for the mount
        let fd = self.open_or_create(
            self.root_fd,
            filename,
            FdStat::default(),
            OpenFlags::CREATE,
            0,
        )?;

        let node = self.get_node(fd)?;
        self.close(fd)?;

        self.storage.mount_node(node, memory)?;

        Ok(())
    }

    // initialize mounted memory with the data stored in the host file
    pub fn init_memory_file(&mut self, filename: &str) -> Result<(), Error> {
        // create a file for the mount
        let fd = self.open_or_create(
            self.root_fd,
            filename,
            FdStat::default(),
            OpenFlags::empty(),
            0,
        )?;

        let node = self.get_node(fd)?;
        self.close(fd)?;

        self.storage.init_mounted_memory(node)
    }

    // store content of the currently active memory file to the file system
    pub fn store_memory_file(&mut self, filename: &str) -> Result<(), Error> {
        // create a file for the mount
        let fd = self.open_or_create(
            self.root_fd,
            filename,
            FdStat::default(),
            OpenFlags::empty(),
            0,
        )?;

        let node = self.get_node(fd)?;
        self.close(fd)?;

        self.storage.store_mounted_memory(node)
    }

    // Unmount memory, the system will continue to work with the file in normal mode.
    pub fn unmount_memory_file(&mut self, filename: &str) -> Result<Box<dyn Memory>, Error> {
        // create a file for the mount
        let fd = self.open_or_create(
            self.root_fd,
            filename,
            FdStat::default(),
            OpenFlags::empty(),
            0,
        )?;

        let node = self.get_node(fd)?;
        self.close(fd)?;

        let memory = self.storage.unmount_node(node)?;

        Ok(memory)
    }

    // Get directory entry for a given directory file descriptor and the entry index.
    pub fn get_direntry(&self, fd: Fd, index: DirEntryIndex) -> Result<DirEntry, Error> {
        self.get_dir(fd)?.get_entry(index, self.storage.as_ref())
    }

    fn put_dir(&mut self, fd: Fd, dir: Dir) {
        self.fd_table.update(fd, FdEntry::Dir(dir))
    }

    // Read file's `fd` contents into `dst`.
    pub fn read(&mut self, fd: Fd, dst: &mut [u8]) -> Result<FileSize, Error> {
        let mut file = self.get_file(fd)?;
        let read_size = file.read_with_cursor(dst, self.storage.as_mut())?;
        self.put_file(fd, file);
        Ok(read_size)
    }

    // Write `src` contents into a file.
    pub fn write(&mut self, fd: Fd, src: &[u8]) -> Result<FileSize, Error> {
        let mut file = self.get_file(fd)?;
        let written_size = file.write_with_cursor(src, self.storage.as_mut())?;
        self.put_file(fd, file);
        Ok(written_size)
    }

    // Read file into a vector of buffers.
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

    // Read file into a vector of buffers at a given offset, the file cursor is NOT updated.
    pub fn read_vec_with_offset(
        &mut self,
        fd: Fd,
        dst: DstIoVec,
        offset: FileSize,
    ) -> Result<FileSize, Error> {
        let file = self.get_file(fd)?;
        let mut read_size = 0;

        for buf in dst {
            let rbuf = unsafe { std::slice::from_raw_parts_mut(buf.buf, buf.len) };

            let size = file.read_with_offset(read_size + offset, rbuf, self.storage.as_mut())?;

            read_size += size;
        }
        self.put_file(fd, file);
        Ok(read_size)
    }

    // Write a vector of buffers into a file at a given offset, the file cursor is updated.
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

    // Write a vector of buffers into a file at a given offset, the file cursor is NOT updated.
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

            let size = file.write_with_offset(written_size + offset, buf, self.storage.as_mut())?;

            written_size += size;
        }
        self.put_file(fd, file);
        Ok(written_size)
    }

    // Position file cursor to a given position.
    pub fn seek(&mut self, fd: Fd, delta: i64, whence: Whence) -> Result<FileSize, Error> {
        let mut file = self.get_file(fd)?;
        let pos = file.seek(delta, whence, self.storage.as_mut())?;
        self.put_file(fd, file);
        Ok(pos)
    }

    // Get the current file cursor position.
    pub fn tell(&mut self, fd: Fd) -> Result<FileSize, Error> {
        let file = self.get_file(fd)?;
        let pos = file.tell();
        Ok(pos)
    }

    // Close the opened file and release the corresponding file descriptor.
    pub fn close(&mut self, fd: Fd) -> Result<(), Error> {
        self.flush(fd)?;
        self.fd_table.close(fd).ok_or(Error::NotFound)?;

        Ok(())
    }

    // Get the metadata for a given file descriptor
    pub fn metadata_from_node(&self, node: Node) -> Result<Metadata, Error> {
        self.storage.get_metadata(node)
    }

    // Get the metadata for a given file descriptor
    pub fn metadata(&self, fd: Fd) -> Result<Metadata, Error> {
        let node = self.get_node(fd)?;
        self.storage.get_metadata(node)
    }

    // update metadata of a given file descriptor
    pub fn set_metadata(&mut self, fd: Fd, metadata: Metadata) -> Result<(), Error> {
        let node = self.get_node(fd)?;
        self.storage.put_metadata(node, metadata);

        Ok(())
    }

    // Update access time.
    pub fn set_accessed_time(&mut self, fd: Fd, time: u64) -> Result<(), Error> {
        let node = self.get_node(fd)?;
        let mut metadata = self.storage.get_metadata(node)?;

        metadata.times.accessed = time;

        self.storage.put_metadata(node, metadata);

        Ok(())
    }

    // Update modification time.
    pub fn set_modified_time(&mut self, fd: Fd, time: u64) -> Result<(), Error> {
        let node = self.get_node(fd)?;
        let mut metadata = self.storage.get_metadata(node)?;

        metadata.times.modified = time;

        self.storage.put_metadata(node, metadata);

        Ok(())
    }

    // Get file or directory stats.
    pub fn get_stat(&self, fd: Fd) -> Result<(FileType, FdStat), Error> {
        match self.fd_table.get(fd) {
            None => Err(Error::NotFound),
            Some(FdEntry::File(file)) => Ok((FileType::RegularFile, file.stat)),
            Some(FdEntry::Dir(dir)) => Ok((FileType::Directory, dir.stat)),
        }
    }

    // Update stats of a given file.
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

    // Get metadata of a file with name `path` in a given folder.
    pub fn open_metadata(&mut self, parent: Fd, path: &str) -> Result<Metadata, Error> {
        let dir = self.get_dir(parent)?;
        let node = find_node(dir.node, path, &mut self.names_cache, self.storage.as_ref())?;
        self.storage.get_metadata(node)
    }

    // Opens of creates a new file.
    pub fn open_or_create(
        &mut self,
        parent: Fd,
        path: &str,
        stat: FdStat,
        flags: OpenFlags,
        ctime: u64,
    ) -> Result<Fd, Error> {
        let dir = self.get_dir(parent)?;

        match find_node(dir.node, path, &mut self.names_cache, self.storage.as_ref()) {
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

    // Opens a file and returns its new file descriptor.
    fn open(&mut self, node: Node, stat: FdStat, flags: OpenFlags) -> Result<Fd, Error> {
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

    // Create a new file named `path` in the given `parent` folder.
    pub fn create_file(
        &mut self,
        parent: Fd,
        path: &str,
        stat: FdStat,
        ctime: u64,
    ) -> Result<Fd, Error> {
        let dir = self.get_dir(parent)?;

        let child = dir.create_file(
            path,
            stat,
            &mut self.names_cache,
            self.storage.as_mut(),
            ctime,
        )?;

        let child_fd = self.fd_table.open(FdEntry::File(child));
        self.put_dir(parent, dir);
        Ok(child_fd)
    }

    // Delete a file by name `path` in the given file folder.
    pub fn remove_file(&mut self, parent: Fd, path: &str) -> Result<(), Error> {
        let dir = self.get_dir(parent)?;
        dir.remove_file(
            path,
            self.fd_table.node_refcount(),
            &mut self.names_cache,
            self.storage.as_mut(),
        )
    }

    // Create a new directory named `path` in the given `parent` folder.
    pub fn create_dir(
        &mut self,
        parent: Fd,
        path: &str,
        stat: FdStat,
        ctime: u64,
    ) -> Result<Fd, Error> {
        let dir = self.get_dir(parent)?;
        let child = dir.create_dir(
            path,
            stat,
            &mut self.names_cache,
            self.storage.as_mut(),
            ctime,
        )?;
        let child_fd = self.fd_table.open(FdEntry::Dir(child));
        self.put_dir(parent, dir);
        Ok(child_fd)
    }

    // Delete a directory by name `path` in the given file folder.
    pub fn remove_dir(&mut self, parent: Fd, path: &str) -> Result<(), Error> {
        let dir = self.get_dir(parent)?;
        dir.remove_dir(
            path,
            self.fd_table.node_refcount(),
            &mut self.names_cache,
            self.storage.as_mut(),
        )
    }

    // Create a hard link to an existing file.
    pub fn create_hard_link(
        &mut self,
        old_fd: Fd,
        old_path: &str,
        new_fd: Fd,
        new_path: &str,
    ) -> Result<Fd, Error> {
        let src_dir = self.get_dir(old_fd)?;
        let dst_dir = self.get_dir(new_fd)?;

        create_hard_link(
            dst_dir.node,
            new_path,
            src_dir.node,
            old_path,
            false,
            &mut self.names_cache,
            self.storage.as_mut(),
        )?;

        let node = find_node(
            dst_dir.node,
            new_path,
            &mut self.names_cache,
            self.storage.as_ref(),
        )?;

        self.open(node, FdStat::default(), OpenFlags::empty())
    }

    // Rename a file.
    pub fn rename(
        &mut self,
        old_fd: Fd,
        old_path: &str,
        new_fd: Fd,
        new_path: &str,
    ) -> Result<Fd, Error> {
        let src_dir = self.get_dir(old_fd)?;
        let dst_dir = self.get_dir(new_fd)?;

        // create a new link
        create_hard_link(
            dst_dir.node,
            new_path,
            src_dir.node,
            old_path,
            true,
            &mut self.names_cache,
            self.storage.as_mut(),
        )?;

        // now unlink the older version
        let (node, _metadata) = rm_dir_entry(
            src_dir.node,
            old_path,
            None,
            self.fd_table.node_refcount(),
            &mut self.names_cache,
            self.storage.as_mut(),
        )?;

        self.open(node, FdStat::default(), OpenFlags::empty())
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

    use ic_stable_structures::memory_manager::{MemoryId, MemoryManager};
    use ic_stable_structures::{Memory, VectorMemory};

    use crate::test_utils::write_text_at_offset;
    use crate::{
        error::Error,
        fs::{DstBuf, FdFlags, SrcBuf},
        runtime::{
            structure_helpers::find_node,
            types::{FdStat, OpenFlags},
        },
        storage::{
            stable::StableStorage,
            types::{FileSize, FileType},
        },
        test_utils::{
            new_vector_memory, read_text_file, test_fs, test_fs_setups, test_fs_transient,
            write_text_fd, write_text_file,
        },
    };

    use super::{Fd, FileSystem};

    pub fn list_files(fs: &mut FileSystem, path: &str) -> Vec<String> {
        use std::str::FromStr;

        let mut res = vec![];

        let dir = fs.root_fd();

        let fd = fs
            .open_or_create(dir, path, FdStat::default(), OpenFlags::DIRECTORY, 0)
            .unwrap();

        let meta = fs.metadata(fd).unwrap();

        let mut entry_index = meta.first_dir_entry;

        while let Some(index) = entry_index {
            let entry = fs.get_direntry(fd, index).unwrap();

            let filename_str: &str =
                std::str::from_utf8(&entry.name.bytes[0..(entry.name.length as usize)]).unwrap();

            let st = String::from_str(filename_str).unwrap();

            res.push(st);

            entry_index = entry.next_entry;
        }

        res
    }

    #[test]
    fn get_root_info() {
        let fs = test_fs();

        let fd = fs.root_fd();
        let path = fs.root_path();

        assert!(fd == 3);
        assert!(path == "/");
    }

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

        let fd = fs
            .create_file(dir, "file.txt", FdStat::default(), 0)
            .unwrap();
        fs.write(fd, "Hello, world!".as_bytes()).unwrap();

        let dir = fs
            .open_or_create(
                fs.root_fd(),
                "test",
                FdStat::default(),
                OpenFlags::empty(),
                0,
            )
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

        fs.create_file(dir, "test1.txt", FdStat::default(), 0)
            .unwrap();
        fs.create_file(dir, "test2.txt", FdStat::default(), 0)
            .unwrap();
        fs.create_file(dir, "test3.txt", FdStat::default(), 0)
            .unwrap();

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

        fs.create_file(dir, "test1.txt", FdStat::default(), 0)
            .unwrap();
        let fd2 = fs
            .create_file(dir, "test2.txt", FdStat::default(), 0)
            .unwrap();
        fs.create_file(dir, "test3.txt", FdStat::default(), 0)
            .unwrap();

        fs.close(fd2).unwrap();

        let fd4 = fs
            .create_file(dir, "test4.txt", FdStat::default(), 0)
            .unwrap();

        assert_eq!(fd2, fd4);
    }

    #[test]
    fn fd_renumber() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let fd1 = fs
            .create_file(dir, "test1.txt", FdStat::default(), 0)
            .unwrap();
        let fd2 = fs
            .create_file(dir, "test2.txt", FdStat::default(), 0)
            .unwrap();

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
    fn read_and_write_vec() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let write_content1 = "This is a sample file content.";
        let write_content2 = "1234567890";

        let write_content = [
            SrcBuf {
                buf: write_content1.as_ptr(),
                len: write_content1.len(),
            },
            SrcBuf {
                buf: write_content2.as_ptr(),
                len: write_content2.len(),
            },
        ];

        let fd = fs
            .open_or_create(dir, "test.txt", FdStat::default(), OpenFlags::CREATE, 0)
            .unwrap();

        fs.write_vec(fd, write_content.as_ref()).unwrap();

        let meta = fs.metadata(fd).unwrap();
        assert_eq!(meta.size, 40);

        fs.seek(fd, 0, crate::fs::Whence::SET).unwrap();

        let mut read_content1 = String::from("......................");
        let mut read_content2 = String::from("......................");

        let read_content = [
            DstBuf {
                buf: read_content1.as_mut_ptr(),
                len: read_content1.len(),
            },
            DstBuf {
                buf: read_content2.as_mut_ptr(),
                len: read_content2.len(),
            },
        ];

        fs.read_vec(fd, &read_content).unwrap();

        assert_eq!("This is a sample file ", read_content1);
        assert_eq!("content.1234567890....", read_content2);

        fs.close(fd).unwrap();
    }

    #[test]
    fn read_and_write_vec_with_offset() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let write_content1 = "This is a sample file content.";
        let write_content2 = "1234567890";

        let write_content = [
            SrcBuf {
                buf: write_content1.as_ptr(),
                len: write_content1.len(),
            },
            SrcBuf {
                buf: write_content2.as_ptr(),
                len: write_content2.len(),
            },
        ];

        let fd = fs
            .open_or_create(dir, "test.txt", FdStat::default(), OpenFlags::CREATE, 0)
            .unwrap();

        fs.write_vec_with_offset(fd, write_content.as_ref(), 2)
            .unwrap();

        let meta = fs.metadata(fd).unwrap();
        assert_eq!(meta.size, 42);

        fs.seek(fd, 0, crate::fs::Whence::SET).unwrap();

        let mut read_content1 = String::from("......................");
        let mut read_content2 = String::from("......................");

        let read_content = [
            DstBuf {
                buf: read_content1.as_mut_ptr(),
                len: read_content1.len(),
            },
            DstBuf {
                buf: read_content2.as_mut_ptr(),
                len: read_content2.len(),
            },
        ];

        fs.read_vec_with_offset(fd, &read_content, 1).unwrap();

        assert_eq!("\0This is a sample file", read_content1);
        assert_eq!(" content.1234567890...", read_content2);

        fs.close(fd).unwrap();
    }

    #[test]
    fn seek_and_write_transient() {
        let mut fs = test_fs_transient();

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
        for mut fs in test_fs_setups("virtual_memory.txt") {
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
    }

    #[test]
    fn cannot_remove_opened_file() {
        for mut fs in test_fs_setups("virtual_memory.txt") {
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
    }

    #[test]
    fn cannot_remove_directory_as_file() {
        for mut fs in test_fs_setups("virtual_memory.txt") {
            let dir = fs.root_fd();

            let fd = fs.create_dir(dir, "test", FdStat::default(), 0).unwrap();
            fs.close(fd).unwrap();

            let err = fs.remove_file(dir, "test").unwrap_err();
            assert_eq!(err, Error::ExpectedToRemoveFile);

            fs.remove_dir(dir, "test").unwrap();
        }
    }

    #[test]
    fn cannot_remove_file_as_directory() {
        for mut fs in test_fs_setups("virtual_memory.txt") {
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

    #[test]
    fn renumber_when_the_alternative_file_exists() {
        for mut fs in test_fs_setups("virtual_memory.txt") {
            let dir = fs.root_fd();

            let fd1 = fs
                .open_or_create(dir, "file1.txt", FdStat::default(), OpenFlags::CREATE, 0)
                .unwrap();

            fs.write(fd1, &[1, 2, 3, 4, 5]).unwrap();

            let pos1 = fs.tell(fd1).unwrap();

            let fd2 = fs
                .open_or_create(dir, "file2.txt", FdStat::default(), OpenFlags::CREATE, 0)
                .unwrap();

            let pos2 = fs.tell(fd2).unwrap();

            assert!(pos1 == 5);
            assert!(pos2 == 0);

            fs.renumber(fd1, fd2).unwrap();

            let pos2_renumbered = fs.tell(fd2).unwrap();

            assert!(pos1 == pos2_renumbered);

            let res = fs.tell(fd1);

            assert!(res.is_err());
        }
    }

    #[test]
    fn renumber_when_the_alternative_file_doesnt_exist() {
        for mut fs in test_fs_setups("file1.txt") {
            let dir = fs.root_fd();

            let fd1 = fs
                .open_or_create(dir, "file1.txt", FdStat::default(), OpenFlags::CREATE, 0)
                .unwrap();

            fs.write(fd1, &[1, 2, 3, 4, 5]).unwrap();

            let pos1 = fs.tell(fd1).unwrap();

            assert!(pos1 == 5);

            let fd2 = 100;

            fs.renumber(fd1, fd2).unwrap();

            let pos2_renumbered = fs.tell(fd2).unwrap();

            assert!(pos1 == pos2_renumbered);

            let res = fs.tell(fd1);

            assert!(res.is_err());
        }
    }

    #[test]
    fn set_modified_set_accessed_time() {
        for mut fs in test_fs_setups("") {
            let dir = fs.root_fd();

            let fd1 = fs
                .open_or_create(dir, "file1.txt", FdStat::default(), OpenFlags::CREATE, 111)
                .unwrap();

            fs.write(fd1, &[1, 2, 3, 4, 5]).unwrap();

            fs.set_accessed_time(fd1, 333).unwrap();
            fs.set_modified_time(fd1, 222).unwrap();

            let metadata = fs.metadata(fd1).unwrap();

            let times = metadata.times;

            assert_eq!(times.accessed, 333);
            assert_eq!(times.modified, 222);
            assert_eq!(times.created, 111);

            assert_eq!(metadata.size, 5);
            assert_eq!(metadata.file_type, FileType::RegularFile);
        }
    }

    #[test]
    fn set_stat_get_stat() {
        let mut fs = test_fs();
        let dir = fs.root_fd();

        let fd1 = fs
            .open_or_create(dir, "file1.txt", FdStat::default(), OpenFlags::CREATE, 111)
            .unwrap();

        fs.write(fd1, &[1, 2, 3, 4, 5]).unwrap();

        let (file_type, mut stat) = fs.get_stat(fd1).unwrap();

        assert_eq!(file_type, FileType::RegularFile);

        assert_eq!(stat.flags, FdFlags::empty());

        stat.flags = FdFlags::APPEND;

        fs.set_stat(fd1, stat).unwrap();

        let (_, stat2) = fs.get_stat(fd1).unwrap();

        assert_eq!(stat2.flags, FdFlags::APPEND);
    }

    fn create_test_file_with_content(
        fs: &mut FileSystem,
        parent: Fd,
        file_name: &str,
        content: Vec<String>,
    ) -> Fd {
        let file_fd = fs
            .create_file(parent, file_name, FdStat::default(), 0)
            .unwrap();

        let mut src = String::from("");

        for str in content.iter() {
            src.push_str(str.as_str());
        }

        let bytes_written = fs.write(file_fd, src.as_bytes()).unwrap();

        assert!(bytes_written > 0);

        file_fd as Fd
    }

    fn create_test_file(fs: &mut FileSystem, parent_fd: Fd, file_name: &str) -> Fd {
        create_test_file_with_content(
            fs,
            parent_fd,
            file_name,
            vec![
                String::from("This is a sample text."),
                String::from("1234567890"),
            ],
        )
    }

    #[test]
    fn link_seek_tell() {
        let mut fs = test_fs();
        let dir = fs.root_fd();

        let root_fd = 3i32;

        let file_name1 = String::from("file.txt");
        let file_name2 = String::from("file_link.txt");

        let file_fd = create_test_file(&mut fs, root_fd as Fd, &file_name1);

        let root_node = fs.storage.as_ref().root_node();
        let node1 = find_node(
            root_node,
            &file_name1,
            &mut fs.names_cache,
            fs.storage.as_ref(),
        )
        .unwrap();

        // test seek and tell
        let position = fs.tell(file_fd).unwrap();

        assert_eq!(position, 32);

        fs.seek(file_fd, 10, crate::fs::Whence::SET).unwrap();

        let position_after_seek = fs.tell(file_fd).unwrap();

        assert_eq!(position_after_seek, 10);

        let mut buf_to_read1 = String::from("...............");

        let bytes_read = fs
            .read(file_fd, unsafe { buf_to_read1.as_bytes_mut() })
            .unwrap();

        assert_eq!(bytes_read, 15);
        assert_eq!(buf_to_read1, "sample text.123");

        // create link
        fs.create_hard_link(dir, &file_name1, dir, &file_name2)
            .unwrap();

        let node2 = find_node(
            root_node,
            &file_name2,
            &mut fs.names_cache,
            fs.storage.as_ref(),
        )
        .unwrap();

        assert_eq!(node1, node2);

        let link_file_fd = fs
            .open_or_create(
                dir,
                "file_link.txt",
                FdStat::default(),
                OpenFlags::empty(),
                0,
            )
            .unwrap();

        fs.seek(link_file_fd, 10, crate::fs::Whence::SET).unwrap();

        let position_link = fs.tell(link_file_fd).unwrap();

        assert_eq!(position_link, 10);

        let mut buf_to_read1 = String::from("................");

        let bytes_read = fs
            .read(link_file_fd, unsafe { buf_to_read1.as_bytes_mut() })
            .unwrap();

        assert_eq!(bytes_read, 16);

        assert_eq!(buf_to_read1, "sample text.1234");
    }

    #[test]
    fn renaming_folder_with_contents() {
        let mut fs = test_fs();
        let root_fd = fs.root_fd();

        let file_name = String::from("dir1/dir2/file.txt");
        create_test_file(&mut fs, root_fd as Fd, &file_name);

        fs.rename(root_fd, "dir1/dir2", root_fd, "dir2").unwrap();

        let file_fd = fs
            .open_or_create(
                root_fd,
                "dir2/file.txt",
                FdStat::default(),
                OpenFlags::empty(),
                0,
            )
            .unwrap();

        fs.seek(file_fd, 10, crate::fs::Whence::SET).unwrap();

        let mut buf_to_read1 = String::from("................");

        let bytes_read = fs
            .read(file_fd, unsafe { buf_to_read1.as_bytes_mut() })
            .unwrap();

        assert_eq!(bytes_read, 16);

        assert_eq!(buf_to_read1, "sample text.1234");
    }

    #[test]
    fn write_and_read_25_files() {
        for mut fs in test_fs_setups("auto/my_file_6.txt") {
            let root_fd = fs.root_fd();
            const SIZE_OF_FILE: usize = 1_000_000;

            // write files
            let dir_name = "auto";
            let file_count: u8 = 25;

            for i in 0..file_count {
                let filename = format!("{}/my_file_{}.txt", dir_name, i);
                let content = format!("{i}");
                let times = SIZE_OF_FILE / content.len();

                write_text_file(&mut fs, root_fd, filename.as_str(), content.as_str(), times)
                    .unwrap();
            }

            // read files
            for i in 0..file_count {
                let filename = format!("{}/my_file_{}.txt", dir_name, i);
                let expected_content = format!("{i}{i}{i}");

                let text_read = read_text_file(
                    &mut fs,
                    root_fd,
                    filename.as_str(),
                    0,
                    expected_content.len(),
                );

                assert_eq!(expected_content, text_read);
            }
        }

        // This test should not crash with an error
    }

    #[test]
    fn empty_path_support() {
        let mut fs = test_fs();
        let root_fd = fs.root_fd();

        write_text_file(&mut fs, root_fd, "f1/f2/text.txt", "content123", 100).unwrap();

        let content = read_text_file(&mut fs, root_fd, "f1/f2/text.txt", 7, 10);
        assert_eq!(content, "123content");

        let content = read_text_file(&mut fs, root_fd, "f1//f2/text.txt", 6, 10);
        assert_eq!(content, "t123conten");

        let content = read_text_file(&mut fs, root_fd, "/f1//f2/text.txt", 5, 10);
        assert_eq!(content, "nt123conte");

        write_text_file(&mut fs, root_fd, "text.txt", "abc", 100).unwrap();

        let content = read_text_file(&mut fs, root_fd, "text.txt", 0, 6);
        assert_eq!(content, "abcabc");

        let content = read_text_file(&mut fs, root_fd, "/text.txt", 0, 6);
        assert_eq!(content, "abcabc");

        let content = read_text_file(&mut fs, root_fd, "///////text.txt", 0, 6);
        assert_eq!(content, "abcabc");

        // This test should not crash with an error
    }

    #[test]
    fn writing_into_mounted_memory() {
        let memory: VectorMemory = new_vector_memory();

        let mut fs = test_fs();

        let root_fd = fs.root_fd();

        fs.mount_memory_file("test.txt", Box::new(memory.clone()))
            .unwrap();

        let content = "ABCDEFG123";

        write_text_file(&mut fs, root_fd, "test.txt", content, 1).unwrap();

        let mut buf = [0u8; 100];

        memory.read(0, &mut buf);

        println!("{:?}", buf);
    }

    #[test]
    fn reading_mounted_memory_after_upgrade() {
        let memory_manager = MemoryManager::init(new_vector_memory());
        let memory = memory_manager.get(MemoryId::new(1));

        let storage = StableStorage::new_with_memory_manager(&memory_manager, 200..210);
        let mut fs = FileSystem::new(Box::new(storage)).unwrap();
        fs.mount_memory_file("test.txt", Box::new(memory.clone()))
            .unwrap();

        let root_fd = fs.root_fd();
        let content = "ABCDEFG123";
        write_text_file(&mut fs, root_fd, "test.txt", content, 2).unwrap();

        // imitate canister upgrade (we keep the memory manager but recreate the file system with the same virtual memories)
        let storage = StableStorage::new_with_memory_manager(&memory_manager, 200..210);
        let mut fs = FileSystem::new(Box::new(storage)).unwrap();
        fs.mount_memory_file("test.txt", Box::new(memory.clone()))
            .unwrap();
        let root_fd = fs.root_fd();

        let content = read_text_file(&mut fs, root_fd, "test.txt", 0, 100);

        assert_eq!(content, "ABCDEFG123ABCDEFG123");
    }

    #[test]
    fn deleting_mounted_file_fails() {
        let memory: VectorMemory = new_vector_memory();

        let mut fs = test_fs();

        let root_fd = fs.root_fd();

        fs.mount_memory_file("test.txt", Box::new(memory.clone()))
            .unwrap();

        let res = fs.remove_file(root_fd, "test.txt");

        assert!(
            res == Err(Error::CannotRemoveMountedMemoryFile),
            "Deleting a mounted file should not be allowed!"
        );

        // check the dir entry still exists after deletion
        let files = list_files(&mut fs, "");

        assert_eq!(files[0], "test.txt".to_string());
    }

    #[test]
    fn mounted_memory_store_and_init_roundtrip() {
        for mut fs in test_fs_setups("") {
            let memory1: VectorMemory = new_vector_memory();
            let memory2: VectorMemory = new_vector_memory();

            let file_name = "test.txt";

            fs.mount_memory_file(file_name, Box::new(memory1.clone()))
                .unwrap();

            let content = "ABCDEFG123";
            let len = content.len();
            let count = 1000;

            memory1.grow(5);

            // fill up memory with some data
            for i in 0..count {
                memory1.write(i as u64 * len as u64, content.as_bytes());
            }

            let fd = fs
                .open_or_create(
                    fs.root_fd,
                    file_name,
                    FdStat::default(),
                    OpenFlags::empty(),
                    0,
                )
                .unwrap();
            let mut metadata = fs.metadata(fd).unwrap();
            metadata.size = len as FileSize * count as FileSize;
            fs.set_metadata(fd, metadata).unwrap();
            fs.close(fd).unwrap();

            // store memory into a file
            fs.store_memory_file(file_name).unwrap();

            fs.unmount_memory_file(file_name).unwrap();

            fs.mount_memory_file(file_name, Box::new(memory2.clone()))
                .unwrap();

            // init new memory into a file
            fs.init_memory_file(file_name).unwrap();

            let mut buf1 = [0u8; 10];
            let mut buf2 = [0u8; 10];

            for i in 0..count {
                memory1.read(i as u64 * len as u64, &mut buf1);
                memory2.read(i as u64 * len as u64, &mut buf2);

                assert_eq!(buf1, buf2);
            }
        }
    }

    #[test]
    fn writing_from_different_file_descriptors() {
        for mut fs in test_fs_setups("f1/f2/text.txt") {
            let root_fd = fs.root_fd();

            let fd1 = fs
                .open_or_create(
                    root_fd,
                    "f1/f2/text.txt",
                    FdStat::default(),
                    OpenFlags::CREATE,
                    40,
                )
                .unwrap();
            let fd2 = fs
                .open_or_create(
                    root_fd,
                    "f1//f2/text.txt",
                    FdStat::default(),
                    OpenFlags::CREATE,
                    44,
                )
                .unwrap();

            write_text_fd(&mut fs, fd1, "abc", 1).unwrap();
            write_text_fd(&mut fs, fd2, "123", 1).unwrap();
            write_text_fd(&mut fs, fd1, "xyz", 1).unwrap();

            let content = read_text_file(&mut fs, root_fd, "/f1/f2/text.txt", 0, 9);

            assert_eq!("123xyz", content);
        }
    }

    #[test]
    fn write_into_empty_filename_fails() {
        for mut fs in test_fs_setups("") {
            let root_fd = fs.root_fd();
            let res = write_text_file(&mut fs, root_fd, "", "content123", 100);
            assert!(res.is_err());
        }
    }

    fn create_file_with_size(filename: &str, size: FileSize, fs: &mut FileSystem) -> Fd {
        let fd = fs
            .open_or_create(
                fs.root_fd,
                filename,
                FdStat::default(),
                OpenFlags::CREATE,
                12,
            )
            .unwrap();
        let mut meta = fs.metadata(fd).unwrap();
        meta.size = size;

        fs.set_metadata(fd, meta).unwrap();

        fd
    }

    // test sparse files
    #[test]
    fn set_size_for_an_empty_file() {
        let filename = "test.txt";

        for mut fs in test_fs_setups(filename) {
            let root_fd = fs.root_fd();
            let fd = create_file_with_size(filename, 15, &mut fs);

            let content = read_text_file(&mut fs, root_fd, filename, 0, 100);

            assert_eq!(content, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");

            write_text_at_offset(&mut fs, fd, "abc", 3, 3).unwrap();

            let content = read_text_file(&mut fs, root_fd, filename, 1, 100);

            assert_eq!(content, "\0\0abcabcabc\0\0\0");
        }
    }

    #[test]
    fn create_file_missing_chunk_in_the_middle() {
        let filename = "test.txt";

        for mut fs in test_fs_setups(filename) {
            let chunk_size = fs.storage.chunk_size();

            let root_fd = fs.root_fd();
            let fd = create_file_with_size(filename, chunk_size as FileSize * 2 + 500, &mut fs);

            let content = read_text_file(&mut fs, root_fd, filename, 0, chunk_size * 10);

            let vec = vec![0; chunk_size * 2 + 500];
            let expected = String::from_utf8(vec).unwrap();

            // expect all zeroes at first
            assert_eq!(content, expected);

            // write some text to the first chunk
            write_text_at_offset(&mut fs, fd, "abc", 33, 3).unwrap();

            // write some text to the 100th position of the third chunk
            write_text_at_offset(&mut fs, fd, "abc", 33, chunk_size as FileSize * 2 + 100).unwrap();

            // read what we have now
            let content = read_text_file(&mut fs, root_fd, filename, 0, chunk_size * 10);

            let mut expected = vec![0u8; chunk_size * 2 + 500];

            let pattern = b"abc".repeat(33);
            expected[3..3 + 99].copy_from_slice(&pattern[..]);
            expected[chunk_size * 2 + 100..chunk_size * 2 + 100 + 99].copy_from_slice(&pattern[..]);

            let expected = String::from_utf8(expected).unwrap();

            assert_eq!(content, expected);
        }
    }

    #[test]
    fn iterate_file_only_middle_chunk_is_present() {
        let filename = "test.txt";

        for mut fs in test_fs_setups(filename) {
            let chunk_size = fs.storage.chunk_size();

            let root_fd = fs.root_fd();
            let fd = create_file_with_size(filename, chunk_size as FileSize * 2 + 500, &mut fs);

            let content = read_text_file(&mut fs, root_fd, filename, 0, chunk_size * 10);

            let vec = vec![0; chunk_size * 2 + 500];

            let expected = String::from_utf8(vec).unwrap();

            assert_eq!(content, expected);

            write_text_at_offset(&mut fs, fd, "abc", 33, chunk_size as FileSize + 100).unwrap();

            let content = read_text_file(&mut fs, root_fd, filename, 0, chunk_size * 10);

            let mut expected = vec![0u8; chunk_size * 2 + 500];

            let pattern = b"abc".repeat(33);
            expected[chunk_size + 100..chunk_size + 100 + 99].copy_from_slice(&pattern[..]);

            let expected = String::from_utf8(expected).unwrap();

            assert_eq!(content, expected);
        }
    }

    #[test]
    fn filename_cached_on_open_or_create() {
        let filename = "test.txt";

        for mut fs in test_fs_setups("") {
            let root_fd = fs.root_fd();
            let fd = fs
                .open_or_create(root_fd, filename, FdStat::default(), OpenFlags::CREATE, 12)
                .unwrap();

            fs.close(fd).unwrap();

            assert_eq!(fs.names_cache.get_nodes().len(), 1);
        }
    }

    #[test]
    fn deleted_file_cannot_be_found() {
        let filename = "test.txt";

        for mut fs in test_fs_setups("") {
            let root_fd = fs.root_fd();
            let fd = fs
                .open_or_create(root_fd, filename, FdStat::default(), OpenFlags::CREATE, 12)
                .unwrap();
            fs.close(fd).unwrap();

            fs.remove_file(root_fd, filename).unwrap();

            assert_eq!(fs.names_cache.get_nodes().len(), 0);

            // check we don't increase cache when the file is opened but not created
            let fd2 =
                fs.open_or_create(root_fd, filename, FdStat::default(), OpenFlags::empty(), 12);

            assert_eq!(fd2, Err(Error::NotFound));
            assert_eq!(fs.names_cache.get_nodes().len(), 0);
        }
    }

    // test sparse files
    #[test]
    fn get_stat_of_a_file_that_doesnt_exist() {
        let filename = "tmp/test.txt";

        let mut fs = test_fs();

        let root_fd = fs.root_fd();

        let file_fd = create_file_with_size(filename, 15, &mut fs);

        let dir_fd = fs
            .open_or_create(root_fd, "tmp", FdStat::default(), OpenFlags::CREATE, 0)
            .unwrap();

        let (filetype, stat) = fs.get_stat(dir_fd).unwrap();

        println!(
            "root = {root_fd}, dir_fd = {dir_fd}, file_fd = {file_fd}, type = {:?}, stat = {:?}",
            filetype, stat
        );

        let opened_fd = fs.open_or_create(
            root_fd,
            "tmp/a.txt",
            FdStat::default(),
            OpenFlags::empty(),
            0,
        );

        println!("opened_fd = {:?}", opened_fd);
    }
}

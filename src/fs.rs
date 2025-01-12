use crate::{
    error::Error,
    runtime::{
        dir::Dir,
        fd::{FdEntry, FdTable},
        file::File,
        structure_helpers::{create_hard_link, find_node, rm_dir_entry},
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

// The main class implementing the API to work with the file system.
pub struct FileSystem {
    root_fd: Fd,
    fd_table: FdTable,
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
                storage,
            });
        }

        let root_node = storage.root_node();
        let root_entry = Dir::new(root_node, FdStat::default(), &*storage)?;
        let root_fd = fd_table.open(FdEntry::Dir(root_entry));

        Ok(Self {
            root_fd,
            fd_table,
            storage,
        })
    }

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

    // Get dir entry for a given directory and the directory index.
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
        self.fd_table.close(fd).ok_or(Error::NotFound)?;

        Ok(())
    }

    // Get the metadata for a given file descriptor
    pub fn metadata(&self, fd: Fd) -> Result<Metadata, Error> {
        let node = self.get_node(fd)?;
        self.storage.get_metadata(node)
    }

    // find metadata for a given file descriptor.
    pub fn metadata_from_node(&self, node: Node) -> Result<Metadata, Error> {
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
    pub fn open_metadata(&self, parent: Fd, path: &str) -> Result<Metadata, Error> {
        let dir = self.get_dir(parent)?;
        let node = find_node(dir.node, path, self.storage.as_ref())?;
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

        match find_node(dir.node, path, self.storage.as_ref()) {
            Ok(node) => self.open(node, stat, flags),
            Err(Error::NotFound) => {
                if !flags.contains(OpenFlags::CREATE) {
                    return Err(Error::NotFound);
                }
                if flags.contains(OpenFlags::DIRECTORY) {
                    return self.create_dir(parent, path, stat, ctime);
                }
                self.create_file(parent, path, stat, ctime)
            }
            Err(err) => Err(err),
        }
    }

    // Opens a file and return its new file descriptor.
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

        let child = dir.create_file(path, stat, self.storage.as_mut(), ctime)?;

        let child_fd = self.fd_table.open(FdEntry::File(child));
        self.put_dir(parent, dir);
        Ok(child_fd)
    }

    // Delete a file by name `path` in the given file folder.
    pub fn remove_file(&mut self, parent: Fd, path: &str) -> Result<(), Error> {
        let dir = self.get_dir(parent)?;
        dir.remove_file(path, self.fd_table.node_refcount(), self.storage.as_mut())
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
        let child = dir.create_dir(path, stat, self.storage.as_mut(), ctime)?;
        let child_fd = self.fd_table.open(FdEntry::Dir(child));
        self.put_dir(parent, dir);
        Ok(child_fd)
    }

    // Delete a directory by name `path` in the given file folder.
    pub fn remove_dir(&mut self, parent: Fd, path: &str) -> Result<(), Error> {
        let dir = self.get_dir(parent)?;
        dir.remove_dir(path, self.fd_table.node_refcount(), self.storage.as_mut())
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
            self.storage.as_mut(),
        )?;

        let node = find_node(dst_dir.node, new_path, self.storage.as_ref())?;

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
            self.storage.as_mut(),
        )?;

        // now unlink the older version
        let (node, _metadata) = rm_dir_entry(
            src_dir.node,
            old_path,
            None,
            self.fd_table.node_refcount(),
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

    pub(crate) fn list_dir_internal(
        &mut self,
        dir_fd: Fd,
        file_type: Option<FileType>,
    ) -> Result<Vec<(Node, String)>, Error> {
        let mut res = vec![];

        let meta = self.metadata(dir_fd)?;

        let mut entry_index = meta.first_dir_entry;

        while let Some(index) = entry_index {
            let entry = self.storage.get_direntry(meta.node, index)?;

            // here we assume the entry value name is correct UTF-8
            let filename = unsafe {
                std::str::from_utf8_unchecked(&entry.name.bytes[..(entry.name.length as usize)])
            }
            .to_string();

            if let Some(file_type) = file_type {
                let meta = self.metadata_from_node(entry.node)?;

                if meta.file_type == file_type {
                    res.push((entry.node, filename));
                }
            } else {
                res.push((entry.node, filename));
            }

            entry_index = entry.next_entry;
        }

        Ok(res)
    }

    /// A convenience method to recursively remove a directory (and all subdirectories/files within) or delete a file, if the entry is a file.
    pub fn remove_recursive(&mut self, parent: Fd, path: &str) -> Result<(), Error> {
        let meta = self.open_metadata(parent, path)?;

        if meta.file_type == FileType::RegularFile {
            return self.remove_file(parent, path);
        }

        // Open the target directory. We use `OpenFlags::DIRECTORY` to ensure
        //    the path is interpreted as a directory (or fail if it doesn't exist).
        let dir_fd =
            self.open_or_create(parent, path, FdStat::default(), OpenFlags::DIRECTORY, 0)?;

        let result = (|| {
            // Find all directory children
            let children = self.list_dir_internal(dir_fd, None)?;

            // For each child, figure out if it's a subdirectory or file and remove accordingly.
            for (child_node, child_name) in children {
                let child_meta = self.storage.get_metadata(child_node)?;

                match child_meta.file_type {
                    FileType::Directory => {
                        // Recurse into the subdirectory.
                        self.remove_recursive(dir_fd, &child_name)?;
                    }
                    FileType::RegularFile => {
                        // Remove the file.
                        self.remove_file(dir_fd, &child_name)?;
                    }
                    FileType::SymbolicLink => {
                        unimplemented!("Symbolic links are not supported yet");
                    }
                }
            }

            Ok(())
        })();

        // close the folder itself before its deletion
        self.close(dir_fd)?;

        // Now that it is empty, remove the directory entry from its parent.
        self.remove_dir(parent, path)?;

        result
    }
}

#[cfg(test)]
mod tests {

    use std::fs;

    use crate::{
        error::Error,
        fs::{DstBuf, FdFlags, SrcBuf},
        runtime::{
            structure_helpers::find_node,
            types::{FdStat, OpenFlags},
        },
        storage::types::{FileSize, FileType},
        test_utils::{read_text_file, test_fs, test_fs_transient, write_text_fd, write_text_file},
    };

    use super::{Fd, FileSystem, Whence};

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

    #[test]
    fn renumber_when_the_alternative_file_exists() {
        let mut fs = test_fs();
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

    #[test]
    fn renumber_when_the_alternative_file_doesnt_exist() {
        let mut fs = test_fs();
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

    #[test]
    fn set_modified_set_accessed_time() {
        let mut fs = test_fs();
        let dir = fs.root_fd();

        let fd1 = fs
            .open_or_create(dir, "file1.txt", FdStat::default(), OpenFlags::CREATE, 111)
            .unwrap();

        fs.write(fd1, &[1, 2, 3, 4, 5]).unwrap();

        fs.set_accessed_time(fd1, 333).unwrap();
        fs.set_modified_time(fd1, 222).unwrap();

        let metadata = fs.metadata(fd1).unwrap();

        let times = metadata.times;

        assert_eq!(times.created, 111);
        assert_eq!(times.accessed, 333);
        assert_eq!(times.modified, 222);

        assert_eq!(metadata.size, 5);
        assert_eq!(metadata.file_type, FileType::RegularFile);
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
        let node1 = find_node(root_node, &file_name1, fs.storage.as_ref()).unwrap();

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

        let node2 = find_node(root_node, &file_name2, fs.storage.as_ref()).unwrap();

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
        let mut fs = test_fs();
        let root_fd = fs.root_fd();
        const SIZE_OF_FILE: usize = 1_000_000;

        // write files
        let dir_name = "auto";
        let file_count: u8 = 25;

        for i in 0..file_count {
            let filename = format!("{}/my_file_{}.txt", dir_name, i);
            let content = format!("{i}");
            let times = SIZE_OF_FILE / content.len();

            println!("Writing to {filename}");

            write_text_file(&mut fs, root_fd, filename.as_str(), content.as_str(), times).unwrap();
        }

        // read files
        for i in 0..file_count {
            let filename = format!("{}/my_file_{}.txt", dir_name, i);
            let expected_content = format!("{i}{i}{i}");

            println!("Reading {}", filename);

            let text_read = read_text_file(
                &mut fs,
                root_fd,
                filename.as_str(),
                0,
                expected_content.len(),
            );

            assert_eq!(expected_content, text_read);
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
    fn writing_from_different_file_descriptors() {
        let mut fs = test_fs();
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

    #[test]
    fn write_into_empty_filename_fails() {
        let mut fs = test_fs();
        let root_fd = fs.root_fd();

        let res = write_text_file(&mut fs, root_fd, "", "content123", 100);

        assert!(res.is_err());
    }

    // deterministic 32-bit pseudo-random number provider
    fn next_rand(cur_rand: u64) -> u64 {
        let a: u64 = 1103515245;
        let c: u64 = 12345;
        let m: u64 = 1 << 31;

        (a.wrapping_mul(cur_rand).wrapping_add(c)) % m
    }

    use crate::storage::stable::StableStorage;
    use ic_stable_structures::DefaultMemoryImpl;

    pub fn generate_random_file_structure(
        min_count: u32, // op count at wich to stop producing more operations
        op_count: u32,  // number of operations to do
        cur_rand: u64,  // current random seed
        depth: u32,     // current folder depth
        parent_fd: Fd,  // host fd
        fs: &mut FileSystem,
    ) -> Result<u32, crate::error::Error> {
        let mut op_count = op_count;
        let mut cur_rand = cur_rand;

        while op_count > 0 {
            op_count -= 1;

            cur_rand = next_rand(cur_rand);
            let action = cur_rand % 10; // e.g., 0..9

            match action {
                0 => {
                    // create a file using open
                    let filename = format!("file{}.txt", op_count);

                    let fd = fs.open_or_create(
                        parent_fd,
                        &filename,
                        FdStat::default(),
                        OpenFlags::CREATE,
                        op_count as u64,
                    )?;

                    fs.seek(fd, op_count as i64 * 100, Whence::SET)?;

                    fs.write(fd, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])?;

                    fs.close(fd)?;
                }
                1 => {
                    // create a file using create_open_file
                    let filename = format!("file{}.txt", op_count);

                    let fd =
                        fs.create_file(parent_fd, &filename, FdStat::default(), op_count as u64);

                    if fd.is_err() {
                        continue;
                    }

                    let fd = fd?;

                    let write_content1 = "12345";
                    let write_content2 = "67890";

                    let src = [
                        SrcBuf {
                            buf: write_content1.as_ptr(),
                            len: write_content1.len(),
                        },
                        SrcBuf {
                            buf: write_content2.as_ptr(),
                            len: write_content2.len(),
                        },
                    ];

                    fs.write_vec_with_offset(fd, src.as_ref(), op_count as FileSize * 1000)?;

                    fs.close(fd)?;
                }

                2 => {
                    // create a directory using mkdir.
                    let dirname = format!("dir{}", op_count);

                    // function might fail because of the naming conflict
                    let fd =
                        fs.create_dir(parent_fd, &dirname, FdStat::default(), op_count as u64)?;
                    fs.close(fd)?;
                }
                3 => {
                    // create a directory using create_open_directory.
                    let dirname = format!("dir{}", op_count);

                    let fd =
                        fs.create_dir(parent_fd, &dirname, FdStat::default(), op_count as u64)?;
                    fs.close(fd)?;
                }
                4 => {
                    // create or open a directory using open
                    let dirname = format!("dir_o{}", op_count);

                    let fd =
                        fs.create_dir(parent_fd, &dirname, FdStat::default(), op_count as u64)?;

                    fs.close(fd)?;
                }

                5 => {
                    // remove a random folder item
                    let files = fs.list_dir_internal(parent_fd, None)?;

                    if !files.is_empty() {
                        let cur_rand = next_rand(cur_rand);
                        let (node, name) = &files[cur_rand as usize % files.len()];

                        let meta = fs.metadata_from_node(*node)?;

                        match meta.file_type {
                            FileType::Directory => {
                                let _ = fs.remove_dir(parent_fd, name);
                            }
                            FileType::RegularFile => {
                                let _ = fs.remove_file(parent_fd, name);
                            }
                            FileType::SymbolicLink => panic!("Symlink are not supported!"),
                        }
                    }
                }

                6 => {
                    // enter subfolder
                    let dirs = fs.list_dir_internal(parent_fd, Some(FileType::Directory))?;

                    if !dirs.is_empty() {
                        let cur_rand = next_rand(cur_rand);
                        let (_node, name) = &dirs[cur_rand as usize % dirs.len()];

                        let dir_fd = fs.open_or_create(
                            parent_fd,
                            name,
                            FdStat::default(),
                            OpenFlags::empty(),
                            op_count as u64,
                        )?;

                        let res = generate_random_file_structure(
                            min_count,
                            op_count,
                            cur_rand,
                            depth + 1,
                            dir_fd,
                            fs,
                        );

                        fs.close(dir_fd)?;

                        op_count = res?;
                    }
                }

                7 => {
                    // exit the current folder
                    if depth > 0 {
                        return Ok(op_count);
                    }
                }

                8 => {
                    let dirs = fs.list_dir_internal(parent_fd, Some(FileType::Directory))?;

                    // Random open/close a file (or directory)
                    if !dirs.is_empty() {
                        let cur_rand = next_rand(cur_rand);
                        let (_node, filename) = &dirs[cur_rand as usize % dirs.len()];

                        let fd = fs.open_or_create(
                            parent_fd,
                            filename,
                            FdStat::default(),
                            OpenFlags::empty(),
                            op_count as u64,
                        )?;

                        fs.close(fd)?;
                    }
                }

                9 => {
                    // occasionly increate counter to cause naming conflicts and provoce errors
                    // don't allow random errors for now
                    //op_count += 2;
                }

                _ => {
                    panic!("Incorrect action {action}");
                }
            }
        }

        Ok(op_count)
    }

    fn list_all_files_as_string(fs: &mut FileSystem) -> Result<String, crate::error::Error> {
        let mut paths = Vec::new();

        scan_directory(fs, fs.root_fd(), "", &mut paths)?;
        Ok(paths.join("\n"))
    }

    fn scan_directory(
        fs: &mut FileSystem,
        dir_fd: u32,
        current_path: &str,
        collected_paths: &mut Vec<String>,
    ) -> Result<(), crate::error::Error> {
        let meta = fs.metadata(dir_fd)?;

        // add current folder as well
        let entry_path = if current_path.is_empty() {
            format!("/. {}", meta.size)
        } else {
            format!("{}/. {}", current_path, meta.size)
        };
        collected_paths.push(entry_path);

        let entries = fs.list_dir_internal(dir_fd, None)?;

        for (entry_node, filename) in entries {
            let meta = fs.metadata_from_node(entry_node)?;

            let entry_path = if current_path.is_empty() {
                format!("/{} {}", filename, meta.size)
            } else {
                format!("{}/{} {}", current_path, filename, meta.size)
            };

            match meta.file_type {
                FileType::Directory => {
                    let child_fd = fs.open_or_create(
                        dir_fd,
                        &filename,
                        FdStat::default(),
                        OpenFlags::DIRECTORY,
                        0,
                    )?;

                    scan_directory(fs, child_fd, &entry_path, collected_paths)?;

                    fs.close(child_fd)?;
                }
                FileType::RegularFile => {
                    collected_paths.push(entry_path);
                }
                FileType::SymbolicLink => todo!(),
            }
        }

        Ok(())
    }

    use ic_stable_structures::memory_manager::VirtualMemory;
    use ic_stable_structures::Memory;
    use ic_stable_structures::VectorMemory;

    pub fn new_vector_memory() -> VectorMemory {
        use std::{cell::RefCell, rc::Rc};

        Rc::new(RefCell::new(Vec::new()))
    }

    pub fn new_vector_memory_init(v: Vec<u8>) -> VectorMemory {
        use std::{cell::RefCell, rc::Rc};

        Rc::new(RefCell::new(v))
    }

    #[test]
    fn test_generator() {
        //let memory = DefaultMemoryImpl::default();
        let memory = new_vector_memory();

        let storage = StableStorage::new(memory.clone());
        let mut fs = FileSystem::new(Box::new(storage)).unwrap();

        let root_fd = fs
            .create_dir(fs.root_fd(), "root_dir", FdStat::default(), 0)
            .unwrap();

        // generate random file structure.
        generate_random_file_structure(0, 1000, 35, 0, root_fd, &mut fs).unwrap();
        fs.close(root_fd).unwrap();

        // test deletion

        // get all files
        let files = list_all_files_as_string(&mut fs).unwrap();

        println!("------------------------------------------");
        println!("FILE STRUCTURE");
        println!("{}", files);

        // store memory into file

        let v = memory.borrow();

        // try to delete the generated folder
        //fs.remove_recursive(fs.root_fd(), "root_dir").unwrap();
        //fs.remove_file(fs.root_fd(), "root_dir/file4.txt").unwrap();
        //fs.remove_dir(fs.root_fd(), "root_dir").unwrap();

        fs::create_dir_all("./tests/res/").unwrap();
        fs::write("./tests/res/memory-v0_4-op35_1000.bin", &*v).unwrap();
        fs::write("./tests/res/structure-v0_4-op35_1000.txt", &files).unwrap();
    }

    #[test]
    fn test_reading_structure() {
        let v = fs::read("./tests/res/memory-v0_4-op35_1000.bin").unwrap();
        let memory = new_vector_memory_init(v);

        let v_files = fs::read("./tests/res/structure-v0_4-op35_1000.txt").unwrap();
        let files_old = std::str::from_utf8(&v_files).unwrap();

        let storage = StableStorage::new(memory);

        let mut fs = FileSystem::new(Box::new(storage)).unwrap();
        let files = list_all_files_as_string(&mut fs).unwrap();

        assert_eq!(files, files_old);

        println!("------------------------------------------");
        println!("FILE STRUCTURE");
        println!("{}", files);
    }
}

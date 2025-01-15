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

pub use crate::runtime::types::{
    Advice, ChunkSize, ChunkType, DstBuf, DstIoVec, Fd, FdFlags, FdStat, OpenFlags, SrcBuf,
    SrcIoVec, Whence,
};
pub use crate::storage::types::FileSize;

// The main class implementing the API to work with the file system.
pub struct FileSystem {
    pub(crate) root_fd: Fd,
    pub(crate) fd_table: FdTable,
    pub(crate) names_cache: FilenameCache,
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
        let root_fd = fd_table.open_root(FdEntry::Dir(root_entry));
        let names_cache = FilenameCache::new();

        Ok(Self {
            root_fd,
            fd_table,
            names_cache,
            storage,
        })
    }

    // Get version of the file system
    pub fn get_storage_version(&self) -> u32 {
        self.storage.get_version()
    }

    // Get the file descriptor of the root folder.
    // This file descriptor is always open and cannot be closed or renumbered.
    pub fn root_fd(&self) -> Fd {
        self.root_fd
    }

    // Get the path of the root folder.
    pub fn root_path(&self) -> &str {
        "/"
    }

    // advice on the usage of the file system,
    // this implementation doesn't take any special action, only checks that the Fd is valid.
    pub fn advice(
        &mut self,
        fd: Fd,
        _offset: FileSize,
        _len: FileSize,
        _advice: Advice,
    ) -> Result<(), Error> {
        let meta = self.metadata(fd)?;

        // this method only works for regular files
        if meta.file_type != FileType::RegularFile {
            return Err(Error::BadFileDescriptor);
        }

        Ok(())
    }

    // The method tries to reserve enough memory for the file.
    // If the file is hosting a mounted memory, the memory will actually grow and take more space,
    // the regular file will only change its size (no additional memory is consumed by its chunks).
    // This method might fail if the resulting file size is above the maximum allowed file size.
    pub fn allocate(
        &mut self,
        fd: Fd,
        _offset: FileSize,
        _additional_size: FileSize,
    ) -> Result<(), Error> {
        let meta = self.metadata(fd)?;

        if meta.file_type != FileType::RegularFile {
            return Err(Error::BadFileDescriptor);
        }

        // TODO: do make file bigger by the given file size
        Ok(())
    }

    // Close the opened file and release the corresponding file descriptor.
    pub fn close(&mut self, fd: Fd) -> Result<(), Error> {
        if fd == crate::runtime::fd::ROOT_FD {
            // we do not close the root fd,
            // and there is no error in trying to close it
            return Ok(());
        }

        // flush if regular file
        match self.fd_table.get(fd) {
            Some(FdEntry::File(file)) => {
                self.storage.flush(file.node);
            }
            Some(FdEntry::Dir(_dir)) => {
                // directories do not need flush
            }
            None => Err(Error::BadFileDescriptor)?,
        };

        self.fd_table.close(fd).ok_or(Error::BadFileDescriptor)?;

        Ok(())
    }

    // Flush any cached changes to the disk.
    pub fn flush(&mut self, fd: Fd) -> Result<(), Error> {
        let file = self.get_file(fd)?;

        self.storage.flush(file.node);

        Ok(())
    }

    // Reassign a file descriptor to a new number, the source descriptor is closed in the process.
    // If the destination descriptor is busy, it is closed in the process.
    pub fn renumber(&mut self, from: Fd, to: Fd) -> Result<(), Error> {
        self.fd_table.renumber(from, to)
    }

    pub(crate) fn get_node(&self, fd: Fd) -> Result<Node, Error> {
        match self.fd_table.get(fd) {
            Some(FdEntry::File(file)) => Ok(file.node),
            Some(FdEntry::Dir(dir)) => Ok(dir.node),
            None => Err(Error::BadFileDescriptor),
        }
    }

    pub(crate) fn get_file(&self, fd: Fd) -> Result<File, Error> {
        match self.fd_table.get(fd) {
            Some(FdEntry::File(file)) => Ok(file.clone()),
            Some(FdEntry::Dir(_)) => Err(Error::BadFileDescriptor),
            None => Err(Error::BadFileDescriptor),
        }
    }

    pub(crate) fn put_file(&mut self, fd: Fd, file: File) {
        self.fd_table.update(fd, FdEntry::File(file))
    }

    pub(crate) fn get_dir(&self, fd: Fd) -> Result<Dir, Error> {
        match self.fd_table.get(fd) {
            Some(FdEntry::Dir(dir)) => Ok(dir.clone()),
            Some(FdEntry::File(_)) => Err(Error::BadFileDescriptor),
            None => Err(Error::BadFileDescriptor),
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
        let fd = self.open(
            self.root_fd,
            filename,
            FdStat::default(),
            OpenFlags::CREATE,
            0,
        )?;

        let result = (|| {
            let file = self.get_file(fd)?;
            self.storage.mount_node(file.node, memory)
        })();

        let _ = self.close(fd);

        result
    }

    // initialize mounted memory with the data stored in the host file
    pub fn init_memory_file(&mut self, filename: &str) -> Result<(), Error> {
        // create a file for the mount
        let fd = self.open(
            self.root_fd,
            filename,
            FdStat::default(),
            OpenFlags::empty(),
            0,
        )?;

        let result = (|| {
            let file = self.get_file(fd)?;
            self.storage.init_mounted_memory(file.node)
        })();

        let _ = self.close(fd);

        result
    }

    // store content of the currently active memory file to the file system
    pub fn store_memory_file(&mut self, filename: &str) -> Result<(), Error> {
        // create a file for the mount
        let fd = self.open(
            self.root_fd,
            filename,
            FdStat::default(),
            OpenFlags::empty(),
            0,
        )?;

        let result = (|| {
            let file = self.get_file(fd)?;
            self.storage.store_mounted_memory(file.node)
        })();

        let _ = self.close(fd);

        result
    }

    // Unmount memory, the system will continue to work with the file in normal mode.
    pub fn unmount_memory_file(&mut self, filename: &str) -> Result<Box<dyn Memory>, Error> {
        // create a file for the mount
        let fd = self.open(
            self.root_fd,
            filename,
            FdStat::default(),
            OpenFlags::empty(),
            0,
        )?;

        let result = (|| {
            let file = self.get_file(fd)?;

            let memory = self.storage.unmount_node(file.node)?;

            Ok(memory)
        })();

        let _ = self.close(fd);

        result
    }

    // Get directory entry for a given directory file descriptor and the entry index.
    pub fn get_direntry(&self, fd: Fd, index: DirEntryIndex) -> Result<DirEntry, Error> {
        self.get_dir(fd)?.get_entry(index, self.storage.as_ref())
    }

    fn get_node_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error> {
        self.storage.get_direntry(node, index)
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

    // Get the metadata for a given node
    pub fn metadata_from_node(&self, node: Node) -> Result<Metadata, Error> {
        self.storage.get_metadata(node)
    }

    // Get the metadata for a given file descriptor
    pub fn metadata(&self, fd: Fd) -> Result<Metadata, Error> {
        let node = self.get_node(fd)?;
        self.storage.get_metadata(node)
    }

    // Update metadata of a given file descriptor
    pub fn set_metadata(&mut self, fd: Fd, metadata: Metadata) -> Result<(), Error> {
        let node = self.get_node(fd)?;
        self.storage.put_metadata(node, &metadata)?;

        Ok(())
    }

    pub fn set_file_size(&mut self, fd: Fd, new_size: FileSize) -> Result<(), Error> {
        let file = self.get_file(fd)?;

        let mut metadata = self.storage.get_metadata(file.node)?;

        metadata.size = new_size;

        self.storage.put_metadata(file.node, &metadata)?;

        Ok(())
    }

    // Set maxmum file size limit in bytes. Reading, writing, and setting the cursor above the limit will result in error.
    // Use this feature to limit how much memory can be consumed by the mounted memory files.
    pub fn set_file_size_limit(&mut self, fd: Fd, max_size: FileSize) -> Result<(), Error> {
        let file = self.get_file(fd)?;

        let mut metadata = self.storage.get_metadata(file.node)?;

        metadata.maximum_size_allowed = Some(max_size);

        self.storage.put_metadata(file.node, &metadata)?;

        Ok(())
    }

    // Update access time.
    pub fn set_accessed_time(&mut self, fd: Fd, time: u64) -> Result<(), Error> {
        let node = self.get_node(fd)?;
        let mut metadata = self.storage.get_metadata(node)?;

        metadata.times.accessed = time;

        self.storage.put_metadata(node, &metadata)?;

        Ok(())
    }

    // Update modification time.
    pub fn set_modified_time(&mut self, fd: Fd, time: u64) -> Result<(), Error> {
        let node = self.get_node(fd)?;
        let mut metadata = self.storage.get_metadata(node)?;

        metadata.times.modified = time;

        self.storage.put_metadata(node, &metadata)?;

        Ok(())
    }

    // Get file or directory stats.
    pub fn get_stat(&self, fd: Fd) -> Result<(FileType, FdStat), Error> {
        match self.fd_table.get(fd) {
            None => Err(Error::BadFileDescriptor),
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
                // file-only stats cause badfd error
                if stat.flags.contains(FdFlags::APPEND) || stat.flags.contains(FdFlags::NONBLOCK) {
                    return Err(Error::BadFileDescriptor);
                }

                let mut dir = dir.clone();
                dir.stat = stat;
                self.put_dir(fd, dir);
                Ok(())
            }
            None => Err(Error::BadFileDescriptor),
        }
    }

    // Get metadata of a file with name `path` in a given folder.
    pub fn open_metadata(&mut self, parent: Fd, path: &str) -> Result<Metadata, Error> {
        let dir = self.get_dir(parent)?;
        let node = find_node(dir.node, path, &mut self.names_cache, self.storage.as_ref())?;
        self.storage.get_metadata(node)
    }

    // Opens a directory or file or creates a new file (depending on the flags provided).
    pub fn open(
        &mut self,
        parent_fd: Fd,
        path: &str,
        stat: FdStat,
        flags: OpenFlags,
        ctime: u64,
    ) -> Result<Fd, Error> {
        let dir = self.get_dir(parent_fd)?;

        match find_node(dir.node, path, &mut self.names_cache, self.storage.as_ref()) {
            Ok(node) => self.open_internal(node, stat, flags),

            Err(Error::NoSuchFileOrDirectory) => {
                if !flags.contains(OpenFlags::CREATE) {
                    return Err(Error::NoSuchFileOrDirectory);
                }

                if flags.contains(OpenFlags::DIRECTORY) {
                    self.create_open_directory(parent_fd, path, stat, ctime)
                } else {
                    self.create_open_file(parent_fd, path, stat, ctime)
                }
            }
            Err(err) => Err(err),
        }
    }

    // Opens an existing file or directory by node and return its new file descriptor.
    fn open_internal(&mut self, node: Node, stat: FdStat, flags: OpenFlags) -> Result<Fd, Error> {
        let metadata = self.storage.get_metadata(node)?;

        match metadata.file_type {
            FileType::Directory => {
                let dir = Dir::new(node, stat, self.storage.as_mut())?;
                let fd = self.fd_table.open(FdEntry::Dir(dir));
                Ok(fd)
            }
            FileType::RegularFile => {
                if flags.contains(OpenFlags::DIRECTORY) {
                    return Err(Error::InvalidArgument);
                }

                let file = File::new(node, stat, self.storage.as_mut())?;

                if flags.contains(OpenFlags::TRUNCATE) {
                    file.truncate(self.storage.as_mut())?;
                }

                let fd = self.fd_table.open(FdEntry::File(file));
                Ok(fd)
            }
            FileType::SymbolicLink => unimplemented!("Symbolic links are not supported"),
        }
    }

    // Create a new file and open it. Function fails if the file exists already.
    pub(crate) fn create_open_file(
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
        )?;

        self.put_dir(parent, dir);

        Ok(())
    }

    // Convenience method, create a new directory without opening it
    pub fn mkdir(&mut self, parent: Fd, path: &str, stat: FdStat, ctime: u64) -> Result<(), Error> {
        let dir = self.get_dir(parent)?;

        dir.create_dir(
            path,
            stat,
            &mut self.names_cache,
            self.storage.as_mut(),
            ctime,
        )?;

        self.put_dir(parent, dir);

        Ok(())
    }

    // create a directory and return an opened file descriptor of it
    pub fn create_open_directory(
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
        )?;

        self.put_dir(parent, dir);

        Ok(())
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
            let entry = self.get_node_direntry(meta.node, index)?;

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
        let dir_fd = self.open(parent, path, FdStat::default(), OpenFlags::DIRECTORY, 0)?;

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

        self.open_internal(node, FdStat::default(), OpenFlags::empty())
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

        self.open_internal(node, FdStat::default(), OpenFlags::empty())
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

use std::{collections::BTreeMap};

use crate::{
    error::Error,
    runtime::file::File,
    storage::{
        types::{
            DirEntry, DirEntryIndex, FileName, FileType, Metadata, Node, Times, FILE_CHUNK_SIZE,
        },
        Storage,
    },
};

use super::types::FdStat;

#[derive(Clone, Debug)]

pub struct Dir {
    pub node: Node,
    pub stat: FdStat,
}

impl Dir {
    pub fn new(node: Node, stat: FdStat, storage: &dyn Storage) -> Result<Self, Error> {
        let file_type = storage.get_metadata(node)?.file_type;
        match file_type {
            FileType::Directory => {}
            FileType::RegularFile => {
                unreachable!("Unexpected file type, expected directory.");
            }
            FileType::SymbolicLink => unimplemented!("Symbolic links are not implemented yet"),
        };
        Ok(Self { node, stat })
    }

    // Create directory entry in the current directory.
    pub fn create_dir(
        &self,
        path: &str,
        stat: FdStat,
        storage: &mut dyn Storage,
        ctime: u64
    ) -> Result<Self, Error> {
        let found = self.find_node(path, storage);
        match found {
            Err(Error::NotFound) => {}
            Ok(_) => return Err(Error::FileAlreadyExists),
            Err(err) => return Err(err),
        }

        let node = storage.new_node();
        storage.put_metadata(
            node,
            Metadata {
                node,
                file_type: FileType::Directory,
                link_count: 1,
                size: 0,
                times: Times {accessed: ctime, modified: ctime, created: ctime},
                first_dir_entry: None,
                last_dir_entry: None,
            },
        );
        self.add_entry(node, path, storage)?;
        Self::new(node, stat, storage)
    }

    // Remove directory entry from the current directory.
    pub fn remove_dir(
        &self,
        path: &str,
        node_refcount: &BTreeMap<Node, usize>,
        storage: &mut dyn Storage,
    ) -> Result<(), Error> {
        let (node, metadata) = self.rm_entry(path, Some(true), node_refcount, storage)?;

        if metadata.link_count == 0 {
            let chunk_cnt = (metadata.size + FILE_CHUNK_SIZE as u64 - 1) / FILE_CHUNK_SIZE as u64;
            for index in 0..chunk_cnt {
                storage.rm_filechunk(node, index as u32);
            }
            storage.rm_metadata(node);
        }

        Ok(())
    }

    // Create file entry in the current directory.
    pub fn create_file(
        &self,
        path: &str,
        stat: FdStat,
        storage: &mut dyn Storage,
        ctime: u64
    ) -> Result<File, Error> {
        let found = self.find_node(path, storage);
        match found {
            Err(Error::NotFound) => {}
            Ok(_) => return Err(Error::FileAlreadyExists),
            Err(err) => return Err(err),
        }

        let node = storage.new_node();
        storage.put_metadata(
            node,
            Metadata {
                node,
                file_type: FileType::RegularFile,
                link_count: 1,
                size: 0,
                times: Times {created: ctime, modified: ctime, accessed: ctime},
                first_dir_entry: None,
                last_dir_entry: None,
            },
        );

        self.add_entry(node, path, storage)?;

        File::new(node, stat, storage)
    }


    // Create a hard link to an existing node
    pub fn create_hard_link(
        &self,
        new_path: &str,
        src_dir: &Dir,
        src_path: &str,
        is_renaming: bool,
        storage: &mut dyn Storage
    ) -> Result<(), Error> {

        // Check if the node exists already.
        let found = self.find_node(new_path, storage);
        match found {
            Err(Error::NotFound) => {}
            Ok(_) => return Err(Error::FileAlreadyExists),
            Err(err) => return Err(err),
        }

        // Get the node and metadata, the node must exist in the source folder.
        let node: Node = src_dir.find_node(src_path, storage)?;

        let mut metadata = storage.get_metadata(node)?;

        // only allow creating a hardlink on a folder if it is a part of renaming and another link will be removed
        if !is_renaming && metadata.file_type == FileType::Directory {
            return Err(Error::InvalidFileType);
        }

        metadata.link_count += 1;
        storage.put_metadata(node, metadata);

        self.add_entry(node, new_path, storage)?;

        Ok(())
    }

    // Remove file entry from the current directory.
    pub fn remove_file(
        &self,
        path: &str,
        node_refcount: &BTreeMap<Node, usize>,
        storage: &mut dyn Storage,
    ) -> Result<(), Error> {
        let (node, metadata) = self.rm_entry(path, Some(false), node_refcount, storage)?;

        if metadata.link_count == 0 {
            let chunk_cnt = (metadata.size + FILE_CHUNK_SIZE as u64 - 1) / FILE_CHUNK_SIZE as u64;
            for index in 0..chunk_cnt {
                storage.rm_filechunk(node, index as u32);
            }
            storage.rm_metadata(node);
        }

        Ok(())
    }

    // Find directory entry node by its name.
    pub fn find_node(&self, path: &str, storage: &dyn Storage) -> Result<Node, Error> {
        let entry_index = self.find_entry_index(path, storage)?;

        let entry = storage.get_direntry(self.node, entry_index)?;

        Ok(entry.node)
    }

    // Iterate directory entries, find entry index by name.
    pub fn find_entry_index(
        &self,
        path: &str,
        storage: &dyn Storage,
    ) -> Result<DirEntryIndex, Error> {
        let path = path.as_bytes();

        let mut next_index = storage.get_metadata(self.node)?.first_dir_entry;

        while let Some(index) = next_index {
            if let Ok(dir_entry) = storage.get_direntry(self.node, index) {
                if &dir_entry.name.bytes[0..path.len()] == path {
                    return Ok(index);
                }

                next_index = dir_entry.next_entry;
            }
        }

        Err(Error::NotFound)
    }

    // Get directory entry by index.
    pub fn get_entry(
        &self,
        index: DirEntryIndex,
        storage: &dyn Storage,
    ) -> Result<DirEntry, Error> {
        storage.get_direntry(self.node, index)
    }

    //  Add new directory entry 
    fn add_entry(
        &self,
        new_node: Node,
        path: &str,
        storage: &mut dyn Storage,
    ) -> Result<(), Error> {
        let mut metadata = storage.get_metadata(self.node)?;
        let name = FileName::new(path)?;

        // start numbering with 1
        let new_entry_index: DirEntryIndex = metadata.last_dir_entry.unwrap_or(0) + 1;

        storage.put_direntry(
            self.node,
            new_entry_index,
            DirEntry {
                node: new_node,
                name,
                next_entry: None,
                prev_entry: metadata.last_dir_entry,
            },
        );

        // update previous last entry
        if let Some(prev_dir_entry_index) = metadata.last_dir_entry {
            let mut prev_dir_entry = storage.get_direntry(self.node, prev_dir_entry_index)?;

            prev_dir_entry.next_entry = Some(new_entry_index);
            storage.put_direntry(self.node, prev_dir_entry_index, prev_dir_entry)
        }

        // update metadata
        metadata.last_dir_entry = Some(new_entry_index);

        if metadata.first_dir_entry.is_none() {
            metadata.first_dir_entry = Some(new_entry_index);
        }
        metadata.size += 1;

        storage.put_metadata(self.node, metadata);

        Ok(())
    }

    /// Remove the directory entry from the current directory by entry name.
    /// 
    /// path            The name of the entry to delete
    /// expect_dir      If true, the directory is deleted. If false - the file is deleted. If the expected entry type does not match with the actual entry - an error is returned.
    /// node_refcount   A map of nodes to check if the file being deleted is opened by multiple file descriptors. Deleting an entry referenced by multiple file descriptors is not allowed and will result in an error.
    /// storage         The reference to the actual storage implementation
    /// is_renaming     true if renaming is in progress, this allows to "delete" a non-empty folder
    /// 
    pub fn rm_entry(
        &self,
        path: &str,
        expect_dir: Option<bool>,
        node_refcount: &BTreeMap<Node, usize>,
        storage: &mut dyn Storage,
    ) -> Result<(Node, Metadata), Error> {
        let mut parent_dir_metadata = storage.get_metadata(self.node)?;

        let removed_entry_index = self.find_entry_index(path, storage)?;

        let removed_dir_entry = storage.get_direntry(self.node, removed_entry_index)?;

        let mut removed_metadata = storage.get_metadata(removed_dir_entry.node)?;

        match removed_metadata.file_type {
            FileType::Directory => {
                if expect_dir == Some(false) {
                    return Err(Error::ExpectedToRemoveFile);
                }

                if removed_metadata.link_count == 1 && removed_metadata.size > 0 {
                    return Err(Error::DirectoryNotEmpty);
                }
            }
            FileType::RegularFile | FileType::SymbolicLink => {
                if expect_dir == Some(true) {
                    return Err(Error::ExpectedToRemoveDirectory);
                }
            }
        }

        if let Some(refcount) = node_refcount.get(&removed_metadata.node) {
            if *refcount > 0 && removed_metadata.link_count == 1 {
                return Err(Error::CannotRemoveOpenedNode);
            }
        }

        // update previous entry
        if let Some(prev_dir_entry_index) = removed_dir_entry.prev_entry {
            let mut prev_dir_entry = storage.get_direntry(self.node, prev_dir_entry_index)?;
            prev_dir_entry.next_entry = removed_dir_entry.next_entry;
            storage.put_direntry(self.node, prev_dir_entry_index, prev_dir_entry)
        }

        // update next entry
        if let Some(next_dir_entry_index) = removed_dir_entry.next_entry {
            let mut next_dir_entry = storage.get_direntry(self.node, next_dir_entry_index)?;
            next_dir_entry.prev_entry = removed_dir_entry.prev_entry;
            storage.put_direntry(self.node, next_dir_entry_index, next_dir_entry)
        }

        // update parent metadata when the last directory entry is removed
        if Some(removed_entry_index) == parent_dir_metadata.last_dir_entry {
            parent_dir_metadata.last_dir_entry = removed_dir_entry.prev_entry;
        }

        // update parent metadata when the first directory entry is removed
        if Some(removed_entry_index) == parent_dir_metadata.first_dir_entry {
            parent_dir_metadata.first_dir_entry = removed_dir_entry.next_entry;
        }

        // dir entry size is reduced by one
        parent_dir_metadata.size -= 1;

        // update parent metadata
        storage.put_metadata(self.node, parent_dir_metadata);

        // remove the entry
        storage.rm_direntry(self.node, removed_entry_index);

        removed_metadata.link_count -= 1;
        storage.put_metadata(removed_metadata.node, removed_metadata.clone());

        Ok((removed_dir_entry.node, removed_metadata))
    }
}

#[cfg(test)]
mod tests {
    use crate::{runtime::types::FdStat, test_utils::test_fs, fs::OpenFlags, error::Error};

    #[test]
    fn remove_middle_file() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let fd = fs.create_file(dir, "test1.txt", FdStat::default(), 0).unwrap();
        fs.close(fd).unwrap();
        let fd = fs.create_file(dir, "test2.txt", FdStat::default(), 0).unwrap();
        fs.close(fd).unwrap();
        let fd = fs.create_file(dir, "test3.txt", FdStat::default(), 0).unwrap();
        fs.close(fd).unwrap();

        let meta = fs.metadata(dir).unwrap();
        assert_eq!(meta.size, 3);

        fs.remove_file(fs.root_fd(), "test2.txt").unwrap();

        let meta = fs.metadata(dir).unwrap();
        assert_eq!(meta.size, 2);

        let entry1_index = meta.first_dir_entry.unwrap();
        let entry1 = fs.get_direntry(dir, entry1_index).unwrap();

        let entry2_index = entry1.next_entry.unwrap();
        let entry2 = fs.get_direntry(dir, entry2_index).unwrap();

        assert_eq!(entry1.prev_entry, None);
        assert_eq!(entry1.next_entry, Some(entry2_index));

        assert_eq!(entry2.prev_entry, Some(entry1_index));
        assert_eq!(entry2.next_entry, None);
    }

    #[test]
    fn create_dir_file_creation_time() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let new_dir_fd = fs.create_dir(dir, "dir1", FdStat::default(), 123).unwrap();

        let new_file_fd = fs.create_file(dir, "test.txt", FdStat::default(), 234).unwrap();

        let dir_meta = fs.metadata(new_dir_fd).unwrap();
        
        assert_eq!(dir_meta.times.created, 123);
        assert_eq!(dir_meta.times.modified, 123);
        assert_eq!(dir_meta.times.accessed, 123);

        let file_meta = fs.metadata(new_file_fd).unwrap();
        assert_eq!(file_meta.times.created, 234);
        assert_eq!(file_meta.times.modified, 234);
        assert_eq!(file_meta.times.accessed, 234);

    }

    #[test]
    fn remove_last_file() {
        let mut fs = test_fs();

        let dir = fs.root_fd();

        let fd = fs.create_file(dir, "test2.txt", FdStat::default(), 0).unwrap();
        fs.close(fd).unwrap();

        fs.remove_file(fs.root_fd(), "test2.txt").unwrap();

        let meta = fs.metadata(fs.root_fd()).unwrap();
        assert_eq!(meta.size, 0);

        assert_eq!(meta.first_dir_entry, None);
        assert_eq!(meta.last_dir_entry, None);
    }

    #[test]
    fn create_hard_link() {
        let mut fs = test_fs();

        let parent_fd = fs.root_fd();

        fs.create_file(parent_fd, "test1.txt", FdStat::default(), 120).unwrap();

        let fd2 = fs.create_hard_link(parent_fd, "test1.txt", parent_fd, "test2.txt").unwrap();

        let metadata = fs.metadata(fd2).unwrap();

        assert_eq!(120, metadata.times.created);
    }

    #[test]
    fn create_directory_hard_link_fails() {
        let mut fs = test_fs();

        let root_fd = fs.root_fd();

        let dir1_fd = fs.create_dir(root_fd, "dir1", FdStat::default(), 120).unwrap();
        let dir2_fd = fs.create_dir(root_fd, "dir2", FdStat::default(), 123).unwrap();
        let _dir3_fd = fs.create_dir(dir1_fd, "dir3", FdStat::default(), 320).unwrap();
        let dir4_res = fs.create_hard_link(dir1_fd, "dir3", dir2_fd, "dir4");

        assert!(dir4_res.is_err());
    }


    #[test]
    fn rename_a_file() {
        let mut fs = test_fs();

        let root_fd = fs.root_fd();

        let dir1_fd = fs.create_dir(root_fd, "dir1", FdStat::default(), 120).unwrap();
        let file_fd = fs.create_file(root_fd, "test1.txt", FdStat::default(), 120).unwrap();

        let fd2 = fs.rename(root_fd, "test1.txt", dir1_fd, "test2.txt").unwrap();

        let meta = fs.metadata(file_fd).unwrap();
        let meta2 = fs.metadata(fd2).unwrap();

        assert_eq!(meta.node, meta2.node);

        let res = fs.open_or_create(root_fd, "test1.txt", FdStat::default(), OpenFlags::empty(), 123);

        assert_eq!(res, Err(Error::NotFound));

        let res = fs.open_or_create(dir1_fd, "test2.txt", FdStat::default(), OpenFlags::empty(), 123);

        assert!(res.is_ok());

    }


    #[test]
    fn rename_a_folder() {
        let mut fs = test_fs();

        let root_fd = fs.root_fd();

        let dir1_fd = fs.create_dir(root_fd, "dir1", FdStat::default(), 120).unwrap();
        let _file_fd = fs.create_file(dir1_fd, "test1.txt", FdStat::default(), 123).unwrap();

        let dir2_fd = fs.create_dir(root_fd, "dir2", FdStat::default(), 125).unwrap();

        let fd2 = fs.rename(root_fd, "dir1", dir2_fd, "dir3").unwrap();

        let meta = fs.metadata(dir1_fd).unwrap();
        let meta2 = fs.metadata(fd2).unwrap();

        assert_eq!(meta.node, meta2.node);

        let res = fs.open_or_create(root_fd, "dir1", FdStat::default(), OpenFlags::empty(), 123);

        assert_eq!(res, Err(Error::NotFound));

        let res = fs.open_or_create(dir2_fd, "dir3", FdStat::default(), OpenFlags::empty(), 123);

        assert!(res.is_ok());

    }



}

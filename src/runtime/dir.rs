use std::collections::BTreeMap;

use crate::{
    error::Error,
    filename_cache::FilenameCache,
    runtime::file::File,
    storage::{
        Storage,
        types::{DirEntry, DirEntryIndex, FileType, Node},
    },
};

use super::{
    structure_helpers::{create_path, find_node, rm_dir_entry},
    types::FdStat,
};

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
            FileType::SymbolicLink => unimplemented!("Symbolic links are not supported"),
        };
        Ok(Self { node, stat })
    }

    // Create directory entry in the current directory.
    pub fn create_dir(
        &self,
        path: &str,
        stat: FdStat,
        names_cache: &mut FilenameCache,
        storage: &mut dyn Storage,
        ctime: u64,
    ) -> Result<Self, Error> {
        let found = find_node(self.node, path, names_cache, storage);

        match found {
            Err(Error::NoSuchFileOrDirectory) => {}
            Ok(_) => return Err(Error::FileExists),
            Err(err) => return Err(err),
        }

        let (node, _leaf_name) =
            create_path(self.node, path, Some(FileType::Directory), ctime, storage)?;

        names_cache.add((self.node, path.to_string()), node);

        Self::new(node, stat, storage)
    }

    // Remove directory entry from the current directory.
    pub fn remove_dir(
        &self,
        path: &str,
        node_refcount: &BTreeMap<Node, usize>,
        names_cache: &mut FilenameCache,
        storage: &mut dyn Storage,
    ) -> Result<(), Error> {
        let (node, metadata) = rm_dir_entry(
            self.node,
            path,
            Some(true),
            false,
            node_refcount,
            names_cache,
            storage,
        )?;

        if metadata.link_count == 0 {
            storage.rm_file(node)?;
        }

        Ok(())
    }

    // Create file entry in the current directory.
    pub fn create_file(
        &self,
        path: &str,
        stat: FdStat,
        names_cache: &mut FilenameCache,
        storage: &mut dyn Storage,
        ctime: u64,
    ) -> Result<File, Error> {
        let found = find_node(self.node, path, names_cache, storage);
        match found {
            Err(Error::NoSuchFileOrDirectory) => {}
            Ok(_) => return Err(Error::FileExists),
            Err(err) => return Err(err),
        }

        let (node, _leaf_name) =
            create_path(self.node, path, Some(FileType::RegularFile), ctime, storage)?;

        names_cache.add((self.node, path.to_string()), node);

        File::new(node, stat, storage)
    }

    // Remove file entry from the current directory.
    pub fn remove_file(
        &self,
        path: &str,
        node_refcount: &BTreeMap<Node, usize>,
        names_cache: &mut FilenameCache,
        storage: &mut dyn Storage,
    ) -> Result<(), Error> {
        let (node, metadata) = rm_dir_entry(
            self.node,
            path,
            Some(false),
            false,
            node_refcount,
            names_cache,
            storage,
        )?;

        if metadata.link_count == 0 {
            storage.rm_file(node)?;
        }

        Ok(())
    }

    // Get directory entry by index.
    pub fn get_entry(
        &self,
        index: DirEntryIndex,
        storage: &dyn Storage,
    ) -> Result<DirEntry, Error> {
        storage.get_direntry(self.node, index)
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        error::Error, fs::OpenFlags, runtime::types::FdStat, test_utils::test_stable_fs_v2,
    };

    #[test]
    fn remove_middle_file() {
        let mut fs = test_stable_fs_v2();

        let dir = fs.root_fd();

        let fd = fs
            .create_open_file(dir, "test1.txt", FdStat::default(), 0)
            .unwrap();
        fs.close(fd).unwrap();
        let fd = fs
            .create_open_file(dir, "test2.txt", FdStat::default(), 0)
            .unwrap();
        fs.close(fd).unwrap();
        let fd = fs
            .create_open_file(dir, "test3.txt", FdStat::default(), 0)
            .unwrap();
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
        let mut fs = test_stable_fs_v2();

        let dir = fs.root_fd();

        let new_dir_fd = fs
            .create_open_directory(dir, "dir1", FdStat::default(), 123)
            .unwrap();

        let new_file_fd = fs
            .create_open_file(dir, "test.txt", FdStat::default(), 234)
            .unwrap();

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
        let mut fs = test_stable_fs_v2();

        let dir = fs.root_fd();

        let fd = fs
            .create_open_file(dir, "test2.txt", FdStat::default(), 0)
            .unwrap();
        fs.close(fd).unwrap();

        fs.remove_file(fs.root_fd(), "test2.txt").unwrap();

        let meta = fs.metadata(fs.root_fd()).unwrap();
        assert_eq!(meta.size, 0);

        assert_eq!(meta.first_dir_entry, None);
        assert_eq!(meta.last_dir_entry, None);
    }

    #[test]
    fn find_entry_index_finds_by_exact_name() {
        let mut fs = test_stable_fs_v2();

        let dir = fs.root_fd();

        let fd = fs
            .create_open_file(dir, "test2.txt", FdStat::default(), 0)
            .unwrap();

        fs.close(fd).unwrap();

        let res = fs.remove_file(fs.root_fd(), "test2.tx");

        assert!(res.is_err());

        let meta = fs.metadata(fs.root_fd()).unwrap();
        assert_eq!(meta.size, 1);

        fs.remove_file(fs.root_fd(), "test2.txt").unwrap();

        let meta = fs.metadata(fs.root_fd()).unwrap();
        assert_eq!(meta.size, 0);
    }

    #[test]
    fn create_hard_link() {
        let mut fs = test_stable_fs_v2();

        let parent_fd = fs.root_fd();

        fs.create_open_file(parent_fd, "test1.txt", FdStat::default(), 120)
            .unwrap();

        let fd2 = fs
            .create_hard_link(parent_fd, "test1.txt", parent_fd, "test2.txt")
            .unwrap();

        let metadata = fs.metadata(fd2).unwrap();

        assert_eq!(120, metadata.times.created);
    }

    #[test]
    fn create_directory_hard_link_fails() {
        let mut fs = test_stable_fs_v2();

        let root_fd = fs.root_fd();

        let dir1_fd = fs
            .create_open_directory(root_fd, "dir1", FdStat::default(), 120)
            .unwrap();
        let dir2_fd = fs
            .create_open_directory(root_fd, "dir2", FdStat::default(), 123)
            .unwrap();
        let _dir3_fd = fs
            .create_open_directory(dir1_fd, "dir3", FdStat::default(), 320)
            .unwrap();
        let dir4_res = fs.create_hard_link(dir1_fd, "dir3", dir2_fd, "dir4");

        assert!(dir4_res.is_err());
    }

    #[test]
    fn open_file_from_a_subfolder() {
        let mut fs = test_stable_fs_v2();

        let root_fd = fs.root_fd();

        let dir1_fd = fs
            .create_open_directory(root_fd, "dir1", FdStat::default(), 120)
            .unwrap();

        let file_fd = fs
            .create_open_file(dir1_fd, "test1.txt", FdStat::default(), 120)
            .unwrap();

        fs.close(file_fd).unwrap();

        let res = fs.open(
            root_fd,
            "test1.txt",
            FdStat::default(),
            OpenFlags::empty(),
            123,
        );

        assert_eq!(res, Err(Error::NoSuchFileOrDirectory));

        let res = fs.open(
            root_fd,
            "dir1/test1.txt",
            FdStat::default(),
            OpenFlags::empty(),
            123,
        );

        //        assert_eq!(res, Err(Error::NotFound));
        assert!(res.is_ok());
    }

    #[test]
    fn rename_a_file() {
        let mut fs = test_stable_fs_v2();

        let root_fd = fs.root_fd();

        let dir1_fd = fs
            .create_open_directory(root_fd, "dir1", FdStat::default(), 120)
            .unwrap();

        let file_fd = fs
            .create_open_file(root_fd, "test1.txt", FdStat::default(), 120)
            .unwrap();

        let fd2 = fs
            .rename(root_fd, "test1.txt", dir1_fd, "test2.txt")
            .unwrap();

        let meta = fs.metadata(file_fd).unwrap();
        let meta2 = fs.metadata(fd2).unwrap();

        assert_eq!(meta.node, meta2.node);

        let res = fs.open(
            root_fd,
            "test1.txt",
            FdStat::default(),
            OpenFlags::empty(),
            123,
        );

        assert_eq!(res, Err(Error::NoSuchFileOrDirectory));

        let res = fs.open(
            dir1_fd,
            "test2.txt",
            FdStat::default(),
            OpenFlags::empty(),
            123,
        );

        assert!(res.is_ok());
    }

    #[test]
    fn rename_a_file_with_subfolders() {
        let mut fs = test_stable_fs_v2();

        let root_fd = fs.root_fd();

        let dir1_fd = fs
            .create_open_directory(root_fd, "dir1", FdStat::default(), 120)
            .unwrap();

        let file_fd = fs
            .create_open_file(root_fd, "test1.txt", FdStat::default(), 120)
            .unwrap();

        let fd2 = fs
            .rename(root_fd, "test1.txt", root_fd, "dir1/test2.txt")
            .unwrap();

        let meta = fs.metadata(file_fd).unwrap();
        let meta2 = fs.metadata(fd2).unwrap();

        assert_eq!(meta.node, meta2.node);

        let res = fs.open(
            root_fd,
            "test1.txt",
            FdStat::default(),
            OpenFlags::empty(),
            123,
        );

        assert_eq!(res, Err(Error::NoSuchFileOrDirectory));

        let res = fs.open(
            dir1_fd,
            "test2.txt",
            FdStat::default(),
            OpenFlags::empty(),
            123,
        );

        assert!(res.is_ok());
    }

    #[test]
    fn rename_a_file_with_subfolders2() {
        let mut fs = test_stable_fs_v2();

        let root_fd = fs.root_fd();

        let dir1_fd = fs
            .create_open_directory(root_fd, "dir1", FdStat::default(), 120)
            .unwrap();

        let file_fd = fs
            .create_open_file(dir1_fd, "test1.txt", FdStat::default(), 120)
            .unwrap();

        let fd2 = fs
            .rename(root_fd, "dir1/test1.txt", root_fd, "test2.txt")
            .unwrap();

        let meta = fs.metadata(file_fd).unwrap();
        let meta2 = fs.metadata(fd2).unwrap();

        assert_eq!(meta.node, meta2.node);

        let res = fs.open(
            dir1_fd,
            "test1.txt",
            FdStat::default(),
            OpenFlags::empty(),
            123,
        );

        assert_eq!(res, Err(Error::NoSuchFileOrDirectory));

        let res = fs.open(
            root_fd,
            "test2.txt",
            FdStat::default(),
            OpenFlags::empty(),
            123,
        );

        assert!(res.is_ok());
    }

    #[test]
    fn rename_a_file_using_path_with_subfolders() {
        let mut fs = test_stable_fs_v2();

        let root_fd = fs.root_fd();

        let dir1_fd = fs
            .create_open_directory(root_fd, "dir1", FdStat::default(), 120)
            .unwrap();

        let file_fd = fs
            .create_open_file(root_fd, "test1.txt", FdStat::default(), 120)
            .unwrap();

        let fd2 = fs
            .rename(root_fd, "test1.txt", root_fd, "dir1/test2.txt")
            .unwrap();

        let meta = fs.metadata(file_fd).unwrap();
        let meta2 = fs.metadata(fd2).unwrap();

        assert_eq!(meta.node, meta2.node);

        let res = fs.open(
            root_fd,
            "test1.txt",
            FdStat::default(),
            OpenFlags::empty(),
            123,
        );

        assert_eq!(res, Err(Error::NoSuchFileOrDirectory));

        let res = fs.open(
            dir1_fd,
            "test2.txt",
            FdStat::default(),
            OpenFlags::empty(),
            123,
        );

        assert!(res.is_ok());
    }

    #[test]
    fn rename_a_folder() {
        let mut fs = test_stable_fs_v2();

        let root_fd = fs.root_fd();

        let dir1_fd = fs
            .create_open_directory(root_fd, "dir1", FdStat::default(), 120)
            .unwrap();

        let _file_fd = fs
            .create_open_file(dir1_fd, "test1.txt", FdStat::default(), 123)
            .unwrap();

        let dir2_fd = fs
            .create_open_directory(root_fd, "dir2", FdStat::default(), 125)
            .unwrap();

        let fd2 = fs.rename(root_fd, "dir1", dir2_fd, "dir3").unwrap();

        let meta = fs.metadata(dir1_fd).unwrap();
        let meta2 = fs.metadata(fd2).unwrap();

        assert_eq!(meta.node, meta2.node);

        let res = fs.open(root_fd, "dir1", FdStat::default(), OpenFlags::empty(), 123);

        assert_eq!(res, Err(Error::NoSuchFileOrDirectory));

        let res = fs.open(dir2_fd, "dir3", FdStat::default(), OpenFlags::empty(), 123);

        assert!(res.is_ok());
    }

    #[test]
    fn rename_a_folder_having_subfolders() {
        let mut fs = test_stable_fs_v2();

        let root_fd = fs.root_fd();

        let dir1_fd = fs
            .create_open_directory(root_fd, "dir1", FdStat::default(), 120)
            .unwrap();

        let _file_fd = fs
            .create_open_file(dir1_fd, "test1.txt", FdStat::default(), 123)
            .unwrap();

        let dir2_fd = fs
            .create_open_directory(root_fd, "dir2", FdStat::default(), 125)
            .unwrap();

        let fd2 = fs.rename(root_fd, "dir1", root_fd, "dir2/dir3").unwrap();

        let meta = fs.metadata(dir1_fd).unwrap();
        let meta2 = fs.metadata(fd2).unwrap();

        assert_eq!(meta.node, meta2.node);

        let res = fs.open(root_fd, "dir1", FdStat::default(), OpenFlags::empty(), 123);

        assert_eq!(res, Err(Error::NoSuchFileOrDirectory));

        let res = fs.open(dir2_fd, "dir3", FdStat::default(), OpenFlags::empty(), 123);

        assert!(res.is_ok());
    }
}

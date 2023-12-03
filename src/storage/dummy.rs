
use crate::error::Error;

use super::{Storage, types::{Node, DirEntryIndex, DirEntry, Metadata, FileChunkIndex, FileSize}};

pub struct DummyStorage {
}

impl DummyStorage {

    pub fn new() -> Self {
        Self {

        }
    }

}

impl Storage for DummyStorage {

    fn root_node(&self) -> Node {
        panic!("Not supported")
    }

    fn new_node(&mut self) -> Node {
        panic!("Not supported")
    }

    fn get_version(&self) -> u32 {
        0
    }
    
    fn get_metadata(&self, _node: Node) -> Result<Metadata, Error> {
        panic!("Not supported")
    }

    fn put_metadata(&mut self, _node: Node, _metadata: Metadata) {
        panic!("Not supported")
    }

    fn rm_metadata(&mut self, _node: Node) {
        panic!("Not supported")
    }

    fn get_direntry(&self, _node: Node, _index: DirEntryIndex) -> Result<DirEntry, Error> {
        panic!("Not supported")
    }

    fn put_direntry(&mut self, _node: Node, _index: DirEntryIndex, _entry: DirEntry) {
        panic!("Not supported")
    }

    fn rm_direntry(&mut self, _node: Node, _index: DirEntryIndex) {
        panic!("Not supported")
    }

    fn read_filechunk(
        &self,
        _node: Node,
        _index: FileChunkIndex,
        _offset: FileSize,
        _buf: &mut [u8],
    ) -> Result<(), Error> {
        panic!("Not supported")
    }

    fn write_filechunk(&mut self, _node: Node, _index: FileChunkIndex, _offset: FileSize, _buf: &[u8]) {
        panic!("Not supported")
    }

    fn rm_filechunk(&mut self, _node: Node, _index: FileChunkIndex) {
        panic!("Not supported")
    }
}


#[cfg(test)]
mod tests {

    use crate::storage::types::{FileType, Times};
    use super::*;

    #[test]
    #[should_panic]
    fn put_metadata_panic() {
        let mut storage = DummyStorage::new();
        let node = storage.new_node();
        storage.put_metadata(
            node,
            Metadata {
                node,
                file_type: FileType::RegularFile,
                link_count: 1,
                size: 10,
                times: Times::default(),
                first_dir_entry: Some(42),
                last_dir_entry: Some(24),
            },
        )
    }


    #[test]
    #[should_panic]
    fn get_metadata_panic() {
        let storage = DummyStorage::new();

        let _ = storage.get_metadata(0u64);
    }  

    #[test]
    #[should_panic]
    fn rm_metadata_panic() {
        let mut storage = DummyStorage::new();
        let node = storage.new_node();

        let _ = storage.rm_metadata(node);
    }   



    #[test]
    #[should_panic]
    fn new_node_panic() {
        let mut storage = DummyStorage::new();
        storage.new_node();
    }

    #[test]
    #[should_panic]
    fn root_node_panic() {
        let storage = DummyStorage::new();
        storage.root_node();
    }


    #[test]
    fn get_version_panic() {
        let storage = DummyStorage::new();
        assert_eq!(0, storage.get_version());
    }

    #[test]
    #[should_panic]
    fn get_direntry_panic() {
        let storage = DummyStorage::new();
        let _ = storage.get_direntry(0u64, 0u32);
    }

    #[test]
    #[should_panic]
    fn put_direntry_panic() {
        let mut storage = DummyStorage::new();
        storage.put_direntry(0u64, 0u32, DirEntry::default());
    }

    #[test]
    #[should_panic]
    fn rm_direntry_panic() {
        let mut storage = DummyStorage::new();
        storage.rm_direntry(0u64, 0u32);
    }

    #[test]
    #[should_panic]
    fn read_filechunk_panic() {
        let storage = DummyStorage::new();
        let _ = storage.read_filechunk(0, 0, 0, &mut []);
    }

    #[test]
    #[should_panic]
    fn write_filechunk_panic() {
        let mut storage = DummyStorage::new();
        storage.write_filechunk(0, 0, 0, &[]);
    }

    #[test]
    #[should_panic]
    fn rm_filechunk_panic() {
        let mut storage = DummyStorage::new();
        storage.rm_filechunk(0, 0);
    }

}

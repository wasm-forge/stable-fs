use crate::{error::Error, fs::ChunkSize, fs::ChunkType};

use super::{
    Storage,
    types::{DirEntry, DirEntryIndex, FileSize, Metadata, Node},
};

pub struct DummyStorage {}

impl DummyStorage {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for DummyStorage {
    fn default() -> Self {
        Self::new()
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

    fn put_metadata(&mut self, _node: Node, _metadata: &Metadata) -> Result<(), Error> {
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

    fn read(&mut self, _node: Node, _offset: FileSize, _buf: &mut [u8]) -> Result<FileSize, Error> {
        panic!("Not supported")
    }

    fn mount_node(
        &mut self,
        _node: Node,
        _memory: Box<dyn ic_stable_structures::Memory>,
    ) -> Result<(), Error> {
        panic!("Not supported")
    }

    fn unmount_node(
        &mut self,
        _node: Node,
    ) -> Result<Box<dyn ic_stable_structures::Memory>, Error> {
        panic!("Not supported")
    }

    fn is_mounted(&self, _node: Node) -> bool {
        panic!("Not supported")
    }

    fn get_mounted_memory(&self, _node: Node) -> Option<&dyn ic_stable_structures::Memory> {
        panic!("Not supported")
    }

    fn init_mounted_memory(&mut self, _node: Node) -> Result<(), Error> {
        panic!("Not supported")
    }

    fn store_mounted_memory(&mut self, _node: Node) -> Result<(), Error> {
        panic!("Not supported")
    }

    fn write(&mut self, _node: Node, _offset: FileSize, _buf: &[u8]) -> Result<FileSize, Error> {
        panic!("Not supported")
    }

    fn rm_file(&mut self, _node: Node) -> Result<(), Error> {
        panic!("Not supported")
    }

    fn set_chunk_size(&mut self, _chunk_size: ChunkSize) -> Result<(), Error> {
        panic!("Not supported")
    }

    fn chunk_size(&self) -> usize {
        panic!("Not supported")
    }

    fn set_chunk_type(&mut self, _chunk_type: ChunkType) {
        panic!("Not supported")
    }

    fn chunk_type(&self) -> ChunkType {
        panic!("Not supported")
    }

    fn flush(&mut self, _node: Node) {
        panic!("Not supported")
    }

    fn resize_file(&mut self, _node: Node, _new_size: FileSize) -> Result<(), Error> {
        panic!("Not supported")
    }

    /*

    fn get_direntries(
        &self,
        _node: Node,
        _initial_index: Option<DirEntryIndex>,
    ) -> Result<Vec<(DirEntryIndex, DirEntry)>, Error> {
        panic!("Not supported")
    }
    */

    fn with_direntries(
        &self,
        _node: Node,
        _initial_index: Option<DirEntryIndex>,
        _f: &mut dyn FnMut(&DirEntryIndex, &DirEntry) -> bool,
    ) {
        panic!("Not supported")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::types::{FileType, Times};

    #[test]
    #[should_panic]
    fn put_metadata_panic() {
        let mut storage = DummyStorage::new();
        let node = storage.new_node();
        storage
            .put_metadata(
                node,
                &Metadata {
                    node,
                    file_type: FileType::RegularFile,
                    link_count: 1,
                    size: 10,
                    times: Times::default(),
                    first_dir_entry: Some(42),
                    last_dir_entry: Some(24),
                    chunk_type: None,
                    maximum_size_allowed: None,
                },
            )
            .unwrap();
    }

    #[test]
    #[should_panic]
    fn get_metadata_panic() {
        let storage = DummyStorage::new();

        let _ = storage.get_metadata(0u64);
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
}

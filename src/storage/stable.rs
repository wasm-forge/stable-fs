use ic_stable_structures::{
    memory_manager::{MemoryId, MemoryManager, VirtualMemory},
    BTreeMap, Memory,
};

use crate::error::Error;

use super::{
    types::{
        DirEntry, DirEntryIndex, FileChunk, FileChunkIndex, FileSize, FileType, Metadata, Node,
        Times,
    },
    Storage,
};

const ROOT_NODE: Node = 0;

const METADATA_MEMORY_INDEX: MemoryId = MemoryId::new(0);
const DIRENTRY_MEMORY_INDEX: MemoryId = MemoryId::new(1);
const FILECHUNK_MEMORY_INDEX: MemoryId = MemoryId::new(2);

pub struct StableStorage<M: Memory> {
    metadata: BTreeMap<Node, Metadata, VirtualMemory<M>>,
    direntry: BTreeMap<(Node, DirEntryIndex), DirEntry, VirtualMemory<M>>,
    filechunk: BTreeMap<(Node, FileChunkIndex), FileChunk, VirtualMemory<M>>,
    next_node: Node,
    // It is not used, but is needed to keep other memories alive.
    _memory_manager: MemoryManager<M>,
}

impl<M: Memory> StableStorage<M> {
    pub fn new(memory: M) -> Self {
        let memory_manager = MemoryManager::init(memory);
        let metadata = Metadata {
            node: ROOT_NODE,
            file_type: FileType::Directory,
            link_count: 1,
            size: 0,
            times: Times::default(),
            first_dir_entry: None,
            last_dir_entry: None,
        };
        let mut result = Self {
            metadata: BTreeMap::init(memory_manager.get(METADATA_MEMORY_INDEX)),
            direntry: BTreeMap::init(memory_manager.get(DIRENTRY_MEMORY_INDEX)),
            filechunk: BTreeMap::init(memory_manager.get(FILECHUNK_MEMORY_INDEX)),
            next_node: ROOT_NODE + 1,
            _memory_manager: memory_manager,
        };
        result.put_metadata(ROOT_NODE, metadata);
        result
    }
}

impl<M: Memory> Storage for StableStorage<M> {
    fn root_node(&self) -> Node {
        ROOT_NODE
    }

    fn new_node(&mut self) -> Node {
        let result = self.next_node;
        self.next_node += 1;
        result
    }

    fn get_metadata(&self, node: Node) -> Result<Metadata, Error> {
        let value = self.metadata.get(&node).ok_or(Error::NotFound)?;
        Ok(value.clone())
    }

    fn put_metadata(&mut self, node: Node, metadata: Metadata) {
        eprintln!("put metadata: {} - {:?}", node, metadata.file_type);
        self.next_node = self.next_node.max(node + 1);
        self.metadata.insert(node, metadata);
    }

    fn rm_metadata(&mut self, node: Node) {
        self.metadata.remove(&node);
    }

    fn get_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error> {
        let value = self.direntry.get(&(node, index)).ok_or(Error::NotFound)?;
        eprintln!(
            "getting dir entry: {} {} - node {}",
            index,
            std::str::from_utf8(&value.name.bytes[0..value.name.length as usize]).unwrap(),
            value.node,
        );
        Ok(value.clone())
    }

    fn put_direntry(&mut self, node: Node, index: DirEntryIndex, entry: DirEntry) {
        eprintln!(
            "adding dir entry: {} {} - node {}",
            index,
            std::str::from_utf8(&entry.name.bytes[0..entry.name.length as usize]).unwrap(),
            entry.node,
        );
        self.direntry.insert((node, index), entry);
    }

    fn rm_direntry(&mut self, node: Node, index: DirEntryIndex) {
        self.direntry.remove(&(node, index));
    }

    fn read_filechunk(
        &self,
        node: Node,
        index: FileChunkIndex,
        offset: FileSize,
        buf: &mut [u8],
    ) -> Result<(), Error> {
        let value = self.filechunk.get(&(node, index)).ok_or(Error::NotFound)?;
        buf.copy_from_slice(&value.bytes[offset as usize..offset as usize + buf.len()]);
        Ok(())
    }

    fn write_filechunk(&mut self, node: Node, index: FileChunkIndex, offset: FileSize, buf: &[u8]) {
        let mut entry = self
            .filechunk
            .get(&(node, index))
            .unwrap_or_else(FileChunk::default);
        entry.bytes[offset as usize..offset as usize + buf.len()].copy_from_slice(buf);
        self.filechunk.insert((node, index), entry);
    }

    fn rm_filechunk(&mut self, node: Node, index: FileChunkIndex) {
        self.filechunk.remove(&(node, index));
    }
}

#[cfg(test)]
mod tests {

    use ic_stable_structures::DefaultMemoryImpl;

    use crate::storage::types::FileName;

    use super::*;

    #[test]
    fn read_and_write_filechunk() {
        let mut storage = StableStorage::new(DefaultMemoryImpl::default());
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
        );
        let metadata = storage.get_metadata(node).unwrap();
        assert_eq!(metadata.node, node);
        assert_eq!(metadata.file_type, FileType::RegularFile);
        assert_eq!(metadata.link_count, 1);
        assert_eq!(metadata.first_dir_entry, Some(42));
        assert_eq!(metadata.last_dir_entry, Some(24));
        storage.write_filechunk(node, 0, 0, &[42; 10]);
        let mut buf = [0; 10];
        storage.read_filechunk(node, 0, 0, &mut buf).unwrap();
        assert_eq!(buf, [42; 10]);
    }

    #[test]
    fn read_and_write_direntry() {
        let mut storage = StableStorage::new(DefaultMemoryImpl::default());
        let node = storage.new_node();
        storage.put_direntry(
            node,
            7,
            DirEntry {
                node,
                name: FileName::new("test").unwrap(),
                next_entry: Some(42),
                prev_entry: Some(24),
            },
        );
        let direntry = storage.get_direntry(node, 7).unwrap();
        assert_eq!(direntry.node, node);
        assert_eq!(direntry.name.bytes, FileName::new("test").unwrap().bytes);
        assert_eq!(direntry.next_entry, Some(42));
        assert_eq!(direntry.prev_entry, Some(24));
    }
}

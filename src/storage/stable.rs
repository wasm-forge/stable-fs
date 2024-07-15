use ic_stable_structures::{
    memory_manager::{MemoryId, MemoryManager, VirtualMemory}, BTreeMap, Cell, Memory
};

use crate::error::Error;

use super::{
    types::{
        DirEntry, DirEntryIndex, FileChunk, FileChunkIndex, FileSize, FileType, Header, Metadata, Node, Times
    },
    Storage,
};

const ROOT_NODE: Node = 0;
const FS_VERSION: u32 = 1;

const DEFAULT_FIRST_MEMORY_INDEX: u8 = 229;
const MAX_MEMORY_INDEX: u8 = 250;

#[repr(C)]
pub struct StableStorage<M: Memory> {
    header: Cell<Header, VirtualMemory<M>>,
    metadata: BTreeMap<Node, Metadata, VirtualMemory<M>>,
    direntry: BTreeMap<(Node, DirEntryIndex), DirEntry, VirtualMemory<M>>,
    filechunk: BTreeMap<(Node, FileChunkIndex), FileChunk, VirtualMemory<M>>,

    // It is not used, but is needed to keep memories alive.
    _memory_manager: Option<MemoryManager<M>>,
}

impl<M: Memory> StableStorage<M> {
    pub fn new(memory: M) -> Self {
        let memory_manager = MemoryManager::init(memory);

        let mut storage = Self::new_with_memory_manager(&memory_manager, DEFAULT_FIRST_MEMORY_INDEX);

        storage._memory_manager = Some(memory_manager);

        storage
    }

    pub fn new_with_memory_manager(
        memory_manager: &MemoryManager<M>,
        first_memory_index: u8,
    ) -> StableStorage<M> {
        if first_memory_index > MAX_MEMORY_INDEX {
            panic!("First memory index must be less than {}", MAX_MEMORY_INDEX);
        }

        let header_memory = memory_manager.get(MemoryId::new(DEFAULT_FIRST_MEMORY_INDEX));
        let metadata_memory = memory_manager.get(MemoryId::new(DEFAULT_FIRST_MEMORY_INDEX + 1u8));
        let direntry_memory = memory_manager.get(MemoryId::new(DEFAULT_FIRST_MEMORY_INDEX + 2u8));
        let filechunk_memory = memory_manager.get(MemoryId::new(DEFAULT_FIRST_MEMORY_INDEX + 3u8));

        let storage = Self::new_with_custom_memories(
            header_memory,
            metadata_memory,
            direntry_memory,
            filechunk_memory,
        );

        storage
    }

    pub fn new_with_custom_memories(
        header: VirtualMemory<M>,
        metadata: VirtualMemory<M>,
        direntry: VirtualMemory<M>,
        filechunk: VirtualMemory<M>,
    ) -> Self {

        let default_header_value = Header {
            version: FS_VERSION,
            next_node: ROOT_NODE + 1,
        };

        let mut result = Self {
            header: Cell::init(header, default_header_value).unwrap(),
            metadata: BTreeMap::init(metadata),
            direntry: BTreeMap::init(direntry),
            filechunk: BTreeMap::init(filechunk),
            _memory_manager: None,
        };

        let version = result.header.get().version;
        
        if version != FS_VERSION {
            panic!("Unsupported file system version");
        }

        match result.get_metadata(ROOT_NODE) {
            Ok(_) => {}
            Err(Error::NotFound) => {
                let metadata = Metadata {
                    node: ROOT_NODE,
                    file_type: FileType::Directory,
                    link_count: 1,
                    size: 0,
                    times: Times::default(),
                    first_dir_entry: None,
                    last_dir_entry: None,
                };
                result.put_metadata(ROOT_NODE, metadata);
            }
            Err(err) => {
                unreachable!("Unexpected error while loading root metadata: {:?}", err);
            }
        }

        result
    }
}

impl<M: Memory> Storage for StableStorage<M> {
    // Get the root node ID of the storage.
    fn root_node(&self) -> Node {
        ROOT_NODE
    }

    // Generate the next available node ID.
    fn new_node(&mut self) -> Node {

        let mut header = self.header.get().clone();

        self.metadata.last_key_value();

        let result = header.next_node;

        header.next_node += 1;

        self.header.set(header).unwrap();

        result
    }

    fn get_version(&self) -> u32 {
        let header = self.header.get();
        header.version
    }

    // Get the metadata associated with the node.
    fn get_metadata(&self, node: Node) -> Result<Metadata, Error> {
        self.metadata.get(&node).ok_or(Error::NotFound)
    }

    // Update the metadata associated with the node.
    fn put_metadata(&mut self, node: Node, metadata: Metadata) {
        self.metadata.insert(node, metadata);
    }

    // Remove the metadata associated with the node.
    fn rm_metadata(&mut self, node: Node) {
        self.metadata.remove(&node);
    }

    // Retrieve the DirEntry instance given the Node and DirEntryIndex.
    fn get_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error> {
        self.direntry.get(&(node, index)).ok_or(Error::NotFound)
    }

    // Update or insert the DirEntry instance given the Node and DirEntryIndex.
    fn put_direntry(&mut self, node: Node, index: DirEntryIndex, entry: DirEntry) {
        self.direntry.insert((node, index), entry);
    }

    // Remove the DirEntry instance given the Node and DirEntryIndex.
    fn rm_direntry(&mut self, node: Node, index: DirEntryIndex) {
        self.direntry.remove(&(node, index));
    }

    // Fill the buffer contents with data of a chosen file chunk.
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

    // Insert of update a selected file chunk with the data provided in buffer.
    fn write_filechunk(&mut self, node: Node, index: FileChunkIndex, offset: FileSize, buf: &[u8]) {
        let mut entry = self.filechunk.get(&(node, index)).unwrap_or_default();
        entry.bytes[offset as usize..offset as usize + buf.len()].copy_from_slice(buf);
        self.filechunk.insert((node, index), entry);
    }

    // Remove file chunk from a given file node.
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
                name: FileName::new("test".as_bytes()).unwrap(),
                next_entry: Some(42),
                prev_entry: Some(24),
            },
        );
        let direntry = storage.get_direntry(node, 7).unwrap();
        assert_eq!(direntry.node, node);
        assert_eq!(
            direntry.name.bytes,
            FileName::new("test".as_bytes()).unwrap().bytes
        );
        assert_eq!(direntry.next_entry, Some(42));
        assert_eq!(direntry.prev_entry, Some(24));
    }
}

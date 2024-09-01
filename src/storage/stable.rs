use std::{collections::HashMap, ops::Range};

use ic_cdk::api::stable::WASM_PAGE_SIZE_IN_BYTES;
use ic_stable_structures::{
    memory_manager::{MemoryId, MemoryManager, VirtualMemory},
    BTreeMap, Cell, Memory,
};

use crate::{
    error::Error,
    runtime::{
        structure_helpers::{get_chunk_infos, grow_memory},
        types::ChunkSize,
        types::ChunkType,
    },
};

use super::{
    allocator::ChunkPtrAllocator,
    types::{
        DirEntry, DirEntryIndex, FileChunk, FileChunkIndex, FileChunkPtr, FileSize, FileType,
        Header, Metadata, Node, Times, FILE_CHUNK_SIZE_V1, MAX_FILE_CHUNK_SIZE_V2,
    },
    Storage,
};

const ROOT_NODE: Node = 0;
const FS_VERSION: u32 = 1;

const DEFAULT_FIRST_MEMORY_INDEX: u8 = 229;

// the maximum index accepted as the end range
const MAX_MEMORY_INDEX: u8 = 254;

// the number of memory indices used by the file system (currently 8 plus some reserved ids)
const MEMORY_INDEX_COUNT: u8 = 10;

const ZEROES: [u8; MAX_FILE_CHUNK_SIZE_V2] = [0u8; MAX_FILE_CHUNK_SIZE_V2];

struct StorageMemories<M: Memory> {
    header_memory: VirtualMemory<M>,
    metadata_memory: VirtualMemory<M>,
    direntry_memory: VirtualMemory<M>,
    filechunk_memory: VirtualMemory<M>,
    mounted_meta_memory: VirtualMemory<M>,

    v2_chunk_ptr_memory: VirtualMemory<M>,
    v2_chunks_memory: VirtualMemory<M>,
    v2_allocator_memory: VirtualMemory<M>,
}

#[repr(C)]
pub struct StableStorage<M: Memory> {
    header: Cell<Header, VirtualMemory<M>>,
    metadata: BTreeMap<Node, Metadata, VirtualMemory<M>>,
    direntry: BTreeMap<(Node, DirEntryIndex), DirEntry, VirtualMemory<M>>,
    filechunk: BTreeMap<(Node, FileChunkIndex), FileChunk, VirtualMemory<M>>,
    mounted_meta: BTreeMap<Node, Metadata, VirtualMemory<M>>,

    v2_chunk_ptr: BTreeMap<(Node, FileChunkIndex), FileChunkPtr, VirtualMemory<M>>,
    v2_chunks: VirtualMemory<M>,
    v2_allocator: ChunkPtrAllocator<M>,

    // It is not used, but is needed to keep memories alive.
    _memory_manager: Option<MemoryManager<M>>,
    // active mounts
    active_mounts: HashMap<Node, Box<dyn Memory>>,

    // chunk type when creating new files
    chunk_type: ChunkType,

    // primitive cache
    last_index: (Node, FileChunkIndex, FileChunkPtr),
}

impl<M: Memory> StableStorage<M> {
    pub fn new(memory: M) -> Self {
        let memory_manager = MemoryManager::init(memory);

        let mut storage = Self::new_with_memory_manager(
            &memory_manager,
            DEFAULT_FIRST_MEMORY_INDEX..DEFAULT_FIRST_MEMORY_INDEX + MEMORY_INDEX_COUNT,
        );

        storage._memory_manager = Some(memory_manager);

        storage
    }

    pub fn new_with_memory_manager(
        memory_manager: &MemoryManager<M>,
        memory_indices: Range<u8>,
    ) -> StableStorage<M> {
        if memory_indices.end - memory_indices.start < MEMORY_INDEX_COUNT {
            panic!(
                "The memory index range must include at least {} incides",
                MEMORY_INDEX_COUNT
            );
        }

        if memory_indices.end > MAX_MEMORY_INDEX {
            panic!(
                "Last memory index must be less than or equal to {}",
                MAX_MEMORY_INDEX
            );
        }

        let header_memory = memory_manager.get(MemoryId::new(memory_indices.start));
        let metadata_memory = memory_manager.get(MemoryId::new(memory_indices.start + 1u8));
        let direntry_memory = memory_manager.get(MemoryId::new(memory_indices.start + 2u8));
        let filechunk_memory = memory_manager.get(MemoryId::new(memory_indices.start + 3u8));
        let mounted_meta_memory = memory_manager.get(MemoryId::new(memory_indices.start + 4u8));

        let v2_chunk_ptr_memory = memory_manager.get(MemoryId::new(memory_indices.start + 5u8));
        let v2_chunks_memory = memory_manager.get(MemoryId::new(memory_indices.start + 7u8));
        let v2_allocator_memory = memory_manager.get(MemoryId::new(memory_indices.start + 6u8));

        let memories = StorageMemories {
            header_memory,
            metadata_memory,
            direntry_memory,
            filechunk_memory,
            mounted_meta_memory,
            v2_chunk_ptr_memory,
            v2_chunks_memory,
            v2_allocator_memory,
        };

        Self::new_with_custom_memories(memories)
    }

    fn new_with_custom_memories(memories: StorageMemories<M>) -> Self {
        let default_header_value = Header {
            version: FS_VERSION,
            next_node: ROOT_NODE + 1,
        };

        let v2_allocator = ChunkPtrAllocator::new(memories.v2_allocator_memory).unwrap();

        let mut result = Self {
            header: Cell::init(memories.header_memory, default_header_value).unwrap(),
            metadata: BTreeMap::init(memories.metadata_memory),
            direntry: BTreeMap::init(memories.direntry_memory),
            filechunk: BTreeMap::init(memories.filechunk_memory),
            mounted_meta: BTreeMap::init(memories.mounted_meta_memory),

            v2_chunk_ptr: BTreeMap::init(memories.v2_chunk_ptr_memory),
            v2_chunks: memories.v2_chunks_memory,
            v2_allocator,

            // transient runtime data
            _memory_manager: None,
            active_mounts: HashMap::new(),
            // default chunk type is V2
            chunk_type: ChunkType::V2,
            last_index: (0, 0, 0),
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

    // write into mounted memory
    fn write_mounted(&self, memory: &dyn Memory, offset: FileSize, buf: &[u8]) -> FileSize {
        let length_to_write = buf.len() as FileSize;

        // grow memory if needed
        let max_address = offset as FileSize + length_to_write;

        grow_memory(memory, max_address);

        memory.write(offset, buf);

        length_to_write
    }

    // Insert of update a selected file chunk with the data provided in a buffer.
    fn write_filechunk_v1(
        &mut self,
        node: Node,
        index: FileChunkIndex,
        offset: FileSize,
        buf: &[u8],
    ) {
        let mut entry = self.filechunk.get(&(node, index)).unwrap_or_default();
        entry.bytes[offset as usize..offset as usize + buf.len()].copy_from_slice(buf);
        self.filechunk.insert((node, index), entry);
    }

    // Insert or update a selected file chunk with the data provided in a buffer.
    fn write_filechunk_v2(
        &mut self,
        node: Node,
        index: FileChunkIndex,
        offset: FileSize,
        buf: &[u8],
    ) {
        let len = buf.len();

        assert!(len <= self.v2_allocator.chunk_size());

        let chunk_ptr = if self.last_index.0 == node && self.last_index.1 == index {
            self.last_index.2
        } else if let Some(ptr) = self.v2_chunk_ptr.get(&(node, index)) {
            ptr
        } else {
            let ptr = self.v2_allocator.allocate();
            // init with 0
            let remainder = self.chunk_size() as FileSize - (offset as FileSize + len as FileSize);

            grow_memory(&self.v2_chunks, ptr + self.chunk_size() as FileSize);

            self.v2_chunks.write(
                ptr + offset + len as FileSize,
                &ZEROES[0..remainder as usize],
            );

            self.v2_chunk_ptr.insert((node, index), ptr);

            ptr
        };

        self.last_index = (node, index, chunk_ptr);

        self.v2_chunks.write(chunk_ptr + offset, buf);
    }

    fn read_chunks_v1(
        &self,
        node: Node,
        offset: FileSize,
        file_size: FileSize,
        buf: &mut [u8],
    ) -> Result<FileSize, Error> {
        let start_index = (offset / FILE_CHUNK_SIZE_V1 as FileSize) as FileChunkIndex;

        let mut chunk_offset = offset - start_index as FileSize * FILE_CHUNK_SIZE_V1 as FileSize;

        let range = (node, start_index)..(node + 1, 0);

        let mut size_read: FileSize = 0;
        let mut remainder = file_size - offset;

        for ((nd, _idx), value) in self.filechunk.range(range) {
            assert!(nd == node);

            // finished reading, buffer full
            if size_read == buf.len() as FileSize {
                break;
            }

            let chunk_space = FILE_CHUNK_SIZE_V1 as FileSize - chunk_offset;

            let to_read = remainder
                .min(chunk_space)
                .min(buf.len() as FileSize - size_read);

            let write_buf = &mut buf[size_read as usize..size_read as usize + to_read as usize];

            write_buf.copy_from_slice(
                &value.bytes[chunk_offset as usize..chunk_offset as usize + to_read as usize],
            );

            chunk_offset = 0;

            size_read += to_read;
            remainder -= to_read;
        }

        Ok(size_read)
    }

    fn read_chunks_v2(
        &mut self,
        node: Node,
        offset: FileSize,
        file_size: FileSize,
        buf: &mut [u8],
    ) -> Result<FileSize, Error> {
        // compute remainder to read
        let mut remainder = file_size - offset;

        // early exit if nothing left to read
        if remainder == 0 {
            return Ok(0 as FileSize);
        }

        let chunk_size = self.chunk_size();

        let start_index = (offset / chunk_size as FileSize) as FileChunkIndex;
        let end_index =
            ((offset + buf.len() as FileSize) / chunk_size as FileSize + 1) as FileChunkIndex;

        let mut chunk_offset = offset - start_index as FileSize * chunk_size as FileSize;

        let mut range = (node, start_index)..(node, end_index);

        let mut size_read: FileSize = 0;

        if self.last_index.0 == node && self.last_index.1 == start_index {
            //
            let chunk_ptr = self.last_index.2;

            let chunk_space = chunk_size as FileSize - chunk_offset;

            let to_read = remainder
                .min(chunk_space)
                .min(buf.len() as FileSize - size_read);

            let read_buf = &mut buf[size_read as usize..size_read as usize + to_read as usize];

            self.v2_chunks.read(chunk_ptr + chunk_offset, read_buf);

            chunk_offset = 0;

            size_read += to_read;
            remainder -= to_read;

            range = (node, start_index + 1)..(node, end_index);
        }

        // early exit, if managed to completely read from the cached ptr
        if size_read == buf.len() as FileSize {
            return Ok(size_read);
        }

        for ((nd, idx), chunk_ptr) in self.v2_chunk_ptr.range(range) {
            assert!(nd == node);

            // finished reading, buffer full
            if size_read == buf.len() as FileSize {
                break;
            }

            let chunk_space = chunk_size as FileSize - chunk_offset;

            let to_read = remainder
                .min(chunk_space)
                .min(buf.len() as FileSize - size_read);

            let read_buf = &mut buf[size_read as usize..size_read as usize + to_read as usize];

            self.v2_chunks.read(chunk_ptr + chunk_offset, read_buf);

            chunk_offset = 0;

            size_read += to_read;
            remainder -= to_read;

            self.last_index = (node, idx, chunk_ptr);
        }

        Ok(size_read)
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
        if self.is_mounted(node) {
            self.mounted_meta.get(&node).ok_or(Error::NotFound)
        } else {
            self.metadata.get(&node).ok_or(Error::NotFound)
        }
    }

    // Update the metadata associated with the node.
    fn put_metadata(&mut self, node: Node, metadata: Metadata) {
        assert_eq!(node, metadata.node, "Node does not match medatada.node!");

        if self.is_mounted(node) {
            self.mounted_meta.insert(node, metadata);
        } else {
            self.metadata.insert(node, metadata);
        }
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

    // Fill the buffer contents with data of a chosen data range.
    fn read(&mut self, node: Node, offset: FileSize, buf: &mut [u8]) -> Result<FileSize, Error> {
        let metadata = self.get_metadata(node)?;

        let file_size = metadata.size;

        if offset >= file_size {
            return Ok(0);
        }

        let size_read = if let Some(memory) = self.active_mounts.get(&node) {
            let remainder = file_size - offset;
            let to_read = remainder.min(buf.len() as FileSize);

            memory.read(offset, &mut buf[..to_read as usize]);
            to_read
        } else {
            let mut use_v2 = true;

            if metadata.size > 0 {
                // try to find the first v2 node, if not found use v1

                // if cache contains the requested value, we know it is v2 already
                if self.last_index.0 != node && self.v2_chunk_ptr.get(&(node, 0)).is_none() {
                    // TODO: adapt to sparse files
                    use_v2 = false;
                }
            }

            if use_v2 {
                self.read_chunks_v2(node, offset, file_size, buf)?
            } else {
                self.read_chunks_v1(node, offset, file_size, buf)?
            }
        };

        Ok(size_read)
    }

    // Write file at the current file cursor, the cursor position will NOT be updated after reading.
    fn write(&mut self, node: Node, offset: FileSize, buf: &[u8]) -> Result<FileSize, Error> {
        let mut metadata = self.get_metadata(node)?;

        let written_size = if let Some(memory) = self.get_mounted_memory(node) {
            self.write_mounted(memory, offset, buf);

            buf.len() as FileSize
        } else {
            let end = offset + buf.len() as FileSize;
            let mut written_size = 0;

            // decide if we use v2, first based on configuration, second: based on actual content
            let mut use_v2 = self.chunk_type == ChunkType::V2;

            if metadata.size > 0 {
                // try to find any v2 node, othersize use v1

                // TODO: make this work with the iterator to support sparse files
                let ptr = self.v2_chunk_ptr.get(&(node, 0));

                if ptr.is_none() {
                    use_v2 = false;
                }
            }

            if use_v2 {
                let chunk_infos = get_chunk_infos(offset, end, self.chunk_size());

                for chunk in chunk_infos.into_iter() {
                    self.write_filechunk_v2(
                        node,
                        chunk.index,
                        chunk.offset,
                        &buf[written_size..written_size + chunk.len as usize],
                    );

                    written_size += chunk.len as usize;
                }
            } else {
                let chunk_infos = get_chunk_infos(offset, end, FILE_CHUNK_SIZE_V1);

                for chunk in chunk_infos.into_iter() {
                    self.write_filechunk_v1(
                        node,
                        chunk.index,
                        chunk.offset,
                        &buf[written_size..written_size + chunk.len as usize],
                    );

                    written_size += chunk.len as usize;
                }
            }

            written_size as FileSize
        };

        let end = offset + buf.len() as FileSize;
        if end > metadata.size {
            metadata.size = end;
            self.put_metadata(node, metadata);
        }

        Ok(written_size)
    }

    //
    fn rm_file(&mut self, node: Node) -> Result<(), Error> {
        if self.is_mounted(node) {
            return Err(Error::CannotRemoveMountedMemoryFile);
        }

        // delete v1 chunks
        let range = (node, 0)..(node + 1, 0);
        let mut chunks: Vec<(Node, FileChunkIndex)> = Vec::new();
        for (k, _v) in self.filechunk.range(range) {
            chunks.push(k);
        }

        for (nd, idx) in chunks.into_iter() {
            assert!(nd == node);
            self.filechunk.remove(&(node, idx));
        }

        // delete v2 chunks
        let range = (node, 0)..(node + 1, 0);
        let mut chunks: Vec<(Node, FileChunkIndex)> = Vec::new();
        for (k, _v) in self.v2_chunk_ptr.range(range) {
            chunks.push(k);
        }
        for (nd, idx) in chunks.into_iter() {
            assert!(nd == node);
            let removed = self.v2_chunk_ptr.remove(&(node, idx));

            if let Some(removed) = removed {
                self.v2_allocator.free(removed);
            }
        }

        // clear cache
        self.last_index = (0, 0, 0);

        // remove metadata
        self.mounted_meta.remove(&node);
        self.metadata.remove(&node);

        Ok(())
    }

    fn mount_node(&mut self, node: Node, memory: Box<dyn Memory>) -> Result<(), Error> {
        if self.is_mounted(node) {
            return Err(Error::MemoryFileIsMountedAlready);
        }

        // do extra meta preparation
        let mut meta = self.metadata.get(&node).ok_or(Error::NotFound)?;

        self.active_mounts.insert(node, memory);

        let new_mounted_meta = if let Some(old_mounted_meta) = self.mounted_meta.get(&node) {
            // we can change here something for the new mounted meta
            old_mounted_meta
        } else {
            // take a copy of the file meta, set size to 0 by default
            meta.size = 0;
            meta
        };

        self.mounted_meta.insert(node, new_mounted_meta);

        Ok(())
    }

    fn unmount_node(&mut self, node: Node) -> Result<Box<dyn Memory>, Error> {
        let memory = self.active_mounts.remove(&node);

        memory.ok_or(Error::MemoryFileIsNotMounted)
    }

    fn is_mounted(&self, node: Node) -> bool {
        self.active_mounts.contains_key(&node)
    }

    fn get_mounted_memory(&self, node: Node) -> Option<&dyn Memory> {
        let res: Option<&Box<dyn Memory>> = self.active_mounts.get(&node);

        res.map(|b| b.as_ref())
    }

    fn init_mounted_memory(&mut self, node: Node) -> Result<(), Error> {
        // temporary disable mount to activate access to the original file
        let memory = self.unmount_node(node)?;

        let meta = self.get_metadata(node)?;
        let file_size = meta.size;

        // grow memory if needed
        grow_memory(memory.as_ref(), file_size);

        let mut remainder = file_size;

        let mut buf = [0u8; WASM_PAGE_SIZE_IN_BYTES as usize];

        let mut offset = 0;

        while remainder > 0 {
            let to_read = remainder.min(buf.len() as FileSize);

            self.read(node, offset, &mut buf[..to_read as usize])?;

            memory.write(offset, &buf[..to_read as usize]);

            offset += to_read;
            remainder -= to_read;
        }

        self.mount_node(node, memory)?;

        self.put_metadata(node, meta);

        Ok(())
    }

    fn store_mounted_memory(&mut self, node: Node) -> Result<(), Error> {
        // get current size of the mounted memory
        let meta = self.get_metadata(node)?;
        let file_size = meta.size;

        // temporary disable mount to activate access to the original file
        let memory = self.unmount_node(node)?;

        // grow memory if needed
        grow_memory(memory.as_ref(), file_size);

        let mut remainder = file_size;

        let mut buf = [0u8; WASM_PAGE_SIZE_IN_BYTES as usize];

        let mut offset = 0;

        while remainder > 0 {
            let to_read = remainder.min(buf.len() as FileSize);

            memory.read(offset, &mut buf[..to_read as usize]);

            self.write(node, offset, &buf[..to_read as usize])?;

            offset += to_read;
            remainder -= to_read;
        }

        self.put_metadata(node, meta);

        self.mount_node(node, memory)?;

        Ok(())
    }

    fn set_chunk_size(&mut self, chunk_size: ChunkSize) -> Result<(), Error> {
        self.v2_allocator.set_chunk_size(chunk_size as usize)
    }

    fn chunk_size(&self) -> usize {
        self.v2_allocator.chunk_size()
    }

    fn set_chunk_type(&mut self, chunk_type: ChunkType) {
        self.chunk_type = chunk_type;
    }

    fn chunk_type(&self) -> ChunkType {
        self.chunk_type
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
        storage.write(node, 0, &[42; 10]).unwrap();
        let mut buf = [0; 10];
        storage.read(node, 0, &mut buf).unwrap();
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

use std::{collections::HashMap, ops::Range};

use ic_cdk::api::stable::WASM_PAGE_SIZE_IN_BYTES;
use ic_stable_structures::{
    memory_manager::{MemoryId, MemoryManager, VirtualMemory},
    BTreeMap, Cell, Memory,
};

use crate::{
    error::Error,
    runtime::structure_helpers::{get_chunk_infos, grow_memory},
};

use super::{
    types::{
        DirEntry, DirEntryIndex, FileChunk, FileChunkIndex, FileChunkPtr, FileSize, FileType,
        Header, Metadata, Node, Times, FILE_CHUNK_SIZE,
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

// index for the first u64 containing chunk pointers
const FIRST_PTR_IDX: u64 = 16; // lower numbers are reserved
                               // index containing the total number of
const AVAILABLE_CHUNKS_LEN_IDX: u64 = 0;
// index containing the next address to use, when there are no reusable indices available
const MAX_PTR_IDX: u64 = 1;

const ZEROES: [u8; FILE_CHUNK_SIZE] = [0u8; FILE_CHUNK_SIZE];

struct ChunkPtrAllocator<M: Memory> {
    v2_available_chunks: VirtualMemory<M>,
}

impl<M: Memory> ChunkPtrAllocator<M> {
    pub fn new(v2_available_chunks: VirtualMemory<M>) -> ChunkPtrAllocator<M> {
        // init avaiable chunks
        if v2_available_chunks.size() == 0 {
            v2_available_chunks.grow(1);
            v2_available_chunks.write(0, &0u64.to_le_bytes());
        }

        ChunkPtrAllocator {
            v2_available_chunks,
        }
    }

    fn read_u64(&self, index: u64) -> u64 {
        let mut b = [0u8; 8];
        self.v2_available_chunks.read(index * 8, &mut b);

        u64::from_le_bytes(b)
    }

    fn write_u64(&self, index: u64, value: u64) {
        // we only need to start checking the size at certain index
        if index + 8 >= WASM_PAGE_SIZE_IN_BYTES / 8 {
            grow_memory(&self.v2_available_chunks, index * 8 + 8);
        }

        self.v2_available_chunks
            .write(index * 8, &value.to_le_bytes());
    }

    fn get_len(&self) -> u64 {
        self.read_u64(AVAILABLE_CHUNKS_LEN_IDX)
    }

    fn set_len(&self, new_len: u64) {
        self.write_u64(AVAILABLE_CHUNKS_LEN_IDX, new_len);
    }

    fn get_next_max_ptr(&self) -> u64 {
        let ret = self.read_u64(MAX_PTR_IDX);

        // store the next max pointer
        self.write_u64(MAX_PTR_IDX, ret + FILE_CHUNK_SIZE as u64);

        ret
    }

    fn get_ptr(&self, index: u64) -> u64 {
        self.read_u64(FIRST_PTR_IDX + index)
    }

    fn set_ptr(&self, index: u64, value: u64) {
        self.write_u64(FIRST_PTR_IDX + index, value);
    }

    #[cfg(test)]
    pub fn available_ptrs(&self) -> Vec<u64> {
        let mut res = Vec::new();

        for i in 0..self.get_len() {
            res.push(self.get_ptr(i));
        }

        res
    }

    fn push_ptr(&self, chunk_ptr: FileChunkPtr) {
        let len = self.get_len();

        self.set_ptr(len, chunk_ptr);

        self.set_len(len + 1);
    }

    fn pop_ptr(&self) -> Option<FileChunkPtr> {
        let mut len = self.get_len();

        if len == 0 {
            return None;
        }

        len -= 1;

        let ptr = self.get_ptr(len);

        self.set_len(len);

        Some(ptr)
    }

    pub fn allocate(&mut self) -> FileChunkPtr {
        // try to take from the available chunks
        if let Some(ptr) = self.pop_ptr() {
            return ptr;
        }

        // if there are no available chunks, take the next max known pointer
        self.get_next_max_ptr()
    }

    #[cfg(test)]
    fn check_free(&self, ptr: FileChunkPtr) {
        if ptr % FILE_CHUNK_SIZE as u64 != 0 {
            panic!(
                "Pointer released {} must be a multiple of FILE_CHUNK_SIZE!",
                ptr
            );
        }

        if self.read_u64(MAX_PTR_IDX) <= ptr {
            panic!("Address {} was never allocated!", ptr);
        }

        for p in self.available_ptrs() {
            if p == ptr {
                panic!("Second free of address {}", ptr);
            }
        }
    }

    pub fn free(&mut self, ptr: FileChunkPtr) {
        #[cfg(test)]
        self.check_free(ptr);

        println!("release: {}", ptr / FILE_CHUNK_SIZE as u64);

        self.push_ptr(ptr);
    }
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
        let mounted_meta = memory_manager.get(MemoryId::new(memory_indices.start + 4u8));

        let v2_chunk_ptr = memory_manager.get(MemoryId::new(memory_indices.start + 5u8));
        let v2_chunks = memory_manager.get(MemoryId::new(memory_indices.start + 7u8));
        let v2_allocator_memory = memory_manager.get(MemoryId::new(memory_indices.start + 6u8));

        Self::new_with_custom_memories(
            header_memory,
            metadata_memory,
            direntry_memory,
            filechunk_memory,
            mounted_meta,
            v2_chunk_ptr,
            v2_chunks,
            v2_allocator_memory,
        )
    }

    fn new_with_custom_memories(
        header: VirtualMemory<M>,
        metadata: VirtualMemory<M>,
        direntry: VirtualMemory<M>,
        filechunk: VirtualMemory<M>,
        mounted_meta: VirtualMemory<M>,

        v2_chunk_ptr: VirtualMemory<M>,
        v2_chunks: VirtualMemory<M>,
        v2_allocator_memory: VirtualMemory<M>,
    ) -> Self {
        let default_header_value = Header {
            version: FS_VERSION,
            next_node: ROOT_NODE + 1,
        };

        let v2_allocator = ChunkPtrAllocator::new(v2_allocator_memory);

        let mut result = Self {
            header: Cell::init(header, default_header_value).unwrap(),
            metadata: BTreeMap::init(metadata),
            direntry: BTreeMap::init(direntry),
            filechunk: BTreeMap::init(filechunk),
            mounted_meta: BTreeMap::init(mounted_meta),

            v2_chunk_ptr: BTreeMap::init(v2_chunk_ptr),
            v2_chunks,
            v2_allocator,

            // runtime data
            _memory_manager: None,
            active_mounts: HashMap::new(),
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

        assert!(len <= FILE_CHUNK_SIZE);

        let chunk_ptr = if let Some(ptr) = self.v2_chunk_ptr.get(&(node, index)) {
            ptr
        } else {
            let ptr = self.v2_allocator.allocate();
            // init with 0
            let remainder = FILE_CHUNK_SIZE as FileSize - (offset as FileSize + len as FileSize);

            grow_memory(&self.v2_chunks, ptr + FILE_CHUNK_SIZE as FileSize);

            self.v2_chunks.write(
                ptr + offset + len as FileSize,
                &ZEROES[0..remainder as usize],
            );

            ptr
        };

        self.v2_chunks.write(chunk_ptr + offset, buf);

        self.v2_chunk_ptr.insert((node, index), chunk_ptr);
    }

    fn read_chunks_v1(
        &self,
        node: Node,
        offset: FileSize,
        file_size: FileSize,
        buf: &mut [u8],
    ) -> Result<FileSize, Error> {
        let start_index = (offset / FILE_CHUNK_SIZE as FileSize) as FileChunkIndex;

        let mut chunk_offset = offset - start_index as FileSize * FILE_CHUNK_SIZE as FileSize;

        let range = (node, start_index)..(node + 1, 0);

        let mut size_read: FileSize = 0;
        let mut remainder = file_size - offset;

        for ((nd, _idx), value) in self.filechunk.range(range) {
            assert!(nd == node);

            // finished reading, buffer full
            if size_read == buf.len() as FileSize {
                break;
            }

            let chunk_space = FILE_CHUNK_SIZE as FileSize - chunk_offset;

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
        &self,
        node: Node,
        offset: FileSize,
        file_size: FileSize,
        buf: &mut [u8],
    ) -> Result<FileSize, Error> {
        let start_index = (offset / FILE_CHUNK_SIZE as FileSize) as FileChunkIndex;

        let mut chunk_offset = offset - start_index as FileSize * FILE_CHUNK_SIZE as FileSize;

        let range = (node, start_index)..(node + 1, 0);

        let mut size_read: FileSize = 0;
        let mut remainder = file_size - offset;

        for ((nd, _idx), chunk_ptr) in self.v2_chunk_ptr.range(range) {
            assert!(nd == node);

            // finished reading, buffer full
            if size_read == buf.len() as FileSize {
                break;
            }

            let chunk_space = FILE_CHUNK_SIZE as FileSize - chunk_offset;

            let to_read = remainder
                .min(chunk_space)
                .min(buf.len() as FileSize - size_read);

            let write_buf = &mut buf[size_read as usize..size_read as usize + to_read as usize];

            self.v2_chunks.read(chunk_ptr + chunk_offset, write_buf);

            chunk_offset = 0;

            size_read += to_read;
            remainder -= to_read;
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
        if self.is_mounted(node) {
            self.mounted_meta.insert(node, metadata);
        } else {
            self.metadata.insert(node, metadata);
        }
    }

    // Remove the metadata associated with the node.
    fn rm_metadata(&mut self, node: Node) {
        if self.is_mounted(node) {
            self.mounted_meta.remove(&node);
        } else {
            self.metadata.remove(&node);
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
    fn read(&self, node: Node, offset: FileSize, buf: &mut [u8]) -> Result<FileSize, Error> {
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
                let ptr = self.v2_chunk_ptr.get(&(node, 0));
                if ptr.is_none() {
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
            let chunk_infos = get_chunk_infos(offset, end);
            let mut written_size = 0;

            // decide if we use v2
            let mut use_v2 = true;
            if metadata.size > 0 {
                // try to find the first v2 node, othersize use v1
                let ptr = self.v2_chunk_ptr.get(&(node, 0));
                if ptr.is_none() {
                    use_v2 = false;
                }
            }

            for chunk in chunk_infos.into_iter() {
                if use_v2 {
                    self.write_filechunk_v2(
                        node,
                        chunk.index,
                        chunk.offset,
                        &buf[written_size..written_size + chunk.len as usize],
                    );
                } else {
                    self.write_filechunk_v1(
                        node,
                        chunk.index,
                        chunk.offset,
                        &buf[written_size..written_size + chunk.len as usize],
                    );
                }

                written_size += chunk.len as usize;
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

    // Remove file chunk from a given file node.
    fn rm_filechunk(&mut self, node: Node, index: FileChunkIndex) {
        let removed = self.v2_chunk_ptr.remove(&(node, index));

        if let Some(removed) = removed {
            self.v2_allocator.free(removed);
        } else {
            self.filechunk.remove(&(node, index));
        }
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
}

#[cfg(test)]
mod tests {

    use ic_stable_structures::DefaultMemoryImpl;

    use crate::{storage::types::FileName, test_utils::new_vector_memory};

    use super::*;

    #[test]
    fn chunk_allocator_allocations() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory);

        assert_eq!(allocator.allocate(), 0);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 2);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 3);

        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr * 2);

        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 2);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 4);

        assert!(allocator.available_ptrs().is_empty());

        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr * 2);
        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr * 3);
        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr * 1);

        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 1);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 3);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 2);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 5);
    }

    #[test]
    fn chunk_allocator_allocations2() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory);

        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 0);
        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr * 0);

        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 0);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 1);
        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr * 1);

        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 1);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 2);
        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr * 1);

        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 1);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 3);
    }

    #[test]
    #[should_panic]
    fn double_release_fails() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory);

        assert_eq!(allocator.allocate(), 0);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 2);
        assert_eq!(allocator.allocate(), FILE_CHUNK_SIZE as FileChunkPtr * 3);

        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr * 2);
        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr * 3);
        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr * 2);
    }

    #[test]
    #[should_panic]
    fn unallocated_release_fails() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory);

        allocator.allocate();
        allocator.free(0);
        allocator.allocate();
        allocator.free(FILE_CHUNK_SIZE as FileChunkPtr);
    }

    #[test]
    #[should_panic]
    fn impossible_address_fails() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory);

        allocator.allocate();
        allocator.free(120);
    }

    #[test]
    fn chunk_allocator_allocations_check_memory_grow() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mem = allocator_memory.clone();

        assert_eq!(mem.size(), 0);
        let mut allocator = ChunkPtrAllocator::new(allocator_memory);
        assert_eq!(mem.size(), 1);

        for _ in 0..10000 {
            allocator.allocate();
        }

        for i in 0..(65536 / 8) - 16 {
            allocator.free(i * FILE_CHUNK_SIZE as FileSize);
        }

        assert_eq!(mem.size(), 1);

        allocator.free(9999 * FILE_CHUNK_SIZE as FileSize);

        assert_eq!(mem.size(), 2);
    }

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

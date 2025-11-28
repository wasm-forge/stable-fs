use ic_stable_structures::{Memory, memory_manager::VirtualMemory};

use crate::{
    error::Error,
    runtime::{
        structure_helpers::{read_obj, write_obj},
        types::ChunkSize,
    },
};

use super::types::{DEFAULT_FILE_CHUNK_SIZE_V2, FileChunkPtr};

// index for the first u64 containing chunk pointers
const FIRST_PTR_IDX: u64 = 16; // lower numbers are reserved

// index containing the chunk size used
const CHUNK_SIZE_IDX: u64 = 1;
// index containing the total number of chunks used
const AVAILABLE_CHUNKS_LEN_IDX: u64 = 2;
// index containing the next address to use, when there are no reusable indices available
const MAX_PTR_IDX: u64 = 3;

pub struct ChunkPtrAllocator<M: Memory> {
    v2_available_chunks: VirtualMemory<M>,
    v2_chunk_size: usize,
}

impl<M: Memory> ChunkPtrAllocator<M> {
    pub fn new(v2_available_chunks: VirtualMemory<M>) -> Result<ChunkPtrAllocator<M>, Error> {
        // init avaiable chunks
        if v2_available_chunks.size() == 0 {
            v2_available_chunks.grow(1);

            // write the magic marker
            let b = [b'F', b'S', b'A', b'1', 0, 0, 0, 0];
            v2_available_chunks.write(0, &b);

            v2_available_chunks.write(8, &0u64.to_le_bytes());
            v2_available_chunks.write(16, &0u64.to_le_bytes());
            v2_available_chunks.write(24, &0u64.to_le_bytes());
        } else {
            // check the marker
            let mut b = [0u8; 4];
            v2_available_chunks.read(0, &mut b);

            // possible accepted markers
            if b != *b"ALO1" && b != *b"FSA1" {
                return Err(Error::IllegalByteSequence);
            }

            if b == *b"ALO1" {
                // overwrite the marker
                let b = [b'F', b'S', b'A', b'1', 0, 0, 0, 0];
                v2_available_chunks.write(0, &b);
            }
        }

        let mut allocator = ChunkPtrAllocator {
            v2_available_chunks,
            v2_chunk_size: 0,
        };

        // init chunk size
        let mut chunk_size = allocator.read_u64(CHUNK_SIZE_IDX) as usize;

        if chunk_size == 0 {
            chunk_size = DEFAULT_FILE_CHUNK_SIZE_V2;
        }

        // initialize chunk size from the stored data or use default
        allocator.set_chunk_size(chunk_size).unwrap();

        Ok(allocator)
    }

    #[inline]
    fn read_u64(&self, index: u64) -> u64 {
        let mut ret = 0u64;
        read_obj(&self.v2_available_chunks, index * 8, &mut ret);

        ret
    }

    #[inline]
    fn write_u64(&self, index: u64, value: u64) {
        write_obj(&self.v2_available_chunks, index * 8, &value);
    }

    fn get_len(&self) -> u64 {
        self.read_u64(AVAILABLE_CHUNKS_LEN_IDX)
    }

    fn set_len(&self, new_len: u64) {
        self.write_u64(AVAILABLE_CHUNKS_LEN_IDX, new_len);
    }

    #[cfg(test)]
    pub fn get_current_max_ptr(&self) -> u64 {
        self.read_u64(MAX_PTR_IDX)
    }

    fn get_next_max_ptr(&self) -> u64 {
        let ret = self.read_u64(MAX_PTR_IDX);

        // store the next max pointer
        self.write_u64(MAX_PTR_IDX, ret + self.chunk_size() as u64);

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

    pub fn set_chunk_size(&mut self, new_size: usize) -> Result<(), Error> {
        // new size must be one of the available values

        if !ChunkSize::VALUES
            .iter()
            .any(|size| *size as usize == new_size)
        {
            return Err(Error::InvalidArgument);
        }

        // we can only set chunk size, if there are no chunks stored in the database, or the chunk size is not changed
        let cur_size = self.read_u64(CHUNK_SIZE_IDX);

        if cur_size == new_size as u64 {
            self.v2_chunk_size = new_size;
            return Ok(());
        }

        if self.read_u64(MAX_PTR_IDX) == 0 {
            // overwrite the new chunk size
            self.write_u64(CHUNK_SIZE_IDX, new_size as u64);
            self.v2_chunk_size = new_size;
            return Ok(());
        }

        Err(Error::InvalidArgument)
    }

    #[inline]
    pub fn chunk_size(&self) -> usize {
        self.v2_chunk_size
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
        if !ptr.is_multiple_of(self.chunk_size() as u64) {
            panic!("Pointer released {ptr} must be a multiple of FILE_CHUNK_SIZE!");
        }

        if self.read_u64(MAX_PTR_IDX) <= ptr {
            panic!("Address {ptr} was never allocated!");
        }

        for p in self.available_ptrs() {
            if p == ptr {
                panic!("Second free of address {ptr}");
            }
        }
    }

    pub fn free(&mut self, ptr: FileChunkPtr) {
        #[cfg(test)]
        self.check_free(ptr);

        self.push_ptr(ptr);
    }
}

#[cfg(test)]
mod tests {
    use ic_stable_structures::{
        Memory,
        memory_manager::{MemoryId, MemoryManager},
    };

    use crate::storage::types::FileSize;

    use crate::test_utils::new_vector_memory;

    use super::*;

    #[test]
    fn chunk_allocator_allocations() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory).unwrap();
        let chunk_size = DEFAULT_FILE_CHUNK_SIZE_V2;

        assert_eq!(allocator.allocate(), 0);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 2);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 3);

        allocator.free(chunk_size as FileChunkPtr * 2);

        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 2);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 4);

        assert!(allocator.available_ptrs().is_empty());

        allocator.free(chunk_size as FileChunkPtr * 2);

        // imitate canister upgrade here
        let mut allocator = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1))).unwrap();

        allocator.free(chunk_size as FileChunkPtr * 3);
        allocator.free(chunk_size as FileChunkPtr);

        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 3);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 2);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 5);
    }

    #[test]
    fn chunk_allocator_allocations2() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory).unwrap();

        assert_eq!(allocator.allocate(), 0);
        allocator.free(0);

        assert_eq!(allocator.allocate(), 0);
        assert_eq!(
            allocator.allocate(),
            DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr
        );
        allocator.free(DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr);

        assert_eq!(
            allocator.allocate(),
            DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr
        );
        assert_eq!(
            allocator.allocate(),
            DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr * 2
        );
        allocator.free(DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr);

        assert_eq!(
            allocator.allocate(),
            DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr
        );
        assert_eq!(
            allocator.allocate(),
            DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr * 3
        );
    }

    #[test]
    fn chunk_allocator_allocations_custom_chunk_size() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory).unwrap();
        let chunk_size = ChunkSize::CHUNK8K as usize;
        allocator.set_chunk_size(chunk_size).unwrap();

        assert_eq!(allocator.allocate(), 0);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 2);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 3);

        allocator.free(chunk_size as FileChunkPtr * 2);

        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 2);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 4);

        assert!(allocator.available_ptrs().is_empty());

        allocator.free(chunk_size as FileChunkPtr * 2);

        // imitate canister upgrade here
        let mut allocator = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1))).unwrap();
        assert_eq!(allocator.chunk_size(), chunk_size);

        allocator.free(chunk_size as FileChunkPtr * 3);
        allocator.free(chunk_size as FileChunkPtr);

        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 3);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 2);
        assert_eq!(allocator.allocate(), chunk_size as FileChunkPtr * 5);
    }

    #[test]
    #[should_panic]
    fn double_release_fails() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory).unwrap();

        assert_eq!(allocator.allocate(), 0);
        assert_eq!(
            allocator.allocate(),
            DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr
        );
        assert_eq!(
            allocator.allocate(),
            DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr * 2
        );
        assert_eq!(
            allocator.allocate(),
            DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr * 3
        );

        allocator.free(DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr * 2);
        allocator.free(DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr * 3);
        allocator.free(DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr * 2);
    }

    #[test]
    #[should_panic]
    fn unallocated_release_fails() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory).unwrap();

        allocator.allocate();
        allocator.free(0);
        allocator.allocate();
        allocator.free(DEFAULT_FILE_CHUNK_SIZE_V2 as FileChunkPtr);
    }

    #[test]
    #[should_panic]
    fn impossible_address_fails() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let allocator_memory = memory_manager.get(MemoryId::new(1));
        let mut allocator = ChunkPtrAllocator::new(allocator_memory).unwrap();

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
        let mut allocator = ChunkPtrAllocator::new(allocator_memory).unwrap();
        assert_eq!(mem.size(), 1);

        for _ in 0..10000 {
            allocator.allocate();
        }

        for i in 0..(65536 / 8) - 16 {
            allocator.free(i * DEFAULT_FILE_CHUNK_SIZE_V2 as FileSize);
        }

        assert_eq!(mem.size(), 1);

        allocator.free(9999 * DEFAULT_FILE_CHUNK_SIZE_V2 as FileSize);

        assert_eq!(mem.size(), 2);
    }

    #[test]
    fn wrong_custom_chunk_size_fails() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let mut allocator = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1))).unwrap();
        let chunk_size = DEFAULT_FILE_CHUNK_SIZE_V2;

        assert_eq!(allocator.allocate(), 0);
        allocator.free(0);

        let mut allocator = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1))).unwrap();

        let res = allocator.set_chunk_size(chunk_size * 2);

        assert!(res.is_err());

        assert_eq!(allocator.chunk_size(), chunk_size);
    }

    #[test]
    fn fsa1_marker_is_written() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let mut allocator = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1))).unwrap();

        assert_eq!(allocator.allocate(), 0);
        allocator.free(0);

        let memory = memory_manager.get(MemoryId::new(1));
        let mut b = [0u8; 4];

        memory.read(0, &mut b);
        assert_eq!(&b[0..4], b"FSA1");
    }

    #[test]
    fn correct_fsa1_marker_is_accepted() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let mut allocator = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1))).unwrap();

        assert_eq!(allocator.allocate(), 0);
        allocator.free(0);

        let res = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1)));

        assert!(res.is_ok());
    }

    #[test]
    fn wrong_fsa1_marker_is_rejected() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let mut allocator = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1))).unwrap();

        assert_eq!(allocator.allocate(), 0);
        allocator.free(0);

        let memory = memory_manager.get(MemoryId::new(1));
        let b = [0u8; 1];

        memory.write(0, &b);

        let res = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1)));

        assert!(res.is_err());
    }

    #[test]
    fn same_custom_chunk_size_succeeds() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let mut allocator = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1))).unwrap();
        let chunk_size = DEFAULT_FILE_CHUNK_SIZE_V2 * 2;
        allocator.set_chunk_size(chunk_size).unwrap();

        assert_eq!(allocator.allocate(), 0);
        allocator.free(0);

        let mut allocator = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1))).unwrap();
        assert_eq!(allocator.chunk_size(), chunk_size);

        allocator.set_chunk_size(chunk_size).unwrap();

        assert_eq!(allocator.chunk_size(), chunk_size);
    }

    #[test]
    fn incorrect_size_fails() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let mut allocator = ChunkPtrAllocator::new(memory_manager.get(MemoryId::new(1))).unwrap();

        for chunk_size in ChunkSize::VALUES.iter() {
            allocator.set_chunk_size(*chunk_size as usize).unwrap();
        }

        assert!(allocator.set_chunk_size(0).is_err());
        assert!(allocator.set_chunk_size(1).is_err());
        assert!(
            allocator
                .set_chunk_size(DEFAULT_FILE_CHUNK_SIZE_V2 + 1)
                .is_err()
        );
        assert!(
            allocator
                .set_chunk_size(DEFAULT_FILE_CHUNK_SIZE_V2 * 3)
                .is_err()
        );
    }
}

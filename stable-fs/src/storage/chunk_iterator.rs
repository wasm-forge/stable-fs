use crate::storage::FileSize;
use crate::storage::Node;
use crate::storage::types::FileChunkIndex;
use crate::storage::types::FileChunkPtr;
use ic_stable_structures;
use ic_stable_structures::BTreeMap;
use ic_stable_structures::Memory;
use ic_stable_structures::memory_manager::VirtualMemory;

use super::ptr_cache::CachedChunkPtr;
use super::ptr_cache::PtrCache;

pub(crate) struct ChunkV2Iterator<'a, M: Memory> {
    node: Node,
    last_index_excluded: FileChunkIndex,
    cur_index: FileChunkIndex,
    ptr_cache: &'a mut PtrCache,
    v2_chunk_ptr: &'a mut BTreeMap<(Node, FileChunkIndex), FileChunkPtr, VirtualMemory<M>>,
}

impl<'a, M: Memory> ChunkV2Iterator<'a, M> {
    pub fn new(
        node: Node,
        offset: FileSize,
        last_address: FileSize,
        chunk_size: FileSize,
        ptr_cache: &'a mut PtrCache,
        v2_chunk_ptr: &'a mut BTreeMap<(Node, FileChunkIndex), FileChunkPtr, VirtualMemory<M>>,
    ) -> Self {
        let cur_index = (offset / chunk_size) as FileChunkIndex;
        let last_index_excluded = (last_address / chunk_size + 1) as FileChunkIndex;

        Self {
            node,
            last_index_excluded,
            cur_index,
            ptr_cache,
            v2_chunk_ptr,
        }
    }
}

impl<M: Memory> Iterator for ChunkV2Iterator<'_, M> {
    type Item = ((Node, FileChunkIndex), CachedChunkPtr);

    fn next(&mut self) -> Option<Self::Item> {
        // we are at the end of the list, return None
        if self.cur_index >= self.last_index_excluded {
            return None;
        }

        // try get cached item first
        let ptr = self.ptr_cache.get((self.node, self.cur_index));

        if let Some(chunk_ptr) = ptr {
            let ret = Some(((self.node, self.cur_index), chunk_ptr));
            self.cur_index += 1;
            return ret;
        }

        // cache failed, resort to reading the ranged values from the iterator
        self.ptr_cache.add_range(
            self.node,
            self.cur_index,
            self.last_index_excluded,
            self.v2_chunk_ptr,
        );

        //
        let found: CachedChunkPtr = self.ptr_cache.get((self.node, self.cur_index)).unwrap();

        let res = Some(((self.node, self.cur_index), found));

        self.cur_index += 1;

        res
    }
}

#[cfg(test)]
mod tests {
    use crate::fs::FileSize;
    use crate::storage::Storage;
    use crate::storage::chunk_iterator::ChunkV2Iterator;
    use crate::storage::ptr_cache::CachedChunkPtr;
    use crate::storage::stable::StableStorage;
    use crate::storage::types::{FileType, Metadata, Node, Times};
    use crate::test_utils::new_vector_memory;
    use ic_stable_structures::Memory;

    fn create_file_with_size<M: Memory>(size: FileSize, storage: &mut StableStorage<M>) -> Node {
        let node = storage.new_node();

        storage
            .put_metadata(
                node,
                &Metadata {
                    node,
                    file_type: FileType::RegularFile,
                    link_count: 1,
                    size,
                    times: Times::default(),
                    chunk_type: Some(storage.chunk_type()),
                    maximum_size_allowed: None,
                    _first_dir_entry: None,
                    _last_dir_entry: None,
                },
            )
            .unwrap();

        node
    }

    #[test]
    fn iterate_short_file() {
        let mut storage = StableStorage::new(new_vector_memory());
        let node = create_file_with_size(0, &mut storage);
        let write_size = storage.chunk_size() * 3 - 100;

        let buf = vec![142u8; write_size];

        storage.write(node, 0, &buf).unwrap();

        let meta = storage.get_metadata(node).unwrap();
        let file_size = meta.size;

        let iterator = ChunkV2Iterator::new(
            node,
            30,
            file_size,
            storage.chunk_size() as FileSize,
            &mut storage.ptr_cache,
            &mut storage.v2_filechunk.v2_chunk_ptr,
        );

        let res_vec: Vec<_> = iterator.collect();

        assert!(res_vec[0].1 != CachedChunkPtr::ChunkMissing);
        assert!(res_vec[1].1 != CachedChunkPtr::ChunkMissing);
        assert!(res_vec[2].1 != CachedChunkPtr::ChunkMissing);
    }

    #[test]
    fn iterate_file_with_size_and_no_stored_chunks() {
        let mut storage = StableStorage::new(new_vector_memory());
        let write_size = (storage.chunk_size() * 3 - 100) as FileSize;

        let node = create_file_with_size(write_size, &mut storage);

        let meta = storage.get_metadata(node).unwrap();
        let file_size = meta.size;

        let iterator = ChunkV2Iterator::new(
            node,
            30,
            file_size,
            storage.chunk_size() as FileSize,
            &mut storage.ptr_cache,
            &mut storage.v2_filechunk.v2_chunk_ptr,
        );

        let res_vec: Vec<_> = iterator.collect();

        assert!(res_vec[0].1 == CachedChunkPtr::ChunkMissing);
        assert!(res_vec[1].1 == CachedChunkPtr::ChunkMissing);
        assert!(res_vec[2].1 == CachedChunkPtr::ChunkMissing);
    }

    #[test]
    fn iterate_file_missing_chunk_in_the_middle() {
        let mut storage = StableStorage::new(new_vector_memory());
        let node = create_file_with_size(0, &mut storage);

        let write_size = (storage.chunk_size() * 3 - 200) as FileSize;

        storage.write(node, 10, &[142u8; 100]).unwrap();
        storage.write(node, write_size, &[142u8; 100]).unwrap();

        let meta = storage.get_metadata(node).unwrap();
        let file_size = meta.size;

        let iterator = ChunkV2Iterator::new(
            node,
            30,
            file_size,
            storage.chunk_size() as FileSize,
            &mut storage.ptr_cache,
            &mut storage.v2_filechunk.v2_chunk_ptr,
        );

        let res_vec: Vec<_> = iterator.collect();

        assert!(res_vec[0].1 != CachedChunkPtr::ChunkMissing);
        assert!(res_vec[1].1 == CachedChunkPtr::ChunkMissing);
        assert!(res_vec[2].1 != CachedChunkPtr::ChunkMissing);
    }

    #[test]
    fn iterate_file_only_middle_chunk_is_present() {
        let mut storage = StableStorage::new(new_vector_memory());
        let file_size = (storage.chunk_size() * 3 - 200) as FileSize;
        let node = create_file_with_size(file_size, &mut storage);

        let write_size = (storage.chunk_size() * 2 - 200) as FileSize;

        storage.write(node, write_size, &[142u8; 102]).unwrap();

        let meta = storage.get_metadata(node).unwrap();
        let file_size = meta.size;

        let iterator = ChunkV2Iterator::new(
            node,
            30,
            file_size,
            storage.chunk_size() as FileSize,
            &mut storage.ptr_cache,
            &mut storage.v2_filechunk.v2_chunk_ptr,
        );

        let res_vec: Vec<_> = iterator.collect();

        assert!(res_vec[0].1 == CachedChunkPtr::ChunkMissing);
        assert!(res_vec[1].1 != CachedChunkPtr::ChunkMissing);
        assert!(res_vec[2].1 == CachedChunkPtr::ChunkMissing);
    }
}

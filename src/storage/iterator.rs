use crate::storage::types::FileChunkIndex;
use crate::storage::types::FileChunkPtr;
use crate::storage::FileSize;
use crate::storage::Node;
use ic_stable_structures;
use ic_stable_structures::memory_manager::VirtualMemory;
use ic_stable_structures::BTreeMap;
use ic_stable_structures::Memory;
use std::collections::HashMap;

pub(crate) struct ChunkV2Iterator<'a, M: Memory> {
    node: Node,
    last_index_excluded: FileChunkIndex,
    cur_index: FileChunkIndex,

    is_prefetched: bool,
    prefetched_pointers: HashMap<(Node, FileChunkIndex), FileChunkPtr>,

    last_index: (Node, FileChunkIndex, FileChunkPtr),
    v2_chunk_ptr: &'a mut BTreeMap<(Node, FileChunkIndex), FileChunkPtr, VirtualMemory<M>>,
}

impl<'a, M: Memory> ChunkV2Iterator<'a, M> {
    pub fn new(
        node: Node,
        offset: FileSize,
        file_size: FileSize,
        chunk_size: FileSize,
        last_index: (Node, FileChunkIndex, FileChunkPtr),
        v2_chunk_ptr: &'a mut BTreeMap<(Node, FileChunkIndex), FileChunkPtr, VirtualMemory<M>>,
    ) -> Self {
        let cur_index = (offset / chunk_size) as FileChunkIndex;
        let last_index_excluded = (file_size / chunk_size + 1) as FileChunkIndex;

        Self {
            node,
            last_index_excluded,
            cur_index,
            is_prefetched: false,
            prefetched_pointers: HashMap::new(),
            last_index,
            v2_chunk_ptr,
        }
    }
}

impl<'a, M: Memory> Iterator for ChunkV2Iterator<'a, M> {
    type Item = ((Node, FileChunkIndex), Option<FileChunkPtr>);

    fn next(&mut self) -> Option<Self::Item> {
        // we are at the end of the list, return None
        if self.cur_index >= self.last_index_excluded {
            return None;
        }

        // try get cached item first
        let last = self.last_index;
        if last.0 == self.node && last.1 == self.cur_index {
            let res = Some(((self.node, self.cur_index), Some(last.2)));
            self.cur_index += 1;
            // return cached value
            return res;
        }

        // cache failed, resort to reading the ranged values from the iterator
        if !self.is_prefetched {
            let range = (self.node, self.cur_index)..(self.node, self.last_index_excluded);
            let items = self.v2_chunk_ptr.range(range);

            for (k, v) in items {
                self.prefetched_pointers.insert(k, v);
            }

            self.is_prefetched = true;
        }

        let found: Option<FileChunkPtr> = self
            .prefetched_pointers
            .get(&(self.node, self.cur_index))
            .copied();

        let res = Some(((self.node, self.cur_index), found));

        self.cur_index += 1;

        res
    }
}

#[cfg(test)]
mod tests {
    use crate::fs::FileSize;
    use crate::storage::iterator::ChunkV2Iterator;
    use crate::storage::stable::StableStorage;
    use crate::storage::types::{FileType, Metadata, Node, Times};
    use crate::storage::Storage;
    use crate::test_utils::new_vector_memory;
    use ic_stable_structures::Memory;

    fn create_file_with_size<M: Memory>(size: FileSize, storage: &mut StableStorage<M>) -> Node {
        let node = storage.new_node();

        storage.put_metadata(
            node,
            Metadata {
                node,
                file_type: FileType::RegularFile,
                link_count: 1,
                size,
                times: Times::default(),
                first_dir_entry: Some(42),
                last_dir_entry: Some(24),
                chunk_type: Some(storage.chunk_type()),
            },
        );
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
            storage.last_index,
            &mut storage.v2_chunk_ptr,
        );

        let res_vec: Vec<_> = iterator.collect();

        assert!(res_vec[0].1.is_some());
        assert!(res_vec[1].1.is_some());
        assert!(res_vec[2].1.is_some());

        println!("{:?}", res_vec);
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
            storage.last_index,
            &mut storage.v2_chunk_ptr,
        );

        let res_vec: Vec<_> = iterator.collect();

        assert!(res_vec[0].1.is_none());
        assert!(res_vec[1].1.is_none());
        assert!(res_vec[2].1.is_none());

        println!("{:?}", res_vec);
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
            storage.last_index,
            &mut storage.v2_chunk_ptr,
        );

        let res_vec: Vec<_> = iterator.collect();

        println!("{:?}", res_vec);

        assert!(res_vec[0].1.is_some());
        assert!(res_vec[1].1.is_none());
        assert!(res_vec[2].1.is_some());
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
            storage.last_index,
            &mut storage.v2_chunk_ptr,
        );

        let res_vec: Vec<_> = iterator.collect();

        println!("{:?}", res_vec);

        assert!(res_vec[0].1.is_none());
        assert!(res_vec[1].1.is_some());
        assert!(res_vec[2].1.is_none());
    }
}

use std::collections::HashMap;

use super::types::{FileChunkIndex, FileChunkPtr, Node};
use ic_stable_structures::memory_manager::VirtualMemory;
use ic_stable_structures::BTreeMap;

const CACHE_CAPACITY: usize = 10000;
// for short reads and writes it is better to cache some more chunks than the minimum required
const MIN_CACHE_CHUNKS: u32 = 100;
// maximum number of chunks to pre-load
const MAX_CACHE_CHUNKS: u32 = 1024;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum CachedChunkPtr {
    // the chunk exists and we know its location
    ChunkExists(FileChunkPtr),
    // the chunk doesn't exist
    ChunkMissing,
}

#[derive(Debug)]
pub(crate) struct PtrCache {
    pointers: HashMap<(Node, FileChunkIndex), CachedChunkPtr>,
}

impl PtrCache {
    pub fn new() -> PtrCache {
        let pointers: HashMap<(Node, FileChunkIndex), CachedChunkPtr> =
            HashMap::with_capacity(CACHE_CAPACITY);
        PtrCache { pointers }
    }

    // add new cache pointer
    pub fn add(&mut self, cache_pairs: Vec<((Node, FileChunkIndex), CachedChunkPtr)>) {
        if self.pointers.len() + cache_pairs.len() > CACHE_CAPACITY {
            self.clear();
        }

        for (key, value) in cache_pairs {
            self.pointers.insert(key, value);
        }
    }

    pub fn add_range<M: ic_stable_structures::Memory>(
        &mut self,
        node: Node,
        from_index: FileChunkIndex,
        to_index: FileChunkIndex,
        v2_chunk_ptr: &BTreeMap<(Node, FileChunkIndex), FileChunkPtr, VirtualMemory<M>>,
    ) {
        let to_index = to_index.min(from_index + MAX_CACHE_CHUNKS);
        let to_index = to_index.max(from_index + MIN_CACHE_CHUNKS);

        let range = (node, from_index)..(node, to_index);

        let items = v2_chunk_ptr.range(range);

        let mut new_cache = Vec::with_capacity(to_index as usize - from_index as usize);

        let mut cur_index = from_index;

        let mut iterator_empty = true;

        for ((n, index), ptr) in items {
            assert!(node == n);

            iterator_empty = false;

            while cur_index < index {
                new_cache.push(((node, cur_index), CachedChunkPtr::ChunkMissing));
                cur_index += 1;
            }

            assert!(cur_index == index);

            new_cache.push(((node, cur_index), CachedChunkPtr::ChunkExists(ptr)));
            cur_index += 1;
        }

        // if no chunks were found, fill the whole vector with missing chunks
        if iterator_empty {
            while cur_index < to_index {
                new_cache.push(((node, cur_index), CachedChunkPtr::ChunkMissing));
                cur_index += 1;
            }
        }

        self.add(new_cache);
    }

    // clear cache completely
    pub fn clear(&mut self) {
        self.pointers.clear();
    }

    pub fn get(&self, key: (Node, FileChunkIndex)) -> std::option::Option<CachedChunkPtr> {
        self.pointers.get(&key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_get() {
        let mut cache = PtrCache::new();
        let key = (5 as Node, 7 as FileChunkIndex);
        let value = CachedChunkPtr::ChunkExists(34 as FileChunkPtr);
        cache.add(vec![(key, value)]);

        assert_eq!(cache.get(key), Some(value));
    }

    #[test]
    fn add_and_get_missing() {
        let mut cache = PtrCache::new();
        let key = (5 as Node, 7 as FileChunkIndex);
        let value = CachedChunkPtr::ChunkMissing;
        cache.add(vec![(key, value)]);

        assert_eq!(cache.get(key), Some(value));
    }

    #[test]
    fn get_none_existing() {
        let mut cache = PtrCache::new();
        let key = (5 as Node, 7 as FileChunkIndex);
        let value = CachedChunkPtr::ChunkExists(34 as FileChunkPtr);
        cache.add(vec![(key, value)]);

        assert_eq!(cache.get((5 as Node, 8 as FileChunkIndex)), None);
    }

    #[test]
    fn test_clear() {
        let mut cache = PtrCache::new();
        let key = (5 as Node, 7 as FileChunkIndex);
        let value = CachedChunkPtr::ChunkExists(34 as FileChunkPtr);
        cache.add(vec![(key, value)]);
        cache.clear();
        assert_eq!(cache.get(key), None);
    }

    #[test]
    fn check_clear_cache_happens() {
        let mut cache = PtrCache::new();

        // arrange
        let mut expected_insertions: Vec<_> = Vec::new();
        for i in CACHE_CAPACITY..(CACHE_CAPACITY + 5) {
            expected_insertions.push((
                (5 as Node, i as FileChunkIndex),
                CachedChunkPtr::ChunkExists(i as u64 * 4096 as FileChunkPtr),
            ));
        }

        // act
        for i in 0..CACHE_CAPACITY + 5 {
            cache.add(vec![(
                (5 as Node, i as FileChunkIndex),
                CachedChunkPtr::ChunkExists(i as u64 * 4096 as FileChunkPtr),
            )]);
        }

        // assert
        assert_eq!(cache.pointers.len(), 5);

        for (k, v) in expected_insertions {
            assert_eq!(v, cache.get(k).unwrap());
        }
    }
}

use std::collections::HashMap;

use super::types::{FileChunkIndex, FileChunkPtr, Node};

static CACHE_CAPACITY: usize = 1000;

pub(crate) struct PtrCache {
    pointers: HashMap<(Node, FileChunkIndex), FileChunkPtr>,
}

impl PtrCache {
    pub fn new() -> PtrCache {
        let pointers: HashMap<(Node, FileChunkIndex), FileChunkPtr> =
            HashMap::with_capacity(CACHE_CAPACITY);
        PtrCache { pointers }
    }

    // add new cache pointer
    pub fn add(&mut self, cache_pairs: Vec<((Node, FileChunkIndex), FileChunkPtr)>) {
        if self.pointers.len() + cache_pairs.len() > CACHE_CAPACITY {
            self.clear();
        }

        for (key, value) in cache_pairs {
            self.pointers.insert(key, value);
        }
    }

    // clear cache completely
    pub fn clear(&mut self) {
        self.pointers.clear();
    }

    pub fn get(&self, key: (Node, FileChunkIndex)) -> std::option::Option<FileChunkPtr> {
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
        let value = 34 as FileChunkPtr;
        cache.add(vec![(key, value)]);

        assert_eq!(cache.get(key), Some(value));
    }

    #[test]
    fn get_none_existing() {
        let mut cache = PtrCache::new();
        let key = (5 as Node, 7 as FileChunkIndex);
        let value = 34 as FileChunkPtr;
        cache.add(vec![(key, value)]);

        assert_eq!(cache.get((5 as Node, 8 as FileChunkIndex)), None);
    }

    #[test]
    fn test_clear() {
        let mut cache = PtrCache::new();
        let key = (5 as Node, 7 as FileChunkIndex);
        let value = 34 as FileChunkPtr;
        cache.add(vec![(key, value)]);
        cache.clear();
        assert_eq!(cache.get(key), None);
    }

    #[test]
    fn check_clear_cache_happens() {
        let mut cache = PtrCache::new();

        for i in 0..CACHE_CAPACITY + 5 {
            cache.add(vec![(
                (5 as Node, i as FileChunkIndex),
                i as u64 * 4096 as FileChunkPtr,
            )]);
        }

        let mut expected_insertions: Vec<_> = Vec::new();
        for i in (CACHE_CAPACITY)..(CACHE_CAPACITY + 5) {
            expected_insertions.push((
                (5 as Node, i as FileChunkIndex),
                i as u64 * 4096 as FileChunkPtr,
            ));
        }

        assert_eq!(cache.pointers.len(), 5);
    }
}

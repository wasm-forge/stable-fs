use std::collections::HashMap;

use super::types::{Metadata, Node};
use std::cell::RefCell;
use std::rc::Rc;

const CACHE_CAPACITY: usize = 100;

#[derive(Debug)]
pub(crate) struct MetadataCache {
    meta: Rc<RefCell<HashMap<Node, Metadata>>>,
}

impl MetadataCache {
    pub fn new() -> MetadataCache {
        let meta: Rc<RefCell<HashMap<Node, Metadata>>> = Rc::new(RefCell::new(HashMap::with_capacity(CACHE_CAPACITY)));
        
        MetadataCache { meta }
    }

    // add new cache meta
    pub fn update(&self, node: Node, new_meta: &Metadata) {

        let mut meta = (*self.meta).borrow_mut();

        if meta.len() + 1 > CACHE_CAPACITY {
            meta.clear();
        }

        meta.insert(node, new_meta.clone());
    }

    // clear cache completely
    pub fn clear(&self) {
        let mut meta = (*self.meta).borrow_mut();

        meta.clear();
    }

    pub fn get(&self, node: Node) -> std::option::Option<Metadata> {
        let meta = (*self.meta).borrow();

        meta.get(&node).cloned()
    }
}

#[cfg(test)]
mod tests {
    use crate::{fs::ChunkType, storage::types::{FileType, Times}};

    use super::*;

    #[test]
    fn cache_initialization() {
        let cache = MetadataCache::new();
        assert_eq!(cache.meta.borrow().len(), 0, "Cache should be empty on initialization");
    }

    #[test]
    fn cache_update_and_get() {
        let cache = MetadataCache::new();
        let node = 1 as Node;
        let metadata = Metadata {
            node,
            file_type: FileType::RegularFile,
            link_count: 1,
            size: 45,
            times: Times {
                accessed: 0,
                modified: 0,
                created: 0,
            },
            first_dir_entry: None,
            last_dir_entry: None,
            chunk_type: Some(ChunkType::V2),
        };
        
        cache.update(node, &metadata);
        
        // Check if the metadata can be retrieved correctly
        let retrieved = cache.get(node);
        assert_eq!(retrieved, Some(metadata), "Retrieved metadata should match inserted metadata");
    }

    #[test]
    fn cache_clear() {
        let cache = MetadataCache::new();
        let node = 1 as Node;
        
        let metadata = Metadata {
            node,
            file_type: FileType::RegularFile,
            link_count: 1,
            size: 45,
            times: Times {
                accessed: 0,
                modified: 0,
                created: 0,
            },
            first_dir_entry: None,
            last_dir_entry: None,
            chunk_type: Some(ChunkType::V2),
        };

        cache.update(node, &metadata);
        cache.clear();

        // Cache should be empty after clear
        assert_eq!(cache.meta.borrow().len(), 0, "Cache should be empty after clearing");
        assert_eq!(cache.get(node), None, "Metadata should be None after cache is cleared");
    }

    #[test]
    fn cache_eviction_when_capacity_exceeded() {
        let cache = MetadataCache::new();
        
        // Fill the cache to its capacity
        for i in 0..CACHE_CAPACITY {
            let node = i as Node;
            
            let metadata = Metadata {
                node,
                file_type: FileType::RegularFile,
                link_count: 1,
                size: 45,
                times: Times {
                    accessed: 0,
                    modified: 0,
                    created: 0,
                },
                first_dir_entry: None,
                last_dir_entry: None,
                chunk_type: Some(ChunkType::V2),
            };

            cache.update(node, &metadata);
        }
        
        assert_eq!(cache.meta.borrow().len(), CACHE_CAPACITY, "Cache should have CACHE_CAPACITY entries");
        
        // Add one more item to trigger eviction (if implemented)
        let extra_node = 1000 as Node;
      
        let extra_metadata = Metadata {
            node: extra_node,
            file_type: FileType::RegularFile,
            link_count: 1,
            size: 475,
            times: Times {
                accessed: 0,
                modified: 0,
                created: 0,
            },
            first_dir_entry: None,
            last_dir_entry: None,
            chunk_type: Some(ChunkType::V2),
        };

        cache.update(extra_node, &extra_metadata);
        
        assert!(cache.meta.borrow().len() <= CACHE_CAPACITY, "Cache should not exceed CACHE_CAPACITY");
        assert!(cache.get(extra_node).is_some(), "Extra node should be in the cache after eviction");
    }
}




use std::collections::HashMap;

use super::types::{Metadata, Node};
use crate::storage::ptr_cache::PtrCache;
use crate::storage::Error;
use ic_stable_structures::memory_manager::VirtualMemory;
use ic_stable_structures::BTreeMap;
use ic_stable_structures::Memory;
use std::cell::RefCell;
use std::rc::Rc;

const CACHE_CAPACITY: usize = 1000;

#[derive(Debug)]
pub(crate) struct MetadataCache {
    meta: Rc<RefCell<HashMap<Node, Metadata>>>,
}

impl MetadataCache {
    pub fn new() -> MetadataCache {
        let meta: Rc<RefCell<HashMap<Node, Metadata>>> =
            Rc::new(RefCell::new(HashMap::with_capacity(CACHE_CAPACITY)));

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

pub(crate) struct MetadataProvider<M: Memory> {
    // only use it with non-mounted files. This reduces metadata search overhead, when the same file is read.
    meta_cache: MetadataCache,

    // only use it with mounted files. This reduces metadata search overhead, when the same file is read.
    mounted_meta_cache: MetadataCache,

    // data about one file or a folder such as creation time, file size, associated chunk type, etc.
    metadata: BTreeMap<Node, Metadata, VirtualMemory<M>>,
    // The metadata of the mounted memory files.
    // * We store this separately from regular file metadata because the same node IDs can be reused for the related files.
    // * We need this metadata because we want the information on the mounted files (such as file size) to survive between canister upgrades.
    mounted_meta: BTreeMap<Node, Metadata, VirtualMemory<M>>,
}

impl<M: Memory> MetadataProvider<M> {
    pub fn new(
        meta_mem: VirtualMemory<M>,
        mounted_meta_mem: VirtualMemory<M>,
    ) -> MetadataProvider<M> {
        MetadataProvider {
            meta_cache: MetadataCache::new(),
            mounted_meta_cache: MetadataCache::new(),
            metadata: BTreeMap::init(meta_mem),
            mounted_meta: BTreeMap::init(mounted_meta_mem),
        }
    }

    pub(crate) fn rm_file(&mut self, node: Node, ptr_cache: &mut PtrCache) {
        // remove metadata
        self.mounted_meta.remove(&node);
        self.metadata.remove(&node);

        self.meta_cache.clear();
        self.mounted_meta_cache.clear();
        ptr_cache.clear();
    }

    pub(crate) fn get_metadata(
        &self,
        node: Node,
        is_mounted: bool,
    ) -> Result<Metadata, crate::error::Error> {
        if is_mounted {
            let meta = self.mounted_meta_cache.get(node);

            if let Some(meta) = meta {
                return Ok(meta);
            }

            let meta = self.mounted_meta.get(&node).ok_or(Error::NotFound);

            if let Ok(ref meta) = meta {
                self.mounted_meta_cache.update(node, meta);
            }

            meta
        } else {
            let meta = self.meta_cache.get(node);

            if let Some(meta) = meta {
                return Ok(meta);
            }

            let meta = self.metadata.get(&node).ok_or(Error::NotFound);

            if let Ok(ref meta) = meta {
                self.meta_cache.update(node, meta);
            }

            meta
        }
    }

    pub(crate) fn put_metadata(&mut self, node: u64, is_mounted: bool, metadata: Metadata) {
        assert_eq!(node, metadata.node, "Node does not match medatada.node!");

        if is_mounted {
            self.mounted_meta_cache.update(node, &metadata);
            self.mounted_meta.insert(node, metadata);
        } else {
            self.meta_cache.update(node, &metadata);
            self.metadata.insert(node, metadata);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        fs::ChunkType,
        storage::types::{FileType, Times},
    };

    use super::*;

    #[test]
    fn cache_initialization() {
        let cache = MetadataCache::new();
        assert_eq!(
            cache.meta.borrow().len(),
            0,
            "Cache should be empty on initialization"
        );
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
        assert_eq!(
            retrieved,
            Some(metadata),
            "Retrieved metadata should match inserted metadata"
        );
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
        assert_eq!(
            cache.meta.borrow().len(),
            0,
            "Cache should be empty after clearing"
        );
        assert_eq!(
            cache.get(node),
            None,
            "Metadata should be None after cache is cleared"
        );
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

        assert_eq!(
            cache.meta.borrow().len(),
            CACHE_CAPACITY,
            "Cache should have CACHE_CAPACITY entries"
        );

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

        assert!(
            cache.meta.borrow().len() <= CACHE_CAPACITY,
            "Cache should not exceed CACHE_CAPACITY"
        );
        assert!(
            cache.get(extra_node).is_some(),
            "Extra node should be in the cache after eviction"
        );
    }
}

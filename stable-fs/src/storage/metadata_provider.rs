use std::collections::HashMap;

use super::allocator::ChunkPtrAllocator;
use super::stable::{ROOT_NODE, V2FileChunks};
use super::types::{FileChunk, FileChunkIndex, FileChunkPtr, FileType, Times};
use super::types::{Metadata, Node};
use crate::fs::FileSize;
use crate::runtime::structure_helpers::{grow_memory, read_obj, write_obj};
use crate::storage::ptr_cache::PtrCache;
use crate::storage::types::ZEROES;
use ic_stable_structures::BTreeMap;
use ic_stable_structures::Memory;
use ic_stable_structures::memory_manager::VirtualMemory;
use std::cell::RefCell;
use std::rc::Rc;

const CACHE_CAPACITY: usize = 1000;

// custom file chunk containing metadata.
pub const METADATA_CHUNK_INDEX: u32 = u32::MAX - 1;
// custom file chunk containing metadata for the mounted drives.
pub const MOUNTED_METADATA_CHUNK_INDEX: u32 = u32::MAX - 2;

// reserve 1024 bytes for future, for storing metadata
pub const MAX_META_SIZE: usize = 1024;

type MetadataCacheMap = HashMap<Node, (Metadata, Option<FileChunkPtr>)>;

#[derive(Debug)]
pub(crate) struct MetadataCache {
    meta: Rc<RefCell<MetadataCacheMap>>,
}

impl MetadataCache {
    pub fn new() -> MetadataCache {
        let meta: Rc<RefCell<MetadataCacheMap>> =
            Rc::new(RefCell::new(HashMap::with_capacity(CACHE_CAPACITY)));

        MetadataCache { meta }
    }

    // add new cache meta
    pub fn update(&self, node: Node, new_meta: &Metadata, new_ptr: Option<FileChunkPtr>) {
        let mut meta = (*self.meta).borrow_mut();

        if meta.len() + 1 > CACHE_CAPACITY {
            meta.clear();
        }

        meta.insert(node, (new_meta.clone(), new_ptr));
    }

    pub fn remove(&self, node: Node) {
        let mut meta = (*self.meta).borrow_mut();
        meta.remove(&node);
    }

    pub fn get(&self, node: Node) -> std::option::Option<(Metadata, Option<FileChunkPtr>)> {
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

    pub(crate) fn remove_metadata(
        &mut self,
        node: Node,
        ptr_cache: &mut PtrCache,
        filechunk: &mut BTreeMap<(Node, FileChunkIndex), FileChunk, VirtualMemory<M>>,
        v2_chunk_ptr: &mut BTreeMap<(Node, FileChunkIndex), FileChunkPtr, VirtualMemory<M>>,
        v2_allocator: &mut ChunkPtrAllocator<M>,
    ) {
        // remove metadata
        self.mounted_meta.remove(&node);
        self.metadata.remove(&node);

        self.meta_cache.remove(node);
        self.mounted_meta_cache.remove(node);

        ptr_cache.clear();

        // removing file data chunks as well
        let range = (node, 0)..(node + 1, 0);

        let mut chunks: Vec<(Node, FileChunkIndex)> = Vec::new();

        for en in filechunk.range(range) {
            chunks.push(*en.key());
        }

        for (nd, idx) in chunks.into_iter() {
            assert!(nd == node);
            filechunk.remove(&(node, idx));
        }

        // delete v2 chunks

        // delete all nodes, including file contents and metadata chunks
        let range = (node, 0)..(node + 1, 0);

        let mut chunks: Vec<(Node, FileChunkIndex)> = Vec::new();

        for en in v2_chunk_ptr.range(range) {
            let k = *en.key();
            chunks.push((k.0, k.1));
        }

        for (nd, idx) in chunks.into_iter() {
            assert!(nd == node);

            let removed = v2_chunk_ptr.remove(&(node, idx));

            if let Some(removed) = removed {
                v2_allocator.free(removed);
            }
        }
    }

    fn load_chunk_meta(&self, v2_chunks: &VirtualMemory<M>, ptr: FileChunkPtr) -> Metadata {
        let mut meta = Metadata::default();
        read_obj(v2_chunks, ptr, &mut meta);
        meta
    }

    fn store_chunk_meta(&self, v2_chunks: &VirtualMemory<M>, ptr: FileChunkPtr, meta: &Metadata) {
        write_obj(v2_chunks, ptr, meta);
    }

    // try to get metadata and the data pointer, or return None if not found.
    pub(crate) fn get_metadata(
        &self,
        node: Node,
        is_mounted: bool,
        v2_chunk_ptr: &BTreeMap<(Node, FileChunkIndex), FileChunkPtr, VirtualMemory<M>>,
        v2_chunks: &VirtualMemory<M>,
    ) -> Option<(Metadata, Option<FileChunkPtr>)> {
        let (meta_index, meta_storage, meta_cache) = if is_mounted {
            (
                MOUNTED_METADATA_CHUNK_INDEX,
                &self.mounted_meta,
                &self.mounted_meta_cache,
            )
        } else {
            (METADATA_CHUNK_INDEX, &self.metadata, &self.meta_cache)
        };

        // try to get meta from cache
        let meta_rec = meta_cache.get(node);

        if let Some(meta_rec) = meta_rec {
            return Some((meta_rec.0.clone(), meta_rec.1));
        }

        // meta not found in cache, try to get it from the file chunks
        let meta_ptr: Option<FileChunkPtr> = v2_chunk_ptr.get(&(node, meta_index));

        if let Some(meta_ptr) = meta_ptr {
            // the chunk pointer is known, now read the contents
            let meta = self.load_chunk_meta(v2_chunks, meta_ptr);

            // update cache
            meta_cache.update(node, &meta, Some(meta_ptr));

            return Some((meta, Some(meta_ptr)));
        }

        // meta not found in chunks, try to get it from the storage
        let meta_found = meta_storage.get(&node);

        if meta_found.is_none() {
            // if root node is not found on the new file system, just return the generated one.
            if node == ROOT_NODE {
                let metadata = Metadata {
                    node: ROOT_NODE,
                    file_type: FileType::Directory,
                    link_count: 1,
                    size: 0,
                    times: Times::default(),
                    chunk_type: None,
                    maximum_size_allowed: None,
                    first_dir_entry: None,
                    last_dir_entry: None,
                };

                return Some((metadata, None));
            }
        }

        // return None, if no metadata was found under given node
        let metadata = meta_found?;

        // update cache
        meta_cache.update(node, &metadata, None);

        Some((metadata, None))
    }

    // put new metadata value, while overwriting the existing record, at this point the metadata should already be validated
    pub(crate) fn put_metadata(
        &mut self,
        node: u64,
        is_mounted: bool,
        metadata: &Metadata,
        meta_ptr: Option<FileChunkPtr>,
        v2: &mut V2FileChunks<M>,
    ) {
        assert_eq!(node, metadata.node, "Node does not match metadata.node!");

        let (meta_index, meta_cache) = if is_mounted {
            (MOUNTED_METADATA_CHUNK_INDEX, &self.mounted_meta_cache)
        } else {
            (METADATA_CHUNK_INDEX, &self.meta_cache)
        };

        // create a new meta pointer if it was not
        let meta_ptr = if let Some(meta_ptr) = meta_ptr {
            meta_ptr
        } else {
            let meta_ptr = v2.v2_allocator.allocate();

            v2.v2_chunk_ptr.insert((node, meta_index), meta_ptr);

            // prefill new memory with 0 (we want to avoid undefined memory)
            grow_memory(
                &v2.v2_chunks,
                meta_ptr as FileSize + MAX_META_SIZE as FileSize,
            );

            v2.v2_chunks.write(meta_ptr, &ZEROES[0..MAX_META_SIZE]);

            meta_ptr
        };

        // store metadata into chunk
        self.store_chunk_meta(&v2.v2_chunks, meta_ptr, metadata);

        // update cache
        meta_cache.update(node, metadata, Some(meta_ptr));
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
            chunk_type: Some(ChunkType::V2),
            maximum_size_allowed: None,
            first_dir_entry: None,
            last_dir_entry: None,
        };

        cache.update(node, &metadata, Some(1024));

        // Check if the metadata can be retrieved correctly
        let retrieved = cache.get(node);
        assert_eq!(
            retrieved,
            Some((metadata, Some(1024))),
            "Retrieved metadata should match inserted metadata"
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
                chunk_type: Some(ChunkType::V2),
                maximum_size_allowed: None,
                first_dir_entry: None,
                last_dir_entry: None,
            };

            cache.update(node, &metadata, Some(1024));
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
            chunk_type: Some(ChunkType::V2),
            maximum_size_allowed: None,
            first_dir_entry: None,
            last_dir_entry: None,
        };

        cache.update(extra_node, &extra_metadata, Some(1024));

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

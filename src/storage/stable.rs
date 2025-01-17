use std::{collections::HashMap, ops::Range};

use crate::storage::types::ZEROES;
use ic_cdk::api::stable::WASM_PAGE_SIZE_IN_BYTES;
use ic_stable_structures::{
    memory_manager::{MemoryId, MemoryManager, VirtualMemory},
    BTreeMap, Cell, Memory,
};

use crate::{
    runtime::structure_helpers::{read_obj, write_obj},
    storage::ptr_cache::CachedChunkPtr,
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
    chunk_iterator::ChunkV2Iterator,
    metadata_provider::MetadataProvider,
    ptr_cache::PtrCache,
    types::{
        DirEntry, DirEntryIndex, FileChunk, FileChunkIndex, FileChunkPtr, FileSize, FileType,
        Header, Metadata, Node, Times, FILE_CHUNK_SIZE_V1, MAX_FILE_CHUNK_INDEX, MAX_FILE_SIZE,
    },
    Storage,
};

pub const ROOT_NODE: Node = 0;
const FS_VERSION: u32 = 1;

const DEFAULT_FIRST_MEMORY_INDEX: u8 = 229;

// the maximum index accepted as the end range
const MAX_MEMORY_INDEX: u8 = 254;

// the number of memory indices used by the file system (currently 8 plus some reserved ids)
const MEMORY_INDEX_COUNT: u8 = 10;

// index containing cached metadata (deprecated)
const MOUNTED_META_PTR: u64 = 16;

enum StorageMemoryIdx {
    Header = 0,
    Metadata = 1,

    DirEntries = 2,
    FileChunksV1 = 3,

    // metadata for mounted files
    MountedMetadata = 4,

    // V2 chunks
    FileChunksV2 = 5,
    ChunkAllocatorV2 = 6,
    FileChunksMemoryV2 = 7,

    // caching helper
    CacheJournal = 8,
}

struct StorageMemories<M: Memory> {
    header_memory: VirtualMemory<M>,
    metadata_memory: VirtualMemory<M>,
    direntry_memory: VirtualMemory<M>,
    filechunk_memory: VirtualMemory<M>,

    mounted_meta_memory: VirtualMemory<M>,

    v2_chunk_ptr_memory: VirtualMemory<M>,
    v2_chunks_memory: VirtualMemory<M>,
    v2_allocator_memory: VirtualMemory<M>,

    cache_journal: VirtualMemory<M>,
}

#[repr(C)]
pub struct V2FileChunks<M: Memory> {
    // the file chunk storage V2, we only store pointers to reduce serialization overheads.
    pub(crate) v2_chunk_ptr: BTreeMap<(Node, FileChunkIndex), FileChunkPtr, VirtualMemory<M>>,
    // the actual storage of the chunks,
    // * we can read and write small fragments of data, no need to read and write in chunk-sized blocks
    // * the pointers in the BTreeMap (Node, FileChunkIndex) -> FileChunkPtr are static,
    //   this allows caching to avoid chunk search overheads.
    pub(crate) v2_chunks: VirtualMemory<M>,
    // keeps information on the chunks currently available.
    // it can be setup to work with different chunk sizes.
    // 4K - the same as chunks V1, 16K - the default, 64K - the biggest chunk size available.
    // the increased chunk size reduces the number of BTree insertions, and increases the performanc.
    pub(crate) v2_allocator: ChunkPtrAllocator<M>,
}

#[repr(C)]
pub struct StableStorage<M: Memory> {
    // some static-sized filesystem data, contains version number and the next node id.
    header: Cell<Header, VirtualMemory<M>>,
    // information about the directory structure.
    direntry: BTreeMap<(Node, DirEntryIndex), DirEntry, VirtualMemory<M>>,
    // actual file data stored in chunks insize BTreeMap.
    filechunk: BTreeMap<(Node, FileChunkIndex), FileChunk, VirtualMemory<M>>,

    // file data stored in V2 file chunks
    pub(crate) v2_filechunk: V2FileChunks<M>,

    // helper object managing file metadata access of all types
    meta_provider: MetadataProvider<M>,

    // It is not used, but is needed to keep memories alive.
    _memory_manager: Option<MemoryManager<M>>,
    // active mounts.
    active_mounts: HashMap<Node, Box<dyn Memory>>,

    // chunk type to use when creating new files.
    chunk_type: ChunkType,

    // chunk pointer cache. This cache reduces chunk search overhead when reading a file,
    // or writing a file over existing data. (the new files still need insert new pointers into the treemap, hence it is rather slow)
    pub(crate) ptr_cache: PtrCache,
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

        let header_memory = memory_manager.get(MemoryId::new(
            memory_indices.start + StorageMemoryIdx::Header as u8,
        ));
        let metadata_memory = memory_manager.get(MemoryId::new(
            memory_indices.start + StorageMemoryIdx::Metadata as u8,
        ));
        let direntry_memory = memory_manager.get(MemoryId::new(
            memory_indices.start + StorageMemoryIdx::DirEntries as u8,
        ));
        let filechunk_memory = memory_manager.get(MemoryId::new(
            memory_indices.start + StorageMemoryIdx::FileChunksV1 as u8,
        ));
        let mounted_meta_memory = memory_manager.get(MemoryId::new(
            memory_indices.start + StorageMemoryIdx::MountedMetadata as u8,
        ));

        let v2_chunk_ptr_memory = memory_manager.get(MemoryId::new(
            memory_indices.start + StorageMemoryIdx::FileChunksV2 as u8,
        ));
        let v2_allocator_memory = memory_manager.get(MemoryId::new(
            memory_indices.start + StorageMemoryIdx::ChunkAllocatorV2 as u8,
        ));
        let v2_chunks_memory = memory_manager.get(MemoryId::new(
            memory_indices.start + StorageMemoryIdx::FileChunksMemoryV2 as u8,
        ));

        let cache_journal = memory_manager.get(MemoryId::new(
            memory_indices.start + StorageMemoryIdx::CacheJournal as u8,
        ));

        let memories = StorageMemories {
            header_memory,
            metadata_memory,
            direntry_memory,
            filechunk_memory,
            mounted_meta_memory,
            v2_chunk_ptr_memory,
            v2_chunks_memory,
            v2_allocator_memory,
            cache_journal,
        };

        Self::new_with_custom_memories(memories)
    }

    // support deprecated storage, recover stored mounted file metadata
    // we have to use a custom node here,
    fn init_size_from_cache_journal(&mut self, journal: &VirtualMemory<M>) {
        // re-define old Metadata type for correct reading
        #[derive(Clone, Default, PartialEq)]
        pub struct MetadataLegacy {
            pub node: Node,
            pub file_type: FileType,
            pub link_count: u64,
            pub size: FileSize,
            pub times: Times,
            pub first_dir_entry: Option<DirEntryIndex>,
            pub last_dir_entry: Option<DirEntryIndex>,
            pub chunk_type: Option<ChunkType>,
        }

        // try recover stored mounted metadata (if any)
        if journal.size() > 0 {
            let mut mounted_node = 0u64;
            let mut mounted_meta = MetadataLegacy::default();

            read_obj(journal, MOUNTED_META_PTR, &mut mounted_node);

            read_obj(journal, MOUNTED_META_PTR + 8, &mut mounted_meta);

            let meta_read = Metadata {
                node: mounted_meta.node,
                file_type: FileType::RegularFile,
                link_count: mounted_meta.link_count,
                size: mounted_meta.size,
                times: mounted_meta.times,
                first_dir_entry: mounted_meta.first_dir_entry,
                last_dir_entry: mounted_meta.last_dir_entry,
                chunk_type: mounted_meta.chunk_type,
                maximum_size_allowed: None,
            };

            if mounted_node != u64::MAX && mounted_node == mounted_meta.node {
                // immediately store the recovered metadata
                self.meta_provider.put_metadata(
                    mounted_node,
                    true,
                    &meta_read,
                    None,
                    &mut self.v2_filechunk,
                );

                // reset cached metadata
                write_obj(journal, MOUNTED_META_PTR, &(u64::MAX as Node));
            }
        }
    }

    fn new_with_custom_memories(memories: StorageMemories<M>) -> Self {
        let default_header_value = Header {
            version: FS_VERSION,
            next_node: ROOT_NODE + 1,
        };

        let v2_allocator = ChunkPtrAllocator::new(memories.v2_allocator_memory).unwrap();
        let ptr_cache = PtrCache::new();
        let v2_chunk_ptr = BTreeMap::init(memories.v2_chunk_ptr_memory);

        let meta_provider =
            MetadataProvider::new(memories.metadata_memory, memories.mounted_meta_memory);

        let mut result = Self {
            header: Cell::init(memories.header_memory, default_header_value).unwrap(),
            direntry: BTreeMap::init(memories.direntry_memory),
            filechunk: BTreeMap::init(memories.filechunk_memory),

            v2_filechunk: V2FileChunks {
                v2_chunk_ptr,
                v2_chunks: memories.v2_chunks_memory,
                v2_allocator,
            },

            // transient runtime data
            _memory_manager: None,
            active_mounts: HashMap::new(),

            // default chunk type is V2
            chunk_type: ChunkType::V2,

            ptr_cache,

            meta_provider,
        };

        // init mounted drive
        result.init_size_from_cache_journal(&memories.cache_journal);

        let version = result.header.get().version;

        if version != FS_VERSION {
            panic!("Unsupported file system version");
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

    fn write_chunks_v2(
        &mut self,
        node: Node,
        offset: FileSize,
        buf: &[u8],
    ) -> Result<FileSize, Error> {
        let mut remainder = buf.len() as FileSize;
        let last_address = offset + remainder;

        let chunk_size = self.chunk_size();

        let start_index = (offset / chunk_size as FileSize) as FileChunkIndex;

        let mut chunk_offset = offset - start_index as FileSize * chunk_size as FileSize;

        let mut size_written: FileSize = 0;

        let write_iter = ChunkV2Iterator::new(
            node,
            offset,
            last_address,
            self.chunk_size() as FileSize,
            &mut self.ptr_cache,
            &mut self.v2_filechunk.v2_chunk_ptr,
        );

        let write_iter: Vec<_> = write_iter.collect();

        for ((nd, index), chunk_ptr) in write_iter {
            assert!(nd == node);

            if remainder == 0 {
                break;
            }

            let to_write = remainder
                .min(chunk_size as FileSize - chunk_offset)
                .min(buf.len() as FileSize - size_written);

            let write_buf =
                &buf[size_written as usize..(size_written as usize + to_write as usize)];

            let chunk_ptr = if let CachedChunkPtr::ChunkExists(ptr) = chunk_ptr {
                ptr
            } else {
                // insert new chunk
                let ptr = self.v2_filechunk.v2_allocator.allocate();

                grow_memory(&self.v2_filechunk.v2_chunks, ptr + chunk_size as FileSize);

                // fill new chunk with zeroes (appart from the area that will be overwritten)

                // fill before written content
                self.v2_filechunk
                    .v2_chunks
                    .write(ptr, &ZEROES[0..chunk_offset as usize]);

                // fill after written content
                self.v2_filechunk.v2_chunks.write(
                    ptr + chunk_offset + to_write as FileSize,
                    &ZEROES[0..(chunk_size - chunk_offset as usize - to_write as usize)],
                );

                // register new chunk pointer
                self.v2_filechunk.v2_chunk_ptr.insert((node, index), ptr);

                //
                self.ptr_cache
                    .add(vec![((node, index), CachedChunkPtr::ChunkExists(ptr))]);

                ptr
            };

            // growing here should not be required as the grow is called during
            // grow_memory(&self.v2_chunks, chunk_ptr + offset + buf.len() as FileSize);
            self.v2_filechunk
                .v2_chunks
                .write(chunk_ptr + chunk_offset, write_buf);

            chunk_offset = 0;
            size_written += to_write;
            remainder -= to_write;
        }

        Ok(size_written)
    }

    fn read_chunks_v1(
        &self,
        node: Node,
        offset: FileSize,
        file_size: FileSize,
        buf: &mut [u8],
    ) -> Result<FileSize, Error> {
        let start_index = (offset / FILE_CHUNK_SIZE_V1 as FileSize) as FileChunkIndex;
        let end_index = ((offset + buf.len() as FileSize) / FILE_CHUNK_SIZE_V1 as FileSize + 1)
            as FileChunkIndex;

        let mut chunk_offset = offset - start_index as FileSize * FILE_CHUNK_SIZE_V1 as FileSize;

        let range = (node, start_index)..(node + 1, 0);

        let mut size_read: FileSize = 0;
        let mut remainder = file_size - offset;

        let mut iter = self.filechunk.range(range);
        let mut cur_fetched = None;

        for cur_index in start_index..end_index {
            let chunk_space = FILE_CHUNK_SIZE_V1 as FileSize - chunk_offset;

            let to_read = remainder
                .min(chunk_space)
                .min(buf.len() as FileSize - size_read);

            // finished reading, buffer full
            if size_read == buf.len() as FileSize {
                break;
            }

            if cur_fetched.is_none() {
                cur_fetched = iter.next();
            }

            let read_buf = &mut buf[size_read as usize..size_read as usize + to_read as usize];

            if let Some(((nd, idx), ref value)) = cur_fetched {
                if idx == cur_index {
                    assert!(nd == node);

                    read_buf.copy_from_slice(
                        &value.bytes
                            [chunk_offset as usize..chunk_offset as usize + to_read as usize],
                    );

                    // consume token
                    cur_fetched = None;
                } else {
                    // fill up with zeroes
                    read_buf.iter_mut().for_each(|m| *m = 0)
                }
            } else {
                // fill up with zeroes
                read_buf.iter_mut().for_each(|m| *m = 0)
            }

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
        // early exit if nothing left to read
        if offset >= file_size {
            return Ok(0 as FileSize);
        }

        // compute remainder to read
        let mut remainder = file_size - offset;

        let chunk_size = self.chunk_size();

        let start_index = (offset / chunk_size as FileSize) as FileChunkIndex;

        let mut chunk_offset = offset - start_index as FileSize * chunk_size as FileSize;

        //let end_index = ((offset + buf.len() as FileSize) / chunk_size as FileSize + 1) as FileChunkIndex;
        //let mut range = (node, start_index)..(node, end_index);

        let mut size_read: FileSize = 0;

        let read_iter = ChunkV2Iterator::new(
            node,
            offset,
            file_size,
            chunk_size as FileSize,
            &mut self.ptr_cache,
            &mut self.v2_filechunk.v2_chunk_ptr,
        );

        for ((nd, _idx), cached_chunk) in read_iter {
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

            if let CachedChunkPtr::ChunkExists(cptr) = cached_chunk {
                self.v2_filechunk
                    .v2_chunks
                    .read(cptr + chunk_offset, read_buf);
            } else {
                // fill read buffer with 0
                read_buf.iter_mut().for_each(|m| *m = 0)
            }

            chunk_offset = 0;
            size_read += to_read;
            remainder -= to_read;
        }

        Ok(size_read)
    }

    fn use_v2(&mut self, metadata: &Metadata, node: u64) -> bool {
        // decide if we use v2 chunks for reading/writing
        let use_v2 = match metadata.chunk_type {
            Some(ChunkType::V2) => true,
            Some(ChunkType::V1) => false,

            // try to figure out, which chunk type to use
            None => {
                if metadata.size > 0 {
                    // try to find any v2 node, othersize use v1
                    let ptr = self
                        .v2_filechunk
                        .v2_chunk_ptr
                        .range((node, 0)..(node, MAX_FILE_CHUNK_INDEX))
                        .next();

                    ptr.is_some()
                } else {
                    self.chunk_type() == ChunkType::V2
                }
            }
        };
        use_v2
    }

    fn validate_metadata_update(
        old_meta: Option<&Metadata>,
        new_meta: &Metadata,
    ) -> Result<(), Error> {
        if let Some(old_meta) = old_meta {
            if old_meta.node != new_meta.node {
                return Err(Error::IllegalByteSequence);
            }
        }

        if let Some(max_size) = new_meta.maximum_size_allowed {
            if new_meta.size > max_size {
                return Err(Error::FileTooLarge);
            }
        }

        Ok(())
    }

    fn resize_file_internal(&mut self, node: Node, new_size: FileSize) -> Result<(), Error> {
        if self.is_mounted(node) {
            // for the mounted node we only update file size in the metadata (no need to delete chunks)
            return Ok(());
        }

        // delete v1 chunks
        let chunk_size = FILE_CHUNK_SIZE_V1;

        let first_deletable_index = (new_size.div_ceil(chunk_size as FileSize)) as FileChunkIndex;

        let range = (node, first_deletable_index)..(node + 1, 0);

        let mut chunks: Vec<(Node, FileChunkIndex)> = Vec::new();

        for (k, _v) in self.filechunk.range(range) {
            chunks.push(k);
        }

        for (nd, idx) in chunks.into_iter() {
            assert!(nd == node);
            self.filechunk.remove(&(node, idx));
        }

        // fill with zeros the last chunk memory above the file size
        if first_deletable_index > 0 {
            let offset = new_size as FileSize % chunk_size as FileSize;

            self.write_filechunk_v1(
                node,
                first_deletable_index - 1,
                offset,
                &ZEROES[0..(chunk_size - offset as usize)],
            );
        }

        // delete v2 chunks

        let chunk_size = self.chunk_size();

        let first_deletable_index = (new_size.div_ceil(chunk_size as FileSize)) as FileChunkIndex;

        let range = (node, first_deletable_index)..(node, MAX_FILE_CHUNK_INDEX);
        let mut chunks: Vec<(Node, FileChunkIndex)> = Vec::new();
        for (k, _v) in self.v2_filechunk.v2_chunk_ptr.range(range) {
            chunks.push(k);
        }

        for (nd, idx) in chunks.into_iter() {
            assert!(nd == node);
            let removed = self.v2_filechunk.v2_chunk_ptr.remove(&(node, idx));

            if let Some(removed) = removed {
                self.v2_filechunk.v2_allocator.free(removed);
            }
        }

        // fill with zeros the last chunk memory above the file size
        if first_deletable_index > 0 {
            let offset = new_size as FileSize % chunk_size as FileSize;
            self.write_chunks_v2(node, new_size, &ZEROES[0..(chunk_size - offset as usize)])?;
        }

        Ok(())
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
        self.meta_provider
            .get_metadata(
                node,
                self.is_mounted(node),
                &self.v2_filechunk.v2_chunk_ptr,
                &self.v2_filechunk.v2_chunks,
            )
            .map(|x| x.0)
            .ok_or(Error::NoSuchFileOrDirectory)
    }

    // Update the metadata associated with the node.
    fn put_metadata(&mut self, node: Node, metadata: &Metadata) -> Result<(), Error> {
        let is_mounted = self.is_mounted(node);

        let meta_rec = self.meta_provider.get_metadata(
            node,
            is_mounted,
            &self.v2_filechunk.v2_chunk_ptr,
            &self.v2_filechunk.v2_chunks,
        );

        let (old_meta, meta_ptr) = match meta_rec.as_ref() {
            Some((m, p)) => (Some(m), *p),
            None => (None, None),
        };

        Self::validate_metadata_update(old_meta, metadata)?;

        if let Some(old_meta) = old_meta {
            // if the size was reduced, we need to delete the file chunks above the file size
            if metadata.size < old_meta.size {
                self.resize_file_internal(node, metadata.size)?;
            }
        }

        self.meta_provider.put_metadata(
            node,
            is_mounted,
            metadata,
            meta_ptr,
            &mut self.v2_filechunk,
        );

        Ok(())
    }

    // Retrieve the DirEntry instance given the Node and DirEntryIndex.
    fn get_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error> {
        self.direntry
            .get(&(node, index))
            .ok_or(Error::NoSuchFileOrDirectory)
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

        let max_size = metadata.maximum_size_allowed.unwrap_or(MAX_FILE_SIZE);
        let file_size = metadata.size.min(max_size);

        if offset >= file_size {
            return Ok(0);
        }

        let size_read = if let Some(memory) = self.active_mounts.get(&node) {
            let remainder = file_size - offset;
            let to_read = remainder.min(buf.len() as FileSize);

            // grow memory also for reading
            grow_memory(memory.as_ref(), offset + to_read);

            memory.read(offset, &mut buf[..to_read as usize]);
            to_read
        } else {
            let use_v2 = self.use_v2(&metadata, node);

            if use_v2 {
                self.read_chunks_v2(node, offset, file_size, buf)?
            } else {
                self.read_chunks_v1(node, offset, file_size, buf)?
            }
        };

        Ok(size_read)
    }

    // Write file at the current file cursor, the cursor position will NOT be updated after writing.
    fn write(&mut self, node: Node, offset: FileSize, buf: &[u8]) -> Result<FileSize, Error> {
        let mut metadata = self.get_metadata(node)?;

        let max_size = metadata.maximum_size_allowed.unwrap_or(MAX_FILE_SIZE);

        if offset + buf.len() as FileSize > max_size {
            return Err(Error::FileTooLarge);
        }

        let written_size = if let Some(memory) = self.get_mounted_memory(node) {
            self.write_mounted(memory, offset, buf);

            buf.len() as FileSize
        } else {
            let end = offset + buf.len() as FileSize;

            let use_v2 = self.use_v2(&metadata, node);

            if use_v2 {
                self.write_chunks_v2(node, offset, buf)?
            } else {
                let chunk_infos = get_chunk_infos(offset, end, FILE_CHUNK_SIZE_V1);

                let mut written = 0usize;

                for chunk in chunk_infos.into_iter() {
                    self.write_filechunk_v1(
                        node,
                        chunk.index,
                        chunk.offset,
                        &buf[written..(written + chunk.len as usize)],
                    );

                    written += chunk.len as usize;
                }

                written as FileSize
            }
        };

        let end = offset + buf.len() as FileSize;
        if end > metadata.size {
            metadata.size = end;
            self.put_metadata(node, &metadata)?;
        }

        Ok(written_size)
    }

    fn resize_file(&mut self, node: Node, new_size: FileSize) -> Result<(), Error> {
        let mut meta = self.get_metadata(node)?;

        meta.size = new_size;

        self.put_metadata(node, &meta)
    }

    //
    fn rm_file(&mut self, node: Node) -> Result<(), Error> {
        if self.is_mounted(node) {
            return Err(Error::DeviceOrResourceBusy);
        }

        self.resize_file(node, 0)?;

        self.meta_provider.remove_metadata(
            node,
            &mut self.ptr_cache,
            &mut self.filechunk,
            &mut self.v2_filechunk.v2_chunk_ptr,
            &mut self.v2_filechunk.v2_allocator,
        );

        Ok(())
    }

    fn mount_node(&mut self, node: Node, memory: Box<dyn Memory>) -> Result<(), Error> {
        if self.is_mounted(node) {
            return Err(Error::DeviceOrResourceBusy);
        }

        // do extra meta preparation
        // get the file metadata (we are not mounted at this point)
        let mut file_meta = self.get_metadata(node)?;

        // activate mount
        self.active_mounts.insert(node, memory);

        if let Ok(_old_mounted_meta) = self.get_metadata(node) {
            // do nothing, we already have the metadata
        } else {
            // take a copy of the file meta, set the size to 0 by default
            file_meta.size = 0;

            // update mounted metadata
            self.put_metadata(node, &file_meta)?;
        };

        Ok(())
    }

    fn unmount_node(&mut self, node: Node) -> Result<Box<dyn Memory>, Error> {
        let memory = self.active_mounts.remove(&node);

        memory.ok_or(Error::NoSuchDevice)
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

        self.put_metadata(node, &meta)?;

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

            // grow memory also for reading
            grow_memory(memory.as_ref(), offset + to_read);

            memory.read(offset, &mut buf[..to_read as usize]);

            self.write(node, offset, &buf[..to_read as usize])?;

            offset += to_read;
            remainder -= to_read;
        }

        self.put_metadata(node, &meta)?;

        self.mount_node(node, memory)?;

        Ok(())
    }

    fn set_chunk_size(&mut self, chunk_size: ChunkSize) -> Result<(), Error> {
        self.v2_filechunk
            .v2_allocator
            .set_chunk_size(chunk_size as usize)
    }

    fn chunk_size(&self) -> usize {
        self.v2_filechunk.v2_allocator.chunk_size()
    }

    fn set_chunk_type(&mut self, chunk_type: ChunkType) {
        self.chunk_type = chunk_type;
    }

    fn chunk_type(&self) -> ChunkType {
        self.chunk_type
    }

    fn flush(&mut self, _node: Node) {
        // nothing to flush, the system immediately stores data on write
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
        storage
            .put_metadata(
                node,
                &Metadata {
                    node,
                    file_type: FileType::RegularFile,
                    link_count: 1,
                    size: 10,
                    times: Times::default(),
                    first_dir_entry: Some(42),
                    last_dir_entry: Some(24),
                    chunk_type: Some(storage.chunk_type()),
                    maximum_size_allowed: None,
                },
            )
            .unwrap();
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

    fn new_file<M: Memory>(storage: &mut StableStorage<M>) -> Node {
        let node = storage.new_node();

        storage
            .put_metadata(
                node,
                &Metadata {
                    node,
                    file_type: FileType::RegularFile,
                    link_count: 1,
                    size: 0,
                    times: Times::default(),
                    first_dir_entry: None,
                    last_dir_entry: None,
                    chunk_type: Some(storage.chunk_type()),
                    maximum_size_allowed: None,
                },
            )
            .unwrap();

        node
    }

    #[test]
    fn read_beyond_file_size() {
        let mut storage = StableStorage::new(DefaultMemoryImpl::default());

        let node = new_file(&mut storage);

        storage.write(node, 0, b"hello").unwrap();

        let mut buf = [0u8; 10];
        let bytes_read = storage.read(node, 3, &mut buf).unwrap();

        assert_eq!(bytes_read, 2);
        assert_eq!(&buf[..2], b"lo");

        assert_eq!(buf[2..], [0; 8]);
    }

    #[test]
    fn switch_chunk_types() {
        let mut storage = StableStorage::new(DefaultMemoryImpl::default());

        storage.set_chunk_type(ChunkType::V1);

        let node_v1 = new_file(&mut storage);

        storage.write(node_v1, 0, b"v1_data").unwrap();

        storage.set_chunk_type(ChunkType::V2);

        let node_v2 = new_file(&mut storage);

        // Write data
        storage.write(node_v2, 0, b"v2_data").unwrap();

        // Confirm reads
        let mut buf_v1 = [0u8; 7];
        storage.read(node_v1, 0, &mut buf_v1).unwrap();
        assert_eq!(&buf_v1, b"v1_data");
        let meta = storage.get_metadata(node_v1).unwrap();
        assert_eq!(meta.chunk_type.unwrap(), ChunkType::V1);

        let mut buf_v2 = [0u8; 7];
        storage.read(node_v2, 0, &mut buf_v2).unwrap();
        assert_eq!(&buf_v2, b"v2_data");
        let meta = storage.get_metadata(node_v2).unwrap();
        assert_eq!(meta.chunk_type.unwrap(), ChunkType::V2);
    }

    #[test]
    fn resize_file_shrink_and_grow() {
        let mut storage = StableStorage::new(DefaultMemoryImpl::default());
        let node = new_file(&mut storage);

        storage.write(node, 0, b"1234567890").unwrap();
        let mut buf = [0u8; 10];
        storage.read(node, 0, &mut buf).unwrap();
        assert_eq!(&buf, b"1234567890");

        // Shrink to 5 bytes
        storage.resize_file(node, 5).unwrap();

        let meta = storage.get_metadata(node).unwrap();
        assert_eq!(meta.size, 5); // Check the metadata reflects new size

        // Reading the file now
        let mut buf_small = [0u8; 10];
        let bytes_read = storage.read(node, 0, &mut buf_small).unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(&buf_small[..5], b"12345");
        assert_eq!(&buf_small[5..], [0; 5]);

        // check zero fill
        let mut meta = storage.get_metadata(node).unwrap();
        meta.size = 10;
        storage.put_metadata(node, &meta).unwrap();

        // Confirm new bytes are zeroed or remain uninitialized, depending on design
        let mut buf_grow = [0u8; 10];
        storage.read(node, 0, &mut buf_grow).unwrap();
        // First 5 bytes should remain "12345", rest must be zero:
        assert_eq!(&buf_grow[..5], b"12345");
        assert_eq!(&buf_grow[5..], [0; 5]);
    }

    #[test]
    fn resize_file_shrink_deletes_v2_chunks() {
        let mut storage = StableStorage::new(DefaultMemoryImpl::default());
        let node = new_file(&mut storage);

        let chunk_size = storage.chunk_size() as FileSize;

        // write something into the second chunk
        storage.write(node, chunk_size + 4, b"1234567890").unwrap();

        // write something into the first chunk
        storage.write(node, 4, b"1234567890").unwrap();

        let mut buf = [0u8; 10];
        // read second chunk
        storage.read(node, chunk_size + 9, &mut buf).unwrap();

        assert_eq!(&buf, b"67890\0\0\0\0\0");

        let chunks: Vec<_> = storage
            .v2_filechunk
            .v2_chunk_ptr
            .range((node, 0)..(node, 5))
            .collect();
        assert_eq!(chunks.len(), 2);

        // Shrink to 5 bytes
        storage.resize_file(node, 5).unwrap();

        let meta = storage.get_metadata(node).unwrap();

        // only one chunk should be present
        assert_eq!(meta.size, 5); // Check the metadata reflects new size

        let chunks: Vec<_> = storage
            .v2_filechunk
            .v2_chunk_ptr
            .range((node, 0)..(node, 5))
            .collect();
        assert_eq!(chunks.len(), 1);

        // for 0 size, all chunks have to be deleted
        storage.resize_file(node, 0).unwrap();
        let chunks: Vec<_> = storage
            .v2_filechunk
            .v2_chunk_ptr
            .range((node, 0)..(node, 5))
            .collect();
        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn resize_file_shrink_deletes_v1_chunks() {
        let mut storage = StableStorage::new(DefaultMemoryImpl::default());
        storage.set_chunk_type(ChunkType::V1);

        let node = new_file(&mut storage);

        let chunk_size = FILE_CHUNK_SIZE_V1 as FileSize;

        // write something into the second chunk
        storage.write(node, chunk_size + 4, b"1234567890").unwrap();

        // write something into the first chunk
        storage.write(node, 4, b"1234567890").unwrap();

        let mut buf = [0u8; 10];
        // read second chunk
        storage.read(node, chunk_size + 9, &mut buf).unwrap();

        assert_eq!(&buf, b"67890\0\0\0\0\0");

        let chunks: Vec<_> = storage.filechunk.range((node, 0)..(node, 5)).collect();
        assert_eq!(chunks.len(), 2);

        // Shrink to 5 bytes
        storage.resize_file(node, 5).unwrap();

        let meta = storage.get_metadata(node).unwrap();

        // only one chunk should be present
        assert_eq!(meta.size, 5); // Check the metadata reflects new size

        let chunks: Vec<_> = storage.filechunk.range((node, 0)..(node, 5)).collect();
        assert_eq!(chunks.len(), 1);

        // for 0 size, all chunks have to be deleted
        storage.resize_file(node, 0).unwrap();
        let chunks: Vec<_> = storage.filechunk.range((node, 0)..(node, 5)).collect();
        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn remove_file_chunks_v2() {
        let mut storage = StableStorage::new(DefaultMemoryImpl::default());

        let chunk_size = storage.chunk_size() as FileSize;

        // some other files present
        let node_other = new_file(&mut storage);
        storage.write(node_other, 0, b"some data").unwrap();

        let node = new_file(&mut storage);

        // write into 5 chunks + 1 metadata chunk, expect to find 6 chunks
        storage.write(node, 0, b"some data").unwrap();
        storage.write(node, chunk_size, b"some data").unwrap();
        storage.write(node, chunk_size * 2, b"some data").unwrap();
        // write into two chunks with one call
        storage
            .write(node, chunk_size * 5 - 2, b"some data")
            .unwrap();

        // some other files present
        let node_other2 = new_file(&mut storage);
        storage.write(node_other2, 0, b"some data").unwrap();

        // check chunk count
        let chunks: Vec<_> = storage
            .v2_filechunk
            .v2_chunk_ptr
            .range((node, 0)..(node + 1, 0))
            .collect();

        // chunks of the given node
        assert_eq!(chunks.len(), 6);

        // check the allocator is also holding 6 chunks of the main file, and 4 chunks from the two other files
        assert_eq!(
            storage.v2_filechunk.v2_allocator.get_current_max_ptr(),
            (6 + 4) * chunk_size
        );

        // Remove file
        storage.rm_file(node).unwrap();

        // Confirm reading fails or returns NotFound
        let mut buf = [0u8; 9];
        let res = storage.read(node, 0, &mut buf);
        assert!(matches!(res, Err(Error::NoSuchFileOrDirectory)));

        // Confirm metadata is removed
        let meta_res = storage.get_metadata(node);
        assert!(matches!(meta_res, Err(Error::NoSuchFileOrDirectory)));

        // check there are no chunks left after deleting the node
        let chunks: Vec<_> = storage
            .v2_filechunk
            .v2_chunk_ptr
            .range((node, 0)..(node + 1, 0))
            .collect();

        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn remove_file_chunks_v1() {
        let mut storage = StableStorage::new(DefaultMemoryImpl::default());
        storage.set_chunk_type(ChunkType::V1);

        let chunk_size = FILE_CHUNK_SIZE_V1 as FileSize;

        // some other files present
        let node_other = new_file(&mut storage);
        storage.write(node_other, 0, b"some data").unwrap();

        let node = new_file(&mut storage);

        // write into 5 chunks
        storage.write(node, 0, b"some data").unwrap();
        storage.write(node, chunk_size, b"some data").unwrap();
        storage.write(node, chunk_size * 2, b"some data").unwrap();
        // write into two chunks with one call
        storage
            .write(node, chunk_size * 5 - 2, b"some data")
            .unwrap();

        // some other files present
        let node_other2 = new_file(&mut storage);
        storage.write(node_other2, 0, b"some data").unwrap();

        // check chunk count
        let chunks: Vec<_> = storage.filechunk.range((node, 0)..(node + 1, 0)).collect();

        // chunks of the given node
        assert_eq!(chunks.len(), 5);

        // check the allocator is holding three chunks for 3 stored metadata
        assert_eq!(
            storage.v2_filechunk.v2_allocator.get_current_max_ptr(),
            storage.chunk_size() as FileSize * 3
        );

        // Remove file
        storage.rm_file(node).unwrap();

        // Confirm reading fails or returns NotFound
        let mut buf = [0u8; 9];
        let res = storage.read(node, 0, &mut buf);
        assert!(matches!(res, Err(Error::NoSuchFileOrDirectory)));

        // Confirm metadata is removed
        let meta_res = storage.get_metadata(node);
        assert!(matches!(meta_res, Err(Error::NoSuchFileOrDirectory)));

        // check there are no chunks left after deleting the node
        let chunks: Vec<_> = storage.filechunk.range((node, 0)..(node + 1, 0)).collect();

        assert_eq!(chunks.len(), 0);
    }
}

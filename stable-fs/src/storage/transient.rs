use std::collections::{BTreeMap, HashMap};

use ic_cdk::stable::WASM_PAGE_SIZE_IN_BYTES;
use ic_stable_structures::Memory;

use crate::{
    error::Error,
    fs::{ChunkSize, ChunkType},
    runtime::structure_helpers::{get_chunk_infos, grow_memory},
    storage::{
        Storage,
        types::{
            DirEntry, DirEntryIndex, FileChunk, FileChunkIndex, FileSize, FileType, Metadata, Node,
            Times, ZEROES,
        },
    },
};

use super::types::{
    DUMMY_DOT_DOT_ENTRY, DUMMY_DOT_DOT_ENTRY_INDEX, DUMMY_DOT_ENTRY, DUMMY_DOT_ENTRY_INDEX,
    FILE_CHUNK_SIZE_V1, Header, MAX_FILE_CHUNK_COUNT, MAX_FILE_ENTRY_INDEX,
};

// The root node ID.
const ROOT_NODE: Node = 0;

const FS_TRANSIENT_VERSION: u32 = 1;

// Transient storage representation.
#[derive(Default)]
pub struct TransientStorage {
    header: Header,
    // Node metadata information.
    metadata: BTreeMap<Node, Metadata>,
    // Directory entries for each of the directory node.
    direntry: BTreeMap<(Node, DirEntryIndex), DirEntry>,
    // File contents for each of the file node.
    filechunk: BTreeMap<(Node, FileChunkIndex), FileChunk>,
    // Mounted memory Node metadata information.
    mounted_meta: BTreeMap<Node, Metadata>,
    // Active mounts.
    active_mounts: HashMap<Node, Box<dyn Memory>>,
}

impl TransientStorage {
    // Initializes a new TransientStorage.
    pub fn new() -> Self {
        let metadata = Metadata {
            node: ROOT_NODE,
            file_type: FileType::Directory,
            link_count: 1,
            size: 0,
            times: Times::default(),
            first_dir_entry: None,
            last_dir_entry: None,
            chunk_type: None,
            maximum_size_allowed: None,
        };

        let mut result = Self {
            header: Header {
                version: 1,
                next_node: ROOT_NODE + 1,
            },
            metadata: Default::default(),
            direntry: Default::default(),
            filechunk: Default::default(),

            mounted_meta: Default::default(),
            active_mounts: Default::default(),
        };

        result
            .put_metadata(ROOT_NODE, &metadata)
            .expect("Failed to create metadata");

        result
    }

    // Insert of update a selected file chunk with the data provided in buffer.
    fn write_filechunk(&mut self, node: Node, index: FileChunkIndex, offset: FileSize, buf: &[u8]) {
        if let Some(memory) = self.get_mounted_memory(node) {
            // grow memory if needed
            let max_address = index as FileSize * FILE_CHUNK_SIZE_V1 as FileSize
                + offset as FileSize
                + buf.len() as FileSize;

            grow_memory(memory, max_address);

            // store data
            let address = index as FileSize * FILE_CHUNK_SIZE_V1 as FileSize + offset as FileSize;
            memory.write(address, buf);
        } else {
            let entry = self.filechunk.entry((node, index)).or_default();
            entry.bytes[offset as usize..offset as usize + buf.len()].copy_from_slice(buf)
        }
    }

    fn validate_metadata_update(
        old_meta: Option<&Metadata>,
        new_meta: &Metadata,
    ) -> Result<(), Error> {
        if let Some(max_size) = new_meta.maximum_size_allowed
            && new_meta.size > max_size
        {
            return Err(Error::FileTooLarge);
        }

        if let Some(old_meta) = old_meta
            && old_meta.node != new_meta.node
        {
            return Err(Error::InvalidArgument);
        }

        Ok(())
    }
}

impl Storage for TransientStorage {
    // Get the root node ID of the storage
    fn root_node(&self) -> Node {
        ROOT_NODE
    }

    // Get version of the file system
    fn get_version(&self) -> u32 {
        FS_TRANSIENT_VERSION
    }

    // Generate the next available node ID.
    fn new_node(&mut self) -> Node {
        let result = self.header.next_node;
        self.header.next_node += 1;
        result
    }

    // Get the metadata associated with the node.
    fn get_metadata(&self, node: Node) -> Result<Metadata, Error> {
        let meta = if self.is_mounted(node) {
            self.mounted_meta
                .get(&node)
                .ok_or(Error::NoSuchFileOrDirectory)?
        } else {
            self.metadata
                .get(&node)
                .ok_or(Error::NoSuchFileOrDirectory)?
        };

        Ok(meta.clone())
    }

    // Update the metadata associated with the node.
    fn put_metadata(&mut self, node: Node, metadata: &Metadata) -> Result<(), Error> {
        let meta_storage = if self.is_mounted(node) {
            &mut self.mounted_meta
        } else {
            &mut self.metadata
        };

        if let Some(existing) = meta_storage.get(&node) {
            Self::validate_metadata_update(Some(existing), metadata)?;
        } else {
            Self::validate_metadata_update(None, metadata)?;
        }

        meta_storage.insert(node, metadata.clone());

        Ok(())
    }

    // Retrieve the DirEntry instance given the Node and DirEntryIndex.
    fn get_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error> {
        let value = self
            .direntry
            .get(&(node, index))
            .ok_or(Error::NoSuchFileOrDirectory)?;
        Ok(value.clone())
    }

    // Update or insert the DirEntry instance given the Node and DirEntryIndex.
    fn put_direntry(&mut self, node: Node, index: DirEntryIndex, entry: DirEntry) {
        self.direntry.insert((node, index), entry);
    }

    // Remove the DirEntry instance given the Node and DirEntryIndex.
    fn rm_direntry(&mut self, node: Node, index: DirEntryIndex) {
        self.direntry.remove(&(node, index));
    }

    // Fill the buffer contents with data
    fn read(&mut self, node: Node, offset: FileSize, buf: &mut [u8]) -> Result<FileSize, Error> {
        let file_size = self.get_metadata(node)?.size;

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
            let start_index = (offset / FILE_CHUNK_SIZE_V1 as FileSize) as FileChunkIndex;

            let end_index = ((offset + buf.len() as FileSize) / FILE_CHUNK_SIZE_V1 as FileSize + 1)
                as FileChunkIndex;

            let mut chunk_offset =
                offset - start_index as FileSize * FILE_CHUNK_SIZE_V1 as FileSize;

            let range = (node, start_index)..(node, MAX_FILE_CHUNK_COUNT);

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

                if let Some(((nd, idx), value)) = cur_fetched {
                    if *idx == cur_index {
                        assert!(*nd == node);

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

            size_read
        };

        Ok(size_read)
    }

    fn resize_file(&mut self, node: Node, new_size: FileSize) -> Result<(), Error> {
        let chunk_size = FILE_CHUNK_SIZE_V1;

        let first_deletable_index = (new_size.div_ceil(chunk_size as FileSize)) as FileChunkIndex;

        let range = (node, 0)..(node, MAX_FILE_CHUNK_COUNT);

        // delete v1 chunks
        let mut chunks: Vec<(Node, FileChunkIndex)> = Vec::new();
        for (k, _v) in self.filechunk.range(range) {
            chunks.push((k.0, k.1));
        }

        for (nd, idx) in chunks.into_iter() {
            assert!(nd == node);
            self.filechunk.remove(&(node, idx));
        }

        // fill with zeros the last chunk memory above the file size
        if first_deletable_index > 0 {
            let offset = new_size as FileSize % chunk_size as FileSize;

            self.write_filechunk(
                node,
                first_deletable_index - 1,
                offset,
                &ZEROES[0..(chunk_size - offset as usize)],
            );
        }

        Ok(())
    }

    fn rm_file(&mut self, node: Node) -> Result<(), Error> {
        if self.is_mounted(node) {
            return Err(Error::DeviceOrResourceBusy);
        }

        self.resize_file(node, 0)?;

        // remove metadata
        self.mounted_meta.remove(&node);
        self.metadata.remove(&node);

        Ok(())
    }

    fn mount_node(&mut self, node: Node, memory: Box<dyn Memory>) -> Result<(), Error> {
        if self.is_mounted(node) {
            return Err(Error::DeviceOrResourceBusy);
        }

        // do extra meta preparation
        let mut meta = self
            .metadata
            .get(&node)
            .ok_or(Error::NoSuchFileOrDirectory)?
            .clone();

        self.active_mounts.insert(node, memory);

        let new_mounted_meta = if let Some(old_mounted_meta) = self.mounted_meta.get(&node) {
            // we can change here something for the new mounted meta
            old_mounted_meta.clone()
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

        memory.ok_or(Error::NoSuchDevice)
    }

    fn is_mounted(&self, node: Node) -> bool {
        self.active_mounts.contains_key(&node)
    }

    fn get_mounted_memory(&self, node: Node) -> Option<&dyn Memory> {
        let res = self.active_mounts.get(&node);

        res.map(|b| b.as_ref())
    }

    fn init_mounted_memory(&mut self, node: Node) -> Result<(), Error> {
        // temporary disable mount to activate access to the original file
        let memory: Box<dyn Memory> = self.unmount_node(node)?;

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
        let memory: Box<dyn Memory> = self.unmount_node(node)?;

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

        self.put_metadata(node, &meta)?;

        self.mount_node(node, memory)?;

        Ok(())
    }

    fn write(&mut self, node: Node, offset: FileSize, buf: &[u8]) -> Result<FileSize, Error> {
        let mut metadata = self.get_metadata(node)?;

        // do not attempt to write 0 bytes to avoid file resize (when writing above file size)
        if buf.is_empty() {
            return Ok(0);
        }

        let end = offset + buf.len() as FileSize;

        if let Some(max_size) = metadata.maximum_size_allowed
            && end > max_size
        {
            return Err(Error::FileTooLarge);
        }

        let chunk_infos = get_chunk_infos(offset, end, FILE_CHUNK_SIZE_V1);
        let mut written_size = 0;
        for chunk in chunk_infos.into_iter() {
            self.write_filechunk(
                node,
                chunk.index,
                chunk.offset,
                &buf[written_size..written_size + chunk.len as usize],
            );
            written_size += chunk.len as usize;
        }

        if end > metadata.size {
            metadata.size = end;
            self.put_metadata(node, &metadata)?;
        }

        Ok(written_size as FileSize)
    }

    fn set_chunk_size(&mut self, _chunk_size: ChunkSize) -> Result<(), Error> {
        // Noop
        Ok(())
    }

    fn chunk_size(&self) -> usize {
        FILE_CHUNK_SIZE_V1
    }

    fn set_chunk_type(&mut self, _chunk_type: ChunkType) {
        // Noop
    }

    fn chunk_type(&self) -> ChunkType {
        ChunkType::V1
    }

    fn flush(&mut self, _node: Node) {
        // Noop
    }

    fn with_direntries(
        &self,
        node: Node,
        initial_index: Option<DirEntryIndex>,
        f: &mut dyn FnMut(&DirEntryIndex, &DirEntry) -> bool,
    ) {
        if initial_index.is_none() {
            let mut dot_entry = DUMMY_DOT_ENTRY;
            dot_entry.1.node = node;
            if !f(&dot_entry.0, &dot_entry.1) {
                return;
            }

            if !f(&DUMMY_DOT_DOT_ENTRY.0, &DUMMY_DOT_DOT_ENTRY.1) {
                return;
            }
        }

        let initial_index = initial_index.unwrap_or(0);

        if initial_index == DUMMY_DOT_ENTRY_INDEX {
            let mut dot_entry = DUMMY_DOT_ENTRY;
            dot_entry.1.node = node;

            if !f(&dot_entry.0, &dot_entry.1) {
                return;
            }

            if !f(&DUMMY_DOT_DOT_ENTRY.0, &DUMMY_DOT_DOT_ENTRY.1) {
                return;
            }
        }

        if initial_index == DUMMY_DOT_DOT_ENTRY_INDEX
            && !f(&DUMMY_DOT_DOT_ENTRY.0, &DUMMY_DOT_DOT_ENTRY.1)
        {
            return;
        }

        let max_index = MAX_FILE_ENTRY_INDEX;

        for ((_node, index), entry) in self
            .direntry
            .range((node, initial_index)..(node, max_index))
        {
            if !f(index, entry) {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_and_write_filechunk() {
        let mut storage = TransientStorage::default();
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
                    first_dir_entry: None,
                    last_dir_entry: None,
                    chunk_type: Some(storage.chunk_type()),
                    maximum_size_allowed: None,
                },
            )
            .unwrap();
        storage.write(node, 0, &[42; 10]).unwrap();
        let mut buf = [0; 10];
        storage.read(node, 0, &mut buf).unwrap();
        assert_eq!(buf, [42; 10]);
    }
}

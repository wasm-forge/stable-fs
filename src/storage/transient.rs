use std::collections::{BTreeMap, HashMap};

use ic_cdk::api::stable::WASM_PAGE_SIZE_IN_BYTES;
use ic_stable_structures::Memory;

use crate::{
    error::Error,
    storage::types::{
        DirEntry, DirEntryIndex, FileChunk, FileChunkIndex, FileSize, FileType, Metadata, Node,
        Times,
    },
    storage::Storage,
};

use super::types::{Header, FILE_CHUNK_SIZE};

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
    // active mounts
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
        result.put_metadata(ROOT_NODE, metadata);
        result
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
            self.mounted_meta.get(&node).ok_or(Error::NotFound)?
        } else {
            self.metadata.get(&node).ok_or(Error::NotFound)?
        };

        Ok(meta.clone())
    }

    // Update the metadata associated with the node.
    fn put_metadata(&mut self, node: Node, metadata: Metadata) {
        if self.is_mounted(node) {
            self.mounted_meta.insert(node, metadata);
        } else {
            self.metadata.insert(node, metadata);
        }
    }

    // Remove the metadata associated with the node.
    fn rm_metadata(&mut self, node: Node) {
        if self.is_mounted(node) {
            self.mounted_meta.remove(&node);
        } else {
            self.metadata.remove(&node);
        }
    }

    // Retrieve the DirEntry instance given the Node and DirEntryIndex.
    fn get_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error> {
        let value = self.direntry.get(&(node, index)).ok_or(Error::NotFound)?;
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

    // Fill the buffer contents with data of a chosen file chunk.
    #[cfg(test)]
    fn read_filechunk(
        &self,
        node: Node,
        index: FileChunkIndex,
        offset: FileSize,
        buf: &mut [u8],
    ) -> Result<(), Error> {
        if let Some(memory) = self.get_mounted_memory(node) {
            // work with memory
            let address = index as FileSize * FILE_CHUNK_SIZE as FileSize + offset as FileSize;
            memory.read(address, buf);
        } else {
            let value = self.filechunk.get(&(node, index)).ok_or(Error::NotFound)?;
            buf.copy_from_slice(&value.bytes[offset as usize..offset as usize + buf.len()]);
        }

        Ok(())
    }

    // Fill the buffer contents with data
    fn read_range(
        &self,
        node: Node,
        offset: FileSize,
        file_size: FileSize,
        buf: &mut [u8],
    ) -> Result<FileSize, Error> {
        if offset >= file_size {
            return Ok(0);
        }

        let size_read = if let Some(memory) = self.get_mounted_memory(node) {
            let remainder = file_size - offset;
            let to_read = remainder.min(buf.len() as FileSize);

            memory.read(offset, &mut buf[..to_read as usize]);
            to_read
        } else {
            let start_index = (offset / FILE_CHUNK_SIZE as FileSize) as FileChunkIndex;

            let mut chunk_offset = offset - start_index as FileSize * FILE_CHUNK_SIZE as FileSize;

            let range = (node, start_index)..(node + 1, 0);

            let mut size_read: FileSize = 0;
            let mut remainder = file_size - offset;

            for ((nd, _idx), value) in self.filechunk.range(range) {
                assert!(*nd == node);

                // finished reading, buffer full
                if size_read == buf.len() as FileSize {
                    break;
                }

                let chunk_space = FILE_CHUNK_SIZE as FileSize - chunk_offset;

                let to_read = remainder
                    .min(chunk_space)
                    .min(buf.len() as FileSize - size_read);

                let write_buf = &mut buf[size_read as usize..size_read as usize + to_read as usize];

                write_buf.copy_from_slice(
                    &value.bytes[chunk_offset as usize..chunk_offset as usize + to_read as usize],
                );

                chunk_offset = 0;

                size_read += to_read;
                remainder -= to_read;
            }

            size_read
        };

        Ok(size_read)
    }

    // Insert of update a selected file chunk with the data provided in buffer.
    fn write_filechunk(&mut self, node: Node, index: FileChunkIndex, offset: FileSize, buf: &[u8]) {
        if let Some(memory) = self.get_mounted_memory(node) {
            
            // grow memory if needed
            let max_address = index as FileSize * FILE_CHUNK_SIZE as FileSize
                + offset as FileSize
                + buf.len() as FileSize;
            let pages_required =
                (max_address + WASM_PAGE_SIZE_IN_BYTES - 1) / WASM_PAGE_SIZE_IN_BYTES;

            let cur_pages = memory.size();

            if cur_pages < pages_required {
                memory.grow(pages_required - cur_pages);
            }

            // store data
            let address = index as FileSize * FILE_CHUNK_SIZE as FileSize + offset as FileSize;
            memory.write(address, buf);
        } else {
            let entry = self.filechunk.entry((node, index)).or_default();
            entry.bytes[offset as usize..offset as usize + buf.len()].copy_from_slice(buf)
        }
    }

    // Remove file chunk from a given file node.
    fn rm_filechunk(&mut self, node: Node, index: FileChunkIndex) {
        self.filechunk.remove(&(node, index));
    }

    fn mount_node(&mut self, node: Node, memory: Box<dyn Memory>) -> Result<(), Error> {
        if self.is_mounted(node) {
            return Err(Error::IsMountedAlready);
        }

        // do extra meta preparation
        let mut meta = self.metadata.get(&node).ok_or(Error::NotFound)?.clone();

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

        memory.ok_or(Error::FileIsNotMounted)
    }

    fn is_mounted(&self, node: Node) -> bool {
        self.active_mounts.contains_key(&node)
    }

    fn get_mounted_memory(&self, node: Node) -> Option<&dyn Memory> {
        let res = self.active_mounts.get(&node);

        res.map(|b| b.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_and_write_filechunk() {
        let mut storage = TransientStorage::default();
        let node = storage.new_node();
        storage.put_metadata(
            node,
            Metadata {
                node,
                file_type: FileType::RegularFile,
                link_count: 1,
                size: 10,
                times: Times::default(),
                first_dir_entry: None,
                last_dir_entry: None,
            },
        );
        storage.write_filechunk(node, 0, 0, &[42; 10]);
        let mut buf = [0; 10];
        storage.read_filechunk(node, 0, 0, &mut buf).unwrap();
        assert_eq!(buf, [42; 10]);
    }
}

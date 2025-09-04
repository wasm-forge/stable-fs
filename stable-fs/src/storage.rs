use ic_stable_structures::Memory;

use crate::{
    error::Error,
    fs::ChunkSize,
    fs::ChunkType,
    storage::types::{DirEntry, DirEntryIndex, FileSize, Metadata, Node},
};

mod allocator;
mod chunk_iterator;
pub mod dummy;
mod journal;
mod metadata_provider;
mod ptr_cache;
pub mod stable;
pub mod transient;
pub mod types;

/// Abstraction of the underlying storage layer.
pub trait Storage {
    /// Get the root node ID of the storage.
    fn root_node(&self) -> Node;

    /// Get version of the file system.
    fn get_version(&self) -> u32;

    /// Generate the next available node ID.
    fn new_node(&mut self) -> Node;

    /// mark node as mounted.
    fn mount_node(&mut self, node: Node, memory: Box<dyn Memory>) -> Result<(), Error>;
    /// mark note as not mounted.
    fn unmount_node(&mut self, node: Node) -> Result<Box<dyn Memory>, Error>;
    /// return true if the node is mounted.
    fn is_mounted(&self, node: Node) -> bool;
    /// return mounted memory related to the node, or None.
    fn get_mounted_memory(&self, node: Node) -> Option<&dyn Memory>;

    /// initialize memory with the contents from file.
    fn init_mounted_memory(&mut self, node: Node) -> Result<(), Error>;
    /// store mounted memory state back to host file.
    fn store_mounted_memory(&mut self, node: Node) -> Result<(), Error>;

    /// Get the metadata associated with the node.
    fn get_metadata(&self, node: Node) -> Result<Metadata, Error>;
    /// Update the metadata associated with the node.
    fn put_metadata(&mut self, node: Node, metadata: &Metadata) -> Result<(), Error>;

    /// Retrieve the DirEntry instance given the Node and DirEntryIndex.
    fn get_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error>;

    /// get entries iterator
    fn with_direntries(
        &self,
        node: Node,
        initial_index: Option<DirEntryIndex>,
        f: &mut dyn FnMut(&DirEntryIndex, &DirEntry) -> bool,
    );

    // Update or insert the DirEntry instance given the Node and DirEntryIndex.
    fn put_direntry(&mut self, node: Node, index: DirEntryIndex, entry: DirEntry);
    // Remove the DirEntry instance given the Node and DirEntryIndex.
    fn rm_direntry(&mut self, node: Node, index: DirEntryIndex);

    // read node data into buf
    fn read(
        &mut self,
        node: Node,
        read_offset: FileSize,
        buf: &mut [u8],
    ) -> Result<FileSize, Error>;

    // Write file at the current file cursor, the cursor position will NOT be updated after reading.
    fn write(&mut self, node: Node, offset: FileSize, buf: &[u8]) -> Result<FileSize, Error>;

    // delete chunks to match the new file size specified
    fn resize_file(&mut self, node: Node, new_size: FileSize) -> Result<(), Error>;

    // remove all file chunks
    fn rm_file(&mut self, node: Node) -> Result<(), Error>;

    // configure desired chunk size
    fn set_chunk_size(&mut self, chunk_size: ChunkSize) -> Result<(), Error>;
    // the current FS chunk size in bytes
    fn chunk_size(&self) -> usize;

    // configure desired chunk type (V1, V2)
    fn set_chunk_type(&mut self, chunk_type: ChunkType);
    fn chunk_type(&self) -> ChunkType;

    // flush changes related to the node
    fn flush(&mut self, node: Node);
}

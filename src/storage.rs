use ic_stable_structures::Memory;

use crate::{
    error::Error,
    storage::types::{DirEntry, DirEntryIndex, FileChunkIndex, FileSize, Metadata, Node},
};

pub mod dummy;
pub mod stable;
pub mod transient;
pub mod types;

// Abstraction of the underlying storage layer.
pub trait Storage {
    // Get the root node ID of the storage.
    fn root_node(&self) -> Node;

    // Get version of the file system.
    fn get_version(&self) -> u32;

    // Generate the next available node ID.
    fn new_node(&mut self) -> Node;

    // mark node as mounted.
    fn mount_node(&mut self, node: Node, memory: Box<dyn Memory>) -> Result<(), Error>;
    // mark note as not mounted.
    fn unmount_node(&mut self, node: Node) -> Result<Box<dyn Memory>, Error>;
    // return true if the node is mounted.
    fn is_mounted(&self, node: Node) -> bool;
    // return mounted memory related to the node, or None.
    fn get_mounted_memory(&self, node: Node) -> Option<&dyn Memory>;

    // initialize memory with the contents from file.
    fn init_mounted_memory(&mut self, node: Node) -> Result<(), Error>;
    // store mounted memory state back to host file.
    fn store_mounted_memory(&mut self, node: Node) -> Result<(), Error>;

    // Get the metadata associated with the node.
    fn get_metadata(&self, node: Node) -> Result<Metadata, Error>;
    // Update the metadata associated with the node.
    fn put_metadata(&mut self, node: Node, metadata: Metadata);

    // Retrieve the DirEntry instance given the Node and DirEntryIndex.
    fn get_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error>;
    // Update or insert the DirEntry instance given the Node and DirEntryIndex.
    fn put_direntry(&mut self, node: Node, index: DirEntryIndex, entry: DirEntry);
    // Remove the DirEntry instance given the Node and DirEntryIndex.
    fn rm_direntry(&mut self, node: Node, index: DirEntryIndex);

    // read node data into buf
    fn read(&self, node: Node, read_offset: FileSize, buf: &mut [u8]) -> Result<FileSize, Error>;

    // Write file at the current file cursor, the cursor position will NOT be updated after reading.
    fn write(&mut self, node: Node, offset: FileSize, buf: &[u8]) -> Result<FileSize, Error>;

    // remove all files and
    fn rm_file(&mut self, node: Node);
    // Remove file chunk from a given file node.
    fn rm_filechunk(&mut self, node: Node, index: FileChunkIndex);
    // Remove the metadata associated with the node.
    fn rm_metadata(&mut self, node: Node);
}

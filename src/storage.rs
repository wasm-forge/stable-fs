use crate::{
    error::Error,
    storage::types::{DirEntry, DirEntryIndex, FileChunkIndex, FileSize, Metadata, Node},
};

pub mod stable;
pub mod transient;
pub mod dummy;
pub mod types;

// Abstraction of the underlying storage layer.
pub trait Storage {
    // Get the root node ID of the storage
    fn root_node(&self) -> Node;

    // Get version of the file system
    fn get_version(&self) -> u32;

    // Generate the next available node ID.
    fn new_node(&mut self) -> Node;

    // Get the metadata associated with the node.
    fn get_metadata(&self, node: Node) -> Result<Metadata, Error>;
    // Update the metadata associated with the node.
    fn put_metadata(&mut self, node: Node, metadata: Metadata);
    // Remove the metadata associated with the node.
    fn rm_metadata(&mut self, node: Node);

    // Retrieve the DirEntry instance given the Node and DirEntryIndex.
    fn get_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error>;
    // Update or insert the DirEntry instance given the Node and DirEntryIndex.
    fn put_direntry(&mut self, node: Node, index: DirEntryIndex, entry: DirEntry);
    // Remove the DirEntry instance given the Node and DirEntryIndex.
    fn rm_direntry(&mut self, node: Node, index: DirEntryIndex);

    // Fill the buffer contents with data of a selected file chunk.
    fn read_filechunk(
        &self,
        node: Node,
        index: FileChunkIndex,
        offset: FileSize,
        buf: &mut [u8],
    ) -> Result<(), Error>;
    // Insert of update a selected file chunk with the data provided in buffer.
    fn write_filechunk(&mut self, node: Node, index: FileChunkIndex, offset: FileSize, buf: &[u8]);
    // Remove file chunk from a given file node.
    fn rm_filechunk(&mut self, node: Node, index: FileChunkIndex);
}

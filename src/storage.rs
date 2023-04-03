use crate::{
    error::Error,
    storage::types::{DirEntry, DirEntryIndex, FileChunkIndex, FileSize, Metadata, Node},
};

pub mod transient;
pub mod types;

// Abstraction of the underlying storage layer.
// This will be implemented using stable-structures.
// This allows test implementations using standard Rust in-memory
// data-structures.
pub trait Storage {
    fn root_node(&self) -> Node;
    fn new_node(&mut self) -> Node;

    fn get_metadata(&self, node: Node) -> Result<Metadata, Error>;
    fn put_metadata(&mut self, node: Node, metadata: Metadata);
    fn rm_metadata(&mut self, node: Node);

    fn get_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error>;
    fn put_direntry(&mut self, node: Node, index: DirEntryIndex, entry: DirEntry);
    fn rm_direntry(&mut self, node: Node, index: DirEntryIndex);

    fn read_filechunk(
        &self,
        node: Node,
        index: FileChunkIndex,
        offset: FileSize,
        buf: &mut [u8],
    ) -> Result<(), Error>;
    fn write_filechunk(&mut self, node: Node, index: FileChunkIndex, offset: FileSize, buf: &[u8]);
    fn rm_filechunk(&mut self, node: Node, index: FileChunkIndex);
}

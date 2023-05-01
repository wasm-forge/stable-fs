use std::collections::BTreeMap;

use crate::{
    error::Error,
    storage::types::{
        DirEntry, DirEntryIndex, FileChunk, FileChunkIndex, FileSize, FileType, Metadata, Node,
        Times,
    },
    storage::Storage,
};

const ROOT_NODE: Node = 0;

#[derive(Debug, Default)]
pub struct TransientStorage {
    metadata: BTreeMap<Node, Metadata>,
    direntry: BTreeMap<(Node, DirEntryIndex), DirEntry>,
    filechunk: BTreeMap<(Node, FileChunkIndex), FileChunk>,
    next_node: Node,
}

impl TransientStorage {
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
            metadata: Default::default(),
            direntry: Default::default(),
            filechunk: Default::default(),
            next_node: ROOT_NODE + 1,
        };
        result.put_metadata(ROOT_NODE, metadata);
        result
    }
}

impl Storage for TransientStorage {
    fn root_node(&self) -> Node {
        ROOT_NODE
    }

    fn new_node(&mut self) -> Node {
        let result = self.next_node;
        self.next_node += 1;
        result
    }

    fn get_metadata(&self, node: Node) -> Result<Metadata, Error> {
        let value = self.metadata.get(&node).ok_or(Error::NotFound)?;
        Ok(value.clone())
    }

    fn put_metadata(&mut self, node: Node, metadata: Metadata) {
        self.next_node = self.next_node.max(node + 1);
        self.metadata.insert(node, metadata);
    }

    fn rm_metadata(&mut self, node: Node) {
        self.metadata.remove(&node);
    }

    fn get_direntry(&self, node: Node, index: DirEntryIndex) -> Result<DirEntry, Error> {
        let value = self.direntry.get(&(node, index)).ok_or(Error::NotFound)?;
        Ok(value.clone())
    }

    fn put_direntry(&mut self, node: Node, index: DirEntryIndex, entry: DirEntry) {
        self.direntry.insert((node, index), entry);
    }

    fn rm_direntry(&mut self, node: Node, index: DirEntryIndex) {
        self.direntry.remove(&(node, index));
    }

    fn read_filechunk(
        &self,
        node: Node,
        index: FileChunkIndex,
        offset: FileSize,
        buf: &mut [u8],
    ) -> Result<(), Error> {
        let value = self.filechunk.get(&(node, index)).ok_or(Error::NotFound)?;
        buf.copy_from_slice(&value.bytes[offset as usize..offset as usize + buf.len()]);
        Ok(())
    }

    fn write_filechunk(&mut self, node: Node, index: FileChunkIndex, offset: FileSize, buf: &[u8]) {
        let entry = self
            .filechunk
            .entry((node, index))
            .or_insert_with(FileChunk::default);
        entry.bytes[offset as usize..offset as usize + buf.len()].copy_from_slice(buf)
    }

    fn rm_filechunk(&mut self, node: Node, index: FileChunkIndex) {
        self.filechunk.remove(&(node, index));
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

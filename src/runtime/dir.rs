use crate::{
    error::Error,
    runtime::file::File,
    storage::{
        types::{DirEntry, DirEntryIndex, FileName, FileType, Metadata, Node, Times},
        Storage,
    },
};

use super::types::FdStat;

#[derive(Clone, Debug)]

pub struct Dir {
    pub node: Node,
    pub stat: FdStat,
}

impl Dir {
    pub fn new(node: Node, stat: FdStat, storage: &dyn Storage) -> Result<Self, Error> {
        let file_type = storage.get_metadata(node)?.file_type;
        match file_type {
            FileType::Directory => {}
            FileType::RegularFile => {
                unreachable!("Unexpected file type, expected directory.");
            }
            FileType::SymbolicLink => unimplemented!("Symbolic links are not implemented yet"),
        };
        Ok(Self { node, stat })
    }

    pub fn create_dir(
        &self,
        path: &str,
        stat: FdStat,
        storage: &mut dyn Storage,
    ) -> Result<Self, Error> {
        let found = self.find_node(path, storage);
        match found {
            Err(Error::NotFound) => {}
            Ok(_) => return Err(Error::FileAlreadyExists),
            Err(err) => return Err(err),
        }

        let node = storage.new_node();
        storage.put_metadata(
            node,
            Metadata {
                node,
                file_type: FileType::Directory,
                link_count: 1,
                size: 0,
                times: Times::default(),
            },
        );
        self.add_entry(node, path, storage)?;
        Self::new(node, stat, storage)
    }

    pub fn create_file(
        &self,
        path: &str,
        stat: FdStat,
        storage: &mut dyn Storage,
    ) -> Result<File, Error> {
        let found = self.find_node(path, storage);
        match found {
            Err(Error::NotFound) => {}
            Ok(_) => return Err(Error::FileAlreadyExists),
            Err(err) => return Err(err),
        }

        let node = storage.new_node();
        storage.put_metadata(
            node,
            Metadata {
                node,
                file_type: FileType::RegularFile,
                link_count: 1,
                size: 0,
                times: Times::default(),
            },
        );

        self.add_entry(node, path, storage)?;

        File::new(node, stat, storage)
    }

    pub fn find_node(&self, path: &str, storage: &dyn Storage) -> Result<Node, Error> {
        let path = path.as_bytes();
        let len = self.len(storage)?;
        for i in 0..len {
            if let Ok(dir_entry) = storage.get_direntry(self.node, i as DirEntryIndex) {
                if &dir_entry.name.bytes[0..path.len()] == path {
                    return Ok(dir_entry.node);
                }
            }
        }
        Err(Error::NotFound)
    }

    fn len(&self, storage: &dyn Storage) -> Result<u64, Error> {
        storage.get_metadata(self.node).map(|m| m.size)
    }

    fn add_entry(&self, node: Node, path: &str, storage: &mut dyn Storage) -> Result<(), Error> {
        let mut metadata = storage.get_metadata(self.node)?;
        let name = FileName::new(path)?;
        storage.put_direntry(self.node, metadata.size as u32, DirEntry { node, name });
        metadata.size += 1;
        storage.put_metadata(self.node, metadata);
        Ok(())
    }
}

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
                first_dir_entry: None,
                last_dir_entry: None,
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
                first_dir_entry: None,
                last_dir_entry: None,
            },
        );

        self.add_entry(node, path, storage)?;

        File::new(node, stat, storage)
    }

    pub fn find_node(&self, path: &str, storage: &dyn Storage) -> Result<Node, Error> {
        
        let entry_index = self.find_entry_index(path, storage)?;

        let entry = storage.get_direntry(self.node, entry_index)?;

        return Ok(entry.node);
    }

    pub fn find_entry_index(&self, path: &str, storage: &dyn Storage) -> Result<DirEntryIndex, Error> {
        let path = path.as_bytes();

        let mut next_index = storage.get_metadata(self.node)?.first_dir_entry;

        while let Some(index) = next_index {

            if let Ok(dir_entry) = storage.get_direntry(self.node, index) {

                if &dir_entry.name.bytes[0..path.len()] == path {
                    return Ok(index);
                }

                next_index = dir_entry.next_entry;
            }
        }

        Err(Error::NotFound)
    }    

    fn len(&self, storage: &dyn Storage) -> Result<u64, Error> {
        storage.get_metadata(self.node).map(|m| m.size)
    }

    pub fn get_entry(&self, index: DirEntryIndex, storage: &dyn Storage) -> Result<DirEntry, Error> {
        storage.get_direntry(self.node, index)
    }

    fn add_entry(&self, new_node: Node, path: &str, storage: &mut dyn Storage) -> Result<(), Error> {

        let mut metadata = storage.get_metadata(self.node)?;
        let name = FileName::new(path)?;

        // start numbering with 1
        let new_entry_index: DirEntryIndex = metadata.last_dir_entry.unwrap_or(0) + 1;

        storage.put_direntry(self.node, new_entry_index, DirEntry { node: new_node, name, next_entry: None, prev_entry: metadata.last_dir_entry });

        // update previous last entry
        if let Some(prev_dir_entry_index) = metadata.last_dir_entry {
            let mut prev_dir_entry = storage.get_direntry(self.node, prev_dir_entry_index)?;

            prev_dir_entry.next_entry = Some(new_entry_index);
            storage.put_direntry(self.node, prev_dir_entry_index, prev_dir_entry)
        }

        // update metadata
        metadata.last_dir_entry = Some(new_entry_index);

        if metadata.first_dir_entry.is_none() {
            metadata.first_dir_entry = Some(new_entry_index);
        }
        metadata.size += 1;

        storage.put_metadata(self.node, metadata);

        Ok(())
    }

    pub fn rm_entry(&self, path: &str, storage: &mut dyn Storage) -> Result<(), Error> {

        let mut metadata = storage.get_metadata(self.node)?;

        let removed_entry_index = self.find_entry_index(path, storage)?;
        
        let removed_dir_entry = storage.get_direntry(self.node, removed_entry_index)?;

        // update previous entry
        if let Some(prev_dir_entry_index) = removed_dir_entry.prev_entry {
            let mut prev_dir_entry = storage.get_direntry(self.node, prev_dir_entry_index)?;
            prev_dir_entry.next_entry = removed_dir_entry.next_entry;
            storage.put_direntry(self.node, prev_dir_entry_index, prev_dir_entry)
        }
        
        // update next entry
        if let Some(next_dir_entry_index) = removed_dir_entry.next_entry {
            let mut next_dir_entry = storage.get_direntry(self.node, next_dir_entry_index)?;
            next_dir_entry.prev_entry = removed_dir_entry.prev_entry;
            storage.put_direntry(self.node, next_dir_entry_index, next_dir_entry)
        }

        // update metadata
        if Some(removed_entry_index) == metadata.last_dir_entry {
            metadata.last_dir_entry = removed_dir_entry.prev_entry;
        }

        if Some(removed_entry_index) == metadata.first_dir_entry {
            metadata.first_dir_entry = removed_dir_entry.next_entry;
        }

        metadata.size -= 1;

        storage.put_metadata(self.node, metadata);

        // remove the entry
        storage.rm_direntry(self.node, removed_entry_index);

        Ok(())
    }

    
}

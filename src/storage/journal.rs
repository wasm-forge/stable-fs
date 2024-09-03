use ic_stable_structures::{memory_manager::VirtualMemory, Memory};

use crate::{
    error::Error,
    runtime::structure_helpers::{read_obj, write_obj},
};

use super::types::{Metadata, Node};

// index containing cached metadata
const MOUNTED_META_PTR: u64 = 16;

pub struct CacheJournal<M: Memory> {
    journal: VirtualMemory<M>,
}

impl<M: Memory> CacheJournal<M> {
    pub fn new(journal: VirtualMemory<M>) -> Result<CacheJournal<M>, Error> {
        let cache_journal = if journal.size() == 0 {
            journal.grow(1);

            // write the magic marker
            let b = [b'F', b'S', b'J', b'1', 0, 0, 0, 0];
            journal.write(0, &b);

            let cache_journal = CacheJournal { journal };

            // reset mounted meta node
            cache_journal.reset_mounted_meta();

            cache_journal
        } else {
            // check the marker
            let mut b = [0u8; 4];
            journal.read(0, &mut b);

            // accepted marker
            if b != *b"FSJ1" {
                return Err(Error::InvalidMagicMarker);
            }

            CacheJournal { journal }
        };

        Ok(cache_journal)
    }

    pub fn read_mounted_meta_node(&self) -> Option<Node> {
        let mut ret = 0u64;
        read_obj(&self.journal, MOUNTED_META_PTR, &mut ret);
        if ret == u64::MAX {
            return None;
        }

        Some(ret)
    }

    pub fn read_mounted_meta(&self, meta: &mut Metadata) {
        read_obj(&self.journal, MOUNTED_META_PTR + 8, meta);
    }

    pub fn reset_mounted_meta(&self) {
        write_obj(&self.journal, MOUNTED_META_PTR, &(u64::MAX as Node));
        write_obj(&self.journal, MOUNTED_META_PTR + 8, &Metadata::default());
    }

    pub fn write_mounted_meta(&self, node: &Node, meta: &Metadata) {
        write_obj(&self.journal, MOUNTED_META_PTR, node);
        write_obj(&self.journal, MOUNTED_META_PTR + 8, meta);
    }
}

#[cfg(test)]
mod tests {
    use ic_stable_structures::memory_manager::{MemoryId, MemoryManager};

    use crate::storage::types::Node;

    use crate::test_utils::new_vector_memory;

    use super::*;

    #[test]
    fn cache_journal_metadata_roundtrip() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let journal_memory = memory_manager.get(MemoryId::new(1));
        let journal = CacheJournal::new(journal_memory).unwrap();

        let node: Node = 123;
        let meta = Metadata {
            node: 123,
            file_type: crate::storage::types::FileType::RegularFile,
            link_count: 1,
            size: 1234,
            times: crate::storage::types::Times {
                accessed: 48,
                modified: 388,
                created: 34,
            },
            first_dir_entry: None,
            last_dir_entry: Some(876),
        };

        let mut node2 = 0;
        let mut meta2 = Metadata::default();

        assert_ne!(node, node2);
        assert_ne!(meta, meta2);

        journal.write_mounted_meta(&node, &meta);

        node2 = journal.read_mounted_meta_node().unwrap();

        journal.read_mounted_meta(&mut meta2);

        assert_eq!(node, node2);
        assert_eq!(meta, meta2);
    }

    #[test]
    fn fsj1_marker_is_written() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let journal_memory = memory_manager.get(MemoryId::new(1));
        let _journal = CacheJournal::new(journal_memory).unwrap();

        let memory = memory_manager.get(MemoryId::new(1));
        let mut b = [0u8; 4];

        memory.read(0, &mut b);
        assert_eq!(&b[0..4], b"FSJ1");
    }

    #[test]
    fn correct_fsj1_marker_is_accepted() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let journal_memory = memory_manager.get(MemoryId::new(1));
        let _journal = CacheJournal::new(journal_memory).unwrap();

        let res = CacheJournal::new(memory_manager.get(MemoryId::new(1)));

        assert!(res.is_ok());
    }

    #[test]
    fn wrong_fsj1_marker_is_rejected() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let journal_memory = memory_manager.get(MemoryId::new(1));
        let _journal = CacheJournal::new(journal_memory).unwrap();

        let memory = memory_manager.get(MemoryId::new(1));
        let b = [0u8; 1];

        memory.write(0, &b);

        let res = CacheJournal::new(memory_manager.get(MemoryId::new(1)));

        assert!(res.is_err());
    }

    #[test]
    fn initial_node_value() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let journal_memory = memory_manager.get(MemoryId::new(1));
        let journal = CacheJournal::new(journal_memory).unwrap();

        assert_eq!(journal.read_mounted_meta_node(), None);
    }

    #[test]
    fn updated_node_value_after_upgrade() {
        let mem = new_vector_memory();
        let memory_manager = MemoryManager::init(mem);
        let journal_memory = memory_manager.get(MemoryId::new(1));
        let journal = CacheJournal::new(journal_memory).unwrap();

        assert_eq!(journal.read_mounted_meta_node(), None);

        let meta = Metadata {
            node: 123,
            file_type: crate::storage::types::FileType::RegularFile,
            link_count: 1,
            size: 1234,
            times: crate::storage::types::Times {
                accessed: 48,
                modified: 388,
                created: 34,
            },
            first_dir_entry: None,
            last_dir_entry: Some(876),
        };

        journal.write_mounted_meta(&123, &meta);

        let journal = CacheJournal::new(memory_manager.get(MemoryId::new(1))).unwrap();

        assert_eq!(journal.read_mounted_meta_node(), Some(123));

        let mut meta2 = Metadata::default();
        journal.read_mounted_meta(&mut meta2);

        assert_eq!(meta, meta2);
    }
}

use ic_stable_structures::{memory_manager::VirtualMemory, Memory};

use crate::error::Error;

pub struct CacheJournal<M: Memory> {
    journal: VirtualMemory<M>,
}

// The cache stored in stable memory for some information that has to be stored between upgrades.
impl<M: Memory> CacheJournal<M> {
    pub fn new(journal: VirtualMemory<M>) -> Result<CacheJournal<M>, Error> {
        let cache_journal = if journal.size() == 0 {
            journal.grow(1);

            // write the magic marker
            let b = [b'F', b'S', b'J', b'1', 0, 0, 0, 0];
            journal.write(0, &b);

            let cache_journal = CacheJournal { journal };

            cache_journal
        } else {
            // check the marker
            let mut b = [0u8; 4];
            journal.read(0, &mut b);

            // accepted marker
            if b != *b"FSJ1" {
                return Err(Error::InvalidMagicMarker);
            }

            let cache_journal = CacheJournal { journal };

            // init local cache variables
            //...

            cache_journal
        };

        Ok(cache_journal)
    }
}

#[cfg(test)]
mod tests {
    use ic_stable_structures::memory_manager::{MemoryId, MemoryManager};

    use crate::storage::types::Node;

    use crate::test_utils::new_vector_memory;

    use super::*;

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
}

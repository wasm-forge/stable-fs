use ic_stable_structures::DefaultMemoryImpl;

use crate::{fs::FileSystem, storage::stable::StableStorage};

pub fn test_fs() -> FileSystem {
    let storage = StableStorage::new(DefaultMemoryImpl::default());
    FileSystem::new(Box::new(storage)).unwrap()
}

use ic_stable_structures::DefaultMemoryImpl;

use crate::{fs::FileSystem, storage::stable::StableStorage};

#[cfg(test)]
pub fn test_fs() -> FileSystem {
    let storage = StableStorage::new(DefaultMemoryImpl::default());
    FileSystem::new(Box::new(storage)).unwrap()
}

#[cfg(test)]
pub fn test_fs_transient() -> FileSystem {
    use crate::storage::transient::TransientStorage;

    let storage = TransientStorage::new();
    FileSystem::new(Box::new(storage)).unwrap()
}

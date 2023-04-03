use crate::{fs::FileSystem, storage::transient::TransientStorage};

pub fn test_fs() -> FileSystem {
    let storage = TransientStorage::new();
    FileSystem::new(Box::new(storage)).unwrap()
}

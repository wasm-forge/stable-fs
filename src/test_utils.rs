use ic_stable_structures::{DefaultMemoryImpl, VectorMemory};

use crate::{error::Error, fs::FileSystem, storage::stable::StableStorage};
use crate::runtime::types::ChunkSize;

#[cfg(test)]
pub fn new_vector_memory() -> VectorMemory {
    use std::{cell::RefCell, rc::Rc};

    Rc::new(RefCell::new(Vec::new()))
}

#[cfg(test)]
pub fn test_fs() -> FileSystem {
    let memory = DefaultMemoryImpl::default();

    let storage = StableStorage::new(memory);
    FileSystem::new(Box::new(storage)).unwrap()
}


#[cfg(test)]
pub fn test_fs_v1() -> FileSystem {
    use crate::storage::stable::ChunkType;

    let memory = DefaultMemoryImpl::default();

    let mut storage = StableStorage::new(memory);
    storage.set_chunk_type(ChunkType::V1);
    FileSystem::new(Box::new(storage)).unwrap()
}

#[cfg(test)]
pub fn test_fs_custom_chunk_size(chunk_size: ChunkSize) -> FileSystem {

    let memory = DefaultMemoryImpl::default();

    let mut storage = StableStorage::new(memory);
    storage.set_chunk_size(chunk_size).unwrap();

    FileSystem::new(Box::new(storage)).unwrap()
}

#[cfg(test)]
pub fn test_fs_transient() -> FileSystem {
    use crate::storage::transient::TransientStorage;

    let storage = TransientStorage::new();
    FileSystem::new(Box::new(storage)).unwrap()
}

#[cfg(test)]
pub fn test_fs_setups(virtual_file_name: &str) -> Vec<FileSystem> {
    use crate::runtime::types::ChunkSize;

    let mut result = Vec::new();

    result.push(test_fs());
    result.push(test_fs_v1());
    result.push(test_fs_custom_chunk_size(ChunkSize::CHUNK4K));
    result.push(test_fs_custom_chunk_size(ChunkSize::CHUNK64K));

    result.push(test_fs_transient());

    if !virtual_file_name.is_empty() {
        let mut fs = test_fs();

        fs.mount_memory_file(virtual_file_name, Box::new(new_vector_memory()))
            .unwrap();

        result.push(fs);

        let mut fs = test_fs_transient();

        fs.mount_memory_file(virtual_file_name, Box::new(new_vector_memory()))
            .unwrap();

        result.push(fs);
    }

    result
}

#[cfg(test)]
pub fn write_text_file(
    fs: &mut FileSystem,
    parent_fd: u32,
    path: &str,
    content: &str,
    times: usize,
) -> Result<(), Error> {
    use crate::fs::{FdStat, OpenFlags};

    let file_fd = fs.open_or_create(parent_fd, path, FdStat::default(), OpenFlags::CREATE, 0)?;

    write_text_fd(fs, file_fd, content, times)
}

#[cfg(test)]
pub fn write_text_fd(
    fs: &mut FileSystem,
    file_fd: u32,
    content: &str,
    times: usize,
) -> Result<(), Error> {
    let mut str = "".to_string();

    for _ in 0..times {
        str.push_str(content)
    }

    fs.write(file_fd, str.as_bytes())?;

    Ok(())
}

#[cfg(test)]
pub fn read_text_file(
    fs: &mut FileSystem,
    parent_fd: u32,
    path: &str,
    offset: usize,
    size: usize,
) -> String {
    use crate::fs::{DstBuf, FdStat, OpenFlags};

    let fd = fs
        .open_or_create(parent_fd, path, FdStat::default(), OpenFlags::empty(), 0)
        .unwrap();

    let mut content = (0..size).map(|_| ".").collect::<String>();

    let read_content = [DstBuf {
        buf: content.as_mut_ptr(),
        len: content.len(),
    }];

    let read = fs
        .read_vec_with_offset(fd, &read_content, offset as u64)
        .unwrap();

    let min = std::cmp::min(read, size as u64) as usize;

    let _ = fs.close(fd);

    content[..min].to_string()
}

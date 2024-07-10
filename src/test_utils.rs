use ic_stable_structures::DefaultMemoryImpl;

use crate::{fs::FileSystem, storage::stable::StableStorage, error::Error};

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

#[cfg(test)]
pub fn write_text_file(fs: &mut FileSystem, parent_fd: u32, path: &str, content: &str, times: usize) -> Result<(), Error> {
    use crate::fs::{FdStat, OpenFlags};

    let file_fd = fs
        .open_or_create(parent_fd, path, FdStat::default(), OpenFlags::CREATE, 0)?;

    let mut str = content.to_string();

    for _ in 0..times {
        str.push_str(content)
    }

    fs.write(file_fd, str.as_bytes()).unwrap();

    Ok(())
}

#[cfg(test)]
pub fn read_text_file(fs: &mut FileSystem, parent_fd: u32, path: &str, offset: usize, size: usize) -> String {
    use crate::fs::{DstBuf, FdStat, OpenFlags};


    let fd = fs.open_or_create(parent_fd, path, FdStat::default(), OpenFlags::empty(), 0).unwrap();        

    let mut content = (0..size).map(|_| ".").collect::<String>();

    let read_content = [
        DstBuf {
            buf: content.as_mut_ptr(),
            len: content.len(),
        },
    ];
    
    let read = fs.read_vec_with_offset(fd, &read_content, offset as u64).unwrap();

    let min = std::cmp::min(read, size as u64) as usize;
    
    let _ = fs.close(fd);

    content[..min].to_string()
}

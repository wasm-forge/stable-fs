use ic_stable_structures::{DefaultMemoryImpl, VectorMemory};

use crate::runtime::types::ChunkSize;
use crate::{error::Error, fs::FileSystem, storage::stable::StableStorage};

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
    use crate::fs::ChunkType;
    use crate::storage::Storage;

    let memory = DefaultMemoryImpl::default();

    let mut storage = StableStorage::new(memory);
    storage.set_chunk_type(ChunkType::V1);
    FileSystem::new(Box::new(storage)).unwrap()
}

#[cfg(test)]
pub fn test_fs_custom_chunk_size(chunk_size: ChunkSize) -> FileSystem {
    use crate::storage::Storage;

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

    let mut result = vec![
        test_fs(),
        test_fs_v1(),
        test_fs_custom_chunk_size(ChunkSize::CHUNK4K),
        test_fs_custom_chunk_size(ChunkSize::CHUNK64K),
        test_fs_transient(),
    ];

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

    let file_fd = fs.open(parent_fd, path, FdStat::default(), OpenFlags::CREATE, 0)?;

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

use crate::fs::{FileSize, SrcBuf};

#[cfg(test)]
pub fn write_text_at_offset(
    fs: &mut FileSystem,
    file_fd: u32,
    content: &str,
    times: usize,
    offset: FileSize,
) -> Result<(), Error> {
    let mut str = "".to_string();

    for _ in 0..times {
        str.push_str(content)
    }

    let src = SrcBuf {
        buf: str.as_ptr(),
        len: str.len(),
    };

    fs.write_vec_with_offset(file_fd, &[src], offset)?;

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
        .open(parent_fd, path, FdStat::default(), OpenFlags::empty(), 0)
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

#[cfg(test)]
mod test_env {
    use crate::runtime::types::DstBuf;
    use crate::runtime::types::Fd;
    use crate::runtime::types::FdStat;
    use crate::runtime::types::OpenFlags;
    use crate::runtime::types::Whence;
    use crate::storage::types::FileType;
    use crate::test_utils::FileSystem;
    use crate::test_utils::SrcBuf;
    use crate::test_utils::StableStorage;
    use ic_stable_structures::memory_manager::MemoryId;
    use ic_stable_structures::memory_manager::MemoryManager;
    use ic_stable_structures::DefaultMemoryImpl;
    use std::cell::RefCell;

    const SEGMENT_SIZE: usize = 1000usize;
    const FILES_COUNT: usize = 10usize;

    thread_local! {
        static MEMORY_MANAGER: RefCell<MemoryManager<DefaultMemoryImpl>> =
            RefCell::new(MemoryManager::init(DefaultMemoryImpl::default()));

        static FS: RefCell<FileSystem> = {

            MEMORY_MANAGER.with(|m| {

                let memory_manager = m.borrow();

                //v0.4
                //let storage = StableStorage::new_with_memory_manager(&memory_manager, 200u8);
                //v0.5, v0.6 ...
                let mut storage = StableStorage::new_with_memory_manager(&memory_manager, 200..210u8);

                // set chunk version to V1
                //storage.set_chunk_type(storage::stable::ChunkType::V1);

                // setup chunk size
                //storage.set_chunk_size(stable_fs::fs::ChunkSize::CHUNK4K).unwrap();
                //storage.set_chunk_size(stable_fs::fs::ChunkSize::CHUNK64K).unwrap();

                let fs = RefCell::new(
                    FileSystem::new(Box::new(storage)).unwrap()
                );

                // use mounted memory
                fs.borrow_mut().mount_memory_file("file.txt", Box::new(memory_manager.get(MemoryId::new(155)))).unwrap();

                fs
            })
        };
    }

    fn file_size(filename: String) -> usize {
        FS.with(|fs| {
            let mut fs = fs.borrow_mut();

            let dir = fs.root_fd();

            let fd = fs
                .open(
                    dir,
                    filename.as_str(),
                    FdStat::default(),
                    OpenFlags::empty(),
                    0,
                )
                .unwrap();

            let meta = fs.metadata(fd).unwrap();

            let size = meta.size;

            size as usize
        })
    }

    thread_local! {
        static BUFFER: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
    }

    pub fn instruction_counter() -> u64 {
        0
    }

    pub fn append_buffer(text: String, times: usize) -> usize {
        BUFFER.with(|buffer| {
            let mut buffer = buffer.borrow_mut();

            if buffer.is_none() {
                *buffer = Some(Vec::new());
            }

            let buffer = buffer.as_mut().unwrap();

            for _ in 0..times {
                buffer.extend_from_slice(text.as_bytes());
            }

            buffer.len()
        })
    }

    pub fn check_buffer(text: String, times: usize) -> usize {
        BUFFER.with(|buffer| {
            let buffer = buffer.borrow_mut();

            let buffer = buffer.as_ref();

            if buffer.is_none() && times == 0 {
                return 0;
            }

            let buffer = buffer.unwrap();

            let mut p = 0;
            let len = text.len();

            let bytes = text.as_bytes();

            for _ in 0..times {
                let buf = &buffer[p..p + len];

                assert_eq!(bytes, buf);

                p += len;
            }

            assert_eq!(buffer.len(), text.len() * times);

            buffer.len()
        })
    }

    pub fn clear_buffer() {
        BUFFER.with(|chunk| {
            let mut chunk = chunk.borrow_mut();

            if chunk.is_none() {
                return;
            }

            let chunk = chunk.as_mut().unwrap();

            // explicitly fill contents with 0
            chunk.fill(0);

            chunk.clear()
        })
    }

    pub fn read_buffer(offset: usize, size: usize) -> String {
        BUFFER.with(|chunk| {
            let mut chunk = chunk.borrow_mut();

            let chunk = chunk.as_mut().unwrap();

            std::str::from_utf8(&chunk[offset..offset + size])
                .unwrap()
                .to_string()
        })
    }

    pub fn store_buffer(filename: String) -> usize {
        let res = BUFFER.with(|chunk| {
            let chunk = chunk.borrow_mut();

            let chunk = chunk.as_ref().unwrap();

            FS.with(|fs| {
                let mut fs = fs.borrow_mut();

                let root_fd = (*fs).root_fd();

                let fd = (*fs)
                    .open(root_fd, &filename, FdStat::default(), OpenFlags::CREATE, 42)
                    .unwrap();

                let write_content = [SrcBuf {
                    buf: chunk.as_ptr(),
                    len: chunk.len(),
                }];

                let res = (*fs).write_vec(fd, write_content.as_ref()).unwrap();

                (*fs).close(fd).unwrap();

                res as usize
            })
        });

        res
    }

    pub fn store_buffer_in_1000b_segments(filename: String) -> (u64, usize) {
        let stime = instruction_counter();

        let res = BUFFER.with(|chunk| {
            let chunk = chunk.borrow_mut();

            let chunk = chunk.as_ref().unwrap();

            FS.with(|fs| {
                let mut fs = fs.borrow_mut();

                let root_fd = (*fs).root_fd();

                let fd = (*fs)
                    .open(root_fd, &filename, FdStat::default(), OpenFlags::CREATE, 42)
                    .unwrap();

                (*fs).seek(fd, 0, Whence::SET).unwrap();

                let len = chunk.len();

                let mut p = 0;
                let part_len = SEGMENT_SIZE;
                let mut res = 0;

                while p < len {
                    let write_len = (len - p).min(part_len);

                    let write_content = [SrcBuf {
                        buf: chunk[p..(p + part_len).min(len)].as_ptr(),
                        len: write_len,
                    }];

                    res += (*fs).write_vec(fd, write_content.as_ref()).unwrap();

                    p += write_len;
                }

                (*fs).close(fd).unwrap();

                res as usize
            })
        });

        let etime = instruction_counter();

        (etime - stime, res)
    }

    pub fn store_buffer_in_1000b_segments_10_files(filename: String) -> (u64, usize) {
        let stime = instruction_counter();

        let res = BUFFER.with(|chunk| {
            let chunk = chunk.borrow_mut();

            let chunk = chunk.as_ref().unwrap();

            FS.with(|fs| {
                let mut fs = fs.borrow_mut();

                let root_fd = (*fs).root_fd();

                let mut fds = Vec::<Fd>::new();

                for i in 0..FILES_COUNT {
                    let fd = (*fs)
                        .open(
                            root_fd,
                            &format!("{}{}", filename, i),
                            FdStat::default(),
                            OpenFlags::CREATE,
                            42,
                        )
                        .unwrap();

                    (*fs).seek(fd, 0, Whence::SET).unwrap();

                    fds.push(fd);
                }

                let len = chunk.len();

                let mut p = 0;
                let part_len = SEGMENT_SIZE;
                let mut res = 0;
                let mut idx = 0;

                while p < len {
                    let fd = fds[idx % FILES_COUNT];

                    let write_len = (len - p).min(part_len);

                    let write_content = [SrcBuf {
                        buf: chunk[p..(p + part_len).min(len)].as_ptr(),
                        len: write_len,
                    }];

                    res += (*fs).write_vec(fd, write_content.as_ref()).unwrap();

                    p += write_len;

                    idx += 1;
                }

                fds.iter_mut().for_each(|fd| (*fs).close(*fd).unwrap());

                res as usize
            })
        });

        let etime = instruction_counter();

        (etime - stime, res)
    }

    pub fn load_buffer(filename: String) -> (u64, usize) {
        let stime = instruction_counter();

        let res = BUFFER.with(|chunk| {
            let mut chunk = chunk.borrow_mut();

            let chunk = chunk.as_mut().unwrap();

            FS.with(|fs| {
                let mut fs = fs.borrow_mut();

                let root_fd = (*fs).root_fd();

                let fd = (*fs)
                    .open(root_fd, &filename, FdStat::default(), OpenFlags::CREATE, 42)
                    .unwrap();

                let size = (*fs).metadata(fd).unwrap().size as usize;

                (*fs).seek(fd, 0, Whence::SET).unwrap();

                let read_content = [DstBuf {
                    buf: chunk.as_mut_ptr(),
                    len: size,
                }];

                unsafe { chunk.set_len(size) };

                let res = (*fs).read_vec(fd, &read_content).unwrap();

                res as usize
            })
        });

        let etime = instruction_counter();

        (etime - stime, res)
    }

    pub fn load_buffer_in_1000b_segments(filename: String) -> (u64, usize) {
        let stime = instruction_counter();

        let res = BUFFER.with(|chunk| {
            let mut chunk = chunk.borrow_mut();

            let chunk = chunk.as_mut().unwrap();

            FS.with(|fs| {
                let mut fs = fs.borrow_mut();

                let root_fd = (*fs).root_fd();

                let fd = (*fs)
                    .open(root_fd, &filename, FdStat::default(), OpenFlags::CREATE, 42)
                    .unwrap();

                let len = (*fs).metadata(fd).unwrap().size as usize;

                (*fs).seek(fd, 0, Whence::SET).unwrap();

                let mut p = 0;
                let part_len = SEGMENT_SIZE;
                let mut res = 0;

                unsafe { chunk.set_len(len) };

                while p < len {
                    let read_len = (len - p).min(part_len);

                    let read_content = [DstBuf {
                        buf: chunk[p..p + read_len].as_mut_ptr(),
                        len: read_len,
                    }];

                    res += (*fs).read_vec(fd, read_content.as_ref()).unwrap();

                    p += read_len;
                }

                res as usize
            })
        });

        let etime = instruction_counter();

        (etime - stime, res)
    }

    pub fn load_buffer_in_1000b_segments_10_files(filename: String) -> (u64, usize) {
        let stime = instruction_counter();

        let res = BUFFER.with(|chunk| {
            let mut chunk = chunk.borrow_mut();

            let chunk = chunk.as_mut().unwrap();

            FS.with(|fs| {
                let mut fs = fs.borrow_mut();

                let root_fd = (*fs).root_fd();

                let mut fds = Vec::<Fd>::new();

                for i in 0..FILES_COUNT {
                    let fd = (*fs)
                        .open(
                            root_fd,
                            &format!("{}{}", filename, i),
                            FdStat::default(),
                            OpenFlags::CREATE,
                            42,
                        )
                        .unwrap();

                    (*fs).seek(fd, 0, Whence::SET).unwrap();
                    fds.push(fd);
                }

                let len = (*fs).metadata(fds[0]).unwrap().size as usize * FILES_COUNT;

                let mut p = 0;
                let part_len = SEGMENT_SIZE;
                let mut res = 0;

                unsafe { chunk.set_len(len) };

                let mut idx = 0;

                while p < len {
                    let fd = fds[idx % FILES_COUNT];

                    let read_len = (len - p).min(part_len);

                    let read_content = [DstBuf {
                        buf: chunk[p..p + read_len].as_mut_ptr(),
                        len: read_len,
                    }];

                    res += (*fs).read_vec(fd, read_content.as_ref()).unwrap();

                    p += read_len;

                    assert!(read_len > 0, "read_len must be greated than 0");

                    idx += 1;
                }

                res as usize
            })
        });

        let etime = instruction_counter();

        (etime - stime, res)
    }

    fn write_100mb(file_name: &str) {
        check_buffer("abc1234567".to_string(), 0);

        append_buffer("abc1234567".to_string(), 10_000_000);

        check_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer("temp2.txt".to_string());

        // bench
        store_buffer(file_name.to_string());
    }

    fn write_100mb_over_existing(file_name: &str) {
        append_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        // bench
        store_buffer(file_name.to_string());
    }

    fn read_100mb(file_name: &str) {
        append_buffer("abc1234567".to_string(), 10_000_000);
        store_buffer(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        check_buffer("abc1234567".to_string(), 10_000_000);
        clear_buffer();
        check_buffer("abc1234567".to_string(), 0);

        // bench
        load_buffer(file_name.to_string());

        check_buffer("abc1234567".to_string(), 10_000_000);
    }

    fn write_100mb_in_segments(file_name: &str) {
        append_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer("temp1.txt".to_string());

        //bench
        store_buffer_in_1000b_segments(file_name.to_string());

        assert_eq!(file_size(file_name.to_string()), 100_000_000);
        clear_buffer();
        load_buffer(file_name.to_string());
        check_buffer("abc1234567".to_string(), 10_000_000);
    }

    fn write_100mb_in_segments_over_existing(file_name: &str) {
        append_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer("temp1.txt".to_string());
        store_buffer(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        // bench
        store_buffer_in_1000b_segments(file_name.to_string());
    }

    fn read_100mb_in_segments(file_name: &str) {
        append_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer("temp1.txt".to_string());
        store_buffer(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        clear_buffer();
        check_buffer("abc1234567".to_string(), 0);

        //bench
        load_buffer_in_1000b_segments(file_name.to_string());

        check_buffer("abc1234567".to_string(), 10_000_000);
    }

    fn write_100mb_in_segments_10_files(file_name: &str) {
        append_buffer("abc1234567".to_string(), 10_000_000);
        //store_buffer("temp1.txt".to_string());

        // bench
        store_buffer_in_1000b_segments_10_files(file_name.to_string());

        clear_buffer();
        load_buffer_in_1000b_segments_10_files(file_name.to_string());
        check_buffer("abc1234567".to_string(), 10_000_000);
    }

    fn write_100mb_in_segments_over_existing_10_files(file_name: &str) {
        append_buffer("abc1234567".to_string(), 10_000_000);

        //store_buffer("temp1.txt".to_string());
        store_buffer_in_1000b_segments_10_files(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        // bench
        store_buffer_in_1000b_segments_10_files(file_name.to_string());
    }

    fn read_20mb_in_segments_10_files(file_name: &str) {
        append_buffer("abc1234567".to_string(), 2_000_000);

        //        store_buffer("temp1.txt".to_string());
        store_buffer_in_1000b_segments_10_files(file_name.to_string());
        //        store_buffer("temp2.txt".to_string());

        clear_buffer();

        // bench
        load_buffer_in_1000b_segments_10_files(file_name.to_string());

        check_buffer("abc1234567".to_string(), 2_000_000);
    }

    fn read_100mb_in_segments_10_files(file_name: &str) {
        append_buffer("abc1234567".to_string(), 10_000_000);

        //        store_buffer("temp1.txt".to_string());
        store_buffer_in_1000b_segments_10_files(file_name.to_string());
        //        store_buffer("temp2.txt".to_string());

        clear_buffer();

        // bench
        load_buffer_in_1000b_segments_10_files(file_name.to_string());

        check_buffer("abc1234567".to_string(), 10_000_000);
    }

    #[test]
    fn file_read_100mb_in_segments_10_files() {
        read_100mb_in_segments_10_files("file.txt")
    }

    #[derive(Debug)]
    struct AssertionError {
        details: String,
    }

    impl AssertionError {
        fn new(msg: &str) -> AssertionError {
            AssertionError {
                details: msg.to_string(),
            }
        }
    }

    impl std::error::Error for AssertionError {
        fn description(&self) -> &str {
            &self.details
        }
    }

    impl std::fmt::Display for AssertionError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}", self.details)
        }
    }

    // deterministic 32-bit pseudo-random number provider
    fn next_rand(cur_rand: u64) -> u64 {
        let a: u64 = 1103515245;
        let c: u64 = 12345;
        let m: u64 = 1 << 31;

        (a.wrapping_mul(cur_rand).wrapping_add(c)) % m
    }

    pub fn generate_random_file_structure(
        op_count: u32, // number of operations to do
        cur_rand: u64, // current random seed
        depth: u32,    // current folder depth
        parent_fd: Fd, // host fd
        fs: &mut FileSystem,
    ) -> Result<u32, crate::error::Error> {
        let mut op_count = op_count;
        let mut cur_rand = cur_rand;

        while op_count > 0 {
            op_count -= 1;

            cur_rand = next_rand(cur_rand);
            let action = cur_rand % 6; // e.g., 0..5

            match action {
                0 => {
                    // create a file
                    let filename = format!("file{}.txt", op_count);
                    let fd = fs.open(
                        parent_fd,
                        &filename,
                        FdStat::default(),
                        OpenFlags::CREATE,
                        op_count as u64,
                    )?;
                    fs.close(fd)?;
                }

                1 => {
                    // create a directory.
                    let dirname = format!("dir{}.txt", op_count);

                    fs.mkdir(parent_fd, &dirname, FdStat::default(), op_count as u64)?;
                }

                2 => {
                    // remove a random folder item
                    let files = fs.list_dir_internal(parent_fd, None)?;

                    if files.len() > 0 {
                        let cur_rand = next_rand(cur_rand);
                        let (node, name) = &files[cur_rand as usize % files.len()];

                        let meta = fs.metadata_from_node(*node)?;

                        match meta.file_type {
                            FileType::Directory => {
                                let _ = fs.remove_dir(parent_fd, &name);
                            }
                            FileType::RegularFile => {
                                let _ = fs.remove_file(parent_fd, &name);
                            }
                            FileType::SymbolicLink => panic!("Symlink are not supported!"),
                        }
                    }
                }

                3 => {
                    // enter subfolder
                    let dirs = fs.list_dir_internal(parent_fd, Some(FileType::Directory))?;

                    if dirs.len() > 0 {
                        let cur_rand = next_rand(cur_rand);
                        let (_node, name) = &dirs[cur_rand as usize % dirs.len()];

                        let dir_fd = fs.open(
                            parent_fd,
                            &name,
                            FdStat::default(),
                            OpenFlags::empty(),
                            op_count as u64,
                        )?;

                        op_count = generate_random_file_structure(
                            op_count,
                            cur_rand,
                            depth + 1,
                            dir_fd,
                            fs,
                        )?;
                    }
                }

                4 => {
                    // exit the current folder
                    if depth > 0 {
                        return Ok(op_count);
                    }
                }

                5 => {
                    let dirs = fs.list_dir_internal(parent_fd, Some(FileType::Directory))?;

                    // Random open/close a file (or directory)
                    if dirs.len() > 0 {
                        let cur_rand = next_rand(cur_rand);
                        let (_node, filename) = &dirs[cur_rand as usize % dirs.len()];

                        let fd = fs.open(
                            parent_fd,
                            &filename,
                            FdStat::default(),
                            OpenFlags::empty(),
                            op_count as u64,
                        )?;

                        fs.close(fd)?;
                    }
                }

                _ => {
                    panic!("Incorrect action {action}");
                }
            }
        }

        Ok(op_count)
    }

    fn list_all_files_as_string(fs: &mut FileSystem) -> Result<String, crate::error::Error> {
        let mut paths = Vec::new();

        scan_directory(fs, fs.root_fd(), "", &mut paths)?;
        Ok(paths.join("\n"))
    }

    fn scan_directory(
        fs: &mut FileSystem,
        dir_fd: u32,
        current_path: &str,
        collected_paths: &mut Vec<String>,
    ) -> Result<(), crate::error::Error> {
        let entries = fs.list_dir_internal(dir_fd, None)?;

        for (entry_node, filename) in entries {
            let entry_path = if current_path.is_empty() {
                format!("/{}", filename)
            } else {
                format!("{}/{}", current_path, filename)
            };

            let meta = fs.metadata_from_node(entry_node)?;

            match meta.file_type {
                FileType::Directory => {
                    let child_fd = fs.open(
                        dir_fd,
                        &filename,
                        FdStat::default(),
                        OpenFlags::DIRECTORY,
                        0,
                    )?;

                    scan_directory(fs, child_fd, &entry_path, collected_paths)?;

                    fs.close(child_fd)?;
                }
                FileType::RegularFile => {
                    collected_paths.push(entry_path);
                }
                FileType::SymbolicLink => todo!(),
            }
        }

        Ok(())
    }

    #[test]
    fn test_generator() {
        let memory = DefaultMemoryImpl::default();

        let storage = StableStorage::new(memory);
        let mut fs = FileSystem::new(Box::new(storage)).unwrap();

        // generate random file structure.
        generate_random_file_structure(100, 36, 0, fs.root_fd(), &mut fs).unwrap();

        // get all files
        let files = list_all_files_as_string(&mut fs).unwrap();

        println!("------------------------------------------");
        println!("FILE STRUCTURE");
        println!("{}", files);
    }
}

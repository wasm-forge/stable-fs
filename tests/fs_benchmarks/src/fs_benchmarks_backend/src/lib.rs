use std::cell::RefCell;

use ic_stable_structures::{
    memory_manager::{MemoryId, MemoryManager},
    DefaultMemoryImpl,
};

use stable_fs::{
    fs::{DstBuf, Fd, FdStat, FileSystem, OpenFlags, SrcBuf, Whence},
    storage::{stable::StableStorage, types::FILE_CHUNK_SIZE_V1},
};

use stable_fs::storage::Storage;

use ic_stable_structures::Memory;

const PROFILING: MemoryId = MemoryId::new(50);

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
            if true {
                let filename = "file.txt";

                fs.borrow_mut().mount_memory_file(filename, Box::new(memory_manager.get(MemoryId::new(155)))).unwrap();

                for i in 0..FILES_COUNT {
                    let fname = &format!("{}{}", filename, i);

                    fs.borrow_mut().mount_memory_file(fname, Box::new(memory_manager.get(MemoryId::new(156+i as u8)))).unwrap();
                }
            }

            fs
        })
    };
}

thread_local! {
    static BUFFER: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

#[ic_cdk::update]
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

#[ic_cdk::update]
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

#[ic_cdk::update]
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

#[ic_cdk::update]
pub fn read_buffer(offset: usize, size: usize) -> String {
    BUFFER.with(|chunk| {
        let mut chunk = chunk.borrow_mut();

        let chunk = chunk.as_mut().unwrap();

        std::str::from_utf8(&chunk[offset..offset + size])
            .unwrap()
            .to_string()
    })
}

#[ic_cdk::update]
pub fn chunk_size() -> usize {
    BUFFER.with(|chunk| {
        let mut chunk = chunk.borrow_mut();

        let chunk = chunk.as_mut().unwrap();

        chunk.len()
    })
}

#[ic_cdk::update]
pub fn store_buffer(filename: String) -> (u64, usize) {
    let stime = ic_cdk::api::instruction_counter();

    let res = BUFFER.with(|chunk| {
        let chunk = chunk.borrow_mut();

        let chunk = chunk.as_ref().unwrap();

        FS.with(|fs| {
            let mut fs = fs.borrow_mut();

            let root_fd = (*fs).root_fd();

            let fd = (*fs)
                .open_or_create(root_fd, &filename, FdStat::default(), OpenFlags::CREATE, 42)
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

    let etime = ic_cdk::api::instruction_counter();

    (etime - stime, res)
}

#[ic_cdk::update]
pub fn store_buffer_in_1000b_segments(filename: String) -> (u64, usize) {
    let stime = ic_cdk::api::instruction_counter();

    let res = BUFFER.with(|chunk| {
        let chunk = chunk.borrow_mut();

        let chunk = chunk.as_ref().unwrap();

        FS.with(|fs| {
            let mut fs = fs.borrow_mut();

            let root_fd = (*fs).root_fd();

            let fd = (*fs)
                .open_or_create(root_fd, &filename, FdStat::default(), OpenFlags::CREATE, 42)
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

    let etime = ic_cdk::api::instruction_counter();

    (etime - stime, res)
}

#[ic_cdk::update]
pub fn store_buffer_in_1000b_segments_10_files(filename: String) -> (u64, usize) {
    let stime = ic_cdk::api::instruction_counter();

    let res = BUFFER.with(|chunk| {
        let chunk = chunk.borrow_mut();

        let chunk = chunk.as_ref().unwrap();

        FS.with(|fs| {
            let mut fs = fs.borrow_mut();

            let root_fd = (*fs).root_fd();

            let mut fds = Vec::<Fd>::new();

            for i in 0..FILES_COUNT {
                let fd = (*fs)
                    .open_or_create(
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

    let etime = ic_cdk::api::instruction_counter();

    (etime - stime, res)
}

#[ic_cdk::update]
pub fn load_buffer(filename: String) -> (u64, usize) {
    let stime = ic_cdk::api::instruction_counter();

    let res = BUFFER.with(|chunk| {
        let mut chunk = chunk.borrow_mut();

        let chunk = chunk.as_mut().unwrap();

        FS.with(|fs| {
            let mut fs = fs.borrow_mut();

            let root_fd = (*fs).root_fd();

            let fd = (*fs)
                .open_or_create(root_fd, &filename, FdStat::default(), OpenFlags::CREATE, 42)
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

    let etime = ic_cdk::api::instruction_counter();

    (etime - stime, res)
}

#[ic_cdk::update]
pub fn load_buffer_in_1000b_segments(filename: String) -> (u64, usize) {
    let stime = ic_cdk::api::instruction_counter();

    let res = BUFFER.with(|chunk| {
        let mut chunk = chunk.borrow_mut();

        let chunk = chunk.as_mut().unwrap();

        FS.with(|fs| {
            let mut fs = fs.borrow_mut();

            let root_fd = (*fs).root_fd();

            let fd = (*fs)
                .open_or_create(root_fd, &filename, FdStat::default(), OpenFlags::CREATE, 42)
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

    let etime = ic_cdk::api::instruction_counter();

    (etime - stime, res)
}

#[ic_cdk::update]
pub fn load_buffer_in_1000b_segments_10_files(filename: String) -> (u64, usize) {
    let stime = ic_cdk::api::instruction_counter();

    let res = BUFFER.with(|chunk| {
        let mut chunk = chunk.borrow_mut();

        let chunk = chunk.as_mut().unwrap();

        FS.with(|fs| {
            let mut fs = fs.borrow_mut();

            let root_fd = (*fs).root_fd();

            let mut fds = Vec::<Fd>::new();

            for i in 0..FILES_COUNT {
                let fd = (*fs)
                    .open_or_create(
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

    let etime = ic_cdk::api::instruction_counter();

    (etime - stime, res)
}

pub fn profiling_init() {
    let memory = MEMORY_MANAGER.with(|m| m.borrow().get(PROFILING));
    memory.grow(4096);
}

#[ic_cdk::init]
fn init() {
    profiling_init();

    FS.with(|_fs| {
        // empty call to create the file system
        // and not waste instructions for the following calls
    });
}

#[ic_cdk::update]
fn read_bytes(filename: String, offset: i64, size: usize) -> (u64, usize) {
    let stime = ic_cdk::api::instruction_counter();

    let mut res = Vec::with_capacity(size);

    FS.with(|fs| {
        let mut fs = fs.borrow_mut();

        let dir = fs.root_fd();

        let fd = fs
            .open_or_create(
                dir,
                filename.as_str(),
                FdStat::default(),
                OpenFlags::empty(),
                0,
            )
            .unwrap();

        let read_content = [DstBuf {
            buf: res.as_mut_ptr(),
            len: size,
        }];

        let len = fs
            .read_vec_with_offset(fd, &read_content, offset as u64)
            .unwrap();

        let _ = fs.close(fd);

        unsafe { res.set_len(len as usize) };
    });

    let etime = ic_cdk::api::instruction_counter();
    (etime - stime, res.len())
}

// delete file
#[ic_cdk::query]
fn delete_file(filename: String) {
    FS.with(|fs| {
        let mut fs = fs.borrow_mut();
        let dir = fs.root_fd();

        fs.remove_file(dir, filename.as_str()).unwrap();
    });
}

// delete folder
#[ic_cdk::query]
fn delete_folder(filename: String) {
    FS.with(|fs| {
        let mut fs = fs.borrow_mut();
        let dir = fs.root_fd();

        fs.remove_dir(dir, filename.as_str()).unwrap();
    });
}

#[ic_cdk::query]
fn file_size(filename: String) -> usize {
    FS.with(|fs| {
        let mut fs = fs.borrow_mut();

        let dir = fs.root_fd();

        let fd = fs
            .open_or_create(
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

#[cfg(feature = "canbench-rs")]
mod benches {
    use super::*;
    use canbench_rs::{bench, bench_fn, BenchResult};

    fn write_100mb(file_name: &str) -> BenchResult {
        check_buffer("abc1234567".to_string(), 0);

        append_buffer("abc1234567".to_string(), 10_000_000);

        check_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer("temp2.txt".to_string());

        bench_fn(|| {
            store_buffer(file_name.to_string());
        })
    }

    fn write_100mb_over_existing(file_name: &str) -> BenchResult {
        append_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        bench_fn(|| {
            store_buffer(file_name.to_string());
        })
    }

    fn read_100mb(file_name: &str) -> BenchResult {
        append_buffer("abc1234567".to_string(), 10_000_000);
        store_buffer(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        check_buffer("abc1234567".to_string(), 10_000_000);
        clear_buffer();
        check_buffer("abc1234567".to_string(), 0);

        let res = bench_fn(|| {
            load_buffer(file_name.to_string());
        });

        check_buffer("abc1234567".to_string(), 10_000_000);

        res
    }

    fn write_100mb_in_segments(file_name: &str) -> BenchResult {
        append_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer("temp1.txt".to_string());

        let res = bench_fn(|| {
            store_buffer_in_1000b_segments(file_name.to_string());
        });

        assert_eq!(file_size(file_name.to_string()), 100_000_000);
        clear_buffer();
        load_buffer(file_name.to_string());
        check_buffer("abc1234567".to_string(), 10_000_000);

        res
    }

    fn write_100mb_in_segments_over_existing(file_name: &str) -> BenchResult {
        append_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer("temp1.txt".to_string());
        store_buffer(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        bench_fn(|| {
            store_buffer_in_1000b_segments(file_name.to_string());
        })
    }

    fn read_100mb_in_segments(file_name: &str) -> BenchResult {
        append_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer("temp1.txt".to_string());
        store_buffer(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        clear_buffer();
        check_buffer("abc1234567".to_string(), 0);

        let res = bench_fn(|| {
            load_buffer_in_1000b_segments(file_name.to_string());
        });

        check_buffer("abc1234567".to_string(), 10_000_000);

        res
    }

    fn write_100mb_in_segments_10_files(file_name: &str) -> BenchResult {
        append_buffer("abc1234567".to_string(), 10_000_000);
        //store_buffer("temp1.txt".to_string());

        let res = bench_fn(|| {
            store_buffer_in_1000b_segments_10_files(file_name.to_string());
        });

        clear_buffer();
        load_buffer_in_1000b_segments_10_files(file_name.to_string());
        check_buffer("abc1234567".to_string(), 10_000_000);

        res
    }

    fn write_100mb_in_segments_over_existing_10_files(file_name: &str) -> BenchResult {
        append_buffer("abc1234567".to_string(), 10_000_000);

        //store_buffer("temp1.txt".to_string());
        store_buffer_in_1000b_segments_10_files(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        bench_fn(|| {
            store_buffer_in_1000b_segments_10_files(file_name.to_string());
        })
    }

    fn read_100mb_in_segments_10_files(file_name: &str) -> BenchResult {
        append_buffer("abc1234567".to_string(), 10_000_000);

        store_buffer("temp1.txt".to_string());
        store_buffer_in_1000b_segments_10_files(file_name.to_string());
        store_buffer("temp2.txt".to_string());

        clear_buffer();

        let res = bench_fn(|| {
            load_buffer_in_1000b_segments_10_files(file_name.to_string());
        });

        check_buffer("abc1234567".to_string(), 10_000_000);

        res
    }

    /////////////////////////////////////////////////////////////////////

    #[bench(raw)]
    fn file_write_100mb() -> BenchResult {
        write_100mb("file.txt")
    }

    #[bench(raw)]
    fn file_write_100mb_over_existing() -> BenchResult {
        write_100mb_over_existing("file.txt")
    }

    #[bench(raw)]
    fn file_read_100mb() -> BenchResult {
        read_100mb("file.txt")
    }

    #[bench(raw)]
    fn file_write_100mb_in_segments() -> BenchResult {
        write_100mb_in_segments("file.txt")
    }

    #[bench(raw)]
    fn file_write_100mb_in_segments_over_existing() -> BenchResult {
        write_100mb_in_segments_over_existing("file.txt")
    }

    #[bench(raw)]
    fn file_read_100mb_in_segments() -> BenchResult {
        read_100mb_in_segments("file.txt")
    }

    #[bench(raw)]
    fn file_write_100mb_in_segments_10_files() -> BenchResult {
        write_100mb_in_segments_10_files("file.txt")
    }

    #[bench(raw)]
    fn file_write_100mb_in_segments_over_existing_10_files() -> BenchResult {
        write_100mb_in_segments_over_existing_10_files("file.txt")
    }

    #[bench(raw)]
    fn file_read_100mb_in_segments_10_files() -> BenchResult {
        read_100mb_in_segments_10_files("file.txt")
    }
}

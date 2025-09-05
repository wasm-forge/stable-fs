use std::{cell::RefCell, str::FromStr};

use ic_stable_structures::{
    memory_manager::{MemoryId, MemoryManager},
    DefaultMemoryImpl, Memory,
};

use ic_cdk::{export_candid, stable::WASM_PAGE_SIZE_IN_BYTES};
use ic_stable_structures::VectorMemory;
use serde::Deserialize;
use serde::Serialize;
use stable_fs::fs::ChunkType;
use stable_fs::storage::types::DirEntryIndex;
use stable_fs::storage::types::FileSize;
use stable_fs::storage::types::FileType;
use stable_fs::storage::types::Metadata;
use stable_fs::storage::types::Node;
use stable_fs::storage::types::Times;
use std::mem;
use std::mem::MaybeUninit;
use std::ptr;

use stable_fs::{
    fs::{DstBuf, FdStat, FileSystem, OpenFlags, SrcBuf, Whence},
    storage::stable::StableStorage,
};

#[ic_cdk::query]
fn greet(name: String) -> String {
    format!("Hello, {name}!")
}

#[ic_cdk::query]
fn greet_times(name: String, times: usize) -> Vec<String> {
    let mut res = Vec::new();

    for _ in 0..times {
        res.push(greet(name.clone()));
    }

    res
}

const PROFILING: MemoryId = MemoryId::new(100);

thread_local! {
    static MEMORY_MANAGER: RefCell<MemoryManager<DefaultMemoryImpl>> =
        RefCell::new(MemoryManager::init(DefaultMemoryImpl::default()));

    static FS: RefCell<FileSystem> = {

        MEMORY_MANAGER.with(|m| {
            let memory_manager = m.borrow();

            let storage = StableStorage::new_with_memory_manager(&memory_manager, 200..210u8);

            let fs = RefCell::new(
                FileSystem::new(Box::new(storage)).unwrap()
            );

            fs.borrow_mut().mount_memory_file("mount_file.txt", Box::new(memory_manager.get(MemoryId::new(155)))).unwrap();

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
pub fn clear_buffer() {
    BUFFER.with(|chunk| {
        let mut chunk = chunk.borrow_mut();

        if chunk.is_none() {
            return;
        }

        let chunk = chunk.as_mut().unwrap();

        // explicitly destroy contents
        (0..chunk.len()).for_each(|i| {
            chunk[i] = 0;
        });

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
                .open(root_fd, &filename, FdStat::default(), OpenFlags::CREATE, 42)
                .unwrap();

            let write_content = [SrcBuf {
                buf: chunk.as_ptr(),
                len: chunk.len(),
            }];

            let res = (*fs).write_vec(fd, write_content.as_ref()).unwrap();

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
                .open(root_fd, &filename, FdStat::default(), OpenFlags::CREATE, 42)
                .unwrap();

            let size = (*fs).seek(fd, 0, Whence::END).unwrap() as usize;

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

pub fn profiling_init() {
    let memory = MEMORY_MANAGER.with(|m| m.borrow().get(PROFILING));
    memory.grow(4096);
}

#[ic_cdk::init]
fn init() {
    profiling_init();

    FS.with(|_fs| {
        // empty call to create the file system for the following calls
    });
}

#[ic_cdk::update]
fn write_kib_text(filename: String, kb_size: usize) -> u64 {
    let stime = ic_cdk::api::instruction_counter();

    FS.with(|fs| {
        let mut fs = fs.borrow_mut();

        let dir = fs.root_fd();

        // 64 byte block
        let text = "0123456789012345678901234567890123456789012345678901234567890123";

        let write_content = [SrcBuf {
            buf: text.as_ptr(),
            len: text.len(),
        }];

        let fd = fs
            .open(
                dir,
                filename.as_str(),
                FdStat::default(),
                OpenFlags::CREATE,
                0,
            )
            .unwrap();

        let _ = fs.seek(fd, 0, Whence::END);

        let times = 1024 * kb_size / text.len();

        for _ in 0..times {
            fs.write_vec(fd, write_content.as_ref()).unwrap();
        }

        let _ = fs.close(fd);
    });

    let etime = ic_cdk::api::instruction_counter();

    etime - stime
}

#[ic_cdk::update]
fn write_mib_text(filename: String, mib_size: usize) -> u64 {
    let stime = ic_cdk::api::instruction_counter();

    FS.with(|fs| {
        let mut fs = fs.borrow_mut();

        let dir = fs.root_fd();

        // create 4kib block
        let mut text1kib =
            "0123456789012345678901234567890123456789012345678901234567890123".to_string();

        while text1kib.len() < 4096 {
            text1kib.push_str(&text1kib.clone());
        }

        let write_content = [SrcBuf {
            buf: text1kib.as_ptr(),
            len: text1kib.len(),
        }];

        let fd = fs
            .open(
                dir,
                filename.as_str(),
                FdStat::default(),
                OpenFlags::CREATE,
                0,
            )
            .unwrap();

        let _ = fs.seek(fd, 0, Whence::END);

        let times = 1024 * 1024 * mib_size / text1kib.len();

        for _ in 0..times {
            fs.write_vec(fd, write_content.as_ref()).unwrap();
        }

        let _ = fs.close(fd);
    });

    let etime = ic_cdk::api::instruction_counter();

    etime - stime
}

#[ic_cdk::update]
fn read_bytes(filename: String, offset: i64, size: usize) -> (u64, usize) {
    let stime = ic_cdk::api::instruction_counter();

    let mut res = Vec::with_capacity(size);

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
fn list_files(path: String) -> Vec<String> {
    let mut res = vec![];

    FS.with(|fs| {
        let mut fs = fs.borrow_mut();
        let dir = fs.root_fd();

        let fd = fs
            .open(
                dir,
                path.as_str(),
                FdStat::default(),
                OpenFlags::DIRECTORY,
                0,
            )
            .unwrap();

        fs.with_direntries(fd, Some(0), &mut |_index, entry| -> bool {
            let filename_str =
                std::str::from_utf8(&entry.name.bytes[0..(entry.name.length as usize)]).unwrap();

            let st = String::from_str(filename_str).unwrap();

            res.push(st);

            true
        })
        .unwrap();
    });

    res
}

#[ic_cdk::query]
fn cat_file(filename: String) -> String {
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

        let mut buf = [0u8; 100];

        let read_size = fs.read(fd, &mut buf).unwrap();

        unsafe {
            let st = std::str::from_utf8_unchecked(&buf[..(read_size as usize)]);

            st.to_string()
        }
    })
}

#[ic_cdk::update]
fn create_depth_folders(path: String, count: usize) -> String {
    FS.with(|fs| {
        let mut fs = fs.borrow_mut();

        let root_dir = fs.root_fd();

        let mut dir_name = "d0".to_string();

        for num in 1..count {
            dir_name = format!("{dir_name}/d{num}");
        }

        fs.mkdir(root_dir, dir_name.as_str(), FdStat::default(), 0)
            .unwrap();

        format!("{path}/{dir_name}")
    })
}

#[ic_cdk::update]
fn create_files(path: String, count: usize) -> u64 {
    let stime = ic_cdk::api::instruction_counter();

    FS.with(|fs| {
        let mut fs = fs.borrow_mut();

        let dir = fs.root_fd();

        for num in 0..count {
            let filename = format!("{path}/{num}.txt");

            let fd = fs
                .open(
                    dir,
                    filename.as_str(),
                    FdStat::default(),
                    OpenFlags::CREATE,
                    0,
                )
                .unwrap();

            let _ = fs.seek(fd, 0, Whence::END);

            // 64 byte block
            let text = format!(
                "0123456789012345678901234567890123456789012345678901234567890123:{filename}"
            );

            let write_content = [SrcBuf {
                buf: text.as_ptr(),
                len: text.len(),
            }];

            fs.write_vec(fd, write_content.as_ref()).unwrap();

            let _ = fs.close(fd);
        }
    });

    let etime = ic_cdk::api::instruction_counter();

    etime - stime
}

#[ic_cdk::update]
fn append_text(filename: String, text: String, times: usize) -> u64 {
    let stime = ic_cdk::api::instruction_counter();

    FS.with(|fs| {
        let mut fs = fs.borrow_mut();

        let dir = fs.root_fd();

        let mut txt = String::with_capacity(text.len() * times);

        for _ in 0..times {
            txt.push_str(&text);
        }

        let write_content = [SrcBuf {
            buf: txt.as_ptr(),
            len: txt.len(),
        }];

        let fd = fs
            .open(
                dir,
                filename.as_str(),
                FdStat::default(),
                OpenFlags::CREATE,
                0,
            )
            .unwrap();

        let _ = fs.seek(fd, 0, Whence::END);

        fs.write_vec(fd, write_content.as_ref()).unwrap();

        let _ = fs.close(fd);
    });

    let etime = ic_cdk::api::instruction_counter();

    etime - stime
}

#[ic_cdk::query]
fn read_text(filename: String, offset: i64, size: usize) -> String {
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
    })
}

#[ic_cdk::query]
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

pub fn new_vector_memory() -> VectorMemory {
    use std::{cell::RefCell, rc::Rc};

    Rc::new(RefCell::new(Vec::new()))
}

#[inline]
pub fn grow_memory(memory: &dyn Memory, max_address: FileSize) {
    let pages_required = max_address.div_ceil(WASM_PAGE_SIZE_IN_BYTES);

    let cur_pages = memory.size();

    if cur_pages < pages_required {
        memory.grow(pages_required - cur_pages);
    }
}

#[inline]
pub fn read_obj<T: Sized>(memory: &dyn Memory, address: u64, obj: &mut T) {
    let obj_size = std::mem::size_of::<T>();

    let obj_bytes = unsafe { std::slice::from_raw_parts_mut(obj as *mut T as *mut u8, obj_size) };

    memory.read(address, obj_bytes);
}

#[inline]
pub fn write_obj<T: Sized>(memory: &dyn Memory, address: u64, obj: &T) {
    let obj_size = std::mem::size_of::<T>();

    let obj_bytes = unsafe { std::slice::from_raw_parts(obj as *const T as *const u8, obj_size) };

    grow_memory(memory, address + obj_size as u64);

    memory.write(address, obj_bytes);
}

#[inline]
pub fn to_binary<T: Sized>(obj: &T) -> Vec<u8> {
    let obj_size = std::mem::size_of::<T>();

    let obj_bytes = unsafe { std::slice::from_raw_parts(obj as *const T as *const u8, obj_size) };

    let ret: Vec<u8> = obj_bytes.to_vec();

    ret
}

#[ic_cdk::query]
fn check_metadata_format() {
    #[repr(C)]
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct MetadataC {
        pub first_dir_entry: Option<DirEntryIndex>,
        pub node: Node,
        pub file_type: FileType,
        pub link_count: u64,
        pub size: FileSize,
        pub times: Times,
        pub last_dir_entry: Option<DirEntryIndex>,
        pub chunk_type: Option<ChunkType>,
    }

    let mem = new_vector_memory();

    let meta_old = Metadata {
        node: 23,
        file_type: FileType::RegularFile,
        link_count: 3,
        size: 123,
        times: Times::default(),
        chunk_type: Some(stable_fs::fs::ChunkType::V2),
        maximum_size_allowed: None,
        last_dir_entry: None,
        first_dir_entry: None,
    };

    write_obj(&mem, 16, &meta_old);

    let mut meta_new = MetadataC::default();

    read_obj(&mem, 16, &mut meta_new);

    assert_eq!(meta_old.node, meta_new.node);
    assert_eq!(meta_old.file_type, meta_new.file_type);
    assert_eq!(meta_old.link_count, meta_new.link_count);
    assert_eq!(meta_old.size, meta_new.size);
    assert_eq!(meta_old.times, meta_new.times);
    assert_eq!(meta_old.chunk_type, meta_new.chunk_type);
}

#[ic_cdk::query]
fn check_metadata_deserialization_into_repr_c() -> u64 {
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct MetadataOld {
        pub node: Node,
        pub file_type: FileType,
        pub link_count: u64,
        pub size: FileSize,
        pub times: Times,
        pub first_dir_entry: Option<DirEntryIndex>,
        pub last_dir_entry: Option<DirEntryIndex>,
        pub chunk_type: Option<ChunkType>,
    }

    #[repr(C)]
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct MetadataReprC {
        pub node: Node,
        pub file_type: FileType,
        pub link_count: u64,
        pub size: FileSize,
        pub times: Times,
        pub _first_dir_entry: Option<DirEntryIndex>,
        pub _last_dir_entry: Option<DirEntryIndex>,
        pub chunk_type: Option<ChunkType>,
    }

    fn meta_to_bytes(meta: &'_ MetadataOld) -> std::borrow::Cow<'_, [u8]> {
        let mut buf = vec![];
        ciborium::ser::into_writer(meta, &mut buf).unwrap();
        std::borrow::Cow::Owned(buf)
    }

    fn meta_from_bytes(bytes: std::borrow::Cow<[u8]>) -> MetadataReprC {
        ciborium::de::from_reader(bytes.as_ref()).unwrap()
    }

    let meta_old = MetadataOld {
        node: 23,
        file_type: FileType::RegularFile,
        link_count: 3,
        size: 123,
        times: Times::default(),
        first_dir_entry: Some(25),
        last_dir_entry: Some(35),
        chunk_type: Some(stable_fs::fs::ChunkType::V2),
    };

    // serialize from old format
    let bytes = meta_to_bytes(&meta_old);

    let stime = ic_cdk::api::instruction_counter();
    // decerialize into C repr
    let meta_new = meta_from_bytes(bytes);
    let etime = ic_cdk::api::instruction_counter();

    assert_eq!(meta_old.node, meta_new.node);
    assert_eq!(meta_old.file_type, meta_new.file_type);
    assert_eq!(meta_old.link_count, meta_new.link_count);
    assert_eq!(meta_old.size, meta_new.size);
    assert_eq!(meta_old.times, meta_new.times);
    assert_eq!(meta_old.chunk_type, meta_new.chunk_type);

    etime - stime
}

#[ic_cdk::query]
fn check_metadata_binary() -> String {
    // 1. Allocate "uninitialized" memory for a Metadata
    let mut uninit = MaybeUninit::<Metadata>::uninit();

    // 2. Fill that memory with 0xfa
    unsafe {
        ptr::write_bytes(
            uninit.as_mut_ptr() as *mut u8,
            0xfa,
            mem::size_of::<Metadata>(),
        );
    }

    // 3. Now "create" the actual Metadata from that memory
    //    (this is only safe if every field of Metadata is
    //    assigned a valid value afterwards)
    let mut meta = unsafe { uninit.assume_init() };

    // 4. Overwrite the fields with actual values
    meta.node = 3;
    meta.file_type = FileType::RegularFile;
    meta.link_count = 6;
    meta.size = 8;
    meta.times = Times {
        accessed: 65u64,
        modified: 66u64,
        created: 67u64,
    };
    meta.chunk_type = Some(stable_fs::fs::ChunkType::V2);
    meta.maximum_size_allowed = Some(0xcdab);

    let vec = to_binary(&meta);
    hex::encode(&vec)
}

export_candid!();

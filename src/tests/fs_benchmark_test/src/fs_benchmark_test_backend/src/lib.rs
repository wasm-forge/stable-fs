use std::{cell::RefCell, str::FromStr};

use stable_fs::{fs::{DstBuf, FdStat, FileSystem, OpenFlags, SrcBuf, Whence}, storage::stable::StableStorage};
use ic_stable_structures::{memory_manager::{MemoryId, MemoryManager}, DefaultMemoryImpl, Memory};



#[ic_cdk::query]
fn greet(name: String) -> String {
    format!("Hello, {}!", name)
}


const PROFILING: MemoryId = MemoryId::new(100);
const WASI_MEMORY_ID: MemoryId = MemoryId::new(1);

thread_local! {
    static MEMORY_MANAGER: RefCell<MemoryManager<DefaultMemoryImpl>> =
        RefCell::new(MemoryManager::init(DefaultMemoryImpl::default()));

    static FS: RefCell<FileSystem> = RefCell::new(
        FileSystem::new(Box::new(StableStorage::new(
            MEMORY_MANAGER.with(|m| m.borrow().get(WASI_MEMORY_ID))
        ))).unwrap()
    );
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

        let write_content = [
            SrcBuf {
                buf: text.as_ptr(),
                len: text.len(),
            },
        ];
    
        let fd = fs
            .open_or_create(dir, filename.as_str(), FdStat::default(), OpenFlags::CREATE, 0)
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
        let mut text1kib = "0123456789012345678901234567890123456789012345678901234567890123".to_string();

        while text1kib.len() < 4096 {
            text1kib.push_str(&text1kib.clone());
        }

        let write_content = [
            SrcBuf {
                buf: text1kib.as_ptr(),
                len: text1kib.len(),
            },
        ];
    
        let fd = fs
            .open_or_create(dir, filename.as_str(), FdStat::default(), OpenFlags::CREATE, 0)
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
fn read_kb(filename: String, kb_size: usize, offset: i64) -> Vec<u8> {
    let size = kb_size * 1024;
    let mut res = Vec::with_capacity(size);

    FS.with(|fs| {

        let mut fs = fs.borrow_mut();

        let dir = fs.root_fd();

        let fd = fs
            .open_or_create(dir, filename.as_str(), FdStat::default(), OpenFlags::empty(), 0)
            .unwrap();

        let read_content = [
            DstBuf {
                buf: res.as_mut_ptr(),
                len: size,
            },
        ];
        
        fs.read_vec_with_offset(fd, &read_content, offset as u64).unwrap();
        
        let _ = fs.close(fd);

    });

    res
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

        let fd = fs.open_or_create(dir, path.as_str(), FdStat::default(), OpenFlags::DIRECTORY, 0).unwrap();

        let meta = fs.metadata(fd).unwrap();

        let mut entry_index = meta.first_dir_entry;

        while let Some(index) = entry_index {

            let entry = fs.get_direntry(fd, index).unwrap();

            let filename_str = std::str::from_utf8(&entry.name.bytes[0..(entry.name.length as usize)]).unwrap();

            let st = String::from_str(filename_str).unwrap();

            res.push(st);

            entry_index = entry.next_entry;
            
        }

    });

    res
}


#[ic_cdk::query]
fn cat_file(filename: String) -> String {
    
    FS.with(|fs| {

        let mut fs = fs.borrow_mut();
        let dir = fs.root_fd();

        let fd = fs
            .open_or_create(dir, filename.as_str() , FdStat::default(), OpenFlags::empty(), 0)
            .unwrap();

        let mut buf = [0u8; 100];

        let read_size = fs.read(fd, &mut buf).unwrap();

        unsafe {
            let st = std::str::from_utf8_unchecked(&buf[..(read_size as usize)]);

            return st.to_string();
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
            dir_name = format!("{}/d{}", dir_name, num);
        }

        fs.create_dir(root_dir, dir_name.as_str(), FdStat::default(), 0).unwrap();

        format!("{}/{}", path, dir_name)

    })
}

#[ic_cdk::update]
fn create_files(path: String, count: usize) -> u64 {
    let stime = ic_cdk::api::instruction_counter();    

    FS.with(|fs| {

        let mut fs = fs.borrow_mut();

        let dir = fs.root_fd();

        for num in 0..count {

            let filename = format!("{}/{}.txt", path, num);

            let fd = fs
                .open_or_create(dir, filename.as_str() , FdStat::default(), OpenFlags::CREATE, 0)
                .unwrap();
            
            let _ = fs.seek(fd, 0, Whence::END);

            // 64 byte block
            let text = format!("0123456789012345678901234567890123456789012345678901234567890123:{}", filename);

            let write_content = [
                SrcBuf {
                    buf: text.as_ptr(),
                    len: text.len(),
                },
            ];
            
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

        let write_content = [
            SrcBuf {
                buf: text.as_ptr(),
                len: text.len(),
            },
        ];
    
        let fd = fs
            .open_or_create(dir, filename.as_str(), FdStat::default(), OpenFlags::CREATE, 0)
            .unwrap();
        
        let _ = fs.seek(fd, 0, Whence::END);

        for _ in 0..times {
            fs.write_vec(fd, write_content.as_ref()).unwrap();
        }
    
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

        let fd = fs.open_or_create(dir, filename.as_str(), FdStat::default(), OpenFlags::empty(), 0).unwrap();        

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
    })

}
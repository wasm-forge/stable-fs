use std::{cell::RefCell, cmp::min, fs::{self, File}, io::{Read, Seek, Write}};

use stable_fs::{fs::{DstBuf, FdStat, FileSystem, OpenFlags, SrcBuf, Whence}, storage::stable::StableStorage};
use ic_stable_structures::{memory_manager::{MemoryId, MemoryManager}, DefaultMemoryImpl, Memory};


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


#[ic_cdk::query]
fn greet(name: String) -> String {
    format!("Greetings, {}!", name)
}


#[ic_cdk::update]
fn read_kb(filename: String, kb_size: usize, offset: u64) -> Vec<u8> {
    let size = kb_size * 1024;

    let mut res = Vec::with_capacity(size);

    let f = File::open(filename).expect("Unable to open file");

    let mut f = std::io::BufReader::new(f);

    f.seek(std::io::SeekFrom::Start(offset)).unwrap();

    f.read(res.as_mut_slice()).unwrap();

    res
}

// delete file
#[ic_cdk::query]
fn delete_file(filename: String) {
    fs::remove_file(filename).unwrap();
}

// delete folder
#[ic_cdk::query]
fn delete_folder(path: String) {
    fs::remove_dir_all(path).unwrap();
}


#[ic_cdk::query]
fn list_files(path: String) -> Vec<String> {
    println!("Reading directory: {}", path);

    let mut res = vec![];
    let entries = fs::read_dir(path).unwrap();

    for entry in entries {
        let entry = entry.unwrap();

        res.push(entry.path().into_os_string().into_string().unwrap());
    }

    res
}

fn list_all_files_recursive(path: &String, files: &mut Vec<String>) {
    
    let entries = fs::read_dir(&path).unwrap();

    for entry in entries {
        let entry = entry.unwrap();

        let folder_name = entry.path().into_os_string().into_string().unwrap();

        println!("{}", &folder_name);
        files.push(folder_name.clone());

        if entry.metadata().unwrap().is_dir() {
            list_all_files_recursive(&folder_name, files);
        }

    }
}

#[ic_cdk::query]
fn list_all_files(path: String) -> Vec<String> {
    println!("Reading directory: {}", path);

    let mut res = vec![];
    list_all_files_recursive(&path, &mut res);

    res
}

#[ic_cdk::update]
fn create_depth_folders(path: String, count: usize) -> String {

    let mut dir_name = "d0".to_string();
    
    for num in 1..count {
        dir_name = format!("{}/d{}", dir_name, num);
    }

    dir_name = format!("{}/{}", path, dir_name);

    fs::create_dir_all(&dir_name).unwrap();

    dir_name
}

#[ic_cdk::update]
fn delete_depth_folders(path: String, count: usize) -> String {

    let mut dir_name = "d0".to_string();
    
    for num in 1..count {
        dir_name = format!("{}/d{}", dir_name, num);
    }

    dir_name = format!("{}/{}", path, dir_name);

    fs::remove_dir_all(&dir_name).unwrap();

    dir_name
}


#[ic_cdk::update]
fn create_files(path: String, count: usize) -> u64 {
    let stime = ic_cdk::api::instruction_counter();

    for num in 0..count {

        let filename = format!("{}/f{}.txt", path, num);
        let mut file = File::create(&filename).unwrap();

        // 64 byte block + file name
        let text = format!("0123456789012345678901234567890123456789012345678901234567890123:{}", filename);

        file.write_all(text.as_bytes()).unwrap();

        file.flush().unwrap();
    }

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

        let min = min(read, size as u64) as usize;

        let _ = fs.close(fd);

        content[..min].to_string()
    })

}

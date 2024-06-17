use std::cell::RefCell;

use stable_fs::{fs::{DstBuf, FdStat, FileSystem, OpenFlags, SrcBuf, Whence}, storage::stable::StableStorage};
use ic_stable_structures::DefaultMemoryImpl;


#[ic_cdk::query]
fn greet(name: String) -> String {
    format!("Hello, {}!", name)
}

thread_local! {
    static FS: RefCell<FileSystem> = RefCell::new(
        FileSystem::new(Box::new(StableStorage::new(DefaultMemoryImpl::default()))
        ).unwrap()
    );
}

#[ic_cdk::init]
fn init() {
    FS.with(|_fs| {
        // empty call to create the file system for the following calls
    });
}

#[ic_cdk::update]
fn empty_call() -> u64 {

    let stime = ic_cdk::api::instruction_counter();    

    FS.with(|fs| {
        let fs = fs.borrow_mut();
        let _dir = fs.root_fd();
    });

    let etime = ic_cdk::api::instruction_counter();    

    etime - stime
}

#[ic_cdk::update]
fn write_mb_text(filename: String, mb_size: usize) -> u64 {
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

        let times = 1024 * 1024 * mb_size / text.len();

        for _ in 0..times {
            fs.write_vec(fd, write_content.as_ref()).unwrap();
        }
    
        let _ = fs.close(fd);

    });

    let etime = ic_cdk::api::instruction_counter();    

    etime - stime
}


#[ic_cdk::update]
fn write_text(filename: String, text: String, times: usize) -> u64 {

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

        fs.seek(fd, offset, Whence::SET).unwrap();
        
        let mut content = (0..size).map(|_| ".").collect::<String>();

        let read_content = [
            DstBuf {
                buf: content.as_mut_ptr(),
                len: content.len(),
            },
        ];
        
        fs.read_vec_with_offset(fd, &read_content, offset as u64).unwrap();
        
        let _ = fs.close(fd);

        content
    })

}

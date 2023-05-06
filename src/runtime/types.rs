use bitflags::bitflags;

#[derive(Copy, Clone, Debug)]
pub struct FdStat {
    pub flags: FdFlags,
    pub rights_base: u64,
    pub rights_inheriting: u64,
}

impl Default for FdStat {
    fn default() -> Self {
        Self {
            flags: FdFlags::empty(),
            rights_base: 0,
            rights_inheriting: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Whence {
    SET,
    CUR,
    END,
}

bitflags! {
    #[derive(Copy, Clone, Debug)]
    pub struct FdFlags: u16 {
        const APPEND = 1;
        const DSYNC = 2;
        const NONBLOCK = 4;
        const RSYNC = 8;
        const SYNC = 16;
    }
}

bitflags! {
    pub struct OpenFlags: u16 {
        /// Create file if it does not exist.
        const CREATE = 1;
        /// Fail if not a directory.
        const DIRECTORY = 2;
        /// Fail if file already exists.
        const EXCLUSIVE = 4;
        /// Truncate file to size 0.
        const TRUNCATE = 8;
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct DstBuf {
    pub buf: *mut u8,
    pub len: usize,
}
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct SrcBuf {
    pub buf: *const u8,
    pub len: usize,
}
pub type SrcIoVec<'a> = &'a [SrcBuf];
pub type DstIoVec<'a> = &'a [DstBuf];

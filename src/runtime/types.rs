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

#[derive(Clone, Copy, Debug)]
pub enum ChunkSize {
    CHUNK4K = 4096,
    CHUNK8K = 8192,
    CHUNK16K = 16384,
    CHUNK32K = 32768,
    CHUNK64K = 65536,
}

impl ChunkSize {
    pub const VALUES: [Self; 5] = [
        Self::CHUNK4K,
        Self::CHUNK8K,
        Self::CHUNK16K,
        Self::CHUNK32K,
        Self::CHUNK64K,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ChunkType {
    V1,
    V2,
}

bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq)]
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

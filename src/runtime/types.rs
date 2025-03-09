#![allow(dead_code)]

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

// file descriptor
pub type Fd = u32;

pub type Rights = u64;
/// The right to invoke `fd_datasync`.
/// If `path_open` is set, includes the right to invoke
/// `path_open` with `fdflags::dsync`.
pub const RIGHTS_FD_DATASYNC: Rights = 1 << 0;
/// The right to invoke `fd_read` and `sock_recv`.
/// If `rights::fd_seek` is set, includes the right to invoke `fd_pread`.
pub const RIGHTS_FD_READ: Rights = 1 << 1;
/// The right to invoke `fd_seek`. This flag implies `rights::fd_tell`.
pub const RIGHTS_FD_SEEK: Rights = 1 << 2;
/// The right to invoke `fd_fdstat_set_flags`.
pub const RIGHTS_FD_FDSTAT_SET_FLAGS: Rights = 1 << 3;
/// The right to invoke `fd_sync`.
/// If `path_open` is set, includes the right to invoke
/// `path_open` with `fdflags::rsync` and `fdflags::dsync`.
pub const RIGHTS_FD_SYNC: Rights = 1 << 4;
/// The right to invoke `fd_seek` in such a way that the file offset
/// remains unaltered (i.e., `whence::cur` with offset zero), or to
/// invoke `fd_tell`.
pub const RIGHTS_FD_TELL: Rights = 1 << 5;
/// The right to invoke `fd_write` and `sock_send`.
/// If `rights::fd_seek` is set, includes the right to invoke `fd_pwrite`.
pub const RIGHTS_FD_WRITE: Rights = 1 << 6;
/// The right to invoke `fd_advise`.
pub const RIGHTS_FD_ADVISE: Rights = 1 << 7;
/// The right to invoke `fd_allocate`.
pub const RIGHTS_FD_ALLOCATE: Rights = 1 << 8;
/// The right to invoke `path_create_directory`.
pub const RIGHTS_PATH_CREATE_DIRECTORY: Rights = 1 << 9;
/// If `path_open` is set, the right to invoke `path_open` with `oflags::creat`.
pub const RIGHTS_PATH_CREATE_FILE: Rights = 1 << 10;
/// The right to invoke `path_link` with the file descriptor as the
/// source directory.
pub const RIGHTS_PATH_LINK_SOURCE: Rights = 1 << 11;
/// The right to invoke `path_link` with the file descriptor as the
/// target directory.
pub const RIGHTS_PATH_LINK_TARGET: Rights = 1 << 12;
/// The right to invoke `path_open`.
pub const RIGHTS_PATH_OPEN: Rights = 1 << 13;
/// The right to invoke `fd_readdir`.
pub const RIGHTS_FD_READDIR: Rights = 1 << 14;
/// The right to invoke `path_readlink`.
pub const RIGHTS_PATH_READLINK: Rights = 1 << 15;
/// The right to invoke `path_rename` with the file descriptor as the source directory.
pub const RIGHTS_PATH_RENAME_SOURCE: Rights = 1 << 16;
/// The right to invoke `path_rename` with the file descriptor as the target directory.
pub const RIGHTS_PATH_RENAME_TARGET: Rights = 1 << 17;
/// The right to invoke `path_filestat_get`.
pub const RIGHTS_PATH_FILESTAT_GET: Rights = 1 << 18;
/// The right to change a file's size (there is no `path_filestat_set_size`).
/// If `path_open` is set, includes the right to invoke `path_open` with `oflags::trunc`.
pub const RIGHTS_PATH_FILESTAT_SET_SIZE: Rights = 1 << 19;
/// The right to invoke `path_filestat_set_times`.
pub const RIGHTS_PATH_FILESTAT_SET_TIMES: Rights = 1 << 20;
/// The right to invoke `fd_filestat_get`.
pub const RIGHTS_FD_FILESTAT_GET: Rights = 1 << 21;
/// The right to invoke `fd_filestat_set_size`.
pub const RIGHTS_FD_FILESTAT_SET_SIZE: Rights = 1 << 22;
/// The right to invoke `fd_filestat_set_times`.
pub const RIGHTS_FD_FILESTAT_SET_TIMES: Rights = 1 << 23;
/// The right to invoke `path_symlink`.
pub const RIGHTS_PATH_SYMLINK: Rights = 1 << 24;
/// The right to invoke `path_remove_directory`.
pub const RIGHTS_PATH_REMOVE_DIRECTORY: Rights = 1 << 25;
/// The right to invoke `path_unlink_file`.
pub const RIGHTS_PATH_UNLINK_FILE: Rights = 1 << 26;
/// If `rights::fd_read` is set, includes the right to invoke `poll_oneoff` to subscribe to `eventtype::fd_read`.
/// If `rights::fd_write` is set, includes the right to invoke `poll_oneoff` to subscribe to `eventtype::fd_write`.
pub const RIGHTS_POLL_FD_READWRITE: Rights = 1 << 27;

#[derive(Copy, Clone, Debug)]
pub struct FdStat {
    pub flags: FdFlags,
    pub rights_base: Rights,
    pub rights_inheriting: Rights,
}

impl Default for FdStat {
    fn default() -> Self {
        Self {
            flags: FdFlags::empty(),
            rights_base: (1 << 27) - 1, // allow anything by default
            rights_inheriting: (1 << 27) - 1,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkType {
    V1 = 1,
    V2 = 2,
}

#[derive(Clone, Copy, Debug)]
pub enum Advice {
    Normal = 0,
    Sequential = 1,
    Random = 2,
    WillNeed = 3,
    DontNeed = 4,
    NoReuse = 5,
}

impl TryFrom<u8> for Advice {
    type Error = crate::error::Error;

    fn try_from(value: u8) -> Result<Self, crate::error::Error> {
        match value {
            0 => Ok(Advice::Normal),
            1 => Ok(Advice::Sequential),
            2 => Ok(Advice::Random),
            3 => Ok(Advice::WillNeed),
            4 => Ok(Advice::DontNeed),
            5 => Ok(Advice::NoReuse),
            _ => Err(crate::error::Error::InvalidArgument),
        }
    }
}

impl From<Advice> for u8 {
    fn from(val: Advice) -> Self {
        match val {
            Advice::Normal => 0,
            Advice::Sequential => 1,
            Advice::Random => 2,
            Advice::WillNeed => 3,
            Advice::DontNeed => 4,
            Advice::NoReuse => 5,
        }
    }
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

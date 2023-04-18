use crate::error::Error;

pub const FILE_CHUNK_SIZE: usize = 4096;
pub const MAX_FILE_NAME: usize = 255;

// The unique identifier of a node, which can be a file or a directory.
// Also known as inode in WASI and other file systems.
pub type Node = u64;

// An integer type for representing file sizes and offsets.
pub type FileSize = u64;

// An index of a file chunk.
pub type FileChunkIndex = u32;

// A file consists of multiple file chunks.
#[derive(Clone, Debug)]
pub struct FileChunk {
    pub bytes: [u8; FILE_CHUNK_SIZE],
}

impl Default for FileChunk {
    fn default() -> Self {
        Self {
            bytes: [0; FILE_CHUNK_SIZE],
        }
    }
}

// Contains metadata of a node.
#[derive(Clone, Debug)]
pub struct Metadata {
    pub node: Node,
    pub file_type: FileType,
    pub link_count: u64,
    pub size: FileSize,
    pub times: Times,
    pub first_dir_entry: Option<DirEntryIndex>,
    pub last_dir_entry: Option<DirEntryIndex>,
}

// The type of a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileType {
    Directory,
    RegularFile,
    SymbolicLink,
}

impl TryFrom<u8> for FileType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            3 => Ok(FileType::Directory),
            4 => Ok(FileType::RegularFile),
            7 => Ok(FileType::SymbolicLink),
            _ => Err(Error::InvalidFileType),
        }
    }
}

impl Into<u8> for FileType {
    fn into(self) -> u8 {
        match self {
            FileType::Directory => 3,
            FileType::RegularFile => 4,
            FileType::SymbolicLink => 7,
        }
    }
}

// The time stats of a node.
#[derive(Clone, Copy, Debug, Default)]
pub struct Times {
    pub accessed: u64,
    pub modified: u64,
    pub created: u64,
}

// The name of a file or a directory. Most operating systems limit the max file
// name length to 255.
#[derive(Clone, Debug)]
pub struct FileName {
    pub length: u8,
    pub bytes: [u8; MAX_FILE_NAME],
}

impl FileName {
    pub fn new(name: &str) -> Result<Self, Error> {
        let name = name.as_bytes();
        let len = name.len();
        if len > MAX_FILE_NAME {
            return Err(Error::NameTooLong);
        }
        let mut bytes = [0; MAX_FILE_NAME];
        (&mut bytes[0..len]).copy_from_slice(name);
        Ok(Self {
            length: len as u8,
            bytes,
        })
    }
}

// An index of a directory entry.
pub type DirEntryIndex = u32;

// A directory contains a list of directory entries.
// Each entry describes a name of a file or a directory.
#[derive(Clone, Debug)]
pub struct DirEntry {
    pub name: FileName,
    pub node: Node,
    pub next_entry: Option<DirEntryIndex>,
    pub prev_entry: Option<DirEntryIndex>,
}

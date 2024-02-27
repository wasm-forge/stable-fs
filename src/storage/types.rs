use crate::error::Error;
use ic_stable_structures::storable::Bound;
use serde::{Deserialize, Serialize};

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

impl ic_stable_structures::Storable for FileChunk {
    fn to_bytes(&self) -> std::borrow::Cow<[u8]> {
        std::borrow::Cow::Borrowed(&self.bytes)
    }

    fn from_bytes(bytes: std::borrow::Cow<[u8]>) -> Self {
        Self {
            bytes: bytes.as_ref().try_into().unwrap(),
        }
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: FILE_CHUNK_SIZE as u32,
        is_fixed_size: true,
    };
}

// Contains metadata of a node.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    pub node: Node,
    pub file_type: FileType,
    pub link_count: u64,
    pub size: FileSize,
    pub times: Times,
    pub first_dir_entry: Option<DirEntryIndex>,
    pub last_dir_entry: Option<DirEntryIndex>,
}

// This value was obtained by printing `Storable::to_bytes().len()`,
// which is 140 and rounding it up to 144.
const MAX_META_SIZE: u32 = 144;

impl ic_stable_structures::Storable for Metadata {
    fn to_bytes(&self) -> std::borrow::Cow<[u8]> {
        let mut buf = vec![0u8; MAX_META_SIZE as usize];

        unsafe { buf.set_len(0) }

        ciborium::ser::into_writer(&self, &mut buf).unwrap();

        unsafe { buf.set_len(MAX_META_SIZE as usize) }

        assert!(
            MAX_META_SIZE >= buf.len() as u32,
            "Metadata size assertion fails: MAX_META_SIZE = {}, buf.len() = {}",
            MAX_META_SIZE,
            buf.len()
        );
        std::borrow::Cow::Owned(buf)
    }

    fn from_bytes(bytes: std::borrow::Cow<[u8]>) -> Self {
        ciborium::de::from_reader(bytes.as_ref()).unwrap()
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_META_SIZE,
        is_fixed_size: true,
    };
}

// The type of a node.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    Directory,
    #[default]
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

impl From<FileType> for u8 {
    fn from(val: FileType) -> Self {
        match val {
            FileType::Directory => 3,
            FileType::RegularFile => 4,
            FileType::SymbolicLink => 7,
        }
    }
}

// The time stats of a node.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Times {
    pub accessed: u64,
    pub modified: u64,
    pub created: u64,
}

// The name of a file or a directory. Most operating systems limit the max file
// name length to 255.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileName {
    pub length: u8,
    #[serde(
        deserialize_with = "deserialize_file_name",
        serialize_with = "serialize_file_name"
    )]
    pub bytes: [u8; MAX_FILE_NAME],
}

fn serialize_file_name<S>(bytes: &[u8; MAX_FILE_NAME], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serde_bytes::Bytes::new(bytes).serialize(serializer)
}

fn deserialize_file_name<'de, D>(deserializer: D) -> Result<[u8; MAX_FILE_NAME], D::Error>
where
    D: serde::Deserializer<'de>,
{
    let bytes: Vec<u8> = serde_bytes::deserialize(deserializer).unwrap();
    let len = bytes.len();
    let bytes_array: [u8; MAX_FILE_NAME] = bytes
        .try_into()
        .map_err(|_| serde::de::Error::invalid_length(len, &"expected MAX_FILE_NAME bytes"))?;
    Ok(bytes_array)
}

impl Default for FileName {
    fn default() -> Self {
        Self {
            length: 0,
            bytes: [0; MAX_FILE_NAME],
        }
    }
}

impl FileName {
    pub fn new(name: &[u8]) -> Result<Self, Error> {
        let len = name.len();
        if len > MAX_FILE_NAME {
            return Err(Error::NameTooLong);
        }
        let mut bytes = [0; MAX_FILE_NAME];
        bytes[0..len].copy_from_slice(name);
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: FileName,
    pub node: Node,
    pub next_entry: Option<DirEntryIndex>,
    pub prev_entry: Option<DirEntryIndex>,
}

// This value was obtained by printing `DirEntry::to_bytes().len()`,
// which is 308 and rounding it up to 320.
pub const MAX_DIR_ENTRY_SIZE: u32 = 320;

impl ic_stable_structures::Storable for DirEntry {
    fn to_bytes(&self) -> std::borrow::Cow<[u8]> {
        let mut buf = vec![0; MAX_DIR_ENTRY_SIZE as usize];

        unsafe { buf.set_len(0) }

        ciborium::ser::into_writer(&self, &mut buf).unwrap();

        unsafe { buf.set_len(MAX_DIR_ENTRY_SIZE as usize) }

        assert!(
            MAX_DIR_ENTRY_SIZE >= buf.len() as u32,
            "DirEntry size assertion fails: MAX_DIR_ENTRY_SIZE = {}, buf.len() = {}",
            MAX_DIR_ENTRY_SIZE,
            buf.len()
        );
        std::borrow::Cow::Owned(buf)
    }

    fn from_bytes(bytes: std::borrow::Cow<[u8]>) -> Self {
        ciborium::de::from_reader(bytes.as_ref()).unwrap()
    }

    const BOUND: ic_stable_structures::storable::Bound =
        ic_stable_structures::storable::Bound::Bounded {
            max_size: MAX_DIR_ENTRY_SIZE,
            is_fixed_size: true,
        };
}

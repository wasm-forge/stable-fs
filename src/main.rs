use bitflags::bitflags;

// The unique identifier of a node, which can be a file or a directory.
// Also known as inode in WASI and other file systems.
type Node = u64;

// Contains metadata of a node.
struct Metadata {
    node: Node,
    file_type: FileType,
    link_count: u32,
    size: u64,
    times: Times,
}

// The type of a node.
enum FileType {
    Directory,
    RegularFile,
    SymbolicLink,
}

// The time stats of a node.
struct Times {
    accessed: u64,
    modified: u64,
    changed: u64,
}

// An index of a file chunk.
type FileChunkIndex = u32;

// A file consists of multiple file chunks.
struct FileChunk {
    bytes: [u8; 4096],
}

// The name of a file or a directory. Most operating systems limit the max file
// name length to 255.
struct Name {
    length: u8,
    bytes: [u8; 255],
}

// An index of a directory entry.
type DirEntryIndex = u32;

// A directory contains a list of directory entries.
// Each entry describes a name of a file or a directory.
struct DirEntry {
    node: Node,
    name: Name,
}

// A descriptor corresponds to an opened file or an opened directory.
// Also known as Fd in WASI and other file systems.
struct Descriptor {
    node: Node,
    cursor: u64,
    file_type: FileType,
    flags: DescriptorFlags,
}

bitflags! {
    pub struct DescriptorFlags: u32 {
        const APPEND = 1;
        const DSYNC = 2;
        const NONBLOCK = 4;
        const RSYNC = 8;
        const SYNC = 16;
    }
}

// Abstraction of the underlying storage layer.
// This will be implemented using stable-structures.
// This allows test implementations using standard Rust in-memory
// data-structures.
trait Storage {
    fn get_metadata(node: Node, metadata: &mut Metadata);
    fn put_metadata(node: Node, metadata: &Metadata);

    fn get_direntry(node: Node, index: DirEntryIndex, entry: &mut DirEntry);
    fn put_direntry(node: Node, index: DirEntryIndex, entry: &DirEntry);

    fn get_filechunk(node: Node, index: FileChunkIndex, offset: u32, buf: &mut [u8]);
    fn put_filechunk(node: Node, index: FileChunkIndex, offset: u32, buf: &[u8]);
}

fn main() {
    println!("Hello, world!");
}



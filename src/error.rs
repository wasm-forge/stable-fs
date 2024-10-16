#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    NotFound,
    InvalidOffset,
    InvalidFileType,
    InvalidFileName,
    InvalidFileDescriptor,
    InvalidBufferLength,
    InvalidOpenFlags,
    InvalidFdFlags,
    FileAlreadyExists,
    MemoryFileIsNotMounted,
    MemoryFileIsMountedAlready,
    NameTooLong,
    DirectoryNotEmpty,
    ExpectedToRemoveFile,
    ExpectedToRemoveDirectory,
    CannotRemoveOpenedNode,
    CannotRemoveMountedMemoryFile,
    IncompatibleChunkSize,
    InvalidMagicMarker,
}

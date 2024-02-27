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
    NameTooLong,
    DirectoryNotEmpty,
    ExpectedToRemoveFile,
    ExpectedToRemoveDirectory,
    CannotRemoveOpenedNode,
}

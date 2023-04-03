#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    NotFound,
    InvalidOffset,
    InvalidFileType,
    InvalidFileDescriptor,
    InvalidBufferLength,
    InvalidOpenFlags,
    InvalidFdFlags,
    FileAlreadyExists,
    NameTooLong,
}

use crate::{
    error::Error,
    runtime::types::{FdFlags, FdStat, Whence},
    storage::{
        types::{FileSize, FileType, Node},
        Storage,
    },
};

#[derive(Clone, Debug)]
pub struct File {
    pub node: Node,
    pub cursor: FileSize,
    pub stat: FdStat,
}

impl File {
    // Create new file entry.
    pub fn new(node: Node, stat: FdStat, storage: &dyn Storage) -> Result<Self, Error> {
        let metadata = storage.get_metadata(node)?;
        let file_type = metadata.file_type;
        match file_type {
            FileType::RegularFile => {}
            FileType::Directory => {
                unreachable!("Unexpected file type, expected a regular file.");
            }
            FileType::SymbolicLink => unimplemented!("Symbolic links are not implemented yet"),
        };
        let cursor = if stat.flags.contains(FdFlags::APPEND) {
            metadata.size
        } else {
            0
        };
        Ok(Self { node, cursor, stat })
    }

    // Seek a position in a file for reading or writing.
    pub fn seek(
        &mut self,
        delta: i64,
        whence: Whence,
        storage: &dyn Storage,
    ) -> Result<FileSize, Error> {
        let size = storage.get_metadata(self.node)?.size;
        let position = match whence {
            Whence::SET => {
                if delta < 0 {
                    return Err(Error::InvalidArgument);
                }
                delta as FileSize
            }
            Whence::CUR => {
                let back = if delta < 0 {
                    (-delta).try_into().map_err(|_| Error::InvalidSeek)?
                } else {
                    0
                };
                let fwd = if delta >= 0 { delta as FileSize } else { 0 };
                if back > self.cursor {
                    return Err(Error::InvalidSeek);
                }
                self.cursor + fwd - back
            }
            Whence::END => {
                let back: FileSize = (-delta).try_into().map_err(|_| Error::InvalidSeek)?;
                if back > size {
                    return Err(Error::InvalidSeek);
                }
                size - back
            }
        };
        self.cursor = position;
        Ok(self.cursor)
    }

    // Get the file's current cursor position.
    pub fn tell(&self) -> FileSize {
        self.cursor
    }

    // Read file at the given cursor position, the cursor position will be updated after reading.
    pub fn read_with_cursor(
        &mut self,
        buf: &mut [u8],
        storage: &mut dyn Storage,
    ) -> Result<FileSize, Error> {
        let read_size = self.read_with_offset(self.cursor, buf, storage)?;
        self.cursor += read_size;
        Ok(read_size)
    }

    // Write file at the current file cursor, the cursor position will be updated after reading.
    pub fn write_with_cursor(
        &mut self,
        buf: &[u8],
        storage: &mut dyn Storage,
    ) -> Result<FileSize, Error> {
        let written_size = self.write_with_offset(self.cursor, buf, storage)?;
        self.cursor += written_size;
        Ok(written_size)
    }

    // Read file at the current file cursor, the cursor position will NOT be updated after reading.
    pub fn read_with_offset(
        &self,
        offset: FileSize,
        buf: &mut [u8],
        storage: &mut dyn Storage,
    ) -> Result<FileSize, Error> {
        if buf.is_empty() {
            return Ok(0 as FileSize);
        }

        let read_size = storage.read(self.node, offset, buf)?;

        Ok(read_size as FileSize)
    }

    // Write file at the current file cursor, the cursor position will NOT be updated after reading.
    pub fn write_with_offset(
        &self,
        offset: FileSize,
        buf: &[u8],
        storage: &mut dyn Storage,
    ) -> Result<FileSize, Error> {
        storage.write(self.node, offset, buf)
    }

    // Truncate file to 0 size.
    pub fn truncate(&self, storage: &mut dyn Storage) -> Result<(), Error> {
        let mut metadata = storage.get_metadata(self.node)?;
        metadata.size = 0;
        storage.put_metadata(self.node, &metadata)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        fs::OpenFlags,
        test_utils::{test_stable_fs_v2, test_fs_setups},
    };

    use super::*;

    #[test]
    fn seek_and_tell() {
        let mut fs = test_stable_fs_v2();
        let fd = fs
            .create_open_file(fs.root_fd(), "test", FdStat::default(), 0)
            .unwrap();

        let mut file = fs.get_test_file(fd);
        let storage = fs.get_test_storage();

        file.write_with_offset(0, &[0; 1000], storage).unwrap();

        assert_eq!(file.tell(), 0);
        let pos = file.seek(10, Whence::CUR, storage).unwrap();
        assert_eq!(pos, 10);
        assert_eq!(file.tell(), 10);

        let pos = file.seek(-9, Whence::CUR, storage).unwrap();
        assert_eq!(pos, 1);
        assert_eq!(file.tell(), 1);

        let err = file.seek(-2, Whence::CUR, storage).unwrap_err();
        assert_eq!(err, Error::InvalidSeek);
        assert_eq!(file.tell(), 1);

        let pos = file.seek(0, Whence::END, storage).unwrap();
        assert_eq!(pos, 1000);
        assert_eq!(file.tell(), 1000);

        let pos = file.seek(500, Whence::SET, storage).unwrap();
        assert_eq!(pos, 500);
        assert_eq!(file.tell(), 500);

        let err = file.seek(-1, Whence::SET, storage).unwrap_err();
        assert_eq!(err, Error::InvalidArgument);
        assert_eq!(file.tell(), 500);

        let pos = file.seek(1001, Whence::SET, storage).unwrap();
        assert_eq!(pos, 1001);
        assert_eq!(file.tell(), 1001);
    }

    #[test]
    fn read_and_write_cursor() {
        let mut fs = test_stable_fs_v2();
        let fd = fs
            .create_open_file(fs.root_fd(), "test", FdStat::default(), 0)
            .unwrap();

        let mut file = fs.get_test_file(fd);
        let storage = fs.get_test_storage();

        for i in 0..1000 {
            let buf = [(i % 256) as u8; 16];
            file.write_with_cursor(&buf, storage).unwrap();
        }
        file.seek(-1000 * 16, Whence::END, storage).unwrap();
        for i in 0..1000 {
            let mut buf = [0; 16];
            file.read_with_cursor(&mut buf, storage).unwrap();
            let expected = [(i % 256) as u8; 16];
            assert_eq!(buf, expected);
        }
    }

    #[test]
    fn read_and_write_offset() {
        let mut fs = test_stable_fs_v2();
        let fd = fs
            .create_open_file(fs.root_fd(), "test", FdStat::default(), 0)
            .unwrap();

        let mut file = fs.get_test_file(fd);
        let storage = fs.get_test_storage();

        for i in 0..1000 {
            let buf = [(i % 256) as u8; 16];
            file.write_with_offset(i * 16, &buf, storage).unwrap();
        }

        file.seek(-1000 * 16, Whence::END, storage).unwrap();
        for i in 0..1000 {
            let mut buf = [0; 16];
            file.read_with_offset(i * 16, &mut buf, storage).unwrap();
            let expected = [(i % 256) as u8; 16];
            assert_eq!(buf, expected);
        }
    }

    #[test]
    fn read_and_write_small_and_big_buffer() {
        let mut fs = test_stable_fs_v2();
        let fd = fs
            .create_open_file(fs.root_fd(), "test", FdStat::default(), 0)
            .unwrap();

        let file = fs.get_test_file(fd);
        let storage = fs.get_test_storage();

        for i in 0..1000 {
            let buf = [(i % 256) as u8; 10];
            file.write_with_offset(i * 16, &buf, storage).unwrap();
        }

        for i in 0..1000 {
            let mut buf1 = [0; 13];
            let mut buf2 = [0; 5000];
            let mut buf3 = [0; 15000];

            let r1 = file.read_with_offset(i * 17, &mut buf1, storage).unwrap() as usize;
            let r2 = file.read_with_offset(i * 17, &mut buf2, storage).unwrap() as usize;
            let _r3 = file.read_with_offset(i * 17, &mut buf3, storage).unwrap() as usize;

            assert_eq!(buf1[..r1], buf2[..r1]);
            assert_eq!(buf2[..r2], buf3[..r2]);
        }
    }

    #[test]
    fn read_and_write_offset_chunk() {
        for mut fs in [test_stable_fs_v2()] {
            //test_fs_setups("test") {
            let fd = fs
                .open(
                    fs.root_fd(),
                    "test",
                    FdStat::default(),
                    OpenFlags::CREATE,
                    0,
                )
                .unwrap();

            let mut file = fs.get_test_file(fd);
            let storage = fs.get_test_storage();

            for i in 0..1000 {
                let buf = [(i % 256) as u8; 16];
                file.write_with_offset(i * 16, &buf, storage).unwrap();
            }

            file.seek(-1000 * 16, Whence::END, storage).unwrap();

            for i in 0..1000 {
                let mut buf = [0; 16];
                file.read_with_offset(i * 16, &mut buf, storage).unwrap();

                let expected = [(i % 256) as u8; 16];
                assert_eq!(buf, expected);
            }
        }
    }

    #[test]
    fn read_and_write_offset_vs_range() {
        for mut fs in test_fs_setups("test") {
            let fd = fs
                .open(
                    fs.root_fd(),
                    "test",
                    FdStat::default(),
                    OpenFlags::CREATE,
                    0,
                )
                .unwrap();

            let file = fs.get_test_file(fd);
            let storage = fs.get_test_storage();

            for i in 0..1000 {
                let buf = [(i % 256) as u8; 16];
                file.write_with_offset(i * 16, &buf, storage).unwrap();
            }

            for i in 0..1000 {
                let mut buf1 = [0; 13];
                let len1 = file.read_with_offset(i * 16, &mut buf1, storage).unwrap();

                let mut buf2 = [0; 13];
                let len2 = file.read_with_offset(i * 16, &mut buf2, storage).unwrap();

                assert_eq!(buf1, buf2);
                assert_eq!(len1, len2);
            }

            for i in 0..2050 {
                let mut buf1 = [0; 5003];
                let len1 = file.read_with_offset(i * 13, &mut buf1, storage).unwrap();

                let mut buf2 = [0; 5003];
                let len2 = file.read_with_offset(i * 13, &mut buf2, storage).unwrap();

                assert_eq!(buf1, buf2);
                assert_eq!(len1, len2);
            }
        }
    }
}

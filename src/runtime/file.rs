use crate::{
    error::Error,
    runtime::types::{FdFlags, FdStat, Whence},
    storage::{
        types::{FileChunkIndex, FileSize, FileType, Node, FILE_CHUNK_SIZE},
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
                    return Err(Error::InvalidOffset);
                }
                delta as FileSize
            }
            Whence::CUR => {
                let back = if delta < 0 {
                    (-delta).try_into().map_err(|_| Error::InvalidOffset)?
                } else {
                    0
                };
                let fwd = if delta >= 0 { delta as FileSize } else { 0 };
                if back > self.cursor {
                    return Err(Error::InvalidOffset);
                }
                self.cursor + fwd - back
            }
            Whence::END => {
                let back: FileSize = (-delta).try_into().map_err(|_| Error::InvalidOffset)?;
                if back > size {
                    return Err(Error::InvalidOffset);
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

    // Read file at the given curson position, the cursor position will be updated after reading.
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

        self.read_with_offset_range(offset, buf, storage)
/* 
        if buf.is_empty() {
            return Ok(0 as FileSize);
        }

        let file_size = storage.get_metadata(self.node)?.size;
        let end = (offset + buf.len() as FileSize).min(file_size);
        let chunk_infos = get_chunk_infos(offset, end);

        let mut read_size = 0;

        for chunk in chunk_infos.into_iter() {
            storage.read_filechunk(
                self.node,
                chunk.index,
                chunk.offset,
                &mut buf[read_size..read_size + chunk.len as usize],
            )?;
            read_size += chunk.len as usize;
        }
        Ok(read_size as FileSize)

        */
    }

    // Read file at the current file cursor, the cursor position will NOT be updated after reading.
    fn read_with_offset_range(
        &self,
        offset: FileSize,
        buf: &mut [u8],
        storage: &mut dyn Storage,
    ) -> Result<FileSize, Error> {
        if buf.is_empty() {
            return Ok(0 as FileSize);
        }

        let file_size = storage.get_metadata(self.node)?.size;
        
        let read_size = storage.read_range(
            self.node,
            offset,
            file_size, 
            buf,
        )?;

        Ok(read_size as FileSize)
    }

    // Write file at the current file cursor, the cursor position will NOT be updated after reading.
    pub fn write_with_offset(
        &self,
        offset: FileSize,
        buf: &[u8],
        storage: &mut dyn Storage,
    ) -> Result<FileSize, Error> {

        let mut metadata = storage.get_metadata(self.node)?;
        let end = offset + buf.len() as FileSize;
        let chunk_infos = get_chunk_infos(offset, end);
        let mut written_size = 0;
        for chunk in chunk_infos.into_iter() {
            storage.write_filechunk(
                self.node,
                chunk.index,
                chunk.offset,
                &buf[written_size..written_size + chunk.len as usize],
            );
            written_size += chunk.len as usize;
        }

        if end > metadata.size {
            metadata.size = end;
            storage.put_metadata(self.node, metadata)
        }
        
        Ok(written_size as FileSize)
    }

    // Truncate file to 0 size.
    pub fn truncate(&self, storage: &mut dyn Storage) -> Result<(), Error> {
        let mut metadata = storage.get_metadata(self.node)?;
        metadata.size = 0;
        storage.put_metadata(self.node, metadata);
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ChunkHandle {
    index: FileChunkIndex,
    offset: FileSize,
    len: FileSize,
}

fn offset_to_file_chunk_index(offset: FileSize) -> FileChunkIndex {
    (offset / FILE_CHUNK_SIZE as FileSize) as FileChunkIndex
}

fn file_chunk_index_to_offset(index: FileChunkIndex) -> FileSize {
    index as FileSize * FILE_CHUNK_SIZE as FileSize
}

fn get_chunk_infos(start: FileSize, end: FileSize) -> Vec<ChunkHandle> {
    let mut result = vec![];
    let start_index = offset_to_file_chunk_index(start);
    let end_index = offset_to_file_chunk_index(end);
    for index in start_index..=end_index {
        let start_of_chunk = file_chunk_index_to_offset(index);
        assert!(start_of_chunk <= end);
        let start_in_chunk = start_of_chunk.max(start) - start_of_chunk;
        let end_in_chunk = (start_of_chunk + FILE_CHUNK_SIZE as FileSize).min(end) - start_of_chunk;
        if start_in_chunk < end_in_chunk {
            result.push(ChunkHandle {
                index,
                offset: start_in_chunk,
                len: end_in_chunk - start_in_chunk,
            });
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{test_fs, test_fs_transient};

    use super::*;

    #[test]
    fn get_chunk_infos_parital() {
        let chunks = get_chunk_infos(
            FILE_CHUNK_SIZE as FileSize - 1,
            2 * FILE_CHUNK_SIZE as FileSize + 1,
        );
        assert_eq!(
            chunks[0],
            ChunkHandle {
                index: 0,
                offset: FILE_CHUNK_SIZE as FileSize - 1,
                len: 1
            }
        );
        assert_eq!(
            chunks[1],
            ChunkHandle {
                index: 1,
                offset: 0,
                len: FILE_CHUNK_SIZE as FileSize,
            }
        );

        assert_eq!(
            chunks[2],
            ChunkHandle {
                index: 2,
                offset: 0,
                len: 1,
            }
        );
    }

    #[test]
    fn get_chunk_infos_full() {
        let chunks = get_chunk_infos(0, 10 * FILE_CHUNK_SIZE as FileSize);
        #[allow(clippy::needless_range_loop)]
        for i in 0..10 {
            assert_eq!(
                chunks[i],
                ChunkHandle {
                    index: i as FileChunkIndex,
                    offset: 0,
                    len: FILE_CHUNK_SIZE as FileSize,
                }
            );
        }
    }

    #[test]
    fn seek_and_tell() {
        let mut fs = test_fs();
        let fd = fs
            .create_file(fs.root_fd(), "test", FdStat::default(), 0)
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
        assert_eq!(err, Error::InvalidOffset);
        assert_eq!(file.tell(), 1);

        let pos = file.seek(0, Whence::END, storage).unwrap();
        assert_eq!(pos, 1000);
        assert_eq!(file.tell(), 1000);

        let pos = file.seek(500, Whence::SET, storage).unwrap();
        assert_eq!(pos, 500);
        assert_eq!(file.tell(), 500);

        let err = file.seek(-1, Whence::SET, storage).unwrap_err();
        assert_eq!(err, Error::InvalidOffset);
        assert_eq!(file.tell(), 500);

        let pos = file.seek(1001, Whence::SET, storage).unwrap();
        assert_eq!(pos, 1001);
        assert_eq!(file.tell(), 1001);
    }

    #[test]
    fn read_and_write_cursor() {
        let mut fs = test_fs();
        let fd = fs
            .create_file(fs.root_fd(), "test", FdStat::default(), 0)
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
        let mut fs = test_fs();
        let fd = fs
            .create_file(fs.root_fd(), "test", FdStat::default(), 0)
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
    fn read_and_write_offset_range() {
        let mut fs = test_fs();
        let fd = fs
            .create_file(fs.root_fd(), "test", FdStat::default(), 0)
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
            file.read_with_offset_range(i * 16, &mut buf, storage).unwrap();

            let expected = [(i % 256) as u8; 16];
            assert_eq!(buf, expected);
        }
    }

    #[test]
    fn read_and_write_offset_vs_range() {
        let mut fs = test_fs();
        let fd = fs
            .create_file(fs.root_fd(), "test", FdStat::default(), 0)
            .unwrap();

        let file = fs.get_test_file(fd);
        let storage = fs.get_test_storage();

        for i in 0..1000 {
            let buf = [(i % 256) as u8; 16];
            file.write_with_offset(i * 16, &buf, storage).unwrap();
        }
        
        for i in 0..1000 {
            let mut buf1 = [0; 13];
            let len1 = file.read_with_offset_range(i * 16, &mut buf1, storage).unwrap();

            let mut buf2 = [0; 13];
            let len2 = file.read_with_offset(i * 16, &mut buf2, storage).unwrap();

            assert_eq!(buf1, buf2);
            assert_eq!(len1, len2);
        }
        
        for i in 0..2050 {
            let mut buf1 = [0; 5003];
            let len1 = file.read_with_offset_range(i * 13, &mut buf1, storage).unwrap();

            let mut buf2 = [0; 5003];
            let len2 = file.read_with_offset(i * 13, &mut buf2, storage).unwrap();

            assert_eq!(buf1, buf2);
            assert_eq!(len1, len2);
        }
    }    


    #[test]
    fn read_and_write_offset_vs_range_transient() {
        let mut fs = test_fs_transient();
        let fd = fs
            .create_file(fs.root_fd(), "test", FdStat::default(), 0)
            .unwrap();

        let file = fs.get_test_file(fd);
        let storage = fs.get_test_storage();

        for i in 0..1000 {
            let buf = [(i % 256) as u8; 16];
            file.write_with_offset(i * 16, &buf, storage).unwrap();
        }
        
        for i in 0..1000 {
            let mut buf1 = [0; 13];
            let len1 = file.read_with_offset_range(i * 16, &mut buf1, storage).unwrap();

            let mut buf2 = [0; 13];
            let len2 = file.read_with_offset(i * 16, &mut buf2, storage).unwrap();

            assert_eq!(buf1, buf2);
            assert_eq!(len1, len2);
        }
        
        for i in 0..2050 {
            let mut buf1 = [0; 5003];
            let len1 = file.read_with_offset_range(i * 13, &mut buf1, storage).unwrap();

            let mut buf2 = [0; 5003];
            let len2 = file.read_with_offset(i * 13, &mut buf2, storage).unwrap();

            assert_eq!(buf1, buf2);
            assert_eq!(len1, len2);
        }
    }    
}

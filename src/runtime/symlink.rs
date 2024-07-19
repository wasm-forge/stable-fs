use crate::{
    error::Error,
    runtime::types::{FdFlags, FdStat},
    storage::{
        types::{FileChunkIndex, FileSize, FileType, Node, FILE_CHUNK_SIZE},
        Storage,
    },
};

#[derive(Clone, Debug)]
pub struct Symlink {
    pub node: Node,
    pub stat: FdStat,
}

impl Symlink {
    // Create new file entry.
    pub fn new(node: Node, stat: FdStat, storage: &dyn Storage) -> Result<Self, Error> {
        let metadata = storage.get_metadata(node)?;
        let file_type = metadata.file_type;
        match file_type {
            FileType::SymbolicLink => {}
            _ => {
                unreachable!("Unexpected file type, expected a symbolic link.");
            }
        };

        Ok(Self { node, stat })
    }

    // Read symlink content into buf.
    pub fn read_content(
        &self,        
        buf: &mut [u8],
        storage: &mut dyn Storage,
    ) -> Result<FileSize, Error> {
        if buf.is_empty() {
            return Ok(0 as FileSize);
        }

        let file_size = storage.get_metadata(self.node)?.size;
        
        let end = (buf.len() as FileSize).min(file_size);

        let chunk_infos = get_chunk_infos(0, end);

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
    }

    // Write symlink content from a buffer.
    pub fn write_content(
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
    use crate::test_utils::test_fs;

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

}

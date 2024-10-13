use ic_cdk::api::stable::WASM_PAGE_SIZE_IN_BYTES;
use ic_stable_structures::Memory;

use crate::{
    error::Error,
    filename_cache::FilenameCache,
    storage::{
        types::{
            ChunkHandle, DirEntry, DirEntryIndex, FileChunkIndex, FileName, FileSize, FileType,
            Metadata, Node, Times,
        },
        Storage,
    },
};
use std::collections::BTreeMap;
#[derive(Debug)]
struct EntryFindResult {
    node: Node,
    parent_dir: Node,
    entry_index: DirEntryIndex,
    prev_entry: Option<DirEntryIndex>,
    next_entry: Option<DirEntryIndex>,
}

fn find_node_with_index(
    parent_dir_node: Node,
    path: &str,
    storage: &dyn Storage,
) -> Result<EntryFindResult, Error> {
    let parts = path.split('/');

    let mut parent_dir_node = parent_dir_node;
    let mut cur_node = parent_dir_node;
    let mut cur_entry_index = 0;
    let mut prev_entry_index = None;
    let mut next_entry_index = None;

    for part in parts {
        if part.is_empty() || part == "." {
            continue;
        }

        if part == ".." {
            return Err(Error::InvalidFileName);
        }

        parent_dir_node = cur_node;
        cur_entry_index = find_entry_index(parent_dir_node, part.as_bytes(), storage)?;
        let entry = storage.get_direntry(cur_node, cur_entry_index)?;

        cur_node = entry.node;
        prev_entry_index = entry.prev_entry;
        next_entry_index = entry.next_entry;
    }

    Ok(EntryFindResult {
        node: cur_node,
        parent_dir: parent_dir_node,
        entry_index: cur_entry_index,
        prev_entry: prev_entry_index,
        next_entry: next_entry_index,
    })
}

// Find directory entry node by its name, paths containing separator '/' are allowed and processed.
pub fn find_node(
    parent_dir_node: Node,
    path: &str,
    names_cache: &mut FilenameCache,
    storage: &dyn Storage,
) -> Result<Node, Error> {
    let filename = path.to_string();
    let key = (parent_dir_node, filename);

    if let Some(node) = names_cache.get(&key) {
        return Ok(node);
    }

    let find_result = find_node_with_index(parent_dir_node, path, storage);

    match find_result {
        Ok(result) => {
            names_cache.add(key, result.node);

            Ok(result.node)
        }
        Err(e) => {
            Err(e)
        }
    }
}

// Create a hard link to an existing node
pub fn create_hard_link(
    parent_dir_node: Node,
    new_path: &str,
    src_dir_node: Node,
    src_path: &str,
    is_renaming: bool,
    names_cache: &mut FilenameCache,
    storage: &mut dyn Storage,
) -> Result<(), Error> {
    // Check if the node exists already.
    let found = find_node(parent_dir_node, new_path, names_cache, storage);
    match found {
        Err(Error::NotFound) => {}
        Ok(_) => return Err(Error::FileAlreadyExists),
        Err(err) => return Err(err),
    }

    // Get the node and metadata, the node must exist in the source folder.
    let node: Node = find_node(src_dir_node, src_path, names_cache, storage)?;
    let mut metadata = storage.get_metadata(node)?;
    let ctime = metadata.times.created;

    //
    let (dir_node, leaf_name) = create_path(parent_dir_node, new_path, None, ctime, storage)?;

    // only allow creating a hardlink on a folder if it is a part of renaming and another link will be removed
    if !is_renaming && metadata.file_type == FileType::Directory {
        return Err(Error::InvalidFileType);
    }

    metadata.link_count += 1;
    storage.put_metadata(node, metadata);

    add_dir_entry(dir_node, node, leaf_name.as_bytes(), storage)?;

    Ok(())
}

pub fn create_dir_entry(
    parent_dir_node: Node,
    entry_name: &[u8],
    entry_type: FileType,
    storage: &mut dyn Storage,
    ctime: u64,
) -> Result<Node, Error> {
    if entry_type != FileType::Directory && entry_type != FileType::RegularFile {
        return Err(Error::InvalidFileType);
    }

    let node = storage.new_node();

    let chunk_type = if entry_type == FileType::RegularFile {
        Some(storage.chunk_type())
    } else {
        None
    };

    storage.put_metadata(
        node,
        Metadata {
            node,
            file_type: entry_type,
            link_count: 1,
            size: 0,
            times: Times {
                accessed: ctime,
                modified: ctime,
                created: ctime,
            },
            first_dir_entry: None,
            last_dir_entry: None,
            chunk_type,
        },
    );

    add_dir_entry(parent_dir_node, node, entry_name, storage)?;

    Ok(node)
}

// create whole path if it doesn't exist
// parent_node        parent folder node
// path               full path
// leaf_type          file type of the last path elemen (RegularFile or Directory)
// storage            file system storage
// ctime              creation time to be used
// returns the node of the last created folder part, return error if creation failed
pub fn create_path<'a>(
    parent_node: Node,
    path: &'a str,
    leaf_type: Option<FileType>,
    ctime: u64,
    storage: &mut dyn Storage,
) -> Result<(Node, &'a str), Error> {
    let parts = path.split('/');

    let mut parent_node = parent_node;
    let mut cur_node = parent_node;

    let mut needs_folder_creation = false;
    let mut last_name = path;
    let mut last_file_type = FileType::Directory;

    for part in parts {
        if part.is_empty() || part == "." {
            continue;
        }

        if part == ".." {
            return Err(Error::InvalidFileName);
        }

        if needs_folder_creation {
            // last_name contains the folder name to create
            if last_file_type != FileType::Directory {
                return Err(Error::InvalidFileType);
            }

            // create new folder
            cur_node = create_dir_entry(
                parent_node,
                last_name.as_bytes(),
                FileType::Directory,
                storage,
                ctime,
            )?;
        }

        last_name = part;
        parent_node = cur_node;

        let path_element = part.as_bytes();

        if !needs_folder_creation {
            let entry_index = find_entry_index(parent_node, path_element, storage);

            match entry_index {
                Ok(entry_index) => {
                    let entry = storage.get_direntry(cur_node, entry_index)?;
                    cur_node = entry.node;

                    let meta = storage.get_metadata(cur_node)?;

                    last_file_type = meta.file_type;
                }
                Err(Error::NotFound) => {
                    needs_folder_creation = true;
                }
                Err(x) => {
                    return Err(x);
                }
            }
        }
    }

    if needs_folder_creation {
        // last_name contains the folder name to create
        if last_file_type != FileType::Directory {
            return Err(Error::InvalidFileType);
        }

        if let Some(leaf_type) = leaf_type {
            // create new folder
            cur_node =
                create_dir_entry(parent_node, last_name.as_bytes(), leaf_type, storage, ctime)?;
        }
    }

    Ok((cur_node, last_name))
}

// Iterate directory entries, find entry index by folder or file name.
pub fn find_entry_index(
    dir_entry_node: Node,
    path_element: &[u8],
    storage: &dyn Storage,
) -> Result<DirEntryIndex, Error> {
    let mut next_index = storage.get_metadata(dir_entry_node)?.first_dir_entry;

    while let Some(index) = next_index {
        if let Ok(dir_entry) = storage.get_direntry(dir_entry_node, index) {
            if dir_entry.name.length as usize == path_element.len()
                && &dir_entry.name.bytes[0..path_element.len()] == path_element
            {
                return Ok(index);
            }

            next_index = dir_entry.next_entry;
        }
    }

    Err(Error::NotFound)
}

//  Add new directory entry
pub fn add_dir_entry(
    parent_dir_node: Node,
    new_node: Node,
    entry_name: &[u8],
    storage: &mut dyn Storage,
) -> Result<(), Error> {
    let mut metadata = storage.get_metadata(parent_dir_node)?;

    let name = FileName::new(entry_name)?;

    // start numbering with 1
    let new_entry_index: DirEntryIndex = metadata.last_dir_entry.unwrap_or(0) + 1;

    storage.put_direntry(
        parent_dir_node,
        new_entry_index,
        DirEntry {
            node: new_node,
            name,
            next_entry: None,
            prev_entry: metadata.last_dir_entry,
        },
    );

    // update previous last entry
    if let Some(prev_dir_entry_index) = metadata.last_dir_entry {
        let mut prev_dir_entry = storage.get_direntry(parent_dir_node, prev_dir_entry_index)?;

        prev_dir_entry.next_entry = Some(new_entry_index);
        storage.put_direntry(parent_dir_node, prev_dir_entry_index, prev_dir_entry)
    }

    // update metadata
    metadata.last_dir_entry = Some(new_entry_index);

    if metadata.first_dir_entry.is_none() {
        metadata.first_dir_entry = Some(new_entry_index);
    }
    metadata.size += 1;

    storage.put_metadata(parent_dir_node, metadata);

    Ok(())
}

/// Remove the directory entry from the current directory by entry name.
///
/// parent_dir_node Parent directory
/// path            The name of the entry to delete
/// expect_dir      If true, the directory is deleted. If false - the file is deleted. If the expected entry type does not match with the actual entry - an error is returned.
/// node_refcount   A map of nodes to check if the file being deleted is opened by multiple file descriptors. Deleting an entry referenced by multiple file descriptors is not allowed and will result in an error.
/// storage         The reference to the actual storage implementation
pub fn rm_dir_entry(
    parent_dir_node: Node,
    path: &str,
    expect_dir: Option<bool>,
    node_refcount: &BTreeMap<Node, usize>,
    names_cache: &mut FilenameCache,
    storage: &mut dyn Storage,
) -> Result<(Node, Metadata), Error> {
    // clear cache
    // todo: clear concrete node, not the whole cache
    names_cache.clear();

    let find_result = find_node_with_index(parent_dir_node, path, storage)?;

    let removed_dir_entry_node = find_result.node;

    if storage.is_mounted(removed_dir_entry_node) {
        return Err(Error::CannotRemoveMountedMemoryFile);
    }

    let parent_dir_node = find_result.parent_dir;
    let removed_entry_index = find_result.entry_index;
    let removed_dir_entry_prev_entry = find_result.prev_entry;
    let removed_dir_entry_next_entry = find_result.next_entry;

    let mut removed_metadata = storage.get_metadata(removed_dir_entry_node)?;

    match removed_metadata.file_type {
        FileType::Directory => {
            if expect_dir == Some(false) {
                return Err(Error::ExpectedToRemoveFile);
            }

            if removed_metadata.link_count == 1 && removed_metadata.size > 0 {
                return Err(Error::DirectoryNotEmpty);
            }
        }
        FileType::RegularFile | FileType::SymbolicLink => {
            if expect_dir == Some(true) {
                return Err(Error::ExpectedToRemoveDirectory);
            }
        }
    }

    if let Some(refcount) = node_refcount.get(&removed_metadata.node) {
        if *refcount > 0 && removed_metadata.link_count == 1 {
            return Err(Error::CannotRemoveOpenedNode);
        }
    }

    // update previous entry
    if let Some(prev_dir_entry_index) = removed_dir_entry_prev_entry {
        let mut prev_dir_entry = storage.get_direntry(parent_dir_node, prev_dir_entry_index)?;
        prev_dir_entry.next_entry = removed_dir_entry_next_entry;
        storage.put_direntry(parent_dir_node, prev_dir_entry_index, prev_dir_entry)
    }

    // update next entry
    if let Some(next_dir_entry_index) = removed_dir_entry_next_entry {
        let mut next_dir_entry = storage.get_direntry(parent_dir_node, next_dir_entry_index)?;
        next_dir_entry.prev_entry = removed_dir_entry_prev_entry;
        storage.put_direntry(parent_dir_node, next_dir_entry_index, next_dir_entry)
    }

    let mut parent_dir_metadata = storage.get_metadata(parent_dir_node)?;

    // update parent metadata when the last directory entry is removed
    if Some(removed_entry_index) == parent_dir_metadata.last_dir_entry {
        parent_dir_metadata.last_dir_entry = removed_dir_entry_prev_entry;
    }

    // update parent metadata when the first directory entry is removed
    if Some(removed_entry_index) == parent_dir_metadata.first_dir_entry {
        parent_dir_metadata.first_dir_entry = removed_dir_entry_next_entry;
    }

    // dir entry size is reduced by one
    parent_dir_metadata.size -= 1;

    // update parent metadata
    storage.put_metadata(parent_dir_node, parent_dir_metadata);

    // remove the entry
    storage.rm_direntry(parent_dir_node, removed_entry_index);

    removed_metadata.link_count -= 1;
    storage.put_metadata(removed_metadata.node, removed_metadata.clone());

    Ok((removed_dir_entry_node, removed_metadata))
}

#[inline]
pub fn grow_memory(memory: &dyn Memory, max_address: FileSize) {
    let pages_required = (max_address + WASM_PAGE_SIZE_IN_BYTES - 1) / WASM_PAGE_SIZE_IN_BYTES;

    let cur_pages = memory.size();

    if cur_pages < pages_required {
        memory.grow(pages_required - cur_pages);
    }
}

#[inline]
pub fn read_obj<T: Sized>(memory: &dyn Memory, address: u64, obj: &mut T) {
    let obj_size = std::mem::size_of::<T>();

    let obj_bytes = unsafe { std::slice::from_raw_parts_mut(obj as *mut T as *mut u8, obj_size) };

    memory.read(address, obj_bytes);
}

#[inline]
pub fn write_obj<T: Sized>(memory: &dyn Memory, address: u64, obj: &T) {
    let obj_size = std::mem::size_of::<T>();

    let obj_bytes = unsafe { std::slice::from_raw_parts(obj as *const T as *const u8, obj_size) };

    grow_memory(memory, address + obj_size as u64);

    memory.write(address, obj_bytes);
}

pub fn offset_to_file_chunk_index(offset: FileSize, chunk_size: usize) -> FileChunkIndex {
    (offset / chunk_size as FileSize) as FileChunkIndex
}

pub fn file_chunk_index_to_offset(index: FileChunkIndex, chunk_size: usize) -> FileSize {
    index as FileSize * chunk_size as FileSize
}

pub fn get_chunk_infos(start: FileSize, end: FileSize, chunk_size: usize) -> Vec<ChunkHandle> {
    let mut result = vec![];
    let start_index = offset_to_file_chunk_index(start, chunk_size);
    let end_index = offset_to_file_chunk_index(end, chunk_size);

    for index in start_index..=end_index {
        let start_of_chunk = file_chunk_index_to_offset(index, chunk_size);

        assert!(start_of_chunk <= end);
        let start_in_chunk = start_of_chunk.max(start) - start_of_chunk;
        let end_in_chunk = (start_of_chunk + chunk_size as FileSize).min(end) - start_of_chunk;
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

    use ic_stable_structures::DefaultMemoryImpl;

    use crate::{
        error::Error,
        filename_cache::FilenameCache,
        runtime::structure_helpers::{create_path, find_node, get_chunk_infos},
        storage::{
            stable::StableStorage,
            types::{ChunkHandle, FileChunkIndex, FileSize, FileType, FILE_CHUNK_SIZE_V1},
            Storage,
        },
    };

    #[test]
    fn get_chunk_infos_parital() {
        let chunks = get_chunk_infos(
            FILE_CHUNK_SIZE_V1 as FileSize - 1,
            2 * FILE_CHUNK_SIZE_V1 as FileSize + 1,
            FILE_CHUNK_SIZE_V1,
        );
        assert_eq!(
            chunks[0],
            ChunkHandle {
                index: 0,
                offset: FILE_CHUNK_SIZE_V1 as FileSize - 1,
                len: 1
            }
        );
        assert_eq!(
            chunks[1],
            ChunkHandle {
                index: 1,
                offset: 0,
                len: FILE_CHUNK_SIZE_V1 as FileSize,
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
        let chunks = get_chunk_infos(0, 10 * FILE_CHUNK_SIZE_V1 as FileSize, FILE_CHUNK_SIZE_V1);
        #[allow(clippy::needless_range_loop)]
        for i in 0..10 {
            assert_eq!(
                chunks[i],
                ChunkHandle {
                    index: i as FileChunkIndex,
                    offset: 0,
                    len: FILE_CHUNK_SIZE_V1 as FileSize,
                }
            );
        }
    }

    #[test]
    fn create_path_with_subfolders() {
        let mut storage_box = Box::new(StableStorage::new(DefaultMemoryImpl::default()));
        let storage = storage_box.as_mut();
        let mut names_cache = FilenameCache::new();

        let root_node = storage.root_node();

        let (test3, _) = create_path(
            root_node,
            "test1/test2/test3",
            Some(FileType::Directory),
            43u64,
            storage,
        )
        .unwrap();
        let (test4, _) = create_path(
            root_node,
            "test1/test2/test4",
            Some(FileType::Directory),
            44u64,
            storage,
        )
        .unwrap();
        let (test6, _) = create_path(
            root_node,
            "test1/test2/test5/test6",
            Some(FileType::Directory),
            45u64,
            storage,
        )
        .unwrap();

        let test1 = find_node(root_node, "test1", &mut names_cache, storage).unwrap();

        let (test7, _) = create_path(
            test1,
            "test2/test4/test7",
            Some(FileType::Directory),
            45u64,
            storage,
        )
        .unwrap();

        let test2_1 = find_node(root_node, "test1/test2", &mut names_cache, storage).unwrap();
        let test2_2 = find_node(test1, "test2", &mut names_cache, storage).unwrap();

        assert_eq!(test2_1, test2_2);

        assert_eq!(
            test3,
            find_node(root_node, "test1/test2/test3", &mut names_cache, storage).unwrap()
        );
        assert_eq!(
            test4,
            find_node(root_node, "test1/test2/test4", &mut names_cache, storage).unwrap()
        );
        assert_eq!(
            test6,
            find_node(
                root_node,
                "test1/test2/test5/test6",
                &mut names_cache,
                storage
            )
            .unwrap()
        );
        assert_eq!(
            test7,
            find_node(
                root_node,
                "test1/test2/test4/test7",
                &mut names_cache,
                storage
            )
            .unwrap()
        );
    }

    #[test]
    fn create_file_with_subfolders() {
        let mut storage_box = Box::new(StableStorage::new(DefaultMemoryImpl::default()));
        let storage = storage_box.as_mut();
        let mut names_cache = FilenameCache::new();

        let root_node = storage.root_node();

        let (test3, _) = create_path(
            root_node,
            "test1/test2/test3.txt",
            Some(FileType::RegularFile),
            43u64,
            storage,
        )
        .unwrap();
        let (test4, _) = create_path(
            root_node,
            "test1/test2/test4",
            Some(FileType::Directory),
            44u64,
            storage,
        )
        .unwrap();
        let (test6, _) = create_path(
            root_node,
            "test1/test2/test5/test6.txt",
            Some(FileType::RegularFile),
            45u64,
            storage,
        )
        .unwrap();

        let test1 = find_node(root_node, "test1", &mut names_cache, storage).unwrap();

        let (test7, _) = create_path(
            test1,
            "test2/test4/test7.txt",
            Some(FileType::RegularFile),
            45u64,
            storage,
        )
        .unwrap();

        let test2_1 = find_node(root_node, "test1/test2", &mut names_cache, storage).unwrap();
        let test2_2 = find_node(test1, "test2", &mut names_cache, storage).unwrap();

        assert_eq!(test2_1, test2_2);

        assert_eq!(
            test3,
            find_node(
                root_node,
                "test1/test2/test3.txt",
                &mut names_cache,
                storage
            )
            .unwrap()
        );
        assert_eq!(
            test4,
            find_node(root_node, "test1/test2/test4", &mut names_cache, storage).unwrap()
        );
        assert_eq!(
            test6,
            find_node(
                root_node,
                "test1/test2/test5/test6.txt",
                &mut names_cache,
                storage
            )
            .unwrap()
        );
        assert_eq!(
            test7,
            find_node(
                root_node,
                "test1/test2/test4/test7.txt",
                &mut names_cache,
                storage
            )
            .unwrap()
        );
    }

    #[test]
    fn creating_on_file_as_parent_fails() {
        let mut storage_box = Box::new(StableStorage::new(DefaultMemoryImpl::default()));
        let storage = storage_box.as_mut();

        let root_node = storage.root_node();

        create_path(
            root_node,
            "test1/test2",
            Some(FileType::RegularFile),
            43u64,
            storage,
        )
        .unwrap();

        let res = create_path(
            root_node,
            "test1/test2/test4",
            Some(FileType::Directory),
            44u64,
            storage,
        );
        assert_eq!(res, Err(Error::InvalidFileType));

        let res = create_path(
            root_node,
            "test1/test2/test4.txt",
            Some(FileType::RegularFile),
            44u64,
            storage,
        );
        assert_eq!(res, Err(Error::InvalidFileType));

        let res = create_path(
            root_node,
            "test1/test2/test3/test4",
            Some(FileType::Directory),
            44u64,
            storage,
        );
        assert_eq!(res, Err(Error::InvalidFileType));

        let res = create_path(
            root_node,
            "test1/test2/test3/test4.txt",
            Some(FileType::RegularFile),
            44u64,
            storage,
        );
        assert_eq!(res, Err(Error::InvalidFileType));
    }

    #[test]
    fn trying_to_create_sym_link_fails() {
        let mut storage_box = Box::new(StableStorage::new(DefaultMemoryImpl::default()));
        let storage = storage_box.as_mut();

        let root_node = storage.root_node();

        let res = create_path(
            root_node,
            "test1/sym_link.txt",
            Some(FileType::SymbolicLink),
            43u64,
            storage,
        );
        assert_eq!(res, Err(Error::InvalidFileType));
    }

    #[test]
    fn delete_file_from_subfolders() {
        let mut storage_box = Box::new(StableStorage::new(DefaultMemoryImpl::default()));
        let storage = storage_box.as_mut();
        let mut names_cache = FilenameCache::new();

        let root_node = storage.root_node();

        let (test3, _) = create_path(
            root_node,
            "test1/test2/test3.txt",
            Some(FileType::RegularFile),
            43u64,
            storage,
        )
        .unwrap();
        let (test4, _) = create_path(
            root_node,
            "test1/test2/test4",
            Some(FileType::Directory),
            44u64,
            storage,
        )
        .unwrap();
        let (test6, _) = create_path(
            root_node,
            "test1/test2/test5/test6.txt",
            Some(FileType::RegularFile),
            45u64,
            storage,
        )
        .unwrap();

        let test1 = find_node(root_node, "test1", &mut names_cache, storage).unwrap();

        let (test7, _) = create_path(
            test1,
            "test2/test4/test7.txt",
            Some(FileType::RegularFile),
            45u64,
            storage,
        )
        .unwrap();

        let test2_1 = find_node(root_node, "test1/test2", &mut names_cache, storage).unwrap();
        let test2_2 = find_node(test1, "test2", &mut names_cache, storage).unwrap();

        assert_eq!(test2_1, test2_2);

        assert_eq!(
            test3,
            find_node(
                root_node,
                "test1/test2/test3.txt",
                &mut names_cache,
                storage
            )
            .unwrap()
        );
        assert_eq!(
            test4,
            find_node(root_node, "test1/test2/test4", &mut names_cache, storage).unwrap()
        );
        assert_eq!(
            test6,
            find_node(
                root_node,
                "test1/test2/test5/test6.txt",
                &mut names_cache,
                storage
            )
            .unwrap()
        );
        assert_eq!(
            test7,
            find_node(
                root_node,
                "test1/test2/test4/test7.txt",
                &mut names_cache,
                storage
            )
            .unwrap()
        );
    }
}

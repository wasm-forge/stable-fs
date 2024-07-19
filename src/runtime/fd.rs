use std::collections::BTreeMap;

use crate::{
    error::Error,
    runtime::{dir::Dir, file::File},
    storage::types::Node,
};

use super::symlink::Symlink;

const RESERVED_FD_COUNT: Fd = 3;

pub type Fd = u32;

#[derive(Clone, Debug)]

pub enum FdEntry {
    File {file: File, path: Vec<Node>},
    Dir {dir: Dir, path: Vec<Node>},
    Symlink {link: Symlink, path: Vec<Node>},
}

impl FdEntry {
    pub fn new_file(file: File, path: Vec<Node>) -> FdEntry {
        FdEntry::File {file, path}
    }
}

//
pub struct FdTable {
    // currentlty open file descriptors.
    table: BTreeMap<Fd, FdEntry>,
    // backward links to see how many file descriptors are currently pointing to any particular node.
    node_refcount: BTreeMap<Node, usize>,
    // the next generated descriptor's ID (if there is nothing to reuse).
    next_fd: Fd,
    // freed file descriptors ready to reuse.
    free_fds: Vec<Fd>,
}

impl FdTable {
    // create a new file descriptor table.
    pub fn new() -> Self {
        Self {
            table: BTreeMap::default(),
            node_refcount: BTreeMap::default(),
            next_fd: RESERVED_FD_COUNT,
            free_fds: vec![],
        }
    }

    // Get the map of node references.
    pub fn node_refcount(&self) -> &BTreeMap<Node, usize> {
        &self.node_refcount
    }

    // Update a file descriptor entry.
    pub fn update(&mut self, fd: Fd, entry: FdEntry) {
        self.insert(fd, entry);
    }

    // Update a file descriptor entry, it returns the old entry if existed.
    pub fn insert(&mut self, fd: Fd, entry: FdEntry) -> Option<FdEntry> {
        self.inc_node_refcount(&entry);
        let prev_entry = self.table.insert(fd, entry);
        if let Some(prev_entry) = prev_entry.as_ref() {
            self.dec_node_refcount(prev_entry);
        }
        prev_entry
    }

    // Get an FdEntry for a given file descriptor.
    pub fn get(&self, fd: Fd) -> Option<&FdEntry> {
        self.table.get(&fd)
    }

    // Open a new file descriptor.
    pub fn open(&mut self, entry: FdEntry) -> Fd {
        let fd = match self.free_fds.pop() {
            Some(fd) => fd,
            None => {
                let fd = self.next_fd;
                self.next_fd += 1;
                fd
            }
        };
        let prev = self.insert(fd, entry);
        assert!(prev.is_none());
        fd
    }

    // Reassign a file descriptor to a new number, the source descriptor is closed in the process.
    // If the destination descriptor is busy, it is closed in the process.
    pub fn renumber(&mut self, src: Fd, dst: Fd) -> Result<(), Error> {
        let old_entry = self.close(src).ok_or(Error::NotFound)?;

        // quietly close the destination file descriptor
        if let Some(_old_dst_entry) = self.close(dst) {
            let removed = self.free_fds.pop().unwrap();
            assert_eq!(removed, dst);
        }

        self.insert(dst, old_entry);

        Ok(())
    }

    // Close file descriptor.
    pub fn close(&mut self, fd: Fd) -> Option<FdEntry> {
        let entry = self.table.remove(&fd);

        if let Some(entry) = entry {
            self.free_fds.push(fd);
            self.dec_node_refcount(&entry);

            Some(entry)
        } else {
            None
        }
    }

    fn inc_node_refcount(&mut self, entry: &FdEntry) {
        let node = match entry {
            FdEntry::File {file, path: _ } => file.node,
            FdEntry::Dir {dir, path: _} => dir.node,
            FdEntry::Symlink {link, path: _} => link.node,
        };
        let refcount = self.node_refcount.entry(node).or_default();
        *refcount += 1;
    }

    fn dec_node_refcount(&mut self, entry: &FdEntry) {

        let node = match entry {
            FdEntry::File {file, path: _} => file.node,
            FdEntry::Dir {dir, path: _} => dir.node,
            FdEntry::Symlink {link, path: _} => link.node,
        };

        let refcount = self.node_refcount.remove(&node);
        if let Some(mut refcount) = refcount {
            refcount -= 1;
            if refcount > 0 {
                self.node_refcount.insert(node, refcount);
            }
        }
    }
}

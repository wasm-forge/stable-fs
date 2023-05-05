use std::collections::BTreeMap;

use crate::{
    error::Error,
    runtime::{dir::Dir, file::File},
    storage::types::Node,
};

const RESERVED_FD_COUNT: Fd = 3;

pub type Fd = u32;

pub enum FdEntry {
    File(File),
    Dir(Dir),
}

pub struct FdTable {
    table: BTreeMap<Fd, FdEntry>,
    node_refcount: BTreeMap<Node, usize>,
    next_fd: Fd,
    free_fds: Vec<Fd>,
}

impl FdTable {
    pub fn new() -> Self {
        Self {
            table: BTreeMap::default(),
            node_refcount: BTreeMap::default(),
            next_fd: RESERVED_FD_COUNT,
            free_fds: vec![],
        }
    }

    pub fn node_refcount(&self) -> &BTreeMap<Node, usize> {
        &self.node_refcount
    }

    pub fn update(&mut self, fd: Fd, entry: FdEntry) {
        self.insert(fd, entry);
    }

    pub fn insert(&mut self, fd: Fd, entry: FdEntry) -> Option<FdEntry> {
        self.inc_node_refcount(&entry);
        let prev_entry = self.table.insert(fd, entry);
        if let Some(prev_entry) = prev_entry.as_ref() {
            self.dec_node_refcount(prev_entry);
        }
        prev_entry
    }

    pub fn get(&self, fd: Fd) -> Option<&FdEntry> {
        self.table.get(&fd)
    }

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

    pub fn renumber(&mut self, src: Fd, dst: Fd) -> Result<(), Error> {
        let old_entry = self.close(src)?;
        self.close(dst)?;

        let removed = self.free_fds.pop().unwrap();
        assert_eq!(removed, dst);

        self.insert(dst, old_entry);

        Ok(())
    }

    pub fn close(&mut self, fd: Fd) -> Result<FdEntry, Error> {
        let entry = self.table.remove(&fd).ok_or(Error::NotFound)?;
        self.free_fds.push(fd);
        self.dec_node_refcount(&entry);
        Ok(entry)
    }

    fn inc_node_refcount(&mut self, entry: &FdEntry) {
        let node = match entry {
            FdEntry::File(file) => file.node,
            FdEntry::Dir(dir) => dir.node,
        };
        let refcount = self.node_refcount.entry(node).or_default();
        *refcount += 1;
    }

    fn dec_node_refcount(&mut self, entry: &FdEntry) {
        let node = match entry {
            FdEntry::File(file) => file.node,
            FdEntry::Dir(dir) => dir.node,
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

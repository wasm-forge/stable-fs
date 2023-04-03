use std::collections::BTreeMap;

use crate::{
    error::Error,
    runtime::{dir::Dir, file::File},
};

const RESERVED_FD_COUNT: Fd = 3;

pub type Fd = u32;

pub enum FdEntry {
    File(File),
    Dir(Dir),
}

pub struct FdTable {
    table: BTreeMap<Fd, FdEntry>,
    next_fd: Fd,
    free_fds: Vec<Fd>,
}

impl FdTable {
    pub fn new() -> Self {
        Self {
            table: BTreeMap::default(),
            next_fd: RESERVED_FD_COUNT,
            free_fds: vec![],
        }
    }

    pub fn update(&mut self, fd: Fd, entry: FdEntry) {
        *self.table.get_mut(&fd).unwrap() = entry;
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
        let prev = self.table.insert(fd, entry);
        assert!(prev.is_none());
        fd
    }

    pub fn close(&mut self, fd: Fd) -> Result<(), Error> {
        self.table.remove(&fd).ok_or(Error::NotFound)?;
        self.free_fds.push(fd);
        Ok(())
    }
}

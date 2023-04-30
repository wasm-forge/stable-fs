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

    pub fn renumber(&mut self, src: Fd, dst: Fd) -> Result<(), Error> {
        
        let old_entry = self.table.remove(&src).ok_or(Error::NotFound)?;
        self.free_fds.push(src);

        // make fd one of the free ids if it was never used
        while self.next_fd <= dst {
            self.free_fds.push(self.next_fd);
            self.next_fd += 1;
        }

        // make sure destination fd is closed
        let _ = self.close(dst);

        let idx = self.free_fds.iter().position(|&v| v == dst).unwrap();

        let removed = self.free_fds.remove(idx);
        assert!(removed == dst);

        self.table.insert(dst, old_entry);

        Ok(())
    }

    pub fn close(&mut self, fd: Fd) -> Result<(), Error> {
        self.table.remove(&fd).ok_or(Error::NotFound)?;
        self.free_fds.push(fd);
        Ok(())
    }
}

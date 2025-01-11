use std::collections::BTreeMap;

use crate::{
    error::Error,
    fs::Fd,
    runtime::{dir::Dir, file::File},
    storage::types::Node,
};

// file descriptor used for the main root entry
pub const ROOT_FD: Fd = 3;
// number of file descriptors reserved for standard streams
const FIRST_AVAILABLE_FD: Fd = 4;

pub enum FdEntry {
    File(File),
    Dir(Dir),
}

//
pub struct FdTable {
    // currently open file descriptors.
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
            next_fd: FIRST_AVAILABLE_FD,
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

    // open root file descriptor
    pub fn open_root(&mut self, entry: FdEntry) -> Fd {
        let fd = ROOT_FD;

        let prev = self.insert(fd, entry);
        assert!(prev.is_none());
        fd
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

    // Copy a file descriptor to a new number, the source descriptor is closed in the process.
    // If the destination descriptor is busy, it is closed in the process.
    pub fn renumber(&mut self, src: Fd, dst: Fd) -> Result<(), Error> {
        if src == dst {
            return Ok(());
        }

        // renumbering between special file descriptors is not allowed
        if dst < FIRST_AVAILABLE_FD || src < FIRST_AVAILABLE_FD {
            return Err(Error::OperationNotPermitted);
        }

        // cannot do renumbering between a file and a folder
        let src_entry: Option<&FdEntry> = self.table.get(&src);
        let dst_entry: Option<&FdEntry> = self.table.get(&dst);

        if let Some(FdEntry::Dir(_s)) = src_entry {
            if let Some(FdEntry::File(_d)) = dst_entry {
                return Err(Error::BadFileDescriptor);
            }
        }

        if let Some(FdEntry::File(_s)) = src_entry {
            if let Some(FdEntry::Dir(_d)) = dst_entry {
                return Err(Error::BadFileDescriptor);
            }
        }

        // now assign the source file descriptor to the destination
        let old_entry = self.close(src).ok_or(Error::BadFileDescriptor)?;

        // quietly close the destination file descriptor
        if let Some(_old_dst_entry) = self.close(dst) {
            // dst should not be reused by anyone else, so we must undo the fd marked for reusal
            let removed = self.free_fds.pop().unwrap();
            // sanity check that the removed fd was indeer dst
            assert_eq!(removed, dst);
        }

        self.insert(dst, old_entry);

        Ok(())
    }

    // Close file descriptor.
    pub fn close(&mut self, fd: Fd) -> Option<FdEntry> {
        if fd == ROOT_FD {
            return None;
        }

        let entry = self.table.remove(&fd);

        if let Some(entry) = entry {
            if fd >= FIRST_AVAILABLE_FD {
                self.free_fds.push(fd);
            }

            self.dec_node_refcount(&entry);

            Some(entry)
        } else {
            None
        }
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

#[cfg(test)]
mod tests {
    use crate::fs::FdStat;

    use super::*;

    #[test]
    fn test_fdtable_new() {
        let fd_table = FdTable::new();
        assert!(fd_table.table.is_empty());
        assert!(fd_table.node_refcount.is_empty());

        assert_eq!(fd_table.next_fd, FIRST_AVAILABLE_FD);
        assert!(fd_table.free_fds.is_empty());
    }

    #[test]
    fn test_fdtable_open_and_get() {
        let mut fd_table = FdTable::new();
        let file = File {
            node: 1,
            cursor: 0,
            stat: FdStat::default(),
        };
        let fd = fd_table.open(FdEntry::File(file));

        // Check that the file descriptor was assigned to the first possible value
        assert_eq!(fd, FIRST_AVAILABLE_FD);

        // Check that the entry exists in the table
        let entry = fd_table.get(fd).unwrap();
        match entry {
            FdEntry::File(file) => assert_eq!(file.node, 1),
            _ => panic!("Expected a file entry"),
        }
    }

    #[test]
    fn test_fdtable_close() {
        let mut fd_table = FdTable::new();
        let file = File {
            node: 1,
            cursor: 0,
            stat: FdStat::default(),
        };
        let fd = fd_table.open(FdEntry::File(file));

        // Close the file descriptor
        let entry = fd_table.close(fd);
        assert!(entry.is_some());

        // Ensure the table no longer has the file descriptor
        assert!(fd_table.get(fd).is_none());

        // Ensure the freed FD is available for reuse
        assert!(fd_table.free_fds.contains(&fd));
    }

    #[test]
    fn test_fdtable_renumber() {
        let mut fd_table = FdTable::new();
        let file = File {
            node: 1,
            cursor: 0,
            stat: FdStat::default(),
        };
        let src_fd = fd_table.open(FdEntry::File(file));
        let dst_fd = 10;

        // Renumber the file descriptor
        fd_table.renumber(src_fd, dst_fd).unwrap();

        // Ensure the old FD no longer exists
        assert!(fd_table.get(src_fd).is_none());

        // Ensure the new FD points to the correct entry
        let entry = fd_table.get(dst_fd).unwrap();
        match entry {
            FdEntry::File(file) => assert_eq!(file.node, 1),
            _ => panic!("Expected a file entry"),
        }
    }

    #[test]
    fn test_renumber_different_types() {
        let mut fd_table = FdTable::new();
        let file = File {
            node: 1,
            cursor: 0,
            stat: FdStat::default(),
        };

        let dir = Dir {
            node: 2,
            stat: FdStat::default(),
        };

        let fd1 = fd_table.open(FdEntry::File(file));
        let fd2 = fd_table.open(FdEntry::Dir(dir));

        let result = fd_table.renumber(fd1, fd2);
        assert_eq!(result, Err(Error::BadFileDescriptor));

        let result = fd_table.renumber(fd2, fd1);
        assert_eq!(result, Err(Error::BadFileDescriptor));
    }

    #[test]
    fn test_fdtable_node_refcount() {
        let mut fd_table = FdTable::new();

        // Open two file descriptors pointing to the same node
        let file1 = File {
            node: 1,
            cursor: 0,
            stat: FdStat::default(),
        };
        let file2 = File {
            node: 1,
            cursor: 0,
            stat: FdStat::default(),
        };

        let fd1 = fd_table.open(FdEntry::File(file1));
        let fd2 = fd_table.open(FdEntry::File(file2));

        // Check node reference count
        assert_eq!(fd_table.node_refcount().get(&1), Some(&2));

        // Close one file descriptor
        fd_table.close(fd1);

        // Check node reference count again
        assert_eq!(fd_table.node_refcount().get(&1), Some(&1));

        // Close the second file descriptor
        fd_table.close(fd2);

        // Ensure the node reference count is now removed
        assert!(fd_table.node_refcount().get(&1).is_none());
    }

    #[test]
    fn test_fdtable_update() {
        let mut fd_table = FdTable::new();

        let file = File {
            node: 1,
            cursor: 0,
            stat: FdStat::default(),
        };
        let fd = fd_table.open(FdEntry::File(file));

        let updated_file = File {
            node: 2,
            cursor: 0,
            stat: FdStat::default(),
        };
        fd_table.update(fd, FdEntry::File(updated_file));

        // Ensure the updated entry is now in the table
        let entry = fd_table.get(fd).unwrap();
        match entry {
            FdEntry::File(file) => assert_eq!(file.node, 2),
            _ => panic!("Expected a file entry"),
        }

        // Ensure the node reference count was updated
        assert!(fd_table.node_refcount().get(&1).is_none());
        assert_eq!(fd_table.node_refcount().get(&2), Some(&1));
    }
}

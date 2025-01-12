# **File System based on Stable Structures (stable-fs)**

This project is a Rust-based file system implementation. It is designed to be a backend for the ic-wasi-polyfill library, but can also be used as a stand-alone project. The file system provides abstractions for managing files, directories with support for mountable memory-files, chunk-based files and sparse files.

## **Key Features**

- **Persistent File System**: Store and retrieve files using stable storage.
- **Memory-Mapped Files**: Vitrual memory can be mounted as one of the files.
- **Chunk-Based Storage**: Files are split into chunks for flexible and efficient access.
- **Sparse File Support**: Manage files with large gaps of unused space.
- **Hard Links**: Create multiple references to the same file.
- **Stateless API**: Operations like reading and writing rely on offsets and cursors for flexibility.

---

## **Usage**

### **Creating a File System**

To initialize a file system, you need a storage implementation. Here's an example:

```rust
use stable-fs::{FileSystem, Storage};
use stable-fs::storage::stable::StableStorage;

let storage = StableStorage::new(host_memory);
let mut fs = FileSystem::new(Box::new(storage)).unwrap();
```

### **Managing Files**

- **Creating Files**:

```rust
let file_fd = fs.create_file(fs.root_fd(), "example.txt", Default::default(), 0).unwrap();
```

- **Writing Data**:

```rust
fs.write(file_fd, b"Hello, world!").unwrap();
```

- **Reading Data**:

```rust
let mut buffer = [0u8; 12];
fs.read(file_fd, &mut buffer).unwrap();
println!("Read data: {:?}", std::str::from_utf8(&buffer).unwrap());
```

### **Directory Operations**

- **Creating Directories**:

```rust
let dir_fd = fs.create_dir(fs.root_fd(), "my_folder", Default::default(), 0).unwrap();
```

- **Listing Files in a Directory**:

```rust
let files = list_files(&mut fs, "my_folder");
println!("Files: {:?}", files);
```

- **Removing Directories**:

```rust
fs.remove_dir(fs.root_fd(), "my_folder").unwrap();
```

### **Memory-Mapped Files**

- **Mounting a Memory-Mapped File**:

```rust
use ic_stable_structures::memory_manager::VectorMemory;

let memory = VectorMemory::default();
fs.mount_memory_file("mapped_file", Box::new(memory)).unwrap();
```

- **Storing Data in Memory-Mapped File**:

```rust
fs.store_memory_file("mapped_file").unwrap();
```

- **Unmounting**:

```rust
fs.unmount_memory_file("mapped_file").unwrap();
```

### **Advanced File Operations**

- **Hard Links**:

```rust
fs.create_hard_link(fs.root_fd(), "original.txt", fs.root_fd(), "linked.txt").unwrap();
```

- **Renaming Files**:

```rust
fs.rename(fs.root_fd(), "old_name.txt", fs.root_fd(), "new_name.txt").unwrap();
```

- **Sparse File Handling**:

```rust
let sparse_fd = fs.create_file(fs.root_fd(), "sparse.txt", Default::default(), 0).unwrap();
fs.set_metadata(sparse_fd, &Metadata { size: 1_000_000, ..Default::default() }).unwrap();
```

---

## **Configuration Options**

You can configure various aspects of the file system:

- **Chunk Size**: Define the chunk size for file storage:
  ```rust
  fs.storage.set_chunk_size(4096).unwrap();
  ```
- **File Size Limits**: Restrict the maximum file size:
  ```rust
  fs.set_file_size_limit(fd, 1_000_000).unwrap();
  ```

---

## **Examples**

### **Basic File Operations**

```rust
let file_fd = fs.create_file(fs.root_fd(), "example.txt", Default::default(), 0).unwrap();
fs.write(file_fd, b"Hello!").unwrap();

let mut buffer = [0u8; 6];
fs.read(file_fd, &mut buffer).unwrap();
println!("File content: {:?}", std::str::from_utf8(&buffer).unwrap());
```

### **Working with Directories**

```rust
let dir_fd = fs.create_dir(fs.root_fd(), "docs", Default::default(), 0).unwrap();
fs.create_file(dir_fd, "file1.txt", Default::default(), 0).unwrap();
fs.create_file(dir_fd, "file2.txt", Default::default(), 0).unwrap();

let files = list_files(&mut fs, "docs");
println!("Files: {:?}", files);
```


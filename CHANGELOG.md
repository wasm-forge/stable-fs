# Changelog


## [v0.11.0]
- Fix mounting on absolute paths
- Improved mounted memory size reset logic
- API change: explicit mount file size policy added
- Update dependencies

## [v0.10.0]
- Optimized work with folders
- Refactor Metadata and DirEntry structures
- Reduce copy of DirEntry
- Add fast direntry lookup by name

## [v0.9.0]
- Switch to ic-stable-structures v0.7
- Refactor project structure
- Integrate ic-test into project for the integration tests
- Update dependencies


## [v0.8.1]
- Switch to ic-cdk v0.18.3
- Switch to pocket-ic v9.0
- Update dependencies


## [v0.8.0]
- Improved Wasi compliance
- Change edition to 2024
- Update dependencies
- Bug fixes
- Faster file look-up
- File read-write access control
- Fixed writing in append-mode
- Switch to pocket-ic v7.0
- Support for '..' in paths
- Return '.' and '..' during folder file list


## [v0.7.3]
- Refactor chunk handling, constant names
- Fix chunk caching error


## [v0.7.2]
- Fix upgrading from chunks v1 to v2


## [v0.7.1]
- Update dependencies
- Error code corrections

## [v0.7.0]
- Update dependencies
- Switched to using WASI error enum
- API improvements
- Some missing WASI functions (advice, allocate) are now part of the file system API
- Additional file structure tests added
- File size limit support added
- Helper function to remove files and folders recursively
- Metadata stored inside V2 chunks with caching for faster access times
- Corrected issues with the root folder Fd


## [v0.6.4]
- Upgrade Pocket-ic client version to V5.0.
- Filename cache added (faster repeated opening of the same file).
- Refactor v2 chunks reading and writing (reuse the same iteration mechanism).
- Medatada cache for regular files (faster overwriting in small segments).
- Dependency updates to the latest versions


## [v0.6.3]
- Add changelog.
- Additional caching for file read and write.
- Fix performance regression of reading small file segments.

## [v0.6.2]
- Sparse file support.
- More testing.
- Read iterator (faster reads).
- Metadata caching in stable memory (faster writes).

## [v0.6.1]
- Add basic caching for chunk read and write.
- Add chunk size and chunk type information to the API.

## [v0.6.0]
- add mounted memory file support.
- add V2 chunks support.
- *API change:* mounted memory file support.

## [v0.5.1]
- use newer ic-cdk version.
- improve project structure.
- ranged read optimization added.

## [v0.5.0]
- *API change:* init with memory manager using memory index range rather than first memory index.

[v0.11.0]: https://github.com/wasm-forge/stable-fs/compare/v0.10.0...v0.11.0
[v0.10.0]: https://github.com/wasm-forge/stable-fs/compare/v0.9.0...v0.10.0
[v0.9.0]: https://github.com/wasm-forge/stable-fs/compare/v0.8.1...v0.9.0
[v0.8.1]: https://github.com/wasm-forge/stable-fs/compare/v0.8.0...v0.8.1
[v0.8.0]: https://github.com/wasm-forge/stable-fs/compare/v0.7.3...v0.8.0
[v0.7.3]: https://github.com/wasm-forge/stable-fs/compare/v0.7.2...v0.7.3
[v0.7.2]: https://github.com/wasm-forge/stable-fs/compare/v0.7.1...v0.7.2
[v0.7.1]: https://github.com/wasm-forge/stable-fs/compare/v0.7.0...v0.7.1
[v0.7.0]: https://github.com/wasm-forge/stable-fs/compare/v0.6.4...v0.7.0
[v0.6.4]: https://github.com/wasm-forge/stable-fs/compare/v0.6.3...v0.6.4
[v0.6.3]: https://github.com/wasm-forge/stable-fs/compare/v0.6.2...v0.6.3
[v0.6.2]: https://github.com/wasm-forge/stable-fs/compare/v0.6.1...v0.6.2
[v0.6.1]: https://github.com/wasm-forge/stable-fs/compare/v0.6.0...v0.6.1
[v0.6.0]: https://github.com/wasm-forge/stable-fs/compare/v0.5.1...v0.6.0
[v0.5.1]: https://github.com/wasm-forge/stable-fs/compare/v0.5.0...v0.5.1
[v0.5.0]: https://github.com/wasm-forge/stable-fs/compare/v0.4.0...v0.5.0


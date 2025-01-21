# Changelog

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


[unreleased]: https://github.com/wasm-forge/stable-fs/compare/v0.7.2...main
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


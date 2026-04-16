# stable-fs

[![Tests](https://github.com/wasm-forge/stable-fs/actions/workflows/ci.yml/badge.svg)](https://github.com/wasm-forge/stable-fs/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/wasm-forge/stable-fs/branch/main/graph/badge.svg)](https://codecov.io/gh/wasm-forge/stable-fs/branch/main/)

A WASI-compatible filesystem for Internet Computer canisters, backed by stable memory so data survives upgrades.


## Features

- **Stable across upgrades** — files and directories are stored in IC stable memory, no data loss on canister upgrade
- **WASI interface** — implements the standard WASI filesystem API for drop-in use with Wasm modules
- **Memory mounting** — mount an IC stable memory region directly as a file, with configurable size policies

## Storage backends

| Backend | Description |
|---|---|
| `StableStorage` | Persists in IC stable memory — use this in production canisters |
| `TransientStorage` | In-memory only — useful for unit tests |

## Limitations

- Symbolic links are not supported
- `allocate()` does not reserve space ahead of time

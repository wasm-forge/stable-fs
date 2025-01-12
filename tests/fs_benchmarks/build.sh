#!/bin/sh

set -e

cargo build --release --target wasm32-unknown-unknown --features canbench-rs

wasi2ic target/wasm32-unknown-unknown/release/fs_benchmarks_backend.wasm target/wasm32-unknown-unknown/release/fs_benchmarks_backend_small.wasm

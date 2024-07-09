#!/bin/bash

rustup target add wasm32-unknown-unknown

cargo install wasi2ic

cd src/tests/fs_benchmark_test

cargo build --release --target wasm32-unknown-unknown

wasi2ic target/wasm32-unknown-unknown/release/fs_benchmark_test_backend.wasm target/wasm32-unknown-unknown/release/fs_benchmark_test_backend_small.wasm

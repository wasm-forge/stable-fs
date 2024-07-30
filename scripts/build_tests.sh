#!/bin/bash

cd tests/fs_benchmark_test

cargo build --release --target wasm32-unknown-unknown

wasi2ic target/wasm32-unknown-unknown/release/fs_benchmark_test_backend.wasm target/wasm32-unknown-unknown/release/fs_benchmark_test_backend_small.wasm

cd ../demo_test_upgraded

cargo build --release --target wasm32-unknown-unknown

wasi2ic target/wasm32-unknown-unknown/release/demo_test_upgraded_backend.wasm target/wasm32-unknown-unknown/release/demo_test_upgraded_backend_small.wasm


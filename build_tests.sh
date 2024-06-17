#!/bin/bash

rustup target add wasm32-unknown-unknown

cd src/tests/demo_test

cargo build --release --target wasm32-unknown-unknown

../../../wasi2ic target/wasm32-unknown-unknown/release/demo_test_backend.wasm target/wasm32-unknown-unknown/release/demo_test_backend_small.wasm

#gzip -f target/wasm32-unknown-unknown/release/demo_test_backend.wasm

cd ../demo_test_upgraded

cargo build --release --target wasm32-unknown-unknown


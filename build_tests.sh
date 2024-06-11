#!/bin/bash

rustup target add wasm32-unknown-unknown

cd src/tests/demo_test

cargo build --release --target wasm32-unknown-unknown

zip target/wasm32-unknown-unknown/release/demo_test_backend.zip target/wasm32-unknown-unknown/release/demo_test_backend.wasm

cd ../demo_test_upgraded

cargo build --release --target wasm32-unknown-unknown

zip target/wasm32-unknown-unknown/release/demo_test_backend.zip target/wasm32-unknown-unknown/release/demo_test_backend.wasm



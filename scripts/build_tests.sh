#!/bin/bash

set -e

cd tests/canister_initial

cargo build --release --target wasm32-unknown-unknown

wasi2ic target/wasm32-unknown-unknown/release/canister_initial_backend.wasm target/wasm32-unknown-unknown/release/canister_initial_backend_small.wasm

cd ../canister_upgraded

cargo build --release --target wasm32-unknown-unknown

wasi2ic target/wasm32-unknown-unknown/release/canister_upgraded_backend.wasm target/wasm32-unknown-unknown/release/canister_upgraded_backend_small.wasm


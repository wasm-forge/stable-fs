#!/bin/bash
set -e

cd tests/canister_initial
cargo build --release --target wasm32-unknown-unknown

cd ../canister_upgraded
cargo build --release --target wasm32-unknown-unknown



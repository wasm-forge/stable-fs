#!/bin/bash
set -e

cargo build -p canister_initial_backend --release --target wasm32-unknown-unknown

candid-extractor target/wasm32-unknown-unknown/release/canister_initial_backend.wasm > test_canisters/canister_initial/src/canister_initial_backend/canister_initial_backend.did

cargo build -p canister_upgraded_backend --release --target wasm32-unknown-unknown

candid-extractor target/wasm32-unknown-unknown/release/canister_upgraded_backend.wasm > test_canisters/canister_upgraded/src/canister_upgraded_backend/canister_upgraded_backend.did

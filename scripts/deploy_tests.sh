#!/bin/bash

set -e

cd tests/canister_initial

dfx canister create canister_initial_backend

dfx canister install --mode reinstall --wasm target/wasm32-unknown-unknown/release/canister_initial_backend_small.wasm -y canister_initial_backend


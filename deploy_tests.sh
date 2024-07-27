#!/bin/bash

cd tests/fs_benchmark_test

dfx canister create fs_benchmark_test_backend

dfx canister install --mode reinstall --wasm target/wasm32-unknown-unknown/release/fs_benchmark_test_backend_small.wasm -y fs_benchmark_test_backend


#!/bin/bash
set -e

cargo build -p canister_initial_backend --release --target wasm32-unknown-unknown

cargo build -p canister_upgraded_backend --release --target wasm32-unknown-unknown



#!/bin/bash

dfx canister call fs_benchmark_test_backend read_buffer '(99999988:nat64, 10:nat64)'

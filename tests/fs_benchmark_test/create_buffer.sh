#!/bin/bash

dfx canister call fs_benchmark_test_backend append_buffer '("abc1234567", 10_000_000: nat64, 100_000_000: nat64 )'



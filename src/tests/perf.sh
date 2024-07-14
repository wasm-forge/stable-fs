#!ic-repl

function install(wasm, args, cycle) {
  let id = call ic.provisional_create_canister_with_cycles(record { settings = null; amount = cycle });
  let S = id.canister_id;
  let _ = call ic.install_code(
    record {
      arg = args;
      wasm_module = gzip(wasm);
      mode = variant { install };
      canister_id = S;
    }
  );
  S
};

function upgrade(id, wasm, args) {
  call ic.install_code(
    record {
      arg = args;
      wasm_module = gzip(wasm);
      mode = variant { upgrade };
      canister_id = id;
    }
  );
};

function uninstall(id) {
  call ic.stop_canister(record { canister_id = id });
  call ic.delete_canister(record { canister_id = id });
};

function get_memory(cid) {
  let _ = call ic.canister_status(record { canister_id = cid });
  _.memory_size
};

let file = "README.md";

let rs_config = record { start_page = 1; page_limit = 1128};

let wasm_name = "fs_benchmark_test/target/wasm32-unknown-unknown/release/fs_benchmark_test_backend_small.wasm";

let cid = install(wasm_profiling(wasm_name, rs_config), encode (), null);

function perf_file_write_10mib() {
  call cid.write_mib_text( "files/test.txt", (10: nat64) );
  flamegraph(cid, "perf_file_write_10mib", "svg/perf_file_write_10mib.svg");
};

function perf_append_text_10kib() {
  call cid.append_text( "test.txt", "some_text_", (1024 : nat64) );
  flamegraph(cid, "perf_append_text_10kib", "svg/perf_append_text_10kib.svg");
};

function perf_append_text_10kib_deep_folder_structure() {
  call cid.append_text( "d0/d1/d2/d3/d4/d5/d6/d7/d8/d9/d10/d11/d12/d13/d14/d15/d16/d17/d18/d19/d20/d21/d22/d23/d24/d25/d26/d27/d28/d29/d30/d31/d32/d33/d34/d35/d36/d37/d38/d39/d40/d41/d42/d43/d44/d45/d46/d47/d48/d49/d50/d51/d52/d53/d54/d55/d56/d57/d58/d59/d60/d61/d62/d63/d64/d65/d66/d67/d68/d69/d70/d71/d72/d73/d74/d75/d76/d77/d78/d79/d80/d81/d82/d83/d84/d85/d86/d87/d88/d89/d90/d91/d92/d93/d94/d95/d96/d97/d98/d99/test.txt", "some_text_", (1024 : nat64) );
  flamegraph(cid, "perf_append_text_10kib_deep_folder_structure", "svg/perf_append_text_10kib_deep_folder_structure.svg");
};

function perf_create_files() {
  call cid.create_files( "files", (100: nat64) );
  flamegraph(cid, "perf_create_files", "svg/perf_create_files.svg");
};

function perf_create_folders() {
  call cid.create_depth_folders("files", (100: nat64));
  flamegraph(cid, "create_depth_folders", "svg/create_depth_folders.svg");
};

function perf_list_files() {
  call cid.list_files("files");
  flamegraph(cid, "perf_list_files", "svg/perf_list_files.svg");
};

function perf_delete_files() {

  call cid.delete_file("files/0.txt");
  flamegraph(cid, "delete_files", "svg/delete_files.svg");
};

function perf_delete_folders() {

  call cid.delete_folder("files/d0/d1/d2/d3/d4/d5/d6/d7/d8/d9/d10/d11/d12/d13/d14/d15/d16/d17/d18/d19/d20/d21/d22/d23/d24/d25/d26/d27/d28/d29/d30/d31/d32/d33/d34/d35/d36/d37/d38/d39/d40/d41/d42/d43/d44/d45/d46/d47/d48/d49/d50/d51/d52/d53/d54/d55/d56/d57/d58/d59/d60/d61/d62/d63/d64/d65/d66/d67/d68/d69/d70/d71/d72/d73/d74/d75/d76/d77/d78/d79/d80/d81/d82/d83/d84/d85/d86/d87/d88/d89/d90/d91/d92/d93/d94/d95/d96/d97/d98/d99");

  flamegraph(cid, "delete_folders", "svg/delete_folders.svg");
};

/// files
perf_file_write_10mib();

perf_append_text_10kib();
perf_append_text_10kib_deep_folder_structure();


//perf_create_files();
//perf_delete_files();
//perf_list_files();

/// folders

//perf_create_folders();
//perf_delete_folders();

uninstall(cid);

//call cid.__toggle_tracing();
//call cid.list_files("files");
//call cid.__toggle_tracing();




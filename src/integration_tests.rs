use candid::{decode_one, encode_one, Principal};
use pocket_ic::{PocketIc, WasmResult};
use std::fs;

const BACKEND_WASM: &str = "src/tests/fs_benchmark_test/target/wasm32-unknown-unknown/release/fs_benchmark_test_backend_small.wasm";
const BACKEND_WASM_UPGRADED: &str = "src/tests/demo_test_upgraded/target/wasm32-unknown-unknown/release/demo_test_upgraded_backend_small.wasm";

fn setup_test_projects() {
    use std::process::Command;
    let _ = Command::new("bash")
        .arg("build_tests.sh")
        .output()
        .expect("Failed to execute command");
}

fn setup() -> (PocketIc, Principal) {
    let pic = PocketIc::new();

    let backend_canister = pic.create_canister();
    pic.add_cycles(backend_canister, 2_000_000_000_000);

    let wasm = fs::read(BACKEND_WASM).expect("Wasm file not found, run 'dfx build'.");

    pic.install_canister(backend_canister, wasm, vec![], None);

    pic.tick();

    (pic, backend_canister)
}

#[test]
fn greet_after_upgrade() {
    setup_test_projects();

    let (pic, backend_canister) = setup();

    let Ok(WasmResult::Reply(response)) = pic.query_call(
        backend_canister,
        Principal::anonymous(),
        "greet",
        encode_one("ICP").unwrap(),
    ) else {
        panic!("Expected reply");
    };
    let result: String = decode_one(&response).unwrap();

    assert_eq!(result, "Hello, ICP!");

    let wasm_upgraded =
        fs::read(BACKEND_WASM_UPGRADED).expect("Wasm file not found, run 'dfx build'.");

    pic.upgrade_canister(backend_canister, wasm_upgraded, vec![], None)
        .unwrap();

    let Ok(WasmResult::Reply(response)) = pic.query_call(
        backend_canister,
        Principal::anonymous(),
        "greet",
        encode_one("ICP").unwrap(),
    ) else {
        panic!("Call failed!");
    };
    let result: String = decode_one(&response).unwrap();

    assert_eq!(result, "Greetings, ICP!");
}

#[test]
fn writing_10mib() {
    setup_test_projects();

    let (pic, backend_canister) = setup();

    let args = candid::encode_args(("test.txt", 10u64)).unwrap();

    let _response = pic
        .update_call(
            backend_canister,
            Principal::anonymous(),
            "write_mib_text",
            args,
        )
        .unwrap();
}

#[test]
fn reading_file_after_upgrade() {
    setup_test_projects();

    let (pic, backend_canister) = setup();

    let _response = pic
        .update_call(
            backend_canister,
            Principal::anonymous(),
            "append_text",
            candid::encode_args(("d1/d2/d3/test1.txt", "test1", 10u64)).unwrap(),
        )
        .unwrap();

    let _response = pic
        .update_call(
            backend_canister,
            Principal::anonymous(),
            "append_text",
            candid::encode_args(("d1/d2/test2.txt", "test2", 10u64)).unwrap(),
        )
        .unwrap();

    let _response = pic
        .update_call(
            backend_canister,
            Principal::anonymous(),
            "append_text",
            candid::encode_args(("test3.txt", "test3", 10u64)).unwrap(),
        )
        .unwrap();

    let _response = pic
        .update_call(
            backend_canister,
            Principal::anonymous(),
            "append_text",
            candid::encode_args(("d1/d2/test2.txt", "abc", 10u64)).unwrap(),
        )
        .unwrap();

    let response = pic
        .query_call(
            backend_canister,
            Principal::anonymous(),
            "read_text",
            candid::encode_args(("d1/d2/test2.txt", 45i64, 100u64)).unwrap(),
        )
        .unwrap();

    if let WasmResult::Reply(response) = response {
        let result: String = decode_one(&response).unwrap();
        assert_eq!(result, "test2abcabcabcabcabcabcabcabcabcabc");
    }

    // do upgrade
    let wasm_upgraded =
        fs::read(BACKEND_WASM_UPGRADED).expect("Wasm file not found, run 'dfx build'.");

    pic.upgrade_canister(backend_canister, wasm_upgraded, vec![], None)
        .unwrap();

    let response = pic
        .query_call(
            backend_canister,
            Principal::anonymous(),
            "read_text",
            candid::encode_args(("d1/d2/test2.txt", 40i64, 15u64)).unwrap(),
        )
        .unwrap();

    if let WasmResult::Reply(response) = response {
        let result: String = decode_one(&response).unwrap();
        assert_eq!(result, "test2test2abcab");
    }
}

#[test]
fn list_folders_after_upgrade() {
    setup_test_projects();

    let (pic, backend_canister) = setup();

    let _response = pic
        .update_call(
            backend_canister,
            Principal::anonymous(),
            "create_files",
            candid::encode_args(("files", 10u64)).unwrap(),
        )
        .unwrap();

    let _response = pic
        .update_call(
            backend_canister,
            Principal::anonymous(),
            "create_files",
            candid::encode_args(("files/f2", 10u64)).unwrap(),
        )
        .unwrap();

    let response = pic
        .query_call(
            backend_canister,
            Principal::anonymous(),
            "list_files",
            encode_one("files").unwrap(),
        )
        .unwrap();

    if let WasmResult::Reply(response) = response {
        let result: Vec<String> = decode_one(&response).unwrap();

        assert_eq!(
            result,
            vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt", "f2"}
        );
    }

    let response = pic
        .query_call(
            backend_canister,
            Principal::anonymous(),
            "list_files",
            encode_one("files/f2").unwrap(),
        )
        .unwrap();

    if let WasmResult::Reply(response) = response {
        let result: Vec<String> = decode_one(&response).unwrap();

        assert_eq!(
            result,
            vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt"}
        );
    }

    // do upgrade
    let wasm_upgraded =
        fs::read(BACKEND_WASM_UPGRADED).expect("Wasm file not found, run 'dfx build'.");

    pic.upgrade_canister(backend_canister, wasm_upgraded, vec![], None)
        .unwrap();

    let response = pic
        .query_call(
            backend_canister,
            Principal::anonymous(),
            "list_files",
            encode_one("files").unwrap(),
        )
        .unwrap();

    if let WasmResult::Reply(response) = response {
        let result: Vec<String> = decode_one(&response).unwrap();

        assert_eq!(
            result,
            vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt", "f2"}
        );
    }

    let response = pic
        .query_call(
            backend_canister,
            Principal::anonymous(),
            "list_files",
            encode_one("files/f2").unwrap(),
        )
        .unwrap();

    if let WasmResult::Reply(response) = response {
        let result: Vec<String> = decode_one(&response).unwrap();

        assert_eq!(
            result,
            vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt"}
        );
    }
}

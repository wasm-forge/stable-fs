use candid::{decode_one, encode_one, Principal};
use pocket_ic::{PocketIc, WasmResult};
use std::fs;

const BACKEND_WASM: &str =
    "src/tests/demo_test/target/wasm32-unknown-unknown/release/demo_test_backend_small.wasm";
const BACKEND_WASM_UPGRADED: &str = "src/tests/demo_test_upgraded/target/wasm32-unknown-unknown/release/demo_test_upgraded_backend.wasm";

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

    (pic, backend_canister)
}

#[test]
fn test_hello() {

    setup_test_projects();

    let (pic, backend_canister) = setup();

    pic.tick();

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
fn test_writing_10mb() {
    setup_test_projects();

    let (pic, backend_canister) = setup();
    
    pic.tick();

    let args = candid::encode_args(("test.txt", 1u64)).unwrap();

    let Ok(WasmResult::Reply(response)) = pic.update_call(
        backend_canister,
        Principal::anonymous(),
        "write_mb_text",
        args,
    ) else {
        panic!("Call failed!");
    };

    // get some return value
    let result: u64 = decode_one(&response).unwrap();

    println!("Value returned {:?}", result);
}

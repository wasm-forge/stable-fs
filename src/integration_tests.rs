use candid::Principal;
use fns::read_text;
use pocket_ic::PocketIc;
use std::sync::Once;
use std::{cell::RefCell, fs};

const BACKEND_WASM: &str = "tests/canister_initial/target/wasm32-unknown-unknown/release/canister_initial_backend_small.wasm";
const BACKEND_WASM_UPGRADED: &str = "tests/canister_upgraded/target/wasm32-unknown-unknown/release/canister_upgraded_backend_small.wasm";

thread_local!(
    static ACTIVE_CANISTER: RefCell<Option<Principal>> = const { RefCell::new(None) };
);

fn set_active_canister(new_canister: Principal) {
    ACTIVE_CANISTER.with(|canister_cell| {
        let mut canister = canister_cell.borrow_mut();
        *canister = Some(new_canister);
    })
}

fn active_canister() -> Principal {
    ACTIVE_CANISTER.with(|canister_cell| {
        let canister = *canister_cell.borrow();
        canister.unwrap()
    })
}

static INIT: Once = Once::new();

fn build_test_projects() {
    INIT.call_once(|| {
        use std::process::Command;
        let _ = Command::new("bash")
            .arg("scripts/build_tests.sh")
            .output()
            .expect("Failed to execute command");
    });
}

fn setup_initial_canister() -> PocketIc {
    build_test_projects();
    let pic = PocketIc::new();

    let wasm = fs::read(BACKEND_WASM).expect("Wasm file not found, run 'dfx build'.");

    let backend_canister = pic.create_canister();

    pic.add_cycles(backend_canister, 2_000_000_000_000_000);

    set_active_canister(backend_canister);

    pic.install_canister(backend_canister, wasm, vec![], None);

    pic.tick();

    pic
}

fn upgrade_canister(pic: &PocketIc) {
    build_test_projects();

    let wasm_upgraded =
        fs::read(BACKEND_WASM_UPGRADED).expect("Wasm file not found, run 'dfx build'.");

    pic.upgrade_canister(active_canister(), wasm_upgraded, vec![], None)
        .unwrap();
}

mod fns {

    use candid::{decode_args, decode_one, encode_one, Principal};
    use pocket_ic::PocketIc;

    use super::active_canister;

    pub(crate) fn greet(pic: &PocketIc, arg: &str) -> String {
        let response = pic
            .query_call(
                active_canister(),
                Principal::anonymous(),
                "greet",
                encode_one(arg).unwrap(),
            )
            .expect("Expected reply");

        let result: String = decode_one(&response).unwrap();
        result
    }

    pub(crate) fn append_text(pic: &PocketIc, filename: &str, content: &str, count: u64) {
        pic.update_call(
            active_canister(),
            Principal::anonymous(),
            "append_text",
            candid::encode_args((filename, content, count)).unwrap(),
        )
        .unwrap();
    }

    pub(crate) fn append_buffer(pic: &PocketIc, content: &str, count: u64) {
        pic.update_call(
            active_canister(),
            Principal::anonymous(),
            "append_buffer",
            candid::encode_args((content, count)).unwrap(),
        )
        .unwrap();
    }

    pub(crate) fn store_buffer(pic: &PocketIc, filename: &str) -> (u64, u64) {
        let response = pic
            .update_call(
                active_canister(),
                Principal::anonymous(),
                "store_buffer",
                candid::encode_one(filename).unwrap(),
            )
            .unwrap();

        let result: (u64, u64) = decode_args(&response).unwrap();

        result
    }

    pub(crate) fn read_text(pic: &PocketIc, filename: &str, offset: i64, size: u64) -> String {
        let response = pic
            .query_call(
                active_canister(),
                Principal::anonymous(),
                "read_text",
                candid::encode_args((filename, offset, size)).unwrap(),
            )
            .unwrap();

        let result: String = decode_one(&response).unwrap();

        result
    }

    pub(crate) fn read_bytes(pic: &PocketIc, filename: &str, offset: i64, size: u64) -> (u64, u64) {
        let response = pic
            .update_call(
                active_canister(),
                Principal::anonymous(),
                "read_bytes",
                candid::encode_args((filename, offset, size)).unwrap(),
            )
            .unwrap();

        let result: (u64, u64) = decode_args(&response).unwrap();

        result
    }

    pub(crate) fn create_files(pic: &PocketIc, path: &str, count: u64) {
        pic.update_call(
            active_canister(),
            Principal::anonymous(),
            "create_files",
            candid::encode_args((path, count)).unwrap(),
        )
        .unwrap();
    }

    pub(crate) fn list_files(pic: &PocketIc, path: &str) -> Vec<String> {
        let response = pic
            .query_call(
                active_canister(),
                Principal::anonymous(),
                "list_files",
                encode_one(path).unwrap(),
            )
            .unwrap();

        let result: Vec<String> = decode_one(&response).unwrap();

        result
    }

    pub(crate) fn check_metadata_binary(pic: &PocketIc) -> String {
        let response = pic
            .query_call(
                active_canister(),
                Principal::anonymous(),
                "check_metadata_binary",
                encode_one(()).unwrap(),
            )
            .unwrap();

        let result: String = decode_one(&response).unwrap();

        result
    }
}

#[test]
fn greet_after_upgrade() {
    let pic = setup_initial_canister();

    let result = fns::greet(&pic, "ICP");

    assert_eq!(result, "Hello, ICP!");

    upgrade_canister(&pic);

    let result = fns::greet(&pic, "ICP");

    assert_eq!(result, "Greetings, ICP!");
}

#[test]
fn writing_10mib() {
    let pic = setup_initial_canister();

    let args = candid::encode_args(("test.txt", 10u64)).unwrap();

    pic.add_cycles(active_canister(), 2_000_000_000_000_000);

    pic.tick();

    let _response = pic
        .update_call(
            active_canister(),
            Principal::anonymous(),
            "write_mib_text",
            args,
        )
        .unwrap();
}

#[test]
fn reading_file_after_upgrade() {
    let pic = setup_initial_canister();

    fns::append_text(&pic, "d1/d2/d3/test1.txt", "test1", 10u64);
    fns::append_text(&pic, "d1/d2/test2.txt", "test2", 10u64);
    fns::append_text(&pic, "test3.txt", "test3", 10u64);
    fns::append_text(&pic, "d1/d2/test2.txt", "abc", 10u64);

    let result = fns::read_text(&pic, "d1/d2/test2.txt", 45i64, 100u64);
    assert_eq!(result, "test2abcabcabcabcabcabcabcabcabcabc");

    // do upgrade
    upgrade_canister(&pic);

    let result = fns::read_text(&pic, "d1/d2/test2.txt", 40i64, 15u64);
    assert_eq!(result, "test2test2abcab");
}

#[test]
fn writing_file_after_upgrade() {
    let pic = setup_initial_canister();

    fns::append_text(&pic, "test1.txt", "test1", 10u64);
    fns::append_text(&pic, "test2.txt", "test2", 10u64);
    fns::append_text(&pic, "test3.txt", "test3", 10u64);
    fns::append_text(&pic, "test2.txt", "abc", 10u64);

    let result = fns::read_text(&pic, "test2.txt", 45i64, 100u64);
    assert_eq!(result, "test2abcabcabcabcabcabcabcabcabcabc");

    // do upgrade
    upgrade_canister(&pic);

    fns::append_text(&pic, "test4.txt", "test4", 10u64);
    fns::append_text(&pic, "test5.txt", "test5", 10u64);
    fns::append_text(&pic, "test6.txt", "test6", 10u64);

    let result = fns::read_text(&pic, "test1.txt", 10i64, 5u64);
    assert_eq!(result, "test1");
    let result = fns::read_text(&pic, "test2.txt", 40i64, 15u64);
    assert_eq!(result, "test2test2abcab");
    let result = fns::read_text(&pic, "test3.txt", 10i64, 5u64);
    assert_eq!(result, "test3");
    let result = fns::read_text(&pic, "test4.txt", 10i64, 5u64);
    assert_eq!(result, "test4");
    let result = fns::read_text(&pic, "test5.txt", 10i64, 5u64);
    assert_eq!(result, "test5");
    let result = fns::read_text(&pic, "test6.txt", 10i64, 5u64);
    assert_eq!(result, "test6");
}

#[test]
fn list_folders_after_upgrade() {
    let pic = setup_initial_canister();

    fns::create_files(&pic, "files", 10);
    fns::create_files(&pic, "files/./f2", 10);

    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt", "f2"},
        fns::list_files(&pic, "files")
    );

    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt"},
        fns::list_files(&pic, "files/f2")
    );

    // do upgrade
    upgrade_canister(&pic);

    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt", "f2"},
        fns::list_files(&pic, "files")
    );

    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt"},
        fns::list_files(&pic, "files/f2")
    );
}

#[test]
fn create_1000_files() {
    let pic = setup_initial_canister();

    let file_count = 250;
    let path1 = "./files1";
    let path2 = "files2//";
    let path3 = "files3";
    let path4 = ".//files4";

    fns::create_files(&pic, path1, file_count);
    fns::create_files(&pic, path2, file_count);
    fns::create_files(&pic, path3, file_count);
    fns::create_files(&pic, path4, file_count);

    let result = fns::list_files(&pic, path2);

    let mut filenames = vec![];

    for i in 0..file_count {
        filenames.push(format!("{i}.txt"))
    }
    assert_eq!(result, filenames);

    let result = fns::list_files(&pic, "");

    let filenames = vec!["mount_file.txt", "files1", "files2", "files3", "files4"];

    assert_eq!(result, filenames);
}

fn no_virtual_names(vec: Vec<String>) -> Vec<String> {
    let mut v = vec;

    v.retain(|v| !(*v).eq("mount_file.txt"));

    v
}

#[test]
fn long_paths_and_file_names() {
    let pic = setup_initial_canister();

    let file_count = 20;

    // maximal file length 255 letters or max possible length with some utf8 chars
    let long_name = "1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDE";
    let long_name2 = "1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCä";
    let long_name3 = "1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF💖567890ABCDEF1234567890A💖";

    let mut path = "".to_string();
    // form long path (total depth - 300 folders)
    for _ in 0..100 {
        path.push_str(long_name);
        path.push('/');
        path.push_str(long_name2);
        path.push('/');
        path.push_str(long_name3);
        path.push('/');
    }

    fns::create_files(&pic, &path, file_count);

    let result = fns::list_files(&pic, &path);

    let mut filenames = vec![];

    for i in 0..file_count {
        filenames.push(format!("{i}.txt"))
    }
    assert_eq!(result, filenames);

    let filenames = vec![long_name];

    let result = fns::list_files(&pic, "");
    let result = no_virtual_names(result);

    assert_eq!(result, filenames);

    // try reading one of the files

    let file_content_start = "0123456789012345678901234567890123456789012345678901234567890123:";
    let file_name = "13.txt";
    let expected_content = format!("{file_content_start}{path}/{file_name}");
    let content_length = expected_content.len();

    let content = read_text(&pic, &format!("{path}/{file_name}"), 0, 100000);
    assert_eq!(expected_content, content);

    let expected_content = "0123:123";
    let content = read_text(
        &pic,
        &format!("{path}/3.txt"),
        60,
        expected_content.len() as u64,
    );
    assert_eq!(expected_content, content);

    let expected_content = "A💖//13.txt";
    let content = read_text(
        &pic,
        &format!("{path}/13.txt"),
        content_length as i64 - expected_content.len() as i64,
        100,
    );

    assert_eq!(expected_content, content);
}

#[test]
fn large_file_read() {
    let pic = setup_initial_canister();

    let filename = "test.txt";

    // create large file
    fns::append_text(&pic, "t1.txt", "abcdef7890", 10_000_000);
    fns::append_text(&pic, "t2.txt", "abcdef7890", 10_000_000);
    fns::append_text(&pic, "t3.txt", "abcdef7890", 10_000_000);
    fns::append_text(&pic, "t4.txt", "abcdef7890", 10_000_000);
    fns::append_text(&pic, filename, "abcdef7890", 10_000_000);

    let (instructions, size) = fns::read_bytes(&pic, filename, 13, 100_000_000);

    println!("instructions {instructions}, size {size}");

    assert!(
        instructions < 3_000_000_000,
        "The call should take less than 3 billion instructions"
    );

    assert_eq!(size, 99_999_987);
}

#[test]
fn large_file_read_after_upgrade() {
    let pic = setup_initial_canister();

    let filename = "mount_file.txt";

    // create large file
    fns::append_text(&pic, "t1.txt", "abcdef7890", 10_000_000);
    fns::append_text(&pic, "t2.txt", "abcdef7890", 10_000_000);
    fns::append_text(&pic, "t3.txt", "abcdef7890", 10_000_000);
    fns::append_text(&pic, "t4.txt", "abcdef7890", 10_000_000);
    fns::append_text(&pic, filename, "abcdef7890", 10_000_000);

    // do upgrade
    upgrade_canister(&pic);

    let (instructions, size) = fns::read_bytes(&pic, filename, 13, 100_000_000);

    println!("instructions {instructions}, size {size}");

    assert!(
        instructions < 3_000_000_000,
        "The call should take less than 3 billion instructions"
    );

    assert_eq!(size, 99_999_987);
}

#[test]
fn large_mounted_file_write() {
    let pic = setup_initial_canister();

    let filename = "mount_file.txt";

    // create large buffer
    fns::append_buffer(&pic, "abcdef7890", 10_000_000);

    let (instructions, size) = fns::store_buffer(&pic, filename);

    println!("instructions {instructions}, size {size}");

    assert!(
        instructions < 14_000_000_000,
        "The call should take less than 3 billion instructions"
    );

    assert_eq!(size, 100_000_000);
}

#[test]
fn large_file_write() {
    let pic = setup_initial_canister();

    let filename = "some_file.txt";

    // create large buffer
    fns::append_buffer(&pic, "abcdef7890", 10_000_000);

    let (instructions, size) = fns::store_buffer(&pic, filename);

    println!("instructions {instructions}, size {size}");

    assert!(
        instructions < 14_000_000_000,
        "The call should take less than 3 billion instructions"
    );

    assert_eq!(size, 100_000_000);
}

#[test]
fn large_file_second_write() {
    let pic = setup_initial_canister();

    let filename = "some_file.txt";

    // create large buffer
    fns::append_buffer(&pic, "abcdef7890", 10_000_000);

    fns::store_buffer(&pic, filename);

    let (instructions, size) = fns::store_buffer(&pic, filename);

    println!("instructions {instructions}, size {size}");

    assert!(
        instructions < 14_000_000_000,
        "The call should take less than 3 billion instructions"
    );

    assert_eq!(size, 100_000_000);
}

#[test]
fn check_metadata_binary() {
    let pic = setup_initial_canister();

    // we should track any changes that affect Metadata binary representation in memory
    // as it is stored directly without explicit serialization for the sake of performance.
    let bin = fns::check_metadata_binary(&pic);

    // object memory is prefilled with 0xfa explicitly in fns::check_metadata_binary to ensure stable test
    assert_eq!(&bin,
        "030000000000000004fafafafafafafa06000000000000000800000000000000410000000000000042000000000000004300000000000000010000000c000000010000000d00000002fafafafafafafa0100000000000000abcd000000000000");
}

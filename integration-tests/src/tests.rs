//! This is a sample test file, it shows an example of how to create actual tests.
//! The file is only generated once and won't be overwritten.

use ic_test::IcpTest;

use crate::{
    bindings::canister_upgraded_backend,
    test_setup::{self, Env},
};

async fn upgrade_canister(env: &Env) {
    let wasm = canister_upgraded_backend::wasm().expect("Wasm not found for the upgrade_canister");

    let user = env.icp_test.icp.default_user().principal;

    env.icp_test
        .icp
        .pic
        .upgrade_canister(
            env.canister_initial_backend.canister_id,
            wasm,
            vec![],
            Some(user),
        )
        .await
        .expect("Failed to upgrade canister!");
}

#[tokio::test]
async fn greet_after_upgrade() {
    let env = test_setup::setup(IcpTest::new().await).await;

    let result = env
        .canister_initial_backend
        .greet("ICP".to_string())
        .call()
        .await;

    assert_eq!(result, "Hello, ICP!");

    upgrade_canister(&env).await;

    let result = env
        .canister_initial_backend
        .greet("ICP".to_string())
        .call()
        .await;

    assert_eq!(result, "Greetings, ICP!");
}

#[tokio::test]
async fn writing_10mib() {
    let env = test_setup::setup(IcpTest::new().await).await;

    env.icp_test.tick().await;

    let _result = env
        .canister_initial_backend
        .write_mib_text("test.txt".to_string(), 10)
        .call()
        .await;
}

#[tokio::test]
async fn reading_file_after_upgrade() {
    let env = test_setup::setup(IcpTest::new().await).await;

    env.canister_initial_backend
        .append_text("d1/d2/d3/test1.txt".to_string(), "test1".to_string(), 10u64)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("d1/d2/test2.txt".to_string(), "test2".to_string(), 10u64)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("test3.txt".to_string(), "test3".to_string(), 10u64)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("d1/d2/test2.txt".to_string(), "abc".to_string(), 10u64)
        .call()
        .await;

    let result = env
        .canister_initial_backend
        .read_text("d1/d2/test2.txt".to_string(), 45i64, 100u64)
        .call()
        .await;

    assert_eq!(result, "test2abcabcabcabcabcabcabcabcabcabc");

    upgrade_canister(&env).await;

    let result = env
        .canister_initial_backend
        .read_text("d1/d2/test2.txt".to_string(), 40i64, 15u64)
        .call()
        .await;

    assert_eq!(result, "test2test2abcab");
}

#[tokio::test]
async fn writing_file_after_upgrade() {
    let env = test_setup::setup(IcpTest::new().await).await;

    env.canister_initial_backend
        .append_text("test1.txt".to_string(), "test1".to_string(), 10u64)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("test2.txt".to_string(), "test2".to_string(), 10u64)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("test3.txt".to_string(), "test3".to_string(), 10u64)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("test2.txt".to_string(), "abc".to_string(), 10u64)
        .call()
        .await;

    let result = env
        .canister_initial_backend
        .read_text("test2.txt".to_string(), 45i64, 100u64)
        .call()
        .await;

    assert_eq!(result, "test2abcabcabcabcabcabcabcabcabcabc");

    upgrade_canister(&env).await;

    env.canister_initial_backend
        .append_text("test4.txt".to_string(), "test4".to_string(), 10u64)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("test5.txt".to_string(), "test5".to_string(), 10u64)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("test6.txt".to_string(), "test6".to_string(), 10u64)
        .call()
        .await;

    let result = env
        .canister_initial_backend
        .read_text("test1.txt".to_string(), 10i64, 5u64)
        .call()
        .await;
    assert_eq!(result, "test1");

    let result = env
        .canister_initial_backend
        .read_text("test2.txt".to_string(), 40i64, 15u64)
        .call()
        .await;
    assert_eq!(result, "test2test2abcab");

    let result = env
        .canister_initial_backend
        .read_text("test3.txt".to_string(), 10i64, 5u64)
        .call()
        .await;
    assert_eq!(result, "test3");

    let result = env
        .canister_initial_backend
        .read_text("test4.txt".to_string(), 10i64, 5u64)
        .call()
        .await;
    assert_eq!(result, "test4");

    let result = env
        .canister_initial_backend
        .read_text("test4.txt".to_string(), 10i64, 5u64)
        .call()
        .await;
    assert_eq!(result, "test4");

    let result = env
        .canister_initial_backend
        .read_text("test4.txt".to_string(), 10i64, 5u64)
        .call()
        .await;
    assert_eq!(result, "test4");
}

#[tokio::test]
async fn list_folders_after_upgrade() {
    let env = test_setup::setup(IcpTest::new().await).await;

    env.canister_initial_backend
        .create_files("files".to_string(), 10)
        .call()
        .await;
    env.canister_initial_backend
        .create_files("files/./f2".to_string(), 10)
        .call()
        .await;

    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt", "f2"},
        env.canister_initial_backend
            .list_files("files".to_string())
            .call()
            .await
    );

    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt"},
        env.canister_initial_backend
            .list_files("files/f2".to_string())
            .call()
            .await
    );

    // do upgrade
    upgrade_canister(&env).await;

    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt", "f2"},
        env.canister_initial_backend
            .list_files("files".to_string())
            .call()
            .await
    );

    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt"},
        env.canister_initial_backend
            .list_files("files/f2".to_string())
            .call()
            .await
    );
}

#[tokio::test]
async fn create_1000_files() {
    let env = test_setup::setup(IcpTest::new().await).await;

    let file_count = 250;
    let path1 = "./files1";
    let path2 = "files2//";
    let path3 = "files3";
    let path4 = ".//files4";

    env.canister_initial_backend
        .create_files(path1.to_string(), file_count)
        .call()
        .await;

    env.canister_initial_backend
        .create_files(path2.to_string(), file_count)
        .call()
        .await;

    env.canister_initial_backend
        .create_files(path3.to_string(), file_count)
        .call()
        .await;

    env.canister_initial_backend
        .create_files(path4.to_string(), file_count)
        .call()
        .await;

    let result = env
        .canister_initial_backend
        .list_files(path2.to_string())
        .call()
        .await;

    let mut filenames = vec![];

    for i in 0..file_count {
        filenames.push(format!("{i}.txt"))
    }
    assert_eq!(result, filenames);

    let result = env
        .canister_initial_backend
        .list_files("".to_string())
        .call()
        .await;

    let filenames = vec!["mount_file.txt", "files1", "files2", "files3", "files4"];

    assert_eq!(result, filenames);
}

fn no_virtual_names(vec: Vec<String>) -> Vec<String> {
    let mut v = vec;

    v.retain(|v| !(*v).eq("mount_file.txt"));

    v
}

#[tokio::test]
async fn long_paths_and_file_names() {
    let env = test_setup::setup(IcpTest::new().await).await;

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

    env.canister_initial_backend
        .create_files(path.to_string(), file_count)
        .call()
        .await;

    let result = env
        .canister_initial_backend
        .list_files(path.to_string())
        .call()
        .await;

    let mut filenames = vec![];

    for i in 0..file_count {
        filenames.push(format!("{i}.txt"))
    }
    assert_eq!(result, filenames);

    let filenames = vec![long_name];

    let result = env
        .canister_initial_backend
        .list_files("".to_string())
        .call()
        .await;
    let result = no_virtual_names(result);

    assert_eq!(result, filenames);

    // try reading one of the files

    let file_content_start = "0123456789012345678901234567890123456789012345678901234567890123:";
    let file_name = "13.txt";
    let expected_content = format!("{file_content_start}{path}/{file_name}");
    let content_length = expected_content.len();

    let content = env
        .canister_initial_backend
        .read_text(format!("{path}/{file_name}"), 0, 100000)
        .call()
        .await;
    assert_eq!(expected_content, content);

    let expected_content = "0123:123";
    let content = env
        .canister_initial_backend
        .read_text(format!("{path}/3.txt"), 60, expected_content.len() as u64)
        .call()
        .await;
    assert_eq!(expected_content, content);

    let expected_content = "A💖//13.txt";
    let content = env
        .canister_initial_backend
        .read_text(
            format!("{path}/13.txt"),
            content_length as i64 - expected_content.len() as i64,
            100,
        )
        .call()
        .await;

    assert_eq!(expected_content, content);
}

#[tokio::test]
async fn large_file_read() {
    let env = test_setup::setup(IcpTest::new().await).await;

    let filename = "test.txt";

    // create large file
    env.canister_initial_backend
        .append_text("t1.txt".to_string(), "abcdef7890".to_string(), 10_000_000)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("t2.txt".to_string(), "abcdef7890".to_string(), 10_000_000)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("t3.txt".to_string(), "abcdef7890".to_string(), 10_000_000)
        .call()
        .await;
    env.canister_initial_backend
        .append_text("t4.txt".to_string(), "abcdef7890".to_string(), 10_000_000)
        .call()
        .await;
    env.canister_initial_backend
        .append_text(filename.to_string(), "abcdef7890".to_string(), 10_000_000)
        .call()
        .await;

    // TODO: ic-test does not support functions returning tuples

    /*
    let (instructions, size) = env
        .canister_initial_backend
        .read_bytes(filename.to_string(), 13, 100_000_000)
        .call()
        .await;

    println!("instructions {instructions}, size {size}");

    assert!(
        instructions < 3_000_000_000,
        "The call should take less than 3 billion instructions"
    );

    assert_eq!(size, 99_999_987);
    */
}

/* TODO: tuple support needs to be fixed in ic-test
#[test]
fn large_file_read_after_upgrade() {
    let env = test_setup::setup(IcpTest::new().await).await;

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

#[tokio::test]
async fn large_mounted_file_write() {
    let env = test_setup::setup(IcpTest::new().await).await;

    let filename = "mount_file.txt";

    // create large buffer
    env.canister_initial_backend
        .append_buffer("abcdef7890".to_string(), 10_000_000)
        .call()
        .await;

    let (instructions, size) = fns::store_buffer(&pic, filename);

    println!("instructions {instructions}, size {size}");

    assert!(
        instructions < 14_000_000_000,
        "The call should take less than 3 billion instructions"
    );

    assert_eq!(size, 100_000_000);
}

#[tokio::test]
async fn large_file_write() {
    let env = test_setup::setup(IcpTest::new().await).await;

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

#[tokio::test]
async fn large_file_second_write() {
    let env = test_setup::setup(IcpTest::new().await).await;

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
*/

#[tokio::test]
async fn check_metadata_binary() {
    let env = test_setup::setup(IcpTest::new().await).await;

    // we should track any changes that affect Metadata binary representation in memory
    // as it is stored directly without explicit serialization for the sake of performance.
    let bin = env
        .canister_initial_backend
        .check_metadata_binary()
        .call()
        .await;

    // object memory is prefilled with 0xfa explicitly in fns::check_metadata_binary to ensure stable test
    assert_eq!(
        &bin,
        "030000000000000004fafafafafafafa06000000000000000800000000000000410000000000000042000000000000004300000000000000010000000c000000010000000d00000002fafafafafafafa0100000000000000abcd000000000000"
    );
}

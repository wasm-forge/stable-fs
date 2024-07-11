use candid::Principal;
use pocket_ic::{common::rest::InstanceId, PocketIc};
use std::{cell::RefCell, fs};

const BACKEND_WASM: &str = "src/tests/fs_benchmark_test/target/wasm32-unknown-unknown/release/fs_benchmark_test_backend_small.wasm";
const BACKEND_WASM_UPGRADED: &str = "src/tests/demo_test_upgraded/target/wasm32-unknown-unknown/release/demo_test_upgraded_backend_small.wasm";


thread_local!(
    static POCKET_IC: RefCell<PocketIc> = RefCell::new(PocketIc::new());
    static ACTIVE_CANISTER: RefCell<Option<Principal>> = RefCell::new(None);
);

pub struct PIc {
    /// The unique ID of this PocketIC instance.
    pub instance_id: InstanceId,
    
    /* 
    http_gateway: Option<HttpGatewayInfo>,
    topology: Topology,
    server_url: Url,
    reqwest_client: reqwest::blocking::Client,
    _log_guard: Option<WorkerGuard>,
    */
}

thread_local!(
    static ID: RefCell<PocketIc> = RefCell::new(PocketIc::new());
);

#[test]
fn test_id() {

    ID.with(|id_cell| {
        let mut id = id_cell.borrow_mut();

        //println!("hello {:?}", *id);

    });
}

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

fn setup_test_projects() {
    use std::process::Command;
    let _ = Command::new("bash")
        .arg("build_tests.sh")
        .output()
        .expect("Failed to execute command");
}

fn setup_initial_canister() {
    setup_test_projects();

    POCKET_IC.with(|pic_cell| {
        let pic = pic_cell.borrow();

/*         let wasm = fs::read(BACKEND_WASM).expect("Wasm file not found, run 'dfx build'.");

        let backend_canister = pic.create_canister();

         set_active_canister(backend_canister);

        pic.install_canister(backend_canister, wasm, vec![], None);
    
        pic.add_cycles(backend_canister, 2_000_000_000);

        pic.tick();
         */
    });

    
}

fn upgrade_canister() {
    setup_test_projects();

    POCKET_IC.with(|pic_cell| {
        let pic = pic_cell.borrow_mut();

        let wasm_upgraded =
        fs::read(BACKEND_WASM_UPGRADED).expect("Wasm file not found, run 'dfx build'.");

        pic.upgrade_canister(active_canister(), wasm_upgraded, vec![], None).unwrap();

        pic.tick();
    });
}

mod fns {
/* 
    use candid::{decode_one, encode_one, Principal};
    use pocket_ic::WasmResult;

    use super::{active_canister, POCKET_IC};
    pub(crate) fn greet(arg: &str) -> String {

        POCKET_IC.with(|pic_cell| {
            let pic = pic_cell.borrow_mut();
    
            let Ok(WasmResult::Reply(response)) = pic.query_call(
                active_canister(),
                Principal::anonymous(),
                "greet",
                encode_one(arg).unwrap(),
            ) else {
                panic!("Expected reply");
            };
            let result: String = decode_one(&response).unwrap();
            result
        })
    
    }

    pub(crate) fn append_text(filename: &str, content: &str, count: u64) {

        POCKET_IC.with(|pic_cell| {
            let pic = pic_cell.borrow_mut();

            pic
            .update_call(
                active_canister(),
                Principal::anonymous(),
                "append_text",
                candid::encode_args((filename, content, count)).unwrap(),
            )
            .unwrap();

        })
    
    }

    pub(crate) fn read_text(filename: &str, offset: i64, size: u64) -> String {

        POCKET_IC.with(|pic_cell| {
            let pic = pic_cell.borrow_mut();

            let response = pic
            .query_call(
                active_canister(),
                Principal::anonymous(),
                "read_text",
                candid::encode_args((filename, offset, size)).unwrap(),
            )
            .unwrap();
    
            if let WasmResult::Reply(response) = response {
                let result: String = decode_one(&response).unwrap();

                return result
            } else {
                panic!("unintended call failure!");
            }
        })
    }


    pub(crate) fn create_files(path: &str, count: u64) {

        POCKET_IC.with(|pic_cell| {
            let pic = pic_cell.borrow_mut();

            pic
        .update_call(
            active_canister(),
            Principal::anonymous(),
            "create_files",
            candid::encode_args((path, count)).unwrap(),
        )
        .unwrap();

        })
    }

    pub(crate) fn list_files(path: &str) -> Vec<String> {

        POCKET_IC.with(|pic_cell| {

            let pic = pic_cell.borrow_mut();

            let response = pic
            .query_call(
                active_canister(),
                Principal::anonymous(),
                "list_files",
                encode_one(path).unwrap(),
            )
            .unwrap();
        
            if let WasmResult::Reply(response) = response {
                let result: Vec<String> = decode_one(&response).unwrap();

                return result
            } else {
                panic!("unintended call failure!");
            }
        })
    }
*/

}


#[test]
fn greet_after_upgrade() {

/*     POCKET_IC.with(|pic_cell| {
        let pic = pic_cell.borrow();

         let wasm = fs::read(BACKEND_WASM).expect("Wasm file not found, run 'dfx build'.");

        let backend_canister = pic.create_canister();

         set_active_canister(backend_canister);

        pic.install_canister(backend_canister, wasm, vec![], None);
    
        pic.add_cycles(backend_canister, 2_000_000_000);

        pic.tick();
         
    });*/


   // setup_initial_canister();

  
    /*
    setup_initial_canister();

    let result = fns::greet("ICP");

    assert_eq!(result, "Hello, ICP!");

    upgrade_canister();

    let result = fns::greet("ICP");

    assert_eq!(result, "Greetings, ICP!");

    */
}

/*
#[test]
fn writing_10mib() {
    setup_initial_canister();

    return;

    let args = candid::encode_args(("test.txt", 10u64)).unwrap();

    POCKET_IC.with(|pic_cell| {
        let pic = pic_cell.borrow_mut();

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
    });

}

#[test]
fn reading_file_after_upgrade() {
    setup_initial_canister();

    fns::append_text("d1/d2/d3/test1.txt", "test1", 10u64);
    fns::append_text("d1/d2/test2.txt", "test2", 10u64);
    fns::append_text("test3.txt", "test3", 10u64);
    fns::append_text("d1/d2/test2.txt", "abc", 10u64);

    let result = fns::read_text("d1/d2/test2.txt", 45i64, 100u64);
    assert_eq!(result, "test2abcabcabcabcabcabcabcabcabcabc");
 
    // do upgrade
    upgrade_canister();

    let result = fns::read_text("d1/d2/test2.txt", 45i64, 15u64);
    assert_eq!(result, "test2test2abcab");

}

#[test]
fn list_folders_after_upgrade() {

    setup_initial_canister();

    fns::create_files("files", 10);
    fns::create_files("files/f2", 10);

    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt", "f2"},
        fns::list_files("files")
    );
    
    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt"},
        fns::list_files("files/f2")
    );

    // do upgrade
    upgrade_canister();

    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt", "f2"},
        fns::list_files("files")
    );
    
    assert_eq!(
        vec! {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt"},
        fns::list_files("files/f2")
    );

}


#[test]
fn creating_1000_files() {
    setup_initial_canister();

    let file_count = 10;
    let path = "files";

    fns::create_files(path, file_count);

    let result = fns::list_files(path);

    let mut filenames = vec![];

    for i in 0..file_count {
        filenames.push(format!("{path}{i}.txt"))
    }

    assert_eq!(result, filenames);

}

*/
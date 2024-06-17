#[ic_cdk::query]
fn greet(name: String) -> String {
    format!("Greetings, {}!", name)
}

use async_graphql::*;

#[derive(SimpleObject)]
pub struct User {
    id: i64,
    name: String,
}

//implement for user a new method
impl User {
    pub fn new(id: i64, name: &str) -> Self {
        Self {
            id,
            name: name.to_string(),
        }
    }
}

pub fn create_greet(name : &str) -> String {
    format!("Hello, {}!", name)
}

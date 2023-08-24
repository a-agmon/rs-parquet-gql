use async_graphql::*;
use crate::models::user::{User, create_greet};
pub mod user;
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn greet(&self, name: String) -> String {
        create_greet(&name)
    }
    async fn users(&self) -> Vec<User> {
        vec![
            User::new(1, "foo"),
            User::new(2, "bar"),
            User::new(3, "baz"),
        ]
    }
}
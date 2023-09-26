use async_graphql::*;

#[derive(SimpleObject)]
pub struct User {
    user_id: String,
    acc_id: String,
    email: String,
    department: String,
    created: i64,
}

//"user_id", "acc_id", "email", "department", "created_at"
impl User {
    pub fn new(
        user_id: &str,
        acc_id: &str,
        email: &str,
        department: &str,
        created_at: i64,
    ) -> Self {
        Self {
            user_id: user_id.to_string(),
            acc_id: acc_id.to_string(),
            email: email.to_string(),
            department: department.to_string(),
            created: created_at,
        }
    }
}

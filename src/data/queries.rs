pub const GET_USERS: &str =
    "SELECT user_id, acc_id, email, department, created_at FROM users LIMIT 100";
pub const GET_USERS_BY_DOMAIN: &str =
    "SELECT id, account_id, email, department, created_date FROM users WHERE email LIKE ?";
pub const GET_USERS_BY_EMAIL: &str =
    "SELECT id, account_id, email, department, created_date FROM users WHERE email = ?";

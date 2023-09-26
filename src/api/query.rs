use super::models::User;
use crate::data::repository::DAO;
use async_graphql::*;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn users<'a>(&self, ctx: &'a Context<'_>) -> Vec<User> {
        let dao = ctx.data::<DAO>().unwrap();
        dao.get_users().await
    }

    async fn users_by_domain<'a>(&self, ctx: &'a Context<'_>, domain: String) -> Vec<User> {
        let dao = ctx.data::<DAO>().unwrap();
        dao.get_user_by_domain(domain.as_str()).await
    }

    async fn users_by_email<'a>(&self, ctx: &'a Context<'_>, email: String) -> Vec<User> {
        let dao = ctx.data::<DAO>().unwrap();
        dao.get_user_by_email(email.as_str()).await
    }
}

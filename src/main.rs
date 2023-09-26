use anyhow::Ok;
use async_graphql::{http::GraphiQLSource, EmptyMutation, EmptySubscription, Schema};
use async_graphql_axum::GraphQL;
use axum::{
    response::{self, IntoResponse},
    routing::get,
    Router, Server,
};

mod api;
mod awsstore;
mod configuration;
mod data;
use api::query::QueryRoot;
use awsstore::S3Client;
use configuration::AppConfiguration;
use data::store::ParquetDB;

async fn graphiql() -> impl IntoResponse {
    response::Html(
        GraphiQLSource::build()
            .endpoint("/")
            .subscription_endpoint("/ws")
            .finish(),
    )
}

async fn initialize_parquetdb() -> anyhow::Result<ParquetDB> {
    println!("starting server");
    let config = AppConfiguration::global();
    let s3_client = S3Client::new().await;
    let files = s3_client
        .list_s3_folders(&config.s3_bucket, &config.s3_key, &config.s3_glob)
        .await
        .unwrap();
    println!("files: {:?}", files);
    println!("connecting to db");
    let db = ParquetDB::new();
    db.load_table(
        &config.s3_bucket,
        files,
        "users",
        config.fields.iter().map(|s| s.as_str()).collect(),
    )
    .await?;
    Ok(db)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = AppConfiguration::load("config.toml".to_string());
    AppConfiguration::set(config);

    let db = initialize_parquetdb().await?;
    let dao = data::repository::DAO::new(db);
    println!("Starting GraphiQL IDE: http://localhost:8888");
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(dao)
        .finish();
    let app = Router::new().route(
        "/",
        get(graphiql).post_service(GraphQL::new(schema.clone())),
    );
    const LOCAL_PORT: u16 = 8888;
    println!("GraphiQL IDE: http://localhost:{LOCAL_PORT}");
    Server::bind(&format!("127.0.0.1:{LOCAL_PORT}").parse().unwrap())
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

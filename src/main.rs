use actix_web::{guard, web, App, HttpResponse, HttpServer};
use async_graphql::{http::GraphiQLSource, EmptyMutation, EmptySubscription, Schema};
use async_graphql_actix_web::GraphQL;

mod models;
use models::QueryRoot;
mod awsstore;
use awsstore::S3Client;
mod data;
use data::ParquetDB;

async fn index_graphiql() -> io::Result<HttpResponse> {
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(
            GraphiQLSource::build()
                .endpoint("/")
                .subscription_endpoint("/")
                .finish(),
        ))
}

//pub type Result<T, E = Error> = std::result::Result<T, E>;

use std::io;
#[actix_web::main]
async fn main() -> io::Result<()> {
    println!("starting server");
    let bucket = "af-eu-west-1-prod-bdnd-dwh";
    let key = "bi-agg-data/sega_neo4j_account_app/sega.db/current_hq_users/data";
    let glob_str = "*4574-95af-bded29dbc837-00001.parquet";
    let s3_client = S3Client::new().await;
    let files = s3_client
        .list_s3_folders(bucket, key, glob_str)
        .await
        .unwrap();
    println!("files: {:?}", files);

    println!("connecting to db");

    let db = ParquetDB::new();
    db.load_table(bucket, files, "users")
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    db.query("select * from users limit 10")
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    println!("Starting GraphiQL IDE: http://localhost:8000");

    HttpServer::new(move || {
        let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
            //    .data(Storage::default())
            .finish();
        App::new()
            .service(
                web::resource("/")
                    .guard(guard::Post())
                    .to(GraphQL::new(schema)),
            )
            .service(web::resource("/").guard(guard::Get()).to(index_graphiql))
    })
    .bind("127.0.0.1:8888")?
    .run()
    .await
}

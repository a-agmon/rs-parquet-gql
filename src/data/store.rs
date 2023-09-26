use anyhow::{Ok, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use url::Url;

pub struct ParquetDB {
    ctx: SessionContext,
}

impl ParquetDB {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }
    pub async fn load_table(
        &self,
        bucket_name: &str,
        parquet_files: Vec<String>,
        table_name: &str,
        fields: Vec<&str>,
    ) -> Result<(), anyhow::Error> {
        let bucket_path = format!("s3://{bucket_name}");
        let s3_bucket_url = Url::parse(&bucket_path).unwrap();
        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .build()?;
        self.ctx
            .runtime_env()
            .register_object_store(&s3_bucket_url, Arc::new(s3));
        let table_paths = parquet_files
            .iter()
            .map(|filename| {
                ListingTableUrl::parse(format!("s3://{bucket_name}/{filename}")).unwrap()
            })
            .collect();
        let table_config = ListingTableConfig::new_with_multi_paths(table_paths)
            .infer(&self.ctx.state())
            .await?;
        let table_provider = Arc::new(ListingTable::try_new(table_config).unwrap());
        let temp_table_name = format!("temp_{}", table_name);
        self.ctx
            .register_table(TableReference::from(&temp_table_name), table_provider)
            .unwrap();
        let ddl_query = format!(
            "create table {} as select {} from {}",
            table_name,
            fields.join(", "),
            &temp_table_name
        );
        println!("running ddl_query: {}", ddl_query);
        self.ctx.sql(&ddl_query).await?;
        println!("finished running ddl_query: {}", ddl_query);
        Ok(())
    }

    pub async fn query_fetch_batches(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(query).await?;
        let results = df.collect().await?;
        Ok(results)
    }
}

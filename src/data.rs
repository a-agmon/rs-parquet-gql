use anyhow::{Ok, Result};
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::*;
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
            "create table {} as select * from {}",
            table_name, &temp_table_name
        );
        self.ctx.sql(&ddl_query).await?;
        Ok(())
    }

    pub async fn query(&self, query: &str) -> Result<()> {
        let df = self.ctx.sql(query).await?;
        let results = df.collect().await?;
        results.iter().for_each(|batch| {
            (0..batch.num_rows()).for_each(|row_index| {
                (0..batch.num_columns()).for_each(|column_index| {
                    let column = batch.column(column_index);
                    match column.data_type() {
                        DataType::Int64 => {
                            let v = Int64Array::extract_value(&column, row_index);
                            println!("Field {}: {}", column_index, v);
                        }
                        DataType::Utf8 => {
                            let v = StringArray::extract_value(&column, row_index);
                            println!("Field {}: {}", column_index, v);
                        }
                        DataType::Timestamp(_, _) => {
                            let v = TimestampMicrosecondArray::extract_value(&column, row_index);
                            println!("Field {}: {}", column_index, v);
                        }
                        _ => {
                            println!("Field {}: {:?}", column_index, column.data_type());
                        }
                    }
                });
            });
        });
        Ok(())
    }
}

trait ExtractValue {
    type OutputType;
    fn extract_value(column: &ArrayRef, row_index: usize) -> Self::OutputType;
}
impl ExtractValue for Int64Array {
    type OutputType = i64;

    fn extract_value(column: &ArrayRef, row_index: usize) -> Self::OutputType {
        let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
        array.value(row_index)
    }
}
impl ExtractValue for StringArray {
    type OutputType = String;

    fn extract_value(column: &ArrayRef, row_index: usize) -> Self::OutputType {
        let array = column.as_any().downcast_ref::<StringArray>().unwrap();
        array.value(row_index).to_string()
    }
}

impl ExtractValue for TimestampMicrosecondArray {
    type OutputType = i64;
    fn extract_value(column: &ArrayRef, row_index: usize) -> Self::OutputType {
        let array = column
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        array.value(row_index)
    }
}

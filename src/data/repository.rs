use super::queries;
use super::store::ParquetDB;
use crate::api::models::User;
use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;

pub struct DAO {
    pub db: ParquetDB,
}

impl DAO {
    pub fn new(db: ParquetDB) -> Self {
        Self { db }
    }

    pub async fn get_user_by_domain(&self, domain: &str) -> Vec<User> {
        let query = queries::GET_USERS_BY_DOMAIN.replace("?", &format!("'%@{}%'", domain));
        let records = self.db.query_fetch_batches(query.as_str()).await.unwrap();
        Self::generate_users_from_batch(&records).await.unwrap()
    }

    pub async fn get_user_by_email(&self, email: &str) -> Vec<User> {
        let query = queries::GET_USERS_BY_EMAIL.replace("?", &format!("'{}'", email));
        let records = self.db.query_fetch_batches(query.as_str()).await.unwrap();
        Self::generate_users_from_batch(&records).await.unwrap()
    }

    pub async fn get_users(&self) -> Vec<User> {
        let records = self
            .db
            .query_fetch_batches(queries::GET_USERS)
            .await
            .unwrap();
        Self::generate_users_from_batch(&records).await.unwrap()
    }

    async fn generate_users_from_batch(records: &Vec<RecordBatch>) -> anyhow::Result<Vec<User>> {
        let users = records
            .iter()
            .flat_map(|batch| {
                let ar_user_id = batch.column_by_name("id").unwrap();
                let ar_acc_id = batch.column_by_name("account_id").unwrap();
                let ar_email = batch.column_by_name("email").unwrap();
                let ar_department = batch.column_by_name("department").unwrap();
                let ar_created = batch.column_by_name("created_date").unwrap();
                (0..batch.num_rows()).map(|row_index| {
                    let userid = StringArray::get_val(ar_user_id, row_index);
                    let accid = StringArray::get_val(ar_acc_id, row_index);
                    let email = StringArray::get_val(ar_email, row_index);
                    let department = StringArray::get_val(ar_department, row_index);
                    let created = TimestampMicrosecondArray::get_val(ar_created, row_index);
                    User::new(&userid, &accid, &email, &department, created)
                })
            })
            .collect::<Vec<User>>();
        Ok(users)
    }
}

pub trait ArrowTransformer {
    type OutputType;
    fn get_val(column: &ArrayRef, row_index: usize) -> Self::OutputType;
}
impl ArrowTransformer for Int64Array {
    type OutputType = i64;

    fn get_val(column: &ArrayRef, row_index: usize) -> Self::OutputType {
        let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
        array.value(row_index)
    }
}
impl ArrowTransformer for StringArray {
    type OutputType = String;

    fn get_val(column: &ArrayRef, row_index: usize) -> Self::OutputType {
        let array = column.as_any().downcast_ref::<StringArray>().unwrap();
        array.value(row_index).to_string()
    }
}

impl ArrowTransformer for TimestampMicrosecondArray {
    type OutputType = i64;
    fn get_val(column: &ArrayRef, row_index: usize) -> Self::OutputType {
        let array = column
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        array.value(row_index)
    }
}

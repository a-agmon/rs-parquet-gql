use aws_sdk_s3 as s3;
use glob::Pattern;

pub struct S3Client {
    client: s3::Client,
}

impl S3Client {
    pub async fn new() -> Self {
        let config = ::aws_config::load_from_env().await;
        let s3client = s3::Client::new(&config);
        S3Client { client: s3client }
    }

    pub async fn list_s3_folders(
        &self,
        bucket: &str,
        key: &str,
        glob_str: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let resp = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(key)
            //.start_after(key)
            .send()
            .await?;

        let glob_pattern = Pattern::new(glob_str)?;

        let files: Vec<String> = resp
            .contents
            .unwrap_or_default()
            .iter()
            .filter_map(|s3_obj| {
                let key = s3_obj.key.as_ref().unwrap();
                match glob_pattern.matches(key) {
                    true => Some(key.to_string()),
                    false => None,
                }
            })
            .collect();

        return Ok(files);
    }
}

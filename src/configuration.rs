use config::{Config, File};
use once_cell::sync::OnceCell;
use serde::Deserialize;


static GLOBAL_CONF: OnceCell<AppConfiguration> = OnceCell::new();

#[derive(Debug, Deserialize)]
pub struct AppConfiguration {
    pub s3_bucket: String,
    pub s3_key: String,
    pub s3_glob: String,
    pub fields: Vec<String>,
}
impl AppConfiguration {
    pub fn load(filename: String) -> Self {
        let settings = Config::builder()
            .add_source(File::with_name(&filename))
            .build()
            .unwrap();
        settings.get("configuration").unwrap()
    }
    pub fn global() -> &'static Self {
        GLOBAL_CONF.get().expect("AppConfiguration not initialized")
    }
    pub fn set(conf: AppConfiguration) {
        GLOBAL_CONF.set(conf).expect("AppConfiguration already initialized");
    }
}

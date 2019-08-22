use parquet::file::metadata::FileMetaDataPtr;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct FileMetadata {
    pub path: String,
    pub version: i32,
    pub created_by: String,
    pub num_columns: i64,
    pub num_rows: i64,
}

impl FileMetadata {
    pub fn from_parquet(path: String, parquet_file_metadata: FileMetaDataPtr) -> Self {
        FileMetadata {
            path: path,
            version: parquet_file_metadata.version(),
            created_by: parquet_file_metadata.created_by().clone().unwrap_or("<unknown>".to_owned()),
            num_columns: parquet_file_metadata.schema_descr().num_columns() as i64,
            num_rows: parquet_file_metadata.num_rows(),
        }
    }
}

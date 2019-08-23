use parquet::file::metadata::FileMetaDataPtr;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct FileMetadata {
    pub path: String,
    pub version: i32,
    pub created_by: String,
    pub column_count: i64,
    pub row_count: i64,
}

impl FileMetadata {
    pub fn from_parquet(path: String, parquet_file_metadata: FileMetaDataPtr) -> Self {
        FileMetadata {
            path: path,
            version: parquet_file_metadata.version(),
            created_by: parquet_file_metadata.created_by().clone().unwrap_or("<unknown>".to_owned()),
            column_count: parquet_file_metadata.schema_descr().num_columns() as i64,
            row_count: parquet_file_metadata.num_rows(),
        }
    }
}

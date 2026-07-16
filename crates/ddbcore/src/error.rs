use thiserror::Error;

#[derive(Debug, Error)]
pub enum DdbCoreError {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("schema reflection failed: {0}")]
    Reflection(String),

    #[error("query execution failed: {0}")]
    Query(String),

    #[error("bulk write failed: {0}")]
    BulkWrite(String),

    #[error("DDL operation failed: {0}")]
    Ddl(String),

    #[error("unsupported operation for this engine: {0}")]
    Unsupported(String),
}

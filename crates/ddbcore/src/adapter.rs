use async_trait::async_trait;
use futures_core::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::error::DdbCoreError;
use crate::schema::{Catalog, CheckConstraint, ForeignKey, IndexColumn, PrimaryKey, Table, TableRef, UniqueConstraint};
use crate::types::DataType;
use crate::value::{Row, Value};

/// How the connection is secured in transit. `ClearText` is the type-level
/// default — callers (e.g. Readactus's free tier) may deliberately choose
/// it; DDBCore itself supports both equally and does not judge the choice.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum EncryptionMode {
    #[default]
    ClearText,
    Tls { verify_cert: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub encryption: EncryptionMode,
    /// Enforced at the connection/session level where the engine supports
    /// it (e.g. Postgres `default_transaction_read_only`), not just a
    /// convention the caller has to remember to honor.
    pub read_only: bool,
}

/// A stream of rows, in table-column order. Backed by a server-side cursor
/// in every adapter — never materializes a full table in memory.
pub type RowStream = BoxStream<'static, Result<Row, DdbCoreError>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Option<PrimaryKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub table: TableRef,
    pub name: String,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub method: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintDefinition {
    PrimaryKey(PrimaryKey),
    ForeignKey(ForeignKey),
    Unique(UniqueConstraint),
    Check(CheckConstraint),
}

/// A single structural change, applied via `Connection::alter_table`. Kept
/// as one enum (rather than separate methods per change) so callers can
/// build up a user-defined migration plan as a `Vec<TableAlteration>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableAlteration {
    AddColumn { table: TableRef, column: ColumnDefinition },
    DropColumn { table: TableRef, column: String },
    AlterColumnType { table: TableRef, column: String, data_type: DataType },
    AddConstraint { table: TableRef, constraint: ConstraintDefinition },
    DropConstraint { table: TableRef, name: String },
    DropIndex { table: TableRef, name: String },
}

/// One implementation per database engine. Produces `Connection`s bound to
/// a specific `ConnectionConfig`.
#[async_trait]
pub trait DatabaseAdapter: Send + Sync {
    async fn connect(&self, config: &ConnectionConfig) -> Result<Box<dyn Connection>, DdbCoreError>;
}

/// Everything DDBCore can do against a live connection. Implemented once
/// per engine; every method here must behave identically from the caller's
/// perspective regardless of which engine is behind it.
#[async_trait]
pub trait Connection: Send + Sync {
    /// Walks the entire catalog visible to this connection's credentials
    /// and returns it as a `Catalog`. Must be exhaustive — every table,
    /// column, constraint, index, trigger, function, view, and sequence
    /// the credential can see.
    async fn reflect_schema(&self) -> Result<Catalog, DdbCoreError>;

    /// Cursor-backed batched read of every row in `table`, in column order
    /// matching `Table::columns`.
    async fn stream_rows(&self, table: &TableRef, batch_size: usize) -> Result<RowStream, DdbCoreError>;

    /// Writes `rows` into `table` using the engine's fastest bulk-load path
    /// (e.g. Postgres `COPY`), not row-by-row `INSERT`s. Returns the number
    /// of rows written.
    async fn bulk_write(&self, table: &TableRef, rows: RowStream) -> Result<u64, DdbCoreError>;

    /// Escape hatch for arbitrary SQL. Parameters are passed positionally
    /// and bound by the driver, never interpolated into the SQL string.
    async fn execute_query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DdbCoreError>;

    async fn create_table(&self, def: &TableDefinition) -> Result<(), DdbCoreError>;
    async fn create_index(&self, def: &IndexDefinition) -> Result<(), DdbCoreError>;
    async fn alter_table(&self, alteration: &TableAlteration) -> Result<(), DdbCoreError>;

    /// Renders a reflected `Table` back into this engine's DDL — the
    /// counterpart to `reflect_schema`, used to recreate a table structure
    /// on a target connection.
    fn render_ddl(&self, table: &Table) -> Result<String, DdbCoreError>;
}

use async_trait::async_trait;
use futures_core::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::error::DdbCoreError;
use crate::schema::{Catalog, CheckConstraint, ForeignKey, IdentityGeneration, IndexColumn, PrimaryKey, Table, TableRef, UniqueConstraint};
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

/// Static description of an engine's SQL dialect and capabilities, so
/// generic callers (the testkit, migration tooling, Readactus) can compose
/// portable SQL without hardcoding any one engine's syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dialect {
    pub name: &'static str,
    /// Identifier quoting characters: `"` for Postgres/standard SQL,
    /// `` ` `` for MySQL, `[`/`]` for SQL Server.
    pub quote_open: char,
    pub quote_close: char,
    pub param_style: ParamStyle,
    pub supports_schemas: bool,
    pub supports_sequences: bool,
    pub supports_drop_table_cascade: bool,
    pub supports_drop_if_exists: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamStyle {
    /// `$1`, `$2`, ... (Postgres)
    Dollar,
    /// `?` positional (MySQL)
    Question,
    /// `@p1`, `@p2`, ... (SQL Server)
    AtNumbered,
}

impl Dialect {
    /// Quotes an identifier, doubling any embedded closing-quote character.
    pub fn quote_ident(&self, ident: &str) -> String {
        let escaped: String = ident
            .chars()
            .flat_map(|c| {
                if c == self.quote_close {
                    vec![c, c]
                } else {
                    vec![c]
                }
            })
            .collect();
        format!("{}{}{}", self.quote_open, escaped, self.quote_close)
    }

    pub fn quote_qualified(&self, schema: &str, name: &str) -> String {
        if self.supports_schemas {
            format!("{}.{}", self.quote_ident(schema), self.quote_ident(name))
        } else {
            self.quote_ident(name)
        }
    }

    /// A best-effort `DROP TABLE` statement in this dialect, using
    /// IF EXISTS / CASCADE only where supported.
    pub fn drop_table_stmt(&self, schema: &str, name: &str) -> String {
        let mut sql = String::from("DROP TABLE ");
        if self.supports_drop_if_exists {
            sql.push_str("IF EXISTS ");
        }
        sql.push_str(&self.quote_qualified(schema, name));
        if self.supports_drop_table_cascade {
            sql.push_str(" CASCADE");
        }
        sql
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<String>,
    /// Identity/auto-increment. When set, adapters emit the engine's
    /// generated-key syntax (`GENERATED ... AS IDENTITY`, `AUTO_INCREMENT`)
    /// and ignore `default` — losing this on a copied table breaks all
    /// future inserts on the target.
    pub identity: Option<IdentityGeneration>,
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
    /// This engine's SQL dialect and capability flags. Generic callers
    /// must build any hand-composed SQL through this rather than assuming
    /// one engine's quoting or feature set.
    fn dialect(&self) -> &'static Dialect;

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

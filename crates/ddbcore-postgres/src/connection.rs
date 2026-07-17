use async_trait::async_trait;
use ddbcore::{
    Catalog, Connection as DdbConnection, ConnectionConfig, DatabaseAdapter, DdbCoreError,
    Dialect, EncryptionMode, IndexDefinition, ParamStyle, Row, RowStream, Schema, StreamOptions,
    Table, TableAlteration, TableDefinition, TableRef, Value,
};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};

use crate::{bulk, ddl, query, reflect, stream};

pub struct PostgresAdapter;

#[async_trait]
impl DatabaseAdapter for PostgresAdapter {
    async fn connect(&self, config: &ConnectionConfig) -> Result<Box<dyn DdbConnection>, DdbCoreError> {
        let ssl_mode = match &config.encryption {
            EncryptionMode::ClearText => PgSslMode::Disable,
            EncryptionMode::Tls { verify_cert: true } => PgSslMode::VerifyFull,
            EncryptionMode::Tls { verify_cert: false } => PgSslMode::Require,
        };

        let mut options = PgConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .database(&config.database)
            .username(&config.username)
            .password(&config.password)
            .ssl_mode(ssl_mode);

        if config.read_only {
            options = options.options([("default_transaction_read_only", "on")]);
        }

        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect_with(options)
            .await
            .map_err(|e| DdbCoreError::Connection(e.to_string()))?;

        Ok(Box::new(PostgresConnection { pool }))
    }
}

pub struct PostgresConnection {
    pub(crate) pool: PgPool,
}

pub static POSTGRES_DIALECT: Dialect = Dialect {
    name: "postgres",
    quote_open: '"',
    quote_close: '"',
    param_style: ParamStyle::Dollar,
    supports_schemas: true,
    supports_sequences: true,
    supports_drop_table_cascade: true,
    supports_drop_if_exists: true,
};

#[async_trait]
impl DdbConnection for PostgresConnection {
    fn dialect(&self) -> &'static Dialect {
        &POSTGRES_DIALECT
    }

    async fn reflect_schema(&self) -> Result<Catalog, DdbCoreError> {
        reflect::reflect_schema(self).await
    }

    async fn reflect_schema_named(&self, schema: &str) -> Result<Schema, DdbCoreError> {
        reflect::reflect_schema_named(self, schema).await
    }

    async fn reflect_table(&self, table: &TableRef) -> Result<Table, DdbCoreError> {
        reflect::reflect_table(self, table).await
    }

    async fn stream_rows(&self, table: &TableRef, options: StreamOptions) -> Result<RowStream, DdbCoreError> {
        stream::stream_rows(self, table, options).await
    }

    async fn bulk_write(&self, table: &TableRef, rows: RowStream) -> Result<u64, DdbCoreError> {
        bulk::bulk_write(self, table, rows).await
    }

    async fn execute_query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DdbCoreError> {
        query::execute_query(self, sql, params).await
    }

    async fn execute_query_stream(&self, sql: &str, params: &[Value]) -> Result<RowStream, DdbCoreError> {
        query::execute_query_stream(self, sql, params).await
    }

    async fn create_table(&self, def: &TableDefinition) -> Result<(), DdbCoreError> {
        ddl::create_table(self, def).await
    }

    async fn create_index(&self, def: &IndexDefinition) -> Result<(), DdbCoreError> {
        ddl::create_index(self, def).await
    }

    async fn alter_table(&self, alteration: &TableAlteration) -> Result<(), DdbCoreError> {
        ddl::alter_table(self, alteration).await
    }

    fn render_ddl(&self, table: &Table) -> Result<Vec<String>, DdbCoreError> {
        ddl::render_ddl(table)
    }
}

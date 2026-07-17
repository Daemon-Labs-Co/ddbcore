use async_trait::async_trait;
use ddbcore::{
    Catalog, Connection as DdbConnection, ConnectionConfig, DatabaseAdapter, DdbCoreError,
    Dialect, EncryptionMode, IndexDefinition, ParamStyle, Row, RowStream, Schema, StreamOptions,
    Table, TableAlteration, TableDefinition, TableRef, Value,
};
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlSslMode};

use crate::{bulk, ddl, query, reflect, stream};

pub struct MySqlAdapter;

#[async_trait]
impl DatabaseAdapter for MySqlAdapter {
    async fn connect(&self, config: &ConnectionConfig) -> Result<Box<dyn DdbConnection>, DdbCoreError> {
        let ssl_mode = match &config.encryption {
            EncryptionMode::ClearText => MySqlSslMode::Disabled,
            EncryptionMode::Tls { verify_cert: true } => MySqlSslMode::VerifyIdentity,
            EncryptionMode::Tls { verify_cert: false } => MySqlSslMode::Required,
        };

        let options = MySqlConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .database(&config.database)
            .username(&config.username)
            .password(&config.password)
            .ssl_mode(ssl_mode);

        let read_only = config.read_only;
        let pool = MySqlPoolOptions::new()
            .max_connections(10)
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    if read_only {
                        sqlx::query("SET SESSION TRANSACTION READ ONLY").execute(&mut *conn).await?;
                    }
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .map_err(|e| DdbCoreError::Connection(e.to_string()))?;

        Ok(Box::new(MySqlConnection { pool }))
    }
}

pub struct MySqlConnection {
    pub(crate) pool: MySqlPool,
}

pub static MYSQL_DIALECT: Dialect = Dialect {
    name: "mysql",
    quote_open: '`',
    quote_close: '`',
    param_style: ParamStyle::Question,
    // MySQL "databases" serve as schemas; qualified names work the same way.
    supports_schemas: true,
    // MariaDB 10.3+ has sequences; plain MySQL does not. Flag conservatively.
    supports_sequences: false,
    // MySQL parses DROP TABLE ... CASCADE but ignores it; don't emit it.
    supports_drop_table_cascade: false,
    supports_drop_if_exists: true,
};

#[async_trait]
impl DdbConnection for MySqlConnection {
    fn dialect(&self) -> &'static Dialect {
        &MYSQL_DIALECT
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

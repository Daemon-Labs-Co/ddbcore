//! Integration tests: spin up a real, throwaway Postgres via `testcontainers`
//! (no manually pre-started container required — `cargo test` is
//! self-contained) and run the engine-agnostic `ddbcore-testkit` contract
//! suite against it through `PostgresAdapter`.

use ddbcore::{ConnectionConfig, DatabaseAdapter, EncryptionMode};
use ddbcore_postgres::PostgresAdapter;
use ddbcore_testkit::testenv;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

async fn start_postgres() -> (ContainerAsync<Postgres>, ConnectionConfig) {
    // Credentials are set explicitly on the container from .env.testing
    // rather than relying on the testcontainers module's defaults, so the
    // connection config below is guaranteed to match.
    let container = Postgres::default()
        .with_tag(testenv::pg_tag())
        .with_env_var("POSTGRES_DB", testenv::pg_database())
        .with_env_var("POSTGRES_USER", testenv::pg_user())
        .with_env_var("POSTGRES_PASSWORD", testenv::pg_password())
        .start()
        .await
        .expect("failed to start postgres container");
    let port = container.get_host_port_ipv4(5432).await.expect("failed to get mapped port");

    let config = ConnectionConfig {
        host: "127.0.0.1".into(),
        port,
        database: testenv::pg_database(),
        username: testenv::pg_user(),
        password: testenv::pg_password(),
        encryption: EncryptionMode::ClearText,
        read_only: false,
    };

    (container, config)
}

#[tokio::test]
async fn create_table_and_reflect_roundtrip() {
    let (_container, config) = start_postgres().await;
    let conn = PostgresAdapter.connect(&config).await.expect("connect failed");
    ddbcore_testkit::create_table_and_reflect_roundtrip(&*conn, "public", "ct_reflect").await;
}

#[tokio::test]
async fn constraints_and_index_roundtrip() {
    let (_container, config) = start_postgres().await;
    let conn = PostgresAdapter.connect(&config).await.expect("connect failed");
    ddbcore_testkit::constraints_and_index_roundtrip(&*conn, "public", "ct_constraints").await;
}

#[tokio::test]
async fn bulk_write_stream_roundtrip() {
    let (_container, config) = start_postgres().await;
    let conn = PostgresAdapter.connect(&config).await.expect("connect failed");
    ddbcore_testkit::bulk_write_stream_roundtrip(&*conn, "public", "ct_bulk").await;
}

#[tokio::test]
async fn render_ddl_recreates_table() {
    let (_container, config) = start_postgres().await;
    let conn = PostgresAdapter.connect(&config).await.expect("connect failed");
    ddbcore_testkit::render_ddl_recreates_table(&*conn, "public", "ct_ddl").await;
}

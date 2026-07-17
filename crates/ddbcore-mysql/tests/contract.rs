//! Integration tests: spin up a real, throwaway MariaDB via
//! `testcontainers` and run the engine-agnostic `ddbcore-testkit`
//! contract suite against it through `MySqlAdapter`.

use ddbcore::{ConnectionConfig, DatabaseAdapter, EncryptionMode};
use ddbcore_mysql::MySqlAdapter;
use ddbcore_testkit::testenv;
use testcontainers_modules::mariadb::Mariadb;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

async fn start_mariadb() -> (ContainerAsync<Mariadb>, ConnectionConfig) {
    // Credentials are set explicitly on the container from .env.testing
    // rather than relying on the testcontainers module's defaults, so the
    // connection config below is guaranteed to match.
    let container = Mariadb::default()
        .with_tag(testenv::mariadb_tag())
        .with_env_var("MARIADB_ROOT_PASSWORD", testenv::mariadb_root_password())
        .with_env_var("MARIADB_DATABASE", testenv::mariadb_database())
        .start()
        .await
        .expect("failed to start mariadb container");
    let port = container.get_host_port_ipv4(3306).await.expect("failed to get mapped port");

    let config = ConnectionConfig {
        host: "127.0.0.1".into(),
        port,
        database: testenv::mariadb_database(),
        username: "root".into(),
        password: testenv::mariadb_root_password(),
        encryption: EncryptionMode::ClearText,
        read_only: false,
    };

    (container, config)
}

#[tokio::test]
async fn create_table_and_reflect_roundtrip() {
    let (_container, config) = start_mariadb().await;
    let conn = MySqlAdapter.connect(&config).await.expect("connect failed");
    ddbcore_testkit::create_table_and_reflect_roundtrip(&*conn, &testenv::mariadb_database(), "ct_reflect").await;
}

#[tokio::test]
async fn constraints_and_index_roundtrip() {
    let (_container, config) = start_mariadb().await;
    let conn = MySqlAdapter.connect(&config).await.expect("connect failed");
    ddbcore_testkit::constraints_and_index_roundtrip(&*conn, &testenv::mariadb_database(), "ct_constraints").await;
}

#[tokio::test]
async fn bulk_write_stream_roundtrip() {
    let (_container, config) = start_mariadb().await;
    let conn = MySqlAdapter.connect(&config).await.expect("connect failed");
    ddbcore_testkit::bulk_write_stream_roundtrip(&*conn, &testenv::mariadb_database(), "ct_bulk").await;
}

#[tokio::test]
async fn render_ddl_recreates_table() {
    let (_container, config) = start_mariadb().await;
    let conn = MySqlAdapter.connect(&config).await.expect("connect failed");
    ddbcore_testkit::render_ddl_recreates_table(&*conn, &testenv::mariadb_database(), "ct_ddl").await;
}

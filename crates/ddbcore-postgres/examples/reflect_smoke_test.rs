use ddbcore::{ConnectionConfig, DatabaseAdapter, EncryptionMode, TableRef};
use ddbcore_postgres::PostgresAdapter;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let adapter = PostgresAdapter;
    let config = ConnectionConfig {
        host: "localhost".into(),
        port: 55432,
        database: "ddbcore_test".into(),
        username: "postgres".into(),
        password: "test".into(),
        encryption: EncryptionMode::ClearText,
        read_only: false, // this smoke test also exercises bulk_write
    };

    let conn = adapter.connect(&config).await?;

    let catalog = conn.reflect_schema().await?;
    for schema in &catalog.schemas {
        if schema.tables.is_empty() && schema.views.is_empty() && schema.sequences.is_empty() {
            continue;
        }
        println!("=== schema {} ===", schema.name);
        for table in &schema.tables {
            println!("table {}.{}", table.schema, table.name);
            println!("  comment: {:?}", table.comment);
            for col in &table.columns {
                println!(
                    "  col {} {:?} nullable={} identity={:?} default={:?}",
                    col.name, col.data_type.category, col.nullable, col.identity_generation, col.default
                );
            }
            println!("  pk: {:?}", table.primary_key);
            println!("  fks: {:?}", table.foreign_keys);
            println!("  unique: {:?}", table.unique_constraints);
            println!("  checks: {:?}", table.check_constraints);
            println!("  indexes: {:?}", table.indexes);
            println!("  triggers: {:?}", table.triggers.iter().map(|t| (&t.name, &t.timing, &t.events, &t.function_name)).collect::<Vec<_>>());
        }
        for view in &schema.views {
            println!("view {}.{} -> {} columns", view.schema, view.name, view.columns.len());
        }
        for seq in &schema.sequences {
            println!("sequence {}.{} start={} inc={}", seq.schema, seq.name, seq.start_value, seq.increment);
        }
        for func in &schema.functions {
            println!("function {}.{} lang={}", func.schema, func.name, func.language);
        }
    }

    println!("\n=== render_ddl(customers) ===");
    let customers = catalog
        .schemas
        .iter()
        .flat_map(|s| &s.tables)
        .find(|t| t.name == "customers")
        .expect("customers table not found");
    println!("{}", conn.render_ddl(customers)?);

    println!("\n=== stream_rows(customers) ===");
    let mut stream = conn
        .stream_rows(&TableRef { schema: "public".into(), name: "customers".into() }, 1)
        .await?;
    let mut count = 0;
    while let Some(row) = stream.next().await {
        let row = row?;
        println!("row: {:?}", row.0);
        count += 1;
    }
    println!("streamed {count} rows");

    println!("\n=== bulk_write(customers -> customers_copy) ===");
    let src = conn.stream_rows(&TableRef { schema: "public".into(), name: "customers".into() }, 100).await?;
    let written = conn.bulk_write(&TableRef { schema: "public".into(), name: "customers_copy".into() }, src).await?;
    println!("bulk_write wrote {written} rows");

    let mut verify = conn.stream_rows(&TableRef { schema: "public".into(), name: "customers_copy".into() }, 100).await?;
    while let Some(row) = verify.next().await {
        println!("copy row: {:?}", row?.0);
    }

    Ok(())
}

//! Engine-agnostic contract tests for `ddbcore::Connection` implementations.
//!
//! Every function here talks to the database only through the `Connection`
//! trait — never raw engine-specific SQL beyond `execute_query`, which is
//! itself part of the trait. That's the point: the exact same test code
//! runs unmodified against any adapter (Postgres today, MySQL/SQL
//! Server/Oracle later), so a passing suite is a real guarantee that the
//! abstraction holds, not just that one engine's adapter happens to work.
//!
//! Each function creates its own uniquely-prefixed tables and cleans up
//! after itself, so callers can run them in parallel against the same
//! database. Failures panic with `assert!`/`expect`, so each function is
//! meant to be called directly from a `#[tokio::test]` in the adapter
//! crate under test.

pub mod testenv;

use ddbcore::{
    Catalog, Column, ColumnDefinition, Connection, ConstraintDefinition, DataType, ForeignKey,
    IndexColumn, IndexDefinition, KeyRange, PrimaryKey, ReferentialAction, Row, RowStream,
    StreamOptions, Table, TableAlteration, TableDefinition, TableRef, TypeCategory,
    UniqueConstraint, Value,
};
use futures::stream::{self, StreamExt};

fn dt(category: TypeCategory) -> DataType {
    DataType { category, native_type: String::new() }
}

fn find_table<'a>(catalog: &'a Catalog, schema: &str, name: &str) -> Option<&'a Table> {
    catalog.schemas.iter().find(|s| s.name == schema)?.tables.iter().find(|t| t.name == name)
}

fn find_column<'a>(table: &'a Table, name: &str) -> Option<&'a Column> {
    table.columns.iter().find(|c| c.name == name)
}

/// Best-effort cleanup composed through the adapter's own dialect — the
/// testkit must never hardcode one engine's quoting or DROP syntax.
async fn cleanup(conn: &dyn Connection, schema: &str, tables: &[&str]) {
    for table in tables {
        let _ = conn.execute_query(&conn.dialect().drop_table_stmt(schema, table), &[]).await;
    }
}

/// `create_table` followed by `reflect_schema` must see exactly what was
/// created: same columns, same types, same nullability, same primary key.
pub async fn create_table_and_reflect_roundtrip(conn: &dyn Connection, schema: &str, prefix: &str) {
    let table_name = format!("{prefix}_basic");
    cleanup(conn, schema, &[&table_name]).await;

    let def = TableDefinition {
        schema: schema.to_string(),
        name: table_name.clone(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: dt(TypeCategory::Integer), nullable: false, default: None, identity: None },
            ColumnDefinition { name: "label".into(), data_type: dt(TypeCategory::VarChar { length: Some(100) }), nullable: true, default: None, identity: None },
            ColumnDefinition { name: "is_active".into(), data_type: dt(TypeCategory::Boolean), nullable: false, default: Some("true".into()), identity: None },
        ],
        primary_key: Some(PrimaryKey { name: None, columns: vec!["id".into()] }),
    };

    conn.create_table(&def).await.expect("create_table failed");

    let catalog = conn.reflect_schema().await.expect("reflect_schema failed");
    let table = find_table(&catalog, schema, &table_name).expect("table not found after create_table");

    assert_eq!(table.columns.len(), 3, "expected 3 columns");

    let id_col = find_column(table, "id").expect("id column missing");
    assert_eq!(id_col.data_type.category, TypeCategory::Integer);
    assert!(!id_col.nullable, "id should be NOT NULL");

    let label_col = find_column(table, "label").expect("label column missing");
    assert!(matches!(label_col.data_type.category, TypeCategory::VarChar { .. }), "label should be VarChar");
    assert!(label_col.nullable, "label should be nullable");

    let pk = table.primary_key.as_ref().expect("primary key missing");
    assert_eq!(pk.columns, vec!["id".to_string()]);

    cleanup(conn, schema, &[&table_name]).await;
}

/// A foreign key, unique constraint, and index added via `alter_table` /
/// `create_index` must all show up in the next `reflect_schema`, with the
/// right columns and the right referential actions.
pub async fn constraints_and_index_roundtrip(conn: &dyn Connection, schema: &str, prefix: &str) {
    let parent = format!("{prefix}_parent");
    let child = format!("{prefix}_child");
    cleanup(conn, schema, &[&child, &parent]).await;

    conn.create_table(&TableDefinition {
        schema: schema.into(),
        name: parent.clone(),
        columns: vec![ColumnDefinition { name: "id".into(), data_type: dt(TypeCategory::Integer), nullable: false, default: None, identity: None }],
        primary_key: Some(PrimaryKey { name: None, columns: vec!["id".into()] }),
    })
    .await
    .expect("create parent table failed");

    conn.create_table(&TableDefinition {
        schema: schema.into(),
        name: child.clone(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: dt(TypeCategory::Integer), nullable: false, default: None, identity: None },
            ColumnDefinition { name: "parent_id".into(), data_type: dt(TypeCategory::Integer), nullable: true, default: None, identity: None },
            ColumnDefinition { name: "code".into(), data_type: dt(TypeCategory::VarChar { length: Some(20) }), nullable: false, default: None, identity: None },
        ],
        primary_key: Some(PrimaryKey { name: None, columns: vec!["id".into()] }),
    })
    .await
    .expect("create child table failed");

    let child_ref = TableRef { schema: schema.into(), name: child.clone() };

    conn.alter_table(&TableAlteration::AddConstraint {
        table: child_ref.clone(),
        constraint: ConstraintDefinition::ForeignKey(ForeignKey {
            name: format!("{child}_parent_fk"),
            columns: vec!["parent_id".into()],
            referenced_schema: schema.into(),
            referenced_table: parent.clone(),
            referenced_columns: vec!["id".into()],
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::NoAction,
        }),
    })
    .await
    .expect("add foreign key failed");

    conn.alter_table(&TableAlteration::AddConstraint {
        table: child_ref.clone(),
        constraint: ConstraintDefinition::Unique(UniqueConstraint { name: format!("{child}_code_unique"), columns: vec!["code".into()] }),
    })
    .await
    .expect("add unique constraint failed");

    conn.create_index(&IndexDefinition {
        table: child_ref.clone(),
        name: format!("{child}_parent_id_idx"),
        columns: vec![IndexColumn { name: "parent_id".into(), descending: false }],
        unique: false,
        method: None,
    })
    .await
    .expect("create_index failed");

    let catalog = conn.reflect_schema().await.expect("reflect_schema failed");
    let child_table = find_table(&catalog, schema, &child).expect("child table not found");

    assert_eq!(child_table.foreign_keys.len(), 1, "expected exactly one foreign key");
    let fk = &child_table.foreign_keys[0];
    assert_eq!(fk.columns, vec!["parent_id".to_string()]);
    assert_eq!(fk.referenced_table, parent);
    assert_eq!(fk.referenced_columns, vec!["id".to_string()]);
    assert_eq!(fk.on_delete, ReferentialAction::Cascade);
    assert_eq!(fk.on_update, ReferentialAction::NoAction);

    assert!(
        child_table.unique_constraints.iter().any(|u| u.columns == vec!["code".to_string()]),
        "expected unique constraint on code"
    );
    assert!(
        child_table.indexes.iter().any(|i| i.name == format!("{child}_parent_id_idx")),
        "expected index on parent_id"
    );

    cleanup(conn, schema, &[&child, &parent]).await;
}

/// Rows pushed through `bulk_write` must come back byte-for-byte identical
/// through `stream_rows`, including NULLs, regardless of the batch size
/// used for streaming.
pub async fn bulk_write_stream_roundtrip(conn: &dyn Connection, schema: &str, prefix: &str) {
    let table = format!("{prefix}_rows");
    cleanup(conn, schema, &[&table]).await;

    conn.create_table(&TableDefinition {
        schema: schema.into(),
        name: table.clone(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: dt(TypeCategory::Integer), nullable: false, default: None, identity: None },
            ColumnDefinition { name: "name".into(), data_type: dt(TypeCategory::Text), nullable: true, default: None, identity: None },
        ],
        primary_key: Some(PrimaryKey { name: None, columns: vec!["id".into()] }),
    })
    .await
    .expect("create_table failed");

    let table_ref = TableRef { schema: schema.into(), name: table.clone() };

    let input_rows = vec![
        Row(vec![Value::Integer(1), Value::Text("alpha".into())]),
        Row(vec![Value::Integer(2), Value::Null]),
        Row(vec![Value::Integer(3), Value::Text("gamma".into())]),
    ];
    let expected = input_rows.clone();
    let source: RowStream = Box::pin(stream::iter(input_rows.into_iter().map(Ok)));

    let written = conn.bulk_write(&table_ref, source).await.expect("bulk_write failed");
    assert_eq!(written, 3, "expected 3 rows written");

    // batch_size=2 deliberately smaller than the row count, so this also
    // exercises the multi-fetch path in `stream_rows`, not just the
    // single-batch case.
    let mut result_stream = conn
        .stream_rows(&table_ref, StreamOptions { batch_size: 2, ..Default::default() })
        .await
        .expect("stream_rows failed");
    let mut got = Vec::new();
    while let Some(row) = result_stream.next().await {
        got.push(row.expect("row decode failed"));
    }
    // Sort on the actual integer id — a debug-string sort would order
    // Integer(10) before Integer(2) once the row count grows.
    fn row_id(r: &Row) -> i32 {
        match r.0.first() {
            Some(Value::Integer(i)) => *i,
            _ => i32::MAX,
        }
    }
    got.sort_by_key(row_id);

    assert_eq!(got.len(), expected.len(), "row count mismatch after roundtrip");
    for (g, e) in got.iter().zip(expected.iter()) {
        assert_eq!(g.0, e.0, "row contents mismatch after roundtrip");
    }

    cleanup(conn, schema, &[&table]).await;
}

/// `render_ddl` on a reflected table must produce DDL that, when executed,
/// recreates a structurally identical table — same columns, same
/// canonical types, in the same order, with identity and unique
/// constraints intact (identity loss breaks all future inserts on the
/// target; unique-backing indexes must not be emitted twice).
pub async fn render_ddl_recreates_table(conn: &dyn Connection, schema: &str, prefix: &str) {
    let original = format!("{prefix}_orig");
    let recreated = format!("{prefix}_recreated");
    cleanup(conn, schema, &[&recreated, &original]).await;

    conn.create_table(&TableDefinition {
        schema: schema.into(),
        name: original.clone(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: dt(TypeCategory::BigInt), nullable: false, default: None, identity: Some(ddbcore::IdentityGeneration::ByDefault) },
            ColumnDefinition { name: "amount".into(), data_type: dt(TypeCategory::Decimal { precision: Some(10), scale: Some(2) }), nullable: true, default: None, identity: None },
            ColumnDefinition { name: "code".into(), data_type: dt(TypeCategory::VarChar { length: Some(20) }), nullable: false, default: None, identity: None },
        ],
        primary_key: Some(PrimaryKey { name: None, columns: vec!["id".into()] }),
    })
    .await
    .expect("create original table failed");

    conn.alter_table(&TableAlteration::AddConstraint {
        table: TableRef { schema: schema.into(), name: original.clone() },
        constraint: ConstraintDefinition::Unique(UniqueConstraint { name: format!("{original}_code_uq"), columns: vec!["code".into()] }),
    })
    .await
    .expect("add unique constraint failed");

    let catalog = conn.reflect_schema().await.expect("reflect_schema failed");
    let original_table = find_table(&catalog, schema, &original).expect("original table not found").clone();

    let statements = conn.render_ddl(&original_table).expect("render_ddl failed");
    for statement in &statements {
        // Replace the bare table name rather than a quoted
        // `"schema"."table"` form — identifier quoting differs per engine
        // (double quotes vs backticks vs brackets), and the prefixed name
        // is a unique token in the rendered DDL either way. Statements are
        // executed one by one as returned — never join-and-resplit on `;`.
        let renamed = statement.replace(&original, &recreated);
        conn.execute_query(&renamed, &[]).await.unwrap_or_else(|e| panic!("executing rendered DDL failed: {renamed}: {e}"));
    }

    let catalog2 = conn.reflect_schema().await.expect("reflect_schema failed");
    let recreated_table = find_table(&catalog2, schema, &recreated).expect("recreated table not found");

    assert_eq!(recreated_table.columns.len(), original_table.columns.len(), "column count mismatch after DDL replay");
    for (orig_col, new_col) in original_table.columns.iter().zip(recreated_table.columns.iter()) {
        assert_eq!(orig_col.name, new_col.name, "column name mismatch after DDL replay");
        assert_eq!(orig_col.data_type.category, new_col.data_type.category, "column type mismatch after DDL replay");
        assert_eq!(orig_col.is_identity, new_col.is_identity, "identity lost/gained on column {} after DDL replay", orig_col.name);
    }

    let id_col = find_column(recreated_table, "id").expect("id column missing after replay");
    assert!(id_col.is_identity, "identity column must survive render_ddl replay");

    assert_eq!(
        recreated_table.unique_constraints.len(),
        original_table.unique_constraints.len(),
        "unique constraint count mismatch after DDL replay"
    );
    assert!(
        recreated_table.unique_constraints.iter().any(|u| u.columns == vec!["code".to_string()]),
        "unique constraint on code lost after DDL replay"
    );

    cleanup(conn, schema, &[&recreated, &original]).await;
}

/// Column projection, key-range restriction, scoped reflection, and the
/// streaming query variant must all behave identically across engines:
/// projected streams return only the requested columns in the requested
/// order; a half-open key range returns exactly the rows in `[lower,
/// upper)`; `reflect_table` sees the same table `reflect_schema` would;
/// `execute_query_stream` yields the same rows `execute_query` returns.
pub async fn stream_options_and_scoped_reflection(conn: &dyn Connection, schema: &str, prefix: &str) {
    let table = format!("{prefix}_opts");
    cleanup(conn, schema, &[&table]).await;

    conn.create_table(&TableDefinition {
        schema: schema.into(),
        name: table.clone(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: dt(TypeCategory::Integer), nullable: false, default: None, identity: None },
            ColumnDefinition { name: "name".into(), data_type: dt(TypeCategory::Text), nullable: true, default: None, identity: None },
        ],
        primary_key: Some(PrimaryKey { name: None, columns: vec!["id".into()] }),
    })
    .await
    .expect("create_table failed");

    let table_ref = TableRef { schema: schema.into(), name: table.clone() };

    let input: Vec<Row> = (1..=5).map(|i| Row(vec![Value::Integer(i), Value::Text(format!("row{i}"))])).collect();
    let source: RowStream = Box::pin(stream::iter(input.into_iter().map(Ok)));
    conn.bulk_write(&table_ref, source).await.expect("bulk_write failed");

    // Scoped reflection sees the table without walking the catalog.
    let reflected = conn.reflect_table(&table_ref).await.expect("reflect_table failed");
    assert_eq!(reflected.name, table);
    assert_eq!(reflected.columns.len(), 2);

    // Projection: only `name`, so every row is a single Text value.
    let mut projected = conn
        .stream_rows(&table_ref, StreamOptions { columns: Some(vec!["name".into()]), ..Default::default() })
        .await
        .expect("projected stream_rows failed");
    let mut projected_count = 0;
    while let Some(row) = projected.next().await {
        let row = row.expect("row decode failed");
        assert_eq!(row.0.len(), 1, "projection must return exactly the requested columns");
        assert!(matches!(row.0[0], Value::Text(_)), "projected column should be the text column");
        projected_count += 1;
    }
    assert_eq!(projected_count, 5);

    // Half-open key range [2, 4) selects ids 2 and 3.
    let mut ranged = conn
        .stream_rows(
            &table_ref,
            StreamOptions {
                key_range: Some(KeyRange { column: "id".into(), lower: Some(Value::Integer(2)), upper: Some(Value::Integer(4)) }),
                ..Default::default()
            },
        )
        .await
        .expect("ranged stream_rows failed");
    let mut ids = Vec::new();
    while let Some(row) = ranged.next().await {
        match &row.expect("row decode failed").0[0] {
            Value::Integer(id) => ids.push(*id),
            other => panic!("expected integer id, got {other:?}"),
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![2, 3], "half-open key range [2,4) must select exactly ids 2 and 3");

    // Zero batch_size must be rejected, not loop forever.
    let zero = conn.stream_rows(&table_ref, StreamOptions { batch_size: 0, ..Default::default() }).await;
    assert!(zero.is_err(), "batch_size 0 must be rejected");

    // Streaming query variant yields the same row count as materialized.
    let sql = format!("SELECT * FROM {}", conn.dialect().quote_qualified(schema, &table));
    let materialized = conn.execute_query(&sql, &[]).await.expect("execute_query failed");
    let mut streamed = conn.execute_query_stream(&sql, &[]).await.expect("execute_query_stream failed");
    let mut streamed_count = 0;
    while let Some(row) = streamed.next().await {
        row.expect("streamed row decode failed");
        streamed_count += 1;
    }
    assert_eq!(streamed_count, materialized.len(), "execute_query_stream must yield the same rows as execute_query");

    cleanup(conn, schema, &[&table]).await;
}

/// Runs every contract test in sequence against the given connection. The
/// per-engine crate can call this directly for a single all-in-one check,
/// or call the individual functions above from separate `#[tokio::test]`s
/// for finer-grained pass/fail reporting.
pub async fn run_all(conn: &dyn Connection, schema: &str, prefix: &str) {
    create_table_and_reflect_roundtrip(conn, schema, &format!("{prefix}_a")).await;
    constraints_and_index_roundtrip(conn, schema, &format!("{prefix}_b")).await;
    bulk_write_stream_roundtrip(conn, schema, &format!("{prefix}_c")).await;
    render_ddl_recreates_table(conn, schema, &format!("{prefix}_d")).await;
    stream_options_and_scoped_reflection(conn, schema, &format!("{prefix}_e")).await;
}

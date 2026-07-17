use std::collections::HashMap;

use ddbcore::{
    Catalog, CheckConstraint, Column, DataType, DdbCoreError, ForeignKey, Function,
    FunctionArgument, IdentityGeneration, Index, IndexColumn, PrimaryKey, ReferentialAction,
    Schema as DdbSchema, Sequence, Table, TableRef, Trigger, TriggerEvent, TriggerTiming,
    UniqueConstraint, View,
};
use sqlx::{MySqlPool, Row};

use crate::connection::MySqlConnection;
use crate::types::map_mysql_type;

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::Reflection(e.to_string())
}

pub(crate) async fn reflect_schema(conn: &MySqlConnection) -> Result<Catalog, DdbCoreError> {
    let schema_names: Vec<String> = sqlx::query_scalar(
        "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA \
         WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
         ORDER BY SCHEMA_NAME",
    )
    .fetch_all(&conn.pool)
    .await
    .map_err(db_err)?;

    let mut schemas = Vec::with_capacity(schema_names.len());
    for schema_name in schema_names {
        schemas.push(reflect_schema_named(conn, &schema_name).await?);
    }

    Ok(Catalog { schemas })
}

/// Scoped reflection: one schema, without walking the rest of the catalog.
pub(crate) async fn reflect_schema_named(conn: &MySqlConnection, schema: &str) -> Result<DdbSchema, DdbCoreError> {
    let pool = &conn.pool;
    let tables = reflect_tables(pool, schema, None).await?;
    let views = reflect_views(pool, schema).await?;
    let sequences = reflect_sequences(pool, schema).await?;
    let functions = reflect_functions(pool, schema).await?;
    Ok(DdbSchema { name: schema.to_string(), tables, views, sequences, functions })
}

/// Scoped reflection: one table, without walking the rest of the catalog.
pub(crate) async fn reflect_table(conn: &MySqlConnection, table: &TableRef) -> Result<Table, DdbCoreError> {
    let mut tables = reflect_tables(&conn.pool, &table.schema, Some(&table.name)).await?;
    tables
        .pop()
        .ok_or_else(|| DdbCoreError::Reflection(format!("table {}.{} not found", table.schema, table.name)))
}

async fn reflect_tables(pool: &MySqlPool, schema: &str, only_table: Option<&str>) -> Result<Vec<Table>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
         AND (? IS NULL OR TABLE_NAME = ?) ORDER BY TABLE_NAME",
    )
    .bind(schema)
    .bind(only_table)
    .bind(only_table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut tables = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("TABLE_NAME").map_err(db_err)?;
        let comment: String = row.try_get("TABLE_COMMENT").map_err(db_err)?;

        let columns = reflect_columns(pool, schema, &name).await?;
        let primary_key = reflect_primary_key(pool, schema, &name).await?;
        let foreign_keys = reflect_foreign_keys(pool, schema, &name).await?;
        let unique_constraints = reflect_unique_constraints(pool, schema, &name).await?;
        let check_constraints = reflect_check_constraints(pool, schema, &name).await?;
        let indexes = reflect_indexes(pool, schema, &name).await?;
        let triggers = reflect_triggers(pool, schema, &name).await?;

        tables.push(Table {
            schema: schema.to_string(),
            name,
            columns,
            primary_key,
            foreign_keys,
            unique_constraints,
            check_constraints,
            // MySQL has no exclusion constraints (Postgres-only feature).
            exclusion_constraints: vec![],
            indexes,
            triggers,
            comment: if comment.is_empty() { None } else { Some(comment) },
            // MySQL partitioning exists but is not yet reflected — its
            // information_schema.PARTITIONS model doesn't map onto the
            // parent/child structure Postgres uses.
            partition_key: None,
            partition_parent: None,
        });
    }
    Ok(tables)
}

async fn reflect_columns(pool: &MySqlPool, schema: &str, table: &str) -> Result<Vec<Column>, DdbCoreError> {
    let rows = sqlx::query(
        // CAST(... AS SIGNED): MySQL and MariaDB disagree on whether
        // ORDINAL_POSITION is BIGINT or BIGINT UNSIGNED, and sqlx refuses
        // to decode unsigned into i64. Casting makes it uniform.
        "SELECT COLUMN_NAME, CAST(ORDINAL_POSITION AS SIGNED) AS ORDINAL_POSITION, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT \
         FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
         ORDER BY ORDINAL_POSITION",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("COLUMN_NAME").map_err(db_err)?;
        let ordinal_position: i64 = row.try_get("ORDINAL_POSITION").map_err(db_err)?;
        let column_type: String = row.try_get("COLUMN_TYPE").map_err(db_err)?;
        let is_nullable: String = row.try_get("IS_NULLABLE").map_err(db_err)?;
        let default_expr: Option<String> = row.try_get("COLUMN_DEFAULT").map_err(db_err)?;
        let extra: String = row.try_get("EXTRA").map_err(db_err)?;
        let comment: String = row.try_get("COLUMN_COMMENT").map_err(db_err)?;

        let category = map_mysql_type(&column_type);
        // MySQL's AUTO_INCREMENT is closer to Postgres's "generated by
        // default" semantics (the value can be explicitly overridden on
        // insert) than "generated always".
        let is_identity = extra.to_lowercase().contains("auto_increment");
        let identity_generation = if is_identity { Some(IdentityGeneration::ByDefault) } else { None };

        columns.push(Column {
            name,
            ordinal_position: ordinal_position as u32,
            data_type: DataType { category, native_type: column_type },
            nullable: is_nullable == "YES",
            default: default_expr,
            is_identity,
            identity_generation,
            comment: if comment.is_empty() { None } else { Some(comment) },
        });
    }
    Ok(columns)
}

async fn reflect_primary_key(pool: &MySqlPool, schema: &str, table: &str) -> Result<Option<PrimaryKey>, DdbCoreError> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT COLUMN_NAME FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' \
         ORDER BY SEQ_IN_INDEX",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(PrimaryKey { name: Some("PRIMARY".to_string()), columns: rows }))
}

fn map_ref_action(rule: &str) -> ReferentialAction {
    match rule.to_uppercase().as_str() {
        "CASCADE" => ReferentialAction::Cascade,
        "SET NULL" => ReferentialAction::SetNull,
        "SET DEFAULT" => ReferentialAction::SetDefault,
        "RESTRICT" => ReferentialAction::Restrict,
        _ => ReferentialAction::NoAction,
    }
}

async fn reflect_foreign_keys(pool: &MySqlPool, schema: &str, table: &str) -> Result<Vec<ForeignKey>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_SCHEMA, \
                kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME, kcu.ORDINAL_POSITION, \
                rc.UPDATE_RULE, rc.DELETE_RULE \
         FROM information_schema.KEY_COLUMN_USAGE kcu \
         JOIN information_schema.REFERENTIAL_CONSTRAINTS rc \
           ON rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
         WHERE kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME = ? AND kcu.REFERENCED_TABLE_NAME IS NOT NULL \
         ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut by_name: HashMap<String, ForeignKey> = HashMap::new();
    let mut order: Vec<String> = Vec::new();

    for row in rows {
        let constraint_name: String = row.try_get("CONSTRAINT_NAME").map_err(db_err)?;
        let column_name: String = row.try_get("COLUMN_NAME").map_err(db_err)?;
        let ref_schema: String = row.try_get("REFERENCED_TABLE_SCHEMA").map_err(db_err)?;
        let ref_table: String = row.try_get("REFERENCED_TABLE_NAME").map_err(db_err)?;
        let ref_column: String = row.try_get("REFERENCED_COLUMN_NAME").map_err(db_err)?;
        let update_rule: String = row.try_get("UPDATE_RULE").map_err(db_err)?;
        let delete_rule: String = row.try_get("DELETE_RULE").map_err(db_err)?;

        let entry = by_name.entry(constraint_name.clone()).or_insert_with(|| {
            order.push(constraint_name.clone());
            ForeignKey {
                name: constraint_name.clone(),
                columns: vec![],
                referenced_schema: ref_schema,
                referenced_table: ref_table,
                referenced_columns: vec![],
                on_delete: map_ref_action(&delete_rule),
                on_update: map_ref_action(&update_rule),
            }
        });
        entry.columns.push(column_name);
        entry.referenced_columns.push(ref_column);
    }

    Ok(order.into_iter().filter_map(|name| by_name.remove(&name)).collect())
}

async fn reflect_unique_constraints(pool: &MySqlPool, schema: &str, table: &str) -> Result<Vec<UniqueConstraint>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT tc.CONSTRAINT_NAME, kcu.COLUMN_NAME \
         FROM information_schema.TABLE_CONSTRAINTS tc \
         JOIN information_schema.KEY_COLUMN_USAGE kcu \
           ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
           AND kcu.TABLE_NAME = tc.TABLE_NAME \
         WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'UNIQUE' \
         ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut by_name: HashMap<String, Vec<String>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for row in rows {
        let name: String = row.try_get("CONSTRAINT_NAME").map_err(db_err)?;
        let column: String = row.try_get("COLUMN_NAME").map_err(db_err)?;
        if !by_name.contains_key(&name) {
            order.push(name.clone());
        }
        by_name.entry(name).or_default().push(column);
    }

    Ok(order.into_iter().map(|name| { let columns = by_name.remove(&name).unwrap_or_default(); UniqueConstraint { name, columns } }).collect())
}

async fn reflect_check_constraints(pool: &MySqlPool, schema: &str, table: &str) -> Result<Vec<CheckConstraint>, DdbCoreError> {
    // Requires MySQL 8.0.16+ / MariaDB 10.2+. Older versions have no
    // CHECK_CONSTRAINTS view and this simply returns nothing.
    let rows = sqlx::query(
        "SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE \
         FROM information_schema.TABLE_CONSTRAINTS tc \
         JOIN information_schema.CHECK_CONSTRAINTS cc \
           ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
         WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'CHECK'",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await;

    let rows = match rows {
        Ok(rows) => rows,
        Err(_) => return Ok(vec![]),
    };

    let mut constraints = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("CONSTRAINT_NAME").map_err(db_err)?;
        let expression: String = row.try_get("CHECK_CLAUSE").map_err(db_err)?;
        constraints.push(CheckConstraint { name, expression });
    }
    Ok(constraints)
}

async fn reflect_indexes(pool: &MySqlPool, schema: &str, table: &str) -> Result<Vec<Index>, DdbCoreError> {
    let rows = sqlx::query(
        // The NOT IN clause excludes indexes that back a UNIQUE constraint
        // — those are already reflected as constraints, and reporting them
        // again as plain indexes makes render_ddl emit two conflicting
        // CREATE statements for the same object.
        "SELECT INDEX_NAME, COLUMN_NAME, CAST(NON_UNIQUE AS SIGNED) AS NON_UNIQUE, INDEX_TYPE \
         FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME <> 'PRIMARY' \
         AND INDEX_NAME NOT IN ( \
             SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'UNIQUE') \
         ORDER BY INDEX_NAME, SEQ_IN_INDEX",
    )
    .bind(schema)
    .bind(table)
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut order: Vec<String> = Vec::new();
    let mut columns_by_name: HashMap<String, Vec<IndexColumn>> = HashMap::new();
    let mut meta_by_name: HashMap<String, (bool, Option<String>)> = HashMap::new();

    for row in rows {
        let index_name: String = row.try_get("INDEX_NAME").map_err(db_err)?;
        let column_name: String = row.try_get("COLUMN_NAME").map_err(db_err)?;
        let non_unique: i64 = row.try_get("NON_UNIQUE").map_err(db_err)?;
        let index_type: String = row.try_get("INDEX_TYPE").map_err(db_err)?;

        if !columns_by_name.contains_key(&index_name) {
            order.push(index_name.clone());
        }
        columns_by_name.entry(index_name.clone()).or_default().push(IndexColumn { name: column_name, descending: false });
        meta_by_name.insert(index_name, (non_unique == 0, Some(index_type)));
    }

    Ok(order
        .into_iter()
        .map(|name| {
            let columns = columns_by_name.remove(&name).unwrap_or_default();
            let (unique, method) = meta_by_name.remove(&name).unwrap_or((false, None));
            Index { name, columns, unique, method }
        })
        .collect())
}

async fn reflect_triggers(pool: &MySqlPool, schema: &str, table: &str) -> Result<Vec<Trigger>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT \
         FROM information_schema.TRIGGERS \
         WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ? \
         ORDER BY TRIGGER_NAME",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut triggers = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("TRIGGER_NAME").map_err(db_err)?;
        let timing: String = row.try_get("ACTION_TIMING").map_err(db_err)?;
        let event: String = row.try_get("EVENT_MANIPULATION").map_err(db_err)?;
        let statement: String = row.try_get("ACTION_STATEMENT").map_err(db_err)?;

        let timing = if timing.eq_ignore_ascii_case("BEFORE") { TriggerTiming::Before } else { TriggerTiming::After };
        let event = match event.to_uppercase().as_str() {
            "INSERT" => TriggerEvent::Insert,
            "UPDATE" => TriggerEvent::Update,
            "DELETE" => TriggerEvent::Delete,
            _ => TriggerEvent::Insert,
        };

        // MySQL/MariaDB triggers are single-event and reference no
        // separate named function — the body is inline.
        triggers.push(Trigger { name, timing, events: vec![event], function_name: None, body: Some(statement) });
    }
    Ok(triggers)
}

async fn reflect_views(pool: &MySqlPool, schema: &str) -> Result<Vec<View>, DdbCoreError> {
    let rows = sqlx::query("SELECT TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME")
        .bind(schema)
        .fetch_all(pool)
        .await
        .map_err(db_err)?;

    let mut views = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("TABLE_NAME").map_err(db_err)?;
        let definition: String = row.try_get("VIEW_DEFINITION").map_err(db_err)?;
        let columns = reflect_columns(pool, schema, &name).await?;
        views.push(View { schema: schema.to_string(), name, definition, columns });
    }
    Ok(views)
}

/// Native sequences are a MariaDB-only feature (10.3+); plain MySQL has
/// no equivalent (it uses AUTO_INCREMENT columns instead, captured via
/// `Column::is_identity`). On MySQL this always returns empty.
async fn reflect_sequences(pool: &MySqlPool, schema: &str) -> Result<Vec<Sequence>, DdbCoreError> {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'SEQUENCE' ORDER BY TABLE_NAME",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let mut sequences = Vec::with_capacity(names.len());
    for name in names {
        let row = sqlx::query(&format!(
            "SELECT min_value, max_value, start_value, increment, cycle_option FROM {}",
            crate::util::quote_qualified(schema, &name)
        ))
        .fetch_one(pool)
        .await
        .map_err(db_err)?;

        let min_value: i64 = row.try_get("min_value").map_err(db_err)?;
        let max_value: i64 = row.try_get("max_value").map_err(db_err)?;
        let start_value: i64 = row.try_get("start_value").map_err(db_err)?;
        let increment: i64 = row.try_get("increment").map_err(db_err)?;
        let cycle_option: String = row.try_get("cycle_option").map_err(db_err)?;

        sequences.push(Sequence {
            schema: schema.to_string(),
            name,
            data_type: DataType { category: ddbcore::TypeCategory::BigInt, native_type: "bigint".to_string() },
            start_value,
            increment,
            min_value: Some(min_value),
            max_value: Some(max_value),
            cycle: cycle_option.eq_ignore_ascii_case("YES"),
        });
    }
    Ok(sequences)
}

/// Function/procedure metadata only. Per-parameter argument parsing isn't
/// implemented yet (unlike Postgres, MySQL's `information_schema` has no
/// single ready-made signature string — it would need aggregating
/// `information_schema.PARAMETERS` — so `arguments` is left empty here).
async fn reflect_functions(pool: &MySqlPool, schema: &str) -> Result<Vec<Function>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT ROUTINE_NAME, DTD_IDENTIFIER, ROUTINE_DEFINITION \
         FROM information_schema.ROUTINES \
         WHERE ROUTINE_SCHEMA = ? \
         ORDER BY ROUTINE_NAME",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut functions = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("ROUTINE_NAME").map_err(db_err)?;
        let return_type: Option<String> = row.try_get("DTD_IDENTIFIER").map_err(db_err)?;
        let body: Option<String> = row.try_get("ROUTINE_DEFINITION").map_err(db_err)?;

        functions.push(Function {
            schema: schema.to_string(),
            name,
            arguments: Vec::<FunctionArgument>::new(),
            return_type,
            language: "SQL".to_string(),
            body,
        });
    }
    Ok(functions)
}

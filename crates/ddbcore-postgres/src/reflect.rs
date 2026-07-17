use std::collections::HashMap;

use ddbcore::{
    Catalog, CheckConstraint, Column, DataType, DdbCoreError, ForeignKey, Function,
    FunctionArgument, IdentityGeneration, Index, IndexColumn, PrimaryKey, ReferentialAction,
    Schema as DdbSchema, Sequence, Table, TableRef, Trigger, TriggerEvent, TriggerTiming,
    UniqueConstraint, View,
};
use sqlx::{PgPool, Row};

use crate::connection::PostgresConnection;
use crate::types::map_pg_type;

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::Reflection(e.to_string())
}

pub(crate) async fn reflect_schema(conn: &PostgresConnection) -> Result<Catalog, DdbCoreError> {
    let schema_names: Vec<String> = sqlx::query_scalar(
        "SELECT schema_name FROM information_schema.schemata \
         WHERE schema_name NOT IN ('pg_catalog', 'information_schema') \
         AND schema_name NOT LIKE 'pg_toast%' AND schema_name NOT LIKE 'pg\\_temp\\_%' \
         ORDER BY schema_name",
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
pub(crate) async fn reflect_schema_named(conn: &PostgresConnection, schema: &str) -> Result<DdbSchema, DdbCoreError> {
    let pool = &conn.pool;
    let enum_types = load_enum_types(pool, schema).await?;
    let tables = reflect_tables(pool, schema, None, &enum_types).await?;
    let views = reflect_views(pool, schema, &enum_types).await?;
    let sequences = reflect_sequences(pool, schema).await?;
    let functions = reflect_functions(pool, schema).await?;
    Ok(DdbSchema { name: schema.to_string(), tables, views, sequences, functions })
}

/// Scoped reflection: one table, without walking the rest of the catalog.
pub(crate) async fn reflect_table(conn: &PostgresConnection, table: &TableRef) -> Result<Table, DdbCoreError> {
    let pool = &conn.pool;
    let enum_types = load_enum_types(pool, &table.schema).await?;
    let mut tables = reflect_tables(pool, &table.schema, Some(&table.name), &enum_types).await?;
    tables
        .pop()
        .ok_or_else(|| DdbCoreError::Reflection(format!("table {}.{} not found", table.schema, table.name)))
}

async fn load_enum_types(pool: &PgPool, schema: &str) -> Result<HashMap<String, Vec<String>>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT t.typname, e.enumlabel \
         FROM pg_type t \
         JOIN pg_enum e ON e.enumtypid = t.oid \
         JOIN pg_namespace n ON n.oid = t.typnamespace \
         WHERE n.nspname = $1 \
         ORDER BY t.typname, e.enumsortorder",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let typname: String = row.try_get("typname").map_err(db_err)?;
        let label: String = row.try_get("enumlabel").map_err(db_err)?;
        map.entry(typname).or_default().push(label);
    }
    Ok(map)
}

async fn reflect_tables(
    pool: &PgPool,
    schema: &str,
    only_table: Option<&str>,
    enum_types: &HashMap<String, Vec<String>>,
) -> Result<Vec<Table>, DdbCoreError> {
    // relkind IN ('r','p'): partitioned parent tables are 'p', not 'r' —
    // excluding them silently reflects the wrong structure for any
    // partitioned database. Partition children carry their parent via
    // pg_inherits (only when relispartition, so plain inheritance isn't
    // misreported as partitioning). `only_table` scopes to one table for
    // reflect_table without duplicating this query.
    let rows = sqlx::query(
        "SELECT c.relname, \
                CASE WHEN c.relkind = 'p' THEN pg_get_partkeydef(c.oid) END AS partition_key, \
                CASE WHEN c.relispartition THEN pn.nspname END AS parent_schema, \
                CASE WHEN c.relispartition THEN pc.relname END AS parent_name \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         LEFT JOIN pg_inherits i ON i.inhrelid = c.oid \
         LEFT JOIN pg_class pc ON pc.oid = i.inhparent \
         LEFT JOIN pg_namespace pn ON pn.oid = pc.relnamespace \
         WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') \
         AND ($2::text IS NULL OR c.relname = $2) ORDER BY c.relname",
    )
    .bind(schema)
    .bind(only_table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut tables = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("relname").map_err(db_err)?;
        let partition_key: Option<String> = row.try_get("partition_key").map_err(db_err)?;
        let parent_schema: Option<String> = row.try_get("parent_schema").map_err(db_err)?;
        let parent_name: Option<String> = row.try_get("parent_name").map_err(db_err)?;
        let partition_parent = match (parent_schema, parent_name) {
            (Some(schema), Some(name)) => Some(TableRef { schema, name }),
            _ => None,
        };

        let columns = reflect_columns(pool, schema, &name, enum_types).await?;
        let primary_key = reflect_primary_key(pool, schema, &name).await?;
        let foreign_keys = reflect_foreign_keys(pool, schema, &name).await?;
        let unique_constraints = reflect_unique_constraints(pool, schema, &name).await?;
        let check_constraints = reflect_check_constraints(pool, schema, &name).await?;
        let indexes = reflect_indexes(pool, schema, &name).await?;
        let triggers = reflect_triggers(pool, schema, &name).await?;
        let comment = reflect_table_comment(pool, schema, &name).await?;
        tables.push(Table {
            schema: schema.to_string(),
            name,
            columns,
            primary_key,
            foreign_keys,
            unique_constraints,
            check_constraints,
            indexes,
            triggers,
            comment,
            partition_key,
            partition_parent,
        });
    }
    Ok(tables)
}

async fn reflect_columns(
    pool: &PgPool,
    schema: &str,
    table: &str,
    enum_types: &HashMap<String, Vec<String>>,
) -> Result<Vec<Column>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT a.attname AS name, a.attnum AS ordinal_position, \
                format_type(a.atttypid, a.atttypmod) AS native_type, \
                NOT a.attnotnull AS nullable, \
                pg_get_expr(ad.adbin, ad.adrelid) AS default_expr, \
                a.attidentity::text AS identity, \
                col_description(a.attrelid, a.attnum) AS comment \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum \
         WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("name").map_err(db_err)?;
        let ordinal_position: i16 = row.try_get("ordinal_position").map_err(db_err)?;
        let native_type: String = row.try_get("native_type").map_err(db_err)?;
        let nullable: bool = row.try_get("nullable").map_err(db_err)?;
        let default_expr: Option<String> = row.try_get("default_expr").map_err(db_err)?;
        let identity: String = row.try_get("identity").map_err(db_err)?;
        let comment: Option<String> = row.try_get("comment").map_err(db_err)?;

        let category = map_pg_type(&native_type, enum_types);
        let is_identity = identity == "a" || identity == "d";
        let identity_generation = match identity.as_str() {
            "a" => Some(IdentityGeneration::Always),
            "d" => Some(IdentityGeneration::ByDefault),
            _ => None,
        };

        columns.push(Column {
            name,
            ordinal_position: ordinal_position as u32,
            data_type: DataType { category, native_type },
            nullable,
            default: default_expr,
            is_identity,
            identity_generation,
            comment,
        });
    }
    Ok(columns)
}

async fn reflect_primary_key(pool: &PgPool, schema: &str, table: &str) -> Result<Option<PrimaryKey>, DdbCoreError> {
    let row = sqlx::query(
        "SELECT con.conname, array_agg(att.attname ORDER BY array_position(con.conkey, att.attnum)) AS columns \
         FROM pg_constraint con \
         JOIN pg_class rel ON rel.oid = con.conrelid \
         JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace \
         JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey) \
         WHERE nsp.nspname = $1 AND rel.relname = $2 AND con.contype = 'p' \
         GROUP BY con.conname",
    )
    .bind(schema)
    .bind(table)
    .fetch_optional(pool)
    .await
    .map_err(db_err)?;

    Ok(match row {
        Some(row) => {
            let name: Option<String> = row.try_get("conname").map_err(db_err)?;
            let columns: Vec<String> = row.try_get("columns").map_err(db_err)?;
            Some(PrimaryKey { name, columns })
        }
        None => None,
    })
}

async fn resolve_column_names(pool: &PgPool, relid: i64, attnums: &[i16]) -> Result<Vec<String>, DdbCoreError> {
    if attnums.is_empty() {
        return Ok(vec![]);
    }
    let rows = sqlx::query("SELECT attnum, attname FROM pg_attribute WHERE attrelid = $1 AND attnum = ANY($2)")
        .bind(relid)
        .bind(attnums)
        .fetch_all(pool)
        .await
        .map_err(db_err)?;

    let mut by_num: HashMap<i16, String> = HashMap::new();
    for row in rows {
        let attnum: i16 = row.try_get("attnum").map_err(db_err)?;
        let attname: String = row.try_get("attname").map_err(db_err)?;
        by_num.insert(attnum, attname);
    }
    attnums
        .iter()
        .map(|n| {
            by_num
                .get(n)
                .cloned()
                .ok_or_else(|| DdbCoreError::Reflection(format!("column attnum {n} not found for relid {relid}")))
        })
        .collect()
}

fn map_ref_action(c: &str) -> ReferentialAction {
    match c {
        "a" => ReferentialAction::NoAction,
        "r" => ReferentialAction::Restrict,
        "c" => ReferentialAction::Cascade,
        "n" => ReferentialAction::SetNull,
        "d" => ReferentialAction::SetDefault,
        _ => ReferentialAction::NoAction,
    }
}

async fn reflect_foreign_keys(pool: &PgPool, schema: &str, table: &str) -> Result<Vec<ForeignKey>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT con.conname, con.conrelid::bigint AS conrelid, con.conkey, \
                con.confrelid::bigint AS confrelid, con.confkey, \
                con.confupdtype::text AS confupdtype, con.confdeltype::text AS confdeltype, \
                rn.nspname AS ref_schema, rc.relname AS ref_table \
         FROM pg_constraint con \
         JOIN pg_class rel ON rel.oid = con.conrelid \
         JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace \
         JOIN pg_class rc ON rc.oid = con.confrelid \
         JOIN pg_namespace rn ON rn.oid = rc.relnamespace \
         WHERE nsp.nspname = $1 AND rel.relname = $2 AND con.contype = 'f' \
         ORDER BY con.conname",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut fks = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("conname").map_err(db_err)?;
        let conrelid: i64 = row.try_get("conrelid").map_err(db_err)?;
        let conkey: Vec<i16> = row.try_get("conkey").map_err(db_err)?;
        let confrelid: i64 = row.try_get("confrelid").map_err(db_err)?;
        let confkey: Vec<i16> = row.try_get("confkey").map_err(db_err)?;
        let confupdtype: String = row.try_get("confupdtype").map_err(db_err)?;
        let confdeltype: String = row.try_get("confdeltype").map_err(db_err)?;
        let ref_schema: String = row.try_get("ref_schema").map_err(db_err)?;
        let ref_table: String = row.try_get("ref_table").map_err(db_err)?;

        let columns = resolve_column_names(pool, conrelid, &conkey).await?;
        let referenced_columns = resolve_column_names(pool, confrelid, &confkey).await?;

        fks.push(ForeignKey {
            name,
            columns,
            referenced_schema: ref_schema,
            referenced_table: ref_table,
            referenced_columns,
            on_delete: map_ref_action(&confdeltype),
            on_update: map_ref_action(&confupdtype),
        });
    }
    Ok(fks)
}

async fn reflect_unique_constraints(pool: &PgPool, schema: &str, table: &str) -> Result<Vec<UniqueConstraint>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT con.conname, array_agg(att.attname ORDER BY array_position(con.conkey, att.attnum)) AS columns \
         FROM pg_constraint con \
         JOIN pg_class rel ON rel.oid = con.conrelid \
         JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace \
         JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey) \
         WHERE nsp.nspname = $1 AND rel.relname = $2 AND con.contype = 'u' \
         GROUP BY con.conname ORDER BY con.conname",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut constraints = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("conname").map_err(db_err)?;
        let columns: Vec<String> = row.try_get("columns").map_err(db_err)?;
        constraints.push(UniqueConstraint { name, columns });
    }
    Ok(constraints)
}

async fn reflect_check_constraints(pool: &PgPool, schema: &str, table: &str) -> Result<Vec<CheckConstraint>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT con.conname, pg_get_constraintdef(con.oid) AS definition \
         FROM pg_constraint con \
         JOIN pg_class rel ON rel.oid = con.conrelid \
         JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace \
         WHERE nsp.nspname = $1 AND rel.relname = $2 AND con.contype = 'c' \
         ORDER BY con.conname",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut constraints = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("conname").map_err(db_err)?;
        let definition: String = row.try_get("definition").map_err(db_err)?;
        // Canonical contract: `expression` is the bare boolean expression.
        // pg_get_constraintdef returns "CHECK (...)" — strip the framing.
        let expression = definition
            .trim()
            .strip_prefix("CHECK (")
            .and_then(|s| s.strip_suffix(')'))
            .map(str::to_string)
            .unwrap_or(definition);
        constraints.push(CheckConstraint { name, expression });
    }
    Ok(constraints)
}

async fn reflect_indexes(pool: &PgPool, schema: &str, table: &str) -> Result<Vec<Index>, DdbCoreError> {
    // The NOT EXISTS clause excludes indexes that back a constraint
    // (unique/PK/exclusion) — those are already reflected as constraints,
    // and reporting them again as plain indexes makes render_ddl emit two
    // conflicting CREATE statements for the same object.
    let rows = sqlx::query(
        "SELECT ic.oid::bigint AS index_oid, ic.relname AS index_name, am.amname AS method, idx.indisunique AS is_unique \
         FROM pg_index idx \
         JOIN pg_class ic ON ic.oid = idx.indexrelid \
         JOIN pg_class tc ON tc.oid = idx.indrelid \
         JOIN pg_namespace n ON n.oid = tc.relnamespace \
         JOIN pg_am am ON am.oid = ic.relam \
         WHERE n.nspname = $1 AND tc.relname = $2 AND NOT idx.indisprimary \
         AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = idx.indexrelid) \
         ORDER BY ic.relname",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut indexes = Vec::with_capacity(rows.len());
    for row in rows {
        let index_oid: i64 = row.try_get("index_oid").map_err(db_err)?;
        let index_name: String = row.try_get("index_name").map_err(db_err)?;
        let method: Option<String> = row.try_get("method").map_err(db_err)?;
        let is_unique: bool = row.try_get("is_unique").map_err(db_err)?;

        let columns = resolve_index_columns(pool, index_oid).await?;
        indexes.push(Index { name: index_name, columns, unique: is_unique, method });
    }
    Ok(indexes)
}

/// Column order is preserved via `WITH ORDINALITY`. Sort direction (ASC vs
/// DESC per column) isn't resolved yet — every column reports `descending:
/// false` for now; that's a known v1 simplification, not a claim of full
/// fidelity.
async fn resolve_index_columns(pool: &PgPool, index_oid: i64) -> Result<Vec<IndexColumn>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT a.attname \
         FROM pg_index idx \
         JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS k(attnum, ord) ON true \
         JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = k.attnum \
         WHERE idx.indexrelid = $1 \
         ORDER BY k.ord",
    )
    .bind(index_oid)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    rows.into_iter()
        .map(|row| {
            let name: String = row.try_get("attname").map_err(db_err)?;
            Ok(IndexColumn { name, descending: false })
        })
        .collect()
}

fn decode_trigger_type(tgtype: i16) -> (TriggerTiming, Vec<TriggerEvent>) {
    let bits = tgtype as u16;
    let timing = if bits & (1 << 6) != 0 {
        TriggerTiming::InsteadOf
    } else if bits & (1 << 1) != 0 {
        TriggerTiming::Before
    } else {
        TriggerTiming::After
    };

    let mut events = Vec::new();
    if bits & (1 << 2) != 0 {
        events.push(TriggerEvent::Insert);
    }
    if bits & (1 << 3) != 0 {
        events.push(TriggerEvent::Delete);
    }
    if bits & (1 << 4) != 0 {
        events.push(TriggerEvent::Update);
    }
    if bits & (1 << 5) != 0 {
        events.push(TriggerEvent::Truncate);
    }
    (timing, events)
}

async fn reflect_triggers(pool: &PgPool, schema: &str, table: &str) -> Result<Vec<Trigger>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT t.tgname, t.tgtype, p.proname AS function_name, pg_get_triggerdef(t.oid) AS definition \
         FROM pg_trigger t \
         JOIN pg_class c ON c.oid = t.tgrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         JOIN pg_proc p ON p.oid = t.tgfoid \
         WHERE n.nspname = $1 AND c.relname = $2 AND NOT t.tgisinternal \
         ORDER BY t.tgname",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut triggers = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("tgname").map_err(db_err)?;
        let tgtype: i16 = row.try_get("tgtype").map_err(db_err)?;
        let function_name: String = row.try_get("function_name").map_err(db_err)?;
        let definition: String = row.try_get("definition").map_err(db_err)?;

        let (timing, events) = decode_trigger_type(tgtype);
        triggers.push(Trigger {
            name,
            timing,
            events,
            function_name: Some(function_name),
            body: Some(definition),
        });
    }
    Ok(triggers)
}

async fn reflect_table_comment(pool: &PgPool, schema: &str, table: &str) -> Result<Option<String>, DdbCoreError> {
    let comment: Option<String> = sqlx::query_scalar(
        "SELECT obj_description(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_one(pool)
    .await
    .map_err(db_err)?;

    Ok(comment)
}

async fn reflect_views(pool: &PgPool, schema: &str, enum_types: &HashMap<String, Vec<String>>) -> Result<Vec<View>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT c.relname AS name, pg_get_viewdef(c.oid) AS definition \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind IN ('v', 'm') ORDER BY c.relname",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut views = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("name").map_err(db_err)?;
        let definition: String = row.try_get("definition").map_err(db_err)?;
        let columns = reflect_columns(pool, schema, &name, enum_types).await?;
        views.push(View { schema: schema.to_string(), name, definition, columns });
    }
    Ok(views)
}

async fn reflect_sequences(pool: &PgPool, schema: &str) -> Result<Vec<Sequence>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT sequencename, data_type::text AS data_type, start_value, increment_by, min_value, max_value, cycle \
         FROM pg_sequences WHERE schemaname = $1 ORDER BY sequencename",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let empty_enums = HashMap::new();
    let mut sequences = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("sequencename").map_err(db_err)?;
        let native_type: String = row.try_get("data_type").map_err(db_err)?;
        let start_value: i64 = row.try_get("start_value").map_err(db_err)?;
        let increment_by: i64 = row.try_get("increment_by").map_err(db_err)?;
        let min_value: Option<i64> = row.try_get("min_value").map_err(db_err)?;
        let max_value: Option<i64> = row.try_get("max_value").map_err(db_err)?;
        let cycle: bool = row.try_get("cycle").map_err(db_err)?;

        let category = map_pg_type(&native_type, &empty_enums);
        sequences.push(Sequence {
            schema: schema.to_string(),
            name,
            data_type: DataType { category, native_type },
            start_value,
            increment: increment_by,
            min_value,
            max_value,
            cycle,
        });
    }
    Ok(sequences)
}

/// Function/procedure metadata only; `internal`/`c`-language routines are
/// excluded because `pg_get_functiondef` can't render a body for them and
/// there is nothing meaningful to capture beyond the name.
async fn reflect_functions(pool: &PgPool, schema: &str) -> Result<Vec<Function>, DdbCoreError> {
    let rows = sqlx::query(
        "SELECT p.proname AS name, pg_get_function_arguments(p.oid) AS args, \
                pg_get_function_result(p.oid) AS return_type, l.lanname AS language, \
                pg_get_functiondef(p.oid) AS body \
         FROM pg_proc p \
         JOIN pg_namespace n ON n.oid = p.pronamespace \
         JOIN pg_language l ON l.oid = p.prolang \
         WHERE n.nspname = $1 AND p.prokind IN ('f', 'p') AND l.lanname NOT IN ('internal', 'c') \
         ORDER BY p.proname",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    let mut functions = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("name").map_err(db_err)?;
        let args: String = row.try_get("args").map_err(db_err)?;
        let return_type: Option<String> = row.try_get("return_type").map_err(db_err)?;
        let language: String = row.try_get("language").map_err(db_err)?;
        let body: Option<String> = row.try_get("body").map_err(db_err)?;

        // Argument list is captured as a single opaque signature string for
        // now rather than parsed per-parameter — bodies are already opaque
        // per-engine text, so full per-arg fidelity isn't load-bearing yet.
        let arguments = if args.trim().is_empty() {
            vec![]
        } else {
            vec![FunctionArgument { name: None, native_type: args }]
        };

        functions.push(Function { schema: schema.to_string(), name, arguments, return_type, language, body });
    }
    Ok(functions)
}

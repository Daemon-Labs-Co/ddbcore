use ddbcore::{DdbCoreError, KeyRange, RowStream, StreamOptions, TableRef, TypeCategory, Value};
use futures::stream::{self, TryStreamExt};
use sqlx::Connection as _;
use uuid::Uuid;

use crate::connection::PostgresConnection;
use crate::query::RowDecoder;
use crate::types::map_pg_type;
use crate::util::{quote_ident, quote_qualified};

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::Query(e.to_string())
}

/// True when the row decoder has a native, correct decode for this
/// category. Everything else (enums, intervals, xml, bit strings,
/// geometrics, unknown extension types) arrives in Postgres's BINARY wire
/// format under prepared statements, and reinterpreting those bytes as
/// text corrupts data — so such columns are cast to ::text server-side in
/// the streaming SELECT instead.
fn decodes_natively(category: &TypeCategory) -> bool {
    match category {
        TypeCategory::Boolean
        | TypeCategory::SmallInt
        | TypeCategory::Integer
        | TypeCategory::BigInt
        | TypeCategory::Decimal { .. }
        | TypeCategory::Real
        | TypeCategory::Double
        | TypeCategory::Char { .. }
        | TypeCategory::VarChar { .. }
        | TypeCategory::Text
        | TypeCategory::Binary { .. }
        | TypeCategory::VarBinary { .. }
        | TypeCategory::Blob
        | TypeCategory::Date
        | TypeCategory::Timestamp { .. }
        | TypeCategory::Uuid
        | TypeCategory::Json { .. } => true,
        // timetz has no NaiveTime decode — cast to ::text like other
        // non-native types.
        TypeCategory::Time { with_timezone, .. } => !with_timezone,
        TypeCategory::Array { element } => decodes_natively(element),
        _ => false,
    }
}

/// Renders a key-range bound as a safe SQL literal. Postgres's DECLARE
/// CURSOR cannot reliably take bind parameters, so range bounds are
/// inlined — with strict escaping, and only for value shapes that make
/// sense as range keys.
fn render_literal(value: &Value) -> Result<String, DdbCoreError> {
    fn quoted(s: &str) -> String {
        format!("'{}'", s.replace('\'', "''"))
    }
    match value {
        Value::SmallInt(n) => Ok(n.to_string()),
        Value::Integer(n) => Ok(n.to_string()),
        Value::BigInt(n) => Ok(n.to_string()),
        Value::Decimal(d) => Ok(d.to_string()),
        Value::Real(f) => Ok(f.to_string()),
        Value::Double(f) => Ok(f.to_string()),
        Value::Boolean(b) => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
        Value::Text(s) => Ok(quoted(s)),
        Value::Uuid(u) => Ok(quoted(&u.to_string())),
        Value::Date(d) => Ok(quoted(&d.to_string())),
        Value::Time(t) => Ok(quoted(&t.to_string())),
        Value::Timestamp(ts) => Ok(quoted(&ts.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())),
        Value::TimestampNaive(ts) => Ok(quoted(&ts.format("%Y-%m-%d %H:%M:%S%.f").to_string())),
        other => Err(DdbCoreError::Unsupported(format!("value not usable as a key-range bound: {other:?}"))),
    }
}

fn render_key_range(range: &KeyRange) -> Result<String, DdbCoreError> {
    let col = quote_ident(&range.column);
    let mut clauses = Vec::with_capacity(2);
    if let Some(lower) = &range.lower {
        clauses.push(format!("{col} >= {}", render_literal(lower)?));
    }
    if let Some(upper) = &range.upper {
        clauses.push(format!("{col} < {}", render_literal(upper)?));
    }
    if clauses.is_empty() {
        return Err(DdbCoreError::Query("key_range must set at least one bound".into()));
    }
    Ok(format!(" WHERE {}", clauses.join(" AND ")))
}

/// Builds the streaming SELECT from the table's reflected column types:
/// natively-decodable columns pass through as-is, everything else is cast
/// to ::text so the wire value is guaranteed readable. Honors column
/// projection and key-range restriction from `options`.
async fn build_select(conn: &PostgresConnection, table: &TableRef, options: &StreamOptions) -> Result<String, DdbCoreError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum",
    )
    .bind(&table.schema)
    .bind(&table.name)
    .fetch_all(&conn.pool)
    .await
    .map_err(db_err)?;

    if rows.is_empty() {
        return Err(DdbCoreError::Query(format!("table {}.{} has no columns or does not exist", table.schema, table.name)));
    }

    // Projection: keep the caller's requested order; error on columns the
    // table doesn't have rather than silently streaming something else.
    let selected: Vec<&(String, String)> = match &options.columns {
        None => rows.iter().collect(),
        Some(requested) => requested
            .iter()
            .map(|want| {
                rows.iter()
                    .find(|(name, _)| name == want)
                    .ok_or_else(|| DdbCoreError::Query(format!("column {want} does not exist on {}.{}", table.schema, table.name)))
            })
            .collect::<Result<_, _>>()?,
    };

    let empty_enums = std::collections::HashMap::new();
    let select_list = selected
        .iter()
        .map(|(name, native_type)| {
            let quoted = quote_ident(name);
            if decodes_natively(&map_pg_type(native_type, &empty_enums)) {
                quoted
            } else {
                format!("{quoted}::text AS {quoted}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let where_clause = match &options.key_range {
        Some(range) => render_key_range(range)?,
        None => String::new(),
    };

    Ok(format!("SELECT {select_list} FROM {}{where_clause}", quote_qualified(&table.schema, &table.name)))
}

/// Reads `table` through a server-side cursor, `batch_size` rows at a
/// time, on a DEDICATED connection detached from the pool: a multi-hour
/// scan never pins a shared pool slot, and dropping the stream mid-way
/// closes the socket (one reconnect) instead of leaving a transaction and
/// cursor pinned on a pooled connection.
pub(crate) async fn stream_rows(conn: &PostgresConnection, table: &TableRef, options: StreamOptions) -> Result<RowStream, DdbCoreError> {
    if options.batch_size == 0 {
        // FETCH FORWARD 0 returns zero rows forever — an infinite loop.
        return Err(DdbCoreError::Query("batch_size must be >= 1".into()));
    }
    let batch_size = options.batch_size;

    let sql = build_select(conn, table, &options).await?;
    let cursor_name = format!("ddbcore_cursor_{}", Uuid::new_v4().simple());

    let mut db = conn.pool.acquire().await.map_err(db_err)?.detach();

    sqlx::query("BEGIN").execute(&mut db).await.map_err(db_err)?;
    sqlx::query(&format!("DECLARE {cursor_name} NO SCROLL CURSOR FOR {sql}"))
        .execute(&mut db)
        .await
        .map_err(db_err)?;

    let fetch_sql = format!("FETCH FORWARD {batch_size} FROM {cursor_name}");

    let batches = stream::try_unfold((db, None::<RowDecoder>, false), move |(mut db, mut decoders, done)| {
        let fetch_sql = fetch_sql.clone();
        async move {
            if done {
                // Normal completion: end the (read-only) transaction and
                // close the dedicated connection gracefully.
                let _ = sqlx::query("COMMIT").execute(&mut db).await;
                let _ = db.close().await;
                return Ok(None);
            }
            let rows = sqlx::query(&fetch_sql).fetch_all(&mut db).await.map_err(db_err)?;
            let is_last = rows.len() < batch_size;
            let batch = match rows.first() {
                None => Vec::new(),
                Some(first) => {
                    let d = decoders.get_or_insert_with(|| RowDecoder::for_row(first));
                    // into_iter: consume each sqlx row buffer as it
                    // converts instead of holding both representations of
                    // the full batch alive.
                    rows.into_iter().map(|row| d.decode(&row)).collect::<Result<Vec<_>, _>>()?
                }
            };
            Ok(Some((batch, (db, decoders, is_last))))
        }
    });

    let rows = batches.map_ok(|batch| stream::iter(batch.into_iter().map(Ok))).try_flatten();

    Ok(Box::pin(rows))
}

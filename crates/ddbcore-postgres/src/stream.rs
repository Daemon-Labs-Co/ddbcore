use ddbcore::{DdbCoreError, RowStream, TableRef, TypeCategory};
use futures::stream::{self, TryStreamExt};
use uuid::Uuid;

use crate::connection::PostgresConnection;
use crate::query::pg_row_to_row;
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
        | TypeCategory::Time { .. }
        | TypeCategory::Timestamp { .. }
        | TypeCategory::Uuid
        | TypeCategory::Json => true,
        TypeCategory::Array { element } => decodes_natively(element),
        _ => false,
    }
}

/// Builds the streaming SELECT list from the table's reflected column
/// types: natively-decodable columns pass through as-is, everything else
/// is cast to ::text so the wire value is guaranteed readable.
async fn build_select(conn: &PostgresConnection, table: &TableRef) -> Result<String, DdbCoreError> {
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

    let empty_enums = std::collections::HashMap::new();
    let select_list = rows
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

    Ok(format!("SELECT {select_list} FROM {}", quote_qualified(&table.schema, &table.name)))
}

/// Reads `table` through a server-side cursor, `batch_size` rows at a
/// time, so arbitrarily large tables never load into memory at once. The
/// backing transaction lives for the lifetime of the stream and is rolled
/// back (a no-op for a read-only cursor) when the stream is dropped.
pub(crate) async fn stream_rows(conn: &PostgresConnection, table: &TableRef, batch_size: usize) -> Result<RowStream, DdbCoreError> {
    let sql = build_select(conn, table).await?;
    let cursor_name = format!("ddbcore_cursor_{}", Uuid::new_v4().simple());

    let mut tx = conn.pool.begin().await.map_err(db_err)?;
    sqlx::query(&format!("DECLARE {cursor_name} NO SCROLL CURSOR FOR {sql}"))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

    let fetch_sql = format!("FETCH FORWARD {batch_size} FROM {cursor_name}");

    let batches = stream::try_unfold((tx, false), move |(mut tx, done)| {
        let fetch_sql = fetch_sql.clone();
        async move {
            if done {
                return Ok(None);
            }
            let rows = sqlx::query(&fetch_sql).fetch_all(&mut *tx).await.map_err(db_err)?;
            let is_last = rows.len() < batch_size;
            let batch = rows.iter().map(pg_row_to_row).collect::<Result<Vec<_>, _>>()?;
            Ok(Some((batch, (tx, is_last))))
        }
    });

    let rows = batches.map_ok(|batch| stream::iter(batch.into_iter().map(Ok))).try_flatten();

    Ok(Box::pin(rows))
}

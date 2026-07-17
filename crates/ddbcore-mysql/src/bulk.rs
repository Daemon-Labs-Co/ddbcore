use ddbcore::{DdbCoreError, Row as DdbRow, RowStream, TableRef, Value};
use futures::StreamExt;

use crate::connection::MySqlConnection;
use crate::util::quote_qualified;

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::BulkWrite(e.to_string())
}

/// Writes `rows` into `table` via batched multi-row `INSERT` statements.
///
/// This is **not** MySQL's fastest possible bulk-load path — that would
/// be `LOAD DATA LOCAL INFILE`, which `sqlx` doesn't expose a clean typed
/// API for (it requires a client-side local-infile handler callback that
/// isn't well-supported through `sqlx`'s query builder). Batched
/// multi-row `INSERT` is still dramatically faster than row-by-row
/// inserts and is fully typed/parameterized rather than hand-serialized
/// text, but it is a deliberate v1 simplification versus the Postgres
/// adapter's true `COPY`-based path.
///
/// Batch size is bounded three ways:
/// - target row count (500), keeping the SQL shape constant across full
///   batches so sqlx's per-connection statement cache keeps hitting;
/// - the MySQL protocol's hard u16 cap of 65,535 placeholders per
///   prepared statement — without this, any table with >= 132 columns
///   fails outright;
/// - an estimated byte budget, so 500 rows of 16 MB blobs can't buffer
///   gigabytes or blow past the server's max_allowed_packet (64 MB
///   default).
pub(crate) async fn bulk_write(conn: &MySqlConnection, table: &TableRef, mut rows: RowStream) -> Result<u64, DdbCoreError> {
    const TARGET_ROWS: usize = 500;
    const MAX_PLACEHOLDERS: usize = u16::MAX as usize;
    const BYTE_BUDGET: usize = 8 << 20; // 8 MiB, well under max_allowed_packet

    let qualified = quote_qualified(&table.schema, &table.name);
    let mut total = 0u64;
    let mut batch: Vec<DdbRow> = Vec::new();
    let mut batch_bytes = 0usize;
    let mut max_rows_per_batch = TARGET_ROWS;

    while let Some(row) = rows.next().await {
        let row = row?;
        // Reject array values rather than silently writing NULL.
        if row.0.iter().any(|v| matches!(v, Value::Array(_))) {
            return Err(DdbCoreError::Unsupported("MySQL has no array type; array values cannot be bulk-written".into()));
        }

        if batch.is_empty() && total == 0 {
            let cols = row.0.len().max(1);
            max_rows_per_batch = TARGET_ROWS.min(MAX_PLACEHOLDERS / cols).max(1);
        }

        batch_bytes += estimated_row_bytes(&row);
        batch.push(row);

        if batch.len() >= max_rows_per_batch || batch_bytes >= BYTE_BUDGET {
            total += flush_batch(conn, &qualified, &batch).await?;
            batch.clear();
            batch_bytes = 0;
        }
    }
    if !batch.is_empty() {
        total += flush_batch(conn, &qualified, &batch).await?;
    }

    Ok(total)
}

/// Rough per-row wire-size estimate: variable-size payloads at their
/// actual length plus a fixed per-value overhead for everything else.
/// Doesn't need to be exact — it only bounds batch memory.
fn estimated_row_bytes(row: &DdbRow) -> usize {
    row.0
        .iter()
        .map(|v| match v {
            Value::Text(s) => s.len() + 16,
            Value::Binary(b) => b.len() + 16,
            Value::Json(j) => match j {
                serde_json::Value::String(s) => s.len() + 32,
                _ => 256,
            },
            _ => 16,
        })
        .sum()
}

async fn flush_batch(conn: &MySqlConnection, qualified_table: &str, batch: &[DdbRow]) -> Result<u64, DdbCoreError> {
    let Some(first) = batch.first() else { return Ok(0) };
    let cols = first.0.len();

    let row_placeholder = format!("({})", vec!["?"; cols].join(", "));
    let placeholders = vec![row_placeholder; batch.len()].join(", ");
    let sql = format!("INSERT INTO {qualified_table} VALUES {placeholders}");

    let mut query = sqlx::query(&sql);
    for row in batch {
        for value in &row.0 {
            query = bind_value(query, value);
        }
    }

    let result = query.execute(&conn.pool).await.map_err(db_err)?;
    Ok(result.rows_affected())
}

fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    value: &'q Value,
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    match value {
        Value::Null => query.bind(None::<String>),
        Value::Boolean(b) => query.bind(b),
        Value::SmallInt(n) => query.bind(n),
        Value::Integer(n) => query.bind(n),
        Value::BigInt(n) => query.bind(n),
        Value::Decimal(d) => query.bind(d),
        Value::Real(f) => query.bind(f),
        Value::Double(f) => query.bind(f),
        Value::Text(s) => query.bind(s),
        Value::Binary(b) => query.bind(b),
        Value::Date(d) => query.bind(d),
        Value::Time(t) => query.bind(t),
        Value::Timestamp(ts) => query.bind(ts.naive_utc()),
        Value::TimestampNaive(ts) => query.bind(ts),
        Value::Uuid(u) => query.bind(u.to_string()),
        Value::Json(j) => query.bind(j),
        // Unreachable: bulk_write rejects rows containing arrays upfront.
        Value::Array(_) => query.bind(None::<String>),
    }
}

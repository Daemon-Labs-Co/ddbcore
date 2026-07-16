use ddbcore::{DdbCoreError, Row as DdbRow, RowStream, TableRef, Value};
use futures::StreamExt;
use sqlx::postgres::PgPoolCopyExt;

use crate::connection::PostgresConnection;
use crate::util::quote_qualified;

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::BulkWrite(e.to_string())
}

/// Writes `rows` into `table` via Postgres `COPY ... FROM STDIN`, the
/// engine's fast bulk-load path — not row-by-row `INSERT`s. Rows are
/// pulled from the stream and flushed to the COPY sink in fixed-size
/// chunks so a multi-hundred-million-row write stays bounded in memory.
pub(crate) async fn bulk_write(conn: &PostgresConnection, table: &TableRef, mut rows: RowStream) -> Result<u64, DdbCoreError> {
    const FLUSH_EVERY: usize = 10_000;

    let sql = format!("COPY {} FROM STDIN WITH (FORMAT text)", quote_qualified(&table.schema, &table.name));
    let mut copy = conn.pool.copy_in_raw(&sql).await.map_err(db_err)?;

    let mut buffer = String::new();
    let mut buffered_rows = 0usize;
    let mut total = 0u64;

    while let Some(row) = rows.next().await {
        let row = row?;
        write_copy_line(&mut buffer, &row);
        buffered_rows += 1;
        total += 1;

        if buffered_rows >= FLUSH_EVERY {
            copy.send(buffer.as_bytes()).await.map_err(db_err)?;
            buffer.clear();
            buffered_rows = 0;
        }
    }

    if !buffer.is_empty() {
        copy.send(buffer.as_bytes()).await.map_err(db_err)?;
    }

    copy.finish().await.map_err(db_err)?;
    Ok(total)
}

fn write_copy_line(buffer: &mut String, row: &DdbRow) {
    for (i, value) in row.0.iter().enumerate() {
        if i > 0 {
            buffer.push('\t');
        }
        write_copy_value(buffer, value);
    }
    buffer.push('\n');
}

/// Encodes one value in Postgres COPY TEXT format: `\N` for NULL, and
/// backslash/tab/newline/carriage-return escaped for everything else.
/// Arrays are best-effort (`{...}` literal syntax) — nested arrays and
/// values containing `,`/`{`/`}` are not escaped correctly yet.
fn write_copy_value(buffer: &mut String, value: &Value) {
    match value {
        Value::Null => buffer.push_str("\\N"),
        Value::Boolean(b) => buffer.push_str(if *b { "t" } else { "f" }),
        Value::SmallInt(n) => buffer.push_str(&n.to_string()),
        Value::Integer(n) => buffer.push_str(&n.to_string()),
        Value::BigInt(n) => buffer.push_str(&n.to_string()),
        Value::Decimal(d) => buffer.push_str(&d.to_string()),
        Value::Real(f) => buffer.push_str(&f.to_string()),
        Value::Double(f) => buffer.push_str(&f.to_string()),
        Value::Text(s) => escape_copy_text(buffer, s),
        Value::Binary(b) => {
            buffer.push_str("\\\\x");
            for byte in b {
                buffer.push_str(&format!("{byte:02x}"));
            }
        }
        Value::Date(d) => buffer.push_str(&d.to_string()),
        Value::Time(t) => buffer.push_str(&t.to_string()),
        Value::Timestamp(ts) => buffer.push_str(&ts.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string()),
        Value::Uuid(u) => buffer.push_str(&u.to_string()),
        Value::Json(j) => escape_copy_text(buffer, &j.to_string()),
        Value::Array(items) => {
            buffer.push('{');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    buffer.push(',');
                }
                write_copy_value(buffer, item);
            }
            buffer.push('}');
        }
    }
}

fn escape_copy_text(buffer: &mut String, s: &str) {
    for ch in s.chars() {
        match ch {
            '\\' => buffer.push_str("\\\\"),
            '\t' => buffer.push_str("\\t"),
            '\n' => buffer.push_str("\\n"),
            '\r' => buffer.push_str("\\r"),
            c => buffer.push(c),
        }
    }
}

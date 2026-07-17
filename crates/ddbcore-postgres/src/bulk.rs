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
fn write_copy_value(buffer: &mut String, value: &Value) {
    match value {
        Value::Null => buffer.push_str("\\N"),
        Value::Text(s) => escape_copy_text(buffer, s),
        Value::Json(j) => escape_copy_text(buffer, &j.to_string()),
        Value::Binary(b) => {
            // COPY text sees `\x...`; the backslash needs COPY escaping.
            buffer.push_str("\\\\x");
            for byte in b {
                buffer.push_str(&format!("{byte:02x}"));
            }
        }
        Value::Array(items) => {
            // Build the array literal first, then COPY-escape the whole
            // thing — the literal's own quoting/escaping (`"…"`, `\"`,
            // `\\`) must survive COPY's escaping layer intact.
            let mut literal = String::new();
            write_array_literal(&mut literal, items);
            escape_copy_text(buffer, &literal);
        }
        scalar => buffer.push_str(&scalar_literal(scalar)),
    }
}

/// The plain text form of a scalar value (no COPY or array-literal
/// escaping applied). Only called for variants that render as simple
/// unquoted tokens plus Text/Binary/Json handled by callers.
fn scalar_literal(value: &Value) -> String {
    match value {
        Value::Boolean(b) => (if *b { "t" } else { "f" }).to_string(),
        Value::SmallInt(n) => n.to_string(),
        Value::Integer(n) => n.to_string(),
        Value::BigInt(n) => n.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Double(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Binary(b) => {
            let mut s = String::with_capacity(2 + b.len() * 2);
            s.push_str("\\x");
            for byte in b {
                s.push_str(&format!("{byte:02x}"));
            }
            s
        }
        Value::Date(d) => d.to_string(),
        Value::Time(t) => t.to_string(),
        Value::Timestamp(ts) => ts.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string(),
        Value::TimestampNaive(ts) => ts.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
        Value::Uuid(u) => u.to_string(),
        Value::Json(j) => j.to_string(),
        Value::Null | Value::Array(_) => unreachable!("handled by callers"),
    }
}

/// Renders a Postgres array literal (`{"a","b",NULL}`) with correct
/// element quoting: every non-null element is double-quoted with `\` and
/// `"` escaped, so elements containing `,`/`{`/`}`/whitespace/quotes
/// cannot split or merge; NULL elements are the bare token `NULL` (an
/// unquoted `\N` here would be the literal string "N").
fn write_array_literal(out: &mut String, items: &[Value]) {
    out.push('{');
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match item {
            Value::Null => out.push_str("NULL"),
            Value::Array(nested) => write_array_literal(out, nested),
            scalar => {
                let text = scalar_literal(scalar);
                out.push('"');
                for ch in text.chars() {
                    if ch == '"' || ch == '\\' {
                        out.push('\\');
                    }
                    out.push(ch);
                }
                out.push('"');
            }
        }
    }
    out.push('}');
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

use std::fmt::{self, Write as _};

use ddbcore::{DdbCoreError, Row as DdbRow, RowStream, TableRef, Value};
use futures::StreamExt;
use sqlx::Connection as _;

use crate::connection::PostgresConnection;
use crate::util::quote_qualified;

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::BulkWrite(e.to_string())
}

/// Writes `rows` into `table` via Postgres `COPY ... FROM STDIN`, the
/// engine's fast bulk-load path. Rows are encoded into a buffer that is
/// flushed on row count OR byte size, so both many-small-rows and
/// few-huge-rows workloads stay bounded in memory.
///
/// The copy runs on a dedicated connection detached from the pool: a
/// multi-hour bulk load never pins a shared pool slot.
pub(crate) async fn bulk_write(conn: &PostgresConnection, table: &TableRef, mut rows: RowStream) -> Result<u64, DdbCoreError> {
    const FLUSH_EVERY_ROWS: usize = 10_000;
    const FLUSH_EVERY_BYTES: usize = 1 << 20; // 1 MiB

    let sql = format!("COPY {} FROM STDIN WITH (FORMAT text)", quote_qualified(&table.schema, &table.name));

    let mut db = conn.pool.acquire().await.map_err(db_err)?.detach();
    let mut copy = db.copy_in_raw(&sql).await.map_err(db_err)?;

    let mut buffer = String::new();
    let mut buffered_rows = 0usize;
    let mut total = 0u64;

    while let Some(row) = rows.next().await {
        let row = row?;
        write_copy_line(&mut buffer, &row);
        buffered_rows += 1;
        total += 1;

        if buffered_rows >= FLUSH_EVERY_ROWS || buffer.len() >= FLUSH_EVERY_BYTES {
            copy.send(buffer.as_bytes()).await.map_err(db_err)?;
            buffer.clear();
            buffered_rows = 0;
        }
    }

    if !buffer.is_empty() {
        copy.send(buffer.as_bytes()).await.map_err(db_err)?;
    }

    copy.finish().await.map_err(db_err)?;
    let _ = db.close().await;
    Ok(total)
}

pub(crate) fn write_copy_line(buffer: &mut String, row: &DdbRow) {
    for (i, value) in row.0.iter().enumerate() {
        if i > 0 {
            buffer.push('\t');
        }
        write_copy_value(buffer, value);
    }
    buffer.push('\n');
}

const HEX: &[u8; 16] = b"0123456789abcdef";

fn push_hex(out: &mut impl fmt::Write, bytes: &[u8]) -> fmt::Result {
    for b in bytes {
        out.write_char(HEX[(b >> 4) as usize] as char)?;
        out.write_char(HEX[(b & 0x0f) as usize] as char)?;
    }
    Ok(())
}

/// Encodes one value in Postgres COPY TEXT format: `\N` for NULL, and
/// backslash/tab/newline/carriage-return escaped for everything else.
///
/// Everything writes DIRECTLY into `buffer` — no per-cell (or worse,
/// per-byte) intermediate `String`s. This is the hottest path in a
/// multi-TB copy; a 1B-row table passes through here 1B times.
fn write_copy_value(buffer: &mut String, value: &Value) {
    match value {
        Value::Null => buffer.push_str("\\N"),
        Value::Text(s) => escape_copy_text(buffer, s),
        // serde_json's Display writes compact JSON straight through the
        // escaping adapter — no intermediate String.
        Value::Json(j) => {
            let _ = write!(CopyEscaper(buffer), "{j}");
        }
        Value::Binary(b) => {
            // COPY text must see `\x...`; the backslash needs COPY escaping.
            buffer.push_str("\\\\x");
            let _ = push_hex(buffer, b);
        }
        Value::Array(items) => {
            // The array literal's own quoting (`"…"`, `\"`, `\\`) must
            // survive COPY's escaping layer — writing through the escaper
            // applies COPY escaping to the assembled literal on the fly.
            let _ = write_array_literal(&mut CopyEscaper(buffer), items);
        }
        scalar => {
            let _ = write_plain_scalar(buffer, scalar);
        }
    }
}

/// Writes a scalar's plain text form (no COPY or array escaping). Integer
/// and float formatting via `fmt::Write` uses stack buffers internally —
/// zero heap allocation per cell.
fn write_plain_scalar(w: &mut impl fmt::Write, value: &Value) -> fmt::Result {
    match value {
        Value::Boolean(b) => w.write_str(if *b { "t" } else { "f" }),
        Value::SmallInt(n) => write!(w, "{n}"),
        Value::Integer(n) => write!(w, "{n}"),
        Value::BigInt(n) => write!(w, "{n}"),
        Value::Decimal(d) => write!(w, "{d}"),
        Value::Real(f) => write!(w, "{f}"),
        Value::Double(f) => write!(w, "{f}"),
        Value::Text(s) => w.write_str(s),
        Value::Binary(b) => {
            w.write_str("\\x")?;
            push_hex(w, b)
        }
        Value::Date(d) => write!(w, "{d}"),
        Value::Time(t) => write!(w, "{t}"),
        Value::Timestamp(ts) => write!(w, "{}", ts.format("%Y-%m-%d %H:%M:%S%.f%:z")),
        Value::TimestampNaive(ts) => write!(w, "{}", ts.format("%Y-%m-%d %H:%M:%S%.f")),
        Value::Uuid(u) => write!(w, "{u}"),
        Value::Json(j) => write!(w, "{j}"),
        Value::Null | Value::Array(_) => unreachable!("handled by callers"),
    }
}

/// Renders a Postgres array literal (`{"a","b",NULL}`) with correct
/// element quoting: every non-null element is double-quoted with `\` and
/// `"` escaped, so elements containing `,`/`{`/`}`/whitespace/quotes
/// cannot split or merge; NULL elements are the bare token `NULL` (an
/// unquoted `\N` here would be the literal string "N").
fn write_array_literal(w: &mut impl fmt::Write, items: &[Value]) -> fmt::Result {
    w.write_char('{')?;
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            w.write_char(',')?;
        }
        match item {
            Value::Null => w.write_str("NULL")?,
            Value::Array(nested) => write_array_literal(w, nested)?,
            scalar => {
                w.write_char('"')?;
                write_plain_scalar(&mut ArrayElemEscaper(w), scalar)?;
                w.write_char('"')?;
            }
        }
    }
    w.write_char('}')
}

/// fmt::Write adapter applying COPY TEXT escaping on the fly.
struct CopyEscaper<'a>(&'a mut String);

impl fmt::Write for CopyEscaper<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        escape_copy_text(self.0, s);
        Ok(())
    }
}

/// fmt::Write adapter applying array-element escaping (`\` and `"`) on
/// the fly, layered over whatever writer is underneath.
struct ArrayElemEscaper<'a, W: fmt::Write>(&'a mut W);

impl<W: fmt::Write> fmt::Write for ArrayElemEscaper<'_, W> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for ch in s.chars() {
            if ch == '"' || ch == '\\' {
                self.0.write_char('\\')?;
            }
            self.0.write_char(ch)?;
        }
        Ok(())
    }
}

/// Span-scanning escape: most text contains none of the four COPY escape
/// bytes, so clean spans are appended with one `push_str` instead of
/// char-by-char. The escape bytes are all single-byte ASCII, so slicing
/// at their positions always lands on UTF-8 boundaries.
fn escape_copy_text(buffer: &mut String, s: &str) {
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let escaped: &str = match b {
            b'\\' => "\\\\",
            b'\t' => "\\t",
            b'\n' => "\\n",
            b'\r' => "\\r",
            _ => continue,
        };
        buffer.push_str(&s[start..i]);
        buffer.push_str(escaped);
        start = i + 1;
    }
    buffer.push_str(&s[start..]);
}

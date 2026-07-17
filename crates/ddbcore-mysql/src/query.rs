use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use ddbcore::{DdbCoreError, Row as DdbRow, RowStream, Value};
use futures::TryStreamExt;
use rust_decimal::Decimal;
use sqlx::mysql::MySqlRow;
use sqlx::{Column as _, Connection as _, Row as _, TypeInfo};
use uuid::Uuid;

use crate::connection::MySqlConnection;

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::Query(e.to_string())
}

pub(crate) async fn execute_query(conn: &MySqlConnection, sql: &str, params: &[Value]) -> Result<Vec<DdbRow>, DdbCoreError> {
    reject_array_params(params)?;

    let mut query = sqlx::query(sql);
    for param in params {
        query = bind_value(query, param);
    }

    let rows = query.fetch_all(&conn.pool).await.map_err(db_err)?;
    let Some(first) = rows.first() else { return Ok(vec![]) };
    let decoders = RowDecoder::for_row(first);
    rows.iter().map(|row| decoders.decode(row)).collect()
}

/// Streaming variant for large ad-hoc result sets. Runs on a dedicated
/// (detached-from-pool) connection: MySQL's wire protocol makes dropping
/// an unfinished resultset on a POOLED connection force the next pool
/// user to read-and-discard every remaining row before its own query runs
/// — on a dedicated connection, dropping the stream just closes the
/// socket (one reconnect).
pub(crate) async fn execute_query_stream(conn: &MySqlConnection, sql: &str, params: &[Value]) -> Result<RowStream, DdbCoreError> {
    reject_array_params(params)?;

    let mut db = conn.pool.acquire().await.map_err(db_err)?.detach();
    let sql = sql.to_string();
    let params: Vec<Value> = params.to_vec();

    let stream = async_stream::try_stream! {
        {
            let mut query = sqlx::query(&sql);
            for param in &params {
                query = bind_value(query, param);
            }
            let mut rows = query.fetch(&mut db).map_err(db_err);
            let mut decoders: Option<RowDecoder> = None;
            while let Some(row) = rows.try_next().await? {
                let d = decoders.get_or_insert_with(|| RowDecoder::for_row(&row));
                yield d.decode(&row)?;
            }
        }
        let _ = db.close().await;
    };

    Ok(Box::pin(stream))
}

pub(crate) fn reject_array_params(params: &[Value]) -> Result<(), DdbCoreError> {
    // Refuse array params outright rather than silently binding NULL —
    // writing NULL where the caller supplied data is corruption. (MySQL
    // has no array type at all.)
    if params.iter().any(|p| matches!(p, Value::Array(_))) {
        return Err(DdbCoreError::Unsupported("MySQL has no array type; array parameters are not supported".into()));
    }
    Ok(())
}

pub(crate) fn bind_value<'q>(
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
        // Unreachable: callers reject array values before binding.
        Value::Array(_) => query.bind(None::<String>),
    }
}

/// Per-column decode plan, built ONCE per result set from the first row's
/// column metadata, then applied by index to every row — replaces
/// per-cell type-name string matching on the hot path.
pub(crate) struct RowDecoder(Vec<ColumnDecoder>);

enum ColumnDecoder {
    I8AsSmall,
    I16,
    I32,
    I64,
    U8AsSmall,
    U16AsInt,
    U32AsBig,
    U64AsDec,
    YearAsInt,
    Dec,
    F32,
    F64,
    Str,
    Bytes,
    /// BIT columns canonicalize to bit-string TEXT ("0101...") — matching
    /// how the Postgres adapter surfaces bit columns (via ::text cast) so
    /// the same TypeCategory::Bit yields the same Value shape on both
    /// engines. Caveat: MySQL returns whole bytes, so BIT(5) renders 8
    /// zero-padded chars where Postgres renders exactly 5.
    BitText,
    Date,
    Time,
    TsNaive,
    Uuid,
    Json,
    /// Unknown wire type: try text (SET arrives as text), fall back to raw
    /// bytes (geometry arrives as WKB) — never UTF-8-reinterpret binary.
    Fallback,
}

impl RowDecoder {
    pub(crate) fn for_row(row: &MySqlRow) -> Self {
        let decoders = row
            .columns()
            .iter()
            .map(|col| match col.type_info().name() {
                "BOOLEAN" | "TINYINT" => ColumnDecoder::I8AsSmall,
                "SMALLINT" => ColumnDecoder::I16,
                "MEDIUMINT" | "INT" => ColumnDecoder::I32,
                "BIGINT" => ColumnDecoder::I64,
                // Unsigned columns MUST decode through unsigned Rust types
                // and widen into the next-larger canonical variant —
                // decoding `INT UNSIGNED` as i32 loses half the range.
                "TINYINT UNSIGNED" => ColumnDecoder::U8AsSmall,
                "SMALLINT UNSIGNED" => ColumnDecoder::U16AsInt,
                "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => ColumnDecoder::U32AsBig,
                // u64 has no lossless signed home; Decimal holds the range.
                "BIGINT UNSIGNED" => ColumnDecoder::U64AsDec,
                "YEAR" => ColumnDecoder::YearAsInt,
                "DECIMAL" => ColumnDecoder::Dec,
                "FLOAT" => ColumnDecoder::F32,
                "DOUBLE" => ColumnDecoder::F64,
                "VARCHAR" | "CHAR" | "TEXT" | "VAR_STRING" | "STRING" | "ENUM" => ColumnDecoder::Str,
                "BLOB" | "VARBINARY" | "BINARY" => ColumnDecoder::Bytes,
                "BIT" => ColumnDecoder::BitText,
                "DATE" => ColumnDecoder::Date,
                "TIME" => ColumnDecoder::Time,
                // Both DATETIME and TIMESTAMP arrive as wall-clock values
                // with no offset — decode as naive; stamping UTC onto them
                // would silently change their meaning.
                "DATETIME" | "TIMESTAMP" => ColumnDecoder::TsNaive,
                "JSON" => ColumnDecoder::Json,
                "UUID" => ColumnDecoder::Uuid,
                _ => ColumnDecoder::Fallback,
            })
            .collect();
        Self(decoders)
    }

    pub(crate) fn decode(&self, row: &MySqlRow) -> Result<DdbRow, DdbCoreError> {
        let mut values = Vec::with_capacity(self.0.len());
        for (i, decoder) in self.0.iter().enumerate() {
            values.push(decode_cell(row, i, decoder)?);
        }
        Ok(DdbRow(values))
    }
}

fn decode_cell(row: &MySqlRow, i: usize, decoder: &ColumnDecoder) -> Result<Value, DdbCoreError> {
    macro_rules! get {
        ($t:ty, $variant:expr) => {{
            let v: Option<$t> = row.try_get(i).map_err(db_err)?;
            Ok(v.map($variant).unwrap_or(Value::Null))
        }};
    }

    match decoder {
        ColumnDecoder::I8AsSmall => get!(i8, |v: i8| Value::SmallInt(v as i16)),
        ColumnDecoder::I16 => get!(i16, Value::SmallInt),
        ColumnDecoder::I32 => get!(i32, Value::Integer),
        ColumnDecoder::I64 => get!(i64, Value::BigInt),
        ColumnDecoder::U8AsSmall => get!(u8, |v: u8| Value::SmallInt(v as i16)),
        ColumnDecoder::U16AsInt => get!(u16, |v: u16| Value::Integer(v as i32)),
        ColumnDecoder::U32AsBig => get!(u32, |v: u32| Value::BigInt(v as i64)),
        ColumnDecoder::U64AsDec => get!(u64, |v: u64| Value::Decimal(Decimal::from(v))),
        ColumnDecoder::YearAsInt => get!(u16, |v: u16| Value::Integer(v as i32)),
        ColumnDecoder::Dec => get!(Decimal, Value::Decimal),
        ColumnDecoder::F32 => get!(f32, Value::Real),
        ColumnDecoder::F64 => get!(f64, Value::Double),
        ColumnDecoder::Str => get!(String, Value::Text),
        ColumnDecoder::Bytes => get!(Vec<u8>, Value::Binary),
        ColumnDecoder::BitText => {
            let v: Option<Vec<u8>> = row.try_get(i).map_err(db_err)?;
            Ok(v.map(|bytes| {
                let mut s = String::with_capacity(bytes.len() * 8);
                for byte in bytes {
                    for bit in (0..8).rev() {
                        s.push(if byte >> bit & 1 == 1 { '1' } else { '0' });
                    }
                }
                Value::Text(s)
            })
            .unwrap_or(Value::Null))
        }
        ColumnDecoder::Date => get!(NaiveDate, Value::Date),
        ColumnDecoder::Time => get!(NaiveTime, Value::Time),
        ColumnDecoder::TsNaive => get!(NaiveDateTime, Value::TimestampNaive),
        ColumnDecoder::Uuid => get!(Uuid, Value::Uuid),
        ColumnDecoder::Json => get!(serde_json::Value, Value::Json),
        ColumnDecoder::Fallback => match row.try_get_unchecked::<Option<String>, _>(i) {
            Ok(v) => Ok(v.map(Value::Text).unwrap_or(Value::Null)),
            Err(_) => {
                let v: Option<Vec<u8>> = row.try_get_unchecked(i).map_err(db_err)?;
                Ok(v.map(Value::Binary).unwrap_or(Value::Null))
            }
        },
    }
}

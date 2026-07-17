use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use ddbcore::{DdbCoreError, Row as DdbRow, Value};
use rust_decimal::Decimal;
use sqlx::mysql::MySqlRow;
use sqlx::{Column as _, Row as _, TypeInfo};
use uuid::Uuid;

use crate::connection::MySqlConnection;

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::Query(e.to_string())
}

pub(crate) async fn execute_query(conn: &MySqlConnection, sql: &str, params: &[Value]) -> Result<Vec<DdbRow>, DdbCoreError> {
    // Refuse array params outright rather than silently binding NULL —
    // writing NULL where the caller supplied data is corruption. (MySQL
    // has no array type at all.)
    if params.iter().any(|p| matches!(p, Value::Array(_))) {
        return Err(DdbCoreError::Unsupported("MySQL has no array type; array parameters are not supported".into()));
    }

    let mut query = sqlx::query(sql);
    for param in params {
        query = bind_value(query, param);
    }

    let rows = query.fetch_all(&conn.pool).await.map_err(db_err)?;
    rows.iter().map(mysql_row_to_row).collect()
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
        // Unreachable: execute_query rejects array params before binding.
        Value::Array(_) => query.bind(None::<String>),
    }
}

/// Decodes a `MySqlRow` into a canonical `Row`. Note: `TINYINT` decodes
/// uniformly as `SmallInt` here, even though `reflect_schema` reports
/// `tinyint(1)` columns as `TypeCategory::Boolean` — the wire-level MySQL
/// protocol doesn't carry the declared display width, so this decode path
/// has no way to know a given `TINYINT` value came from a `tinyint(1)`
/// column versus a wider one. Callers that need boolean semantics on such
/// a column should check the reflected schema for the category rather
/// than infer it from a streamed value.
pub(crate) fn mysql_row_to_row(row: &MySqlRow) -> Result<DdbRow, DdbCoreError> {
    let mut values = Vec::with_capacity(row.columns().len());
    for (i, col) in row.columns().iter().enumerate() {
        let type_name = col.type_info().name();
        let value = decode_column(row, i, type_name)?;
        values.push(value);
    }
    Ok(DdbRow(values))
}

fn decode_column(row: &MySqlRow, i: usize, type_name: &str) -> Result<Value, DdbCoreError> {
    macro_rules! get {
        ($t:ty, $variant:expr) => {{
            let v: Option<$t> = row.try_get(i).map_err(db_err)?;
            Ok(v.map($variant).unwrap_or(Value::Null))
        }};
    }

    match type_name {
        "BOOLEAN" | "TINYINT" => get!(i8, |v: i8| Value::SmallInt(v as i16)),
        "SMALLINT" => get!(i16, Value::SmallInt),
        "MEDIUMINT" | "INT" => get!(i32, Value::Integer),
        "BIGINT" => get!(i64, Value::BigInt),
        // Unsigned columns MUST decode through unsigned Rust types and
        // widen into the next-larger canonical variant — decoding
        // `INT UNSIGNED` as i32 loses the upper half of the range.
        "TINYINT UNSIGNED" => get!(u8, |v: u8| Value::SmallInt(v as i16)),
        "SMALLINT UNSIGNED" => get!(u16, |v: u16| Value::Integer(v as i32)),
        "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => get!(u32, |v: u32| Value::BigInt(v as i64)),
        // u64 has no lossless signed home; Decimal holds the full range.
        "BIGINT UNSIGNED" => get!(u64, |v: u64| Value::Decimal(Decimal::from(v))),
        "YEAR" => get!(u16, |v: u16| Value::Integer(v as i32)),
        "DECIMAL" => get!(Decimal, Value::Decimal),
        "FLOAT" => get!(f32, Value::Real),
        "DOUBLE" => get!(f64, Value::Double),
        "VARCHAR" | "CHAR" | "TEXT" | "VAR_STRING" | "STRING" | "ENUM" => get!(String, Value::Text),
        "BLOB" | "VARBINARY" | "BINARY" | "BIT" => get!(Vec<u8>, Value::Binary),
        "DATE" => get!(NaiveDate, Value::Date),
        "TIME" => get!(NaiveTime, Value::Time),
        // Both DATETIME and TIMESTAMP arrive as wall-clock values with no
        // offset on the wire — decode as naive; stamping UTC onto them
        // would silently change their meaning.
        "DATETIME" | "TIMESTAMP" => get!(NaiveDateTime, Value::TimestampNaive),
        "JSON" => get!(serde_json::Value, Value::Json),
        // MySQL has no native UUID column type; if one shows up stored
        // as CHAR(36)/BINARY(16) text, it will already have been caught
        // above — this arm exists only in case a future type alias maps
        // here directly.
        "UUID" => get!(Uuid, Value::Uuid),
        // Anything else (SET, geometry, ...): SET arrives as text and
        // decodes fine as String; geometry arrives as binary WKB, where
        // UTF-8 reinterpretation is corruption — fall back to raw bytes
        // as Binary, never garbage text.
        _ => {
            match row.try_get_unchecked::<Option<String>, _>(i) {
                Ok(v) => Ok(v.map(Value::Text).unwrap_or(Value::Null)),
                Err(_) => {
                    let v: Option<Vec<u8>> = row.try_get_unchecked(i).map_err(db_err)?;
                    Ok(v.map(Value::Binary).unwrap_or(Value::Null))
                }
            }
        }
    }
}

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use ddbcore::{DdbCoreError, Row as DdbRow, Value};
use rust_decimal::Decimal;
use sqlx::postgres::PgRow;
use sqlx::{Column as _, Row as _, TypeInfo};
use uuid::Uuid;

use crate::connection::PostgresConnection;

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::Query(e.to_string())
}

pub(crate) async fn execute_query(conn: &PostgresConnection, sql: &str, params: &[Value]) -> Result<Vec<DdbRow>, DdbCoreError> {
    let mut query = sqlx::query(sql);
    for param in params {
        query = bind_value(query, param);
    }

    let rows = query.fetch_all(&conn.pool).await.map_err(db_err)?;
    rows.iter().map(pg_row_to_row).collect()
}

fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: &'q Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
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
        Value::Timestamp(ts) => query.bind(ts),
        Value::Uuid(u) => query.bind(u),
        Value::Json(j) => query.bind(j),
        Value::Array(_) => query.bind(None::<String>), // array param binding not yet supported
    }
}

/// Decodes a `PgRow` into a canonical `Row`, one `Value` per column. Known
/// Postgres wire types decode into their matching `Value` variant; any
/// native type this doesn't recognize falls back to a text decode rather
/// than failing the whole row, since Postgres can represent almost
/// anything (enums, network types, ranges, ...) in text form.
pub(crate) fn pg_row_to_row(row: &PgRow) -> Result<DdbRow, DdbCoreError> {
    let mut values = Vec::with_capacity(row.columns().len());
    for (i, col) in row.columns().iter().enumerate() {
        let type_name = col.type_info().name();
        let value = decode_column(row, i, type_name)?;
        values.push(value);
    }
    Ok(DdbRow(values))
}

fn decode_column(row: &PgRow, i: usize, type_name: &str) -> Result<Value, DdbCoreError> {
    // Array columns (e.g. `TEXT[]`) must decode via `Vec<T>`, never via the
    // scalar `T` path — the wire format is entirely different, and
    // decoding array bytes as a scalar string silently produces garbage
    // rather than an error, which is worse than failing loudly.
    if let Some(elem_type) = type_name.strip_suffix("[]") {
        return decode_array_column(row, i, elem_type);
    }

    macro_rules! get {
        ($t:ty, $variant:expr) => {{
            let v: Option<$t> = row.try_get(i).map_err(db_err)?;
            Ok(v.map($variant).unwrap_or(Value::Null))
        }};
    }

    match type_name {
        "BOOL" => get!(bool, Value::Boolean),
        "INT2" => get!(i16, Value::SmallInt),
        "INT4" => get!(i32, Value::Integer),
        "INT8" => get!(i64, Value::BigInt),
        "NUMERIC" => get!(Decimal, Value::Decimal),
        "FLOAT4" => get!(f32, Value::Real),
        "FLOAT8" => get!(f64, Value::Double),
        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "CITEXT" => get!(String, Value::Text),
        "BYTEA" => get!(Vec<u8>, Value::Binary),
        "DATE" => get!(NaiveDate, Value::Date),
        "TIME" => get!(NaiveTime, Value::Time),
        "TIMESTAMP" | "TIMESTAMPTZ" => get!(DateTime<Utc>, Value::Timestamp),
        "UUID" => get!(Uuid, Value::Uuid),
        "JSON" | "JSONB" => get!(serde_json::Value, Value::Json),
        // Anything else (enums, inet/cidr/macaddr, ranges, geometric types,
        // ...): fall back to a text decode rather than erroring the row
        // out. `try_get_unchecked` is required here — sqlx's normal
        // `try_get` refuses to decode a column whose declared Postgres
        // type isn't one of String's known-compatible types (TEXT,
        // VARCHAR, ...), even though the wire value is perfectly readable
        // as text for things like enums.
        _ => {
            let v: Option<String> = row.try_get_unchecked(i).map_err(db_err)?;
            Ok(v.map(Value::Text).unwrap_or(Value::Null))
        }
    }
}

fn decode_array_column(row: &PgRow, i: usize, elem_type: &str) -> Result<Value, DdbCoreError> {
    macro_rules! get_vec {
        ($t:ty, $variant:expr) => {{
            let v: Option<Vec<Option<$t>>> = row.try_get(i).map_err(db_err)?;
            Ok(v.map(|items| Value::Array(items.into_iter().map(|x| x.map($variant).unwrap_or(Value::Null)).collect()))
                .unwrap_or(Value::Null))
        }};
    }

    match elem_type {
        "BOOL" => get_vec!(bool, Value::Boolean),
        "INT2" => get_vec!(i16, Value::SmallInt),
        "INT4" => get_vec!(i32, Value::Integer),
        "INT8" => get_vec!(i64, Value::BigInt),
        "NUMERIC" => get_vec!(Decimal, Value::Decimal),
        "FLOAT4" => get_vec!(f32, Value::Real),
        "FLOAT8" => get_vec!(f64, Value::Double),
        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "CITEXT" => get_vec!(String, Value::Text),
        "DATE" => get_vec!(NaiveDate, Value::Date),
        "TIME" => get_vec!(NaiveTime, Value::Time),
        "TIMESTAMP" | "TIMESTAMPTZ" => get_vec!(DateTime<Utc>, Value::Timestamp),
        "UUID" => get_vec!(Uuid, Value::Uuid),
        "JSON" | "JSONB" => get_vec!(serde_json::Value, Value::Json),
        // Enum arrays and other exotic element types: sqlx has no built-in
        // `Vec<T>` decode to fall back to via `try_get_unchecked` the way
        // the scalar path does, so this remains a known gap rather than a
        // silent-corruption risk — it errors instead of guessing.
        other => Err(DdbCoreError::Query(format!("unsupported array element type: {other}[]"))),
    }
}

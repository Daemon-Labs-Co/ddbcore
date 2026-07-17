use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use ddbcore::{DdbCoreError, Row as DdbRow, RowStream, Value};
use futures::TryStreamExt;
use rust_decimal::Decimal;
use sqlx::postgres::PgRow;
use sqlx::{Column as _, Connection as _, Row as _, TypeInfo};
use uuid::Uuid;

use crate::connection::PostgresConnection;

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::Query(e.to_string())
}

pub(crate) async fn execute_query(conn: &PostgresConnection, sql: &str, params: &[Value]) -> Result<Vec<DdbRow>, DdbCoreError> {
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

/// Streaming variant for large ad-hoc result sets: rows are yielded as
/// they arrive off the wire. Runs on a dedicated (detached-from-pool)
/// connection so a long-running result never pins a shared pool slot, and
/// dropping the stream mid-way closes the socket rather than leaving the
/// remainder to be drained by the next pool user.
pub(crate) async fn execute_query_stream(conn: &PostgresConnection, sql: &str, params: &[Value]) -> Result<RowStream, DdbCoreError> {
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
    // writing NULL where the caller supplied data is corruption.
    if params.iter().any(|p| matches!(p, Value::Array(_))) {
        return Err(DdbCoreError::Unsupported("array parameter binding is not implemented yet".into()));
    }
    Ok(())
}

pub(crate) fn bind_value<'q>(
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
        Value::TimestampNaive(ts) => query.bind(ts),
        Value::Uuid(u) => query.bind(u),
        Value::Json(j) => query.bind(j),
        // Unreachable: callers reject array params before binding.
        Value::Array(_) => query.bind(None::<String>),
    }
}

/// Per-column decode plan, built ONCE per result set from the first row's
/// column metadata, then applied by index to every row. This replaces
/// per-cell type-name string matching (and, for unknown types, a failing
/// String decode retried as bytes) with a single upfront decision per
/// column — on a 100M-row stream that removes ~1B redundant string
/// comparisons from the hot path.
pub(crate) struct RowDecoder(Vec<ColumnDecoder>);

enum ColumnDecoder {
    Bool,
    I16,
    I32,
    I64,
    Dec,
    F32,
    F64,
    Str,
    Bytes,
    Date,
    Time,
    TsNaive,
    TsUtc,
    Uuid,
    Json,
    Array(ArrayKind),
    /// Unknown wire type: try text, fall back to raw bytes — never
    /// UTF-8-reinterpret binary data. (stream_rows avoids this entirely by
    /// casting such columns to ::text server-side; this arm is reachable
    /// only from ad-hoc execute_query SQL.)
    Fallback,
}

#[derive(Clone, Copy)]
enum ArrayKind {
    Bool,
    I16,
    I32,
    I64,
    Dec,
    F32,
    F64,
    Str,
    Date,
    Time,
    TsNaive,
    TsUtc,
    Uuid,
    Json,
    Unsupported,
}

impl RowDecoder {
    pub(crate) fn for_row(row: &PgRow) -> Self {
        let decoders = row
            .columns()
            .iter()
            .map(|col| {
                let type_name = col.type_info().name();
                if let Some(elem) = type_name.strip_suffix("[]") {
                    return ColumnDecoder::Array(array_kind(elem));
                }
                match type_name {
                    "BOOL" => ColumnDecoder::Bool,
                    "INT2" => ColumnDecoder::I16,
                    "INT4" => ColumnDecoder::I32,
                    "INT8" => ColumnDecoder::I64,
                    "NUMERIC" => ColumnDecoder::Dec,
                    "FLOAT4" => ColumnDecoder::F32,
                    "FLOAT8" => ColumnDecoder::F64,
                    "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "CITEXT" => ColumnDecoder::Str,
                    "BYTEA" => ColumnDecoder::Bytes,
                    "DATE" => ColumnDecoder::Date,
                    "TIME" => ColumnDecoder::Time,
                    // TIMESTAMP (no timezone) must decode as naive —
                    // stamping it UTC would silently change the data's
                    // meaning. TIMESTAMPTZ genuinely carries an instant.
                    "TIMESTAMP" => ColumnDecoder::TsNaive,
                    "TIMESTAMPTZ" => ColumnDecoder::TsUtc,
                    "UUID" => ColumnDecoder::Uuid,
                    "JSON" | "JSONB" => ColumnDecoder::Json,
                    _ => ColumnDecoder::Fallback,
                }
            })
            .collect();
        Self(decoders)
    }

    pub(crate) fn decode(&self, row: &PgRow) -> Result<DdbRow, DdbCoreError> {
        let mut values = Vec::with_capacity(self.0.len());
        for (i, decoder) in self.0.iter().enumerate() {
            values.push(decode_cell(row, i, decoder)?);
        }
        Ok(DdbRow(values))
    }
}

fn array_kind(elem_type: &str) -> ArrayKind {
    match elem_type {
        "BOOL" => ArrayKind::Bool,
        "INT2" => ArrayKind::I16,
        "INT4" => ArrayKind::I32,
        "INT8" => ArrayKind::I64,
        "NUMERIC" => ArrayKind::Dec,
        "FLOAT4" => ArrayKind::F32,
        "FLOAT8" => ArrayKind::F64,
        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "CITEXT" => ArrayKind::Str,
        "DATE" => ArrayKind::Date,
        "TIME" => ArrayKind::Time,
        "TIMESTAMP" => ArrayKind::TsNaive,
        "TIMESTAMPTZ" => ArrayKind::TsUtc,
        "UUID" => ArrayKind::Uuid,
        "JSON" | "JSONB" => ArrayKind::Json,
        _ => ArrayKind::Unsupported,
    }
}

fn decode_cell(row: &PgRow, i: usize, decoder: &ColumnDecoder) -> Result<Value, DdbCoreError> {
    macro_rules! get {
        ($t:ty, $variant:expr) => {{
            let v: Option<$t> = row.try_get(i).map_err(db_err)?;
            Ok(v.map($variant).unwrap_or(Value::Null))
        }};
    }

    match decoder {
        ColumnDecoder::Bool => get!(bool, Value::Boolean),
        ColumnDecoder::I16 => get!(i16, Value::SmallInt),
        ColumnDecoder::I32 => get!(i32, Value::Integer),
        ColumnDecoder::I64 => get!(i64, Value::BigInt),
        ColumnDecoder::Dec => get!(Decimal, Value::Decimal),
        ColumnDecoder::F32 => get!(f32, Value::Real),
        ColumnDecoder::F64 => get!(f64, Value::Double),
        ColumnDecoder::Str => get!(String, Value::Text),
        ColumnDecoder::Bytes => get!(Vec<u8>, Value::Binary),
        ColumnDecoder::Date => get!(NaiveDate, Value::Date),
        ColumnDecoder::Time => get!(NaiveTime, Value::Time),
        ColumnDecoder::TsNaive => get!(NaiveDateTime, Value::TimestampNaive),
        ColumnDecoder::TsUtc => get!(DateTime<Utc>, Value::Timestamp),
        ColumnDecoder::Uuid => get!(Uuid, Value::Uuid),
        ColumnDecoder::Json => get!(serde_json::Value, Value::Json),
        ColumnDecoder::Array(kind) => decode_array_cell(row, i, *kind),
        ColumnDecoder::Fallback => match row.try_get_unchecked::<Option<String>, _>(i) {
            Ok(v) => Ok(v.map(Value::Text).unwrap_or(Value::Null)),
            Err(_) => {
                let v: Option<Vec<u8>> = row.try_get_unchecked(i).map_err(db_err)?;
                Ok(v.map(Value::Binary).unwrap_or(Value::Null))
            }
        },
    }
}

fn decode_array_cell(row: &PgRow, i: usize, kind: ArrayKind) -> Result<Value, DdbCoreError> {
    macro_rules! get_vec {
        ($t:ty, $variant:expr) => {{
            let v: Option<Vec<Option<$t>>> = row.try_get(i).map_err(db_err)?;
            Ok(v.map(|items| Value::Array(items.into_iter().map(|x| x.map($variant).unwrap_or(Value::Null)).collect()))
                .unwrap_or(Value::Null))
        }};
    }

    match kind {
        ArrayKind::Bool => get_vec!(bool, Value::Boolean),
        ArrayKind::I16 => get_vec!(i16, Value::SmallInt),
        ArrayKind::I32 => get_vec!(i32, Value::Integer),
        ArrayKind::I64 => get_vec!(i64, Value::BigInt),
        ArrayKind::Dec => get_vec!(Decimal, Value::Decimal),
        ArrayKind::F32 => get_vec!(f32, Value::Real),
        ArrayKind::F64 => get_vec!(f64, Value::Double),
        ArrayKind::Str => get_vec!(String, Value::Text),
        ArrayKind::Date => get_vec!(NaiveDate, Value::Date),
        ArrayKind::Time => get_vec!(NaiveTime, Value::Time),
        ArrayKind::TsNaive => get_vec!(NaiveDateTime, Value::TimestampNaive),
        ArrayKind::TsUtc => get_vec!(DateTime<Utc>, Value::Timestamp),
        ArrayKind::Uuid => get_vec!(Uuid, Value::Uuid),
        ArrayKind::Json => get_vec!(serde_json::Value, Value::Json),
        // Enum arrays and other exotic element types: errors rather than
        // guessing — a silent-corruption risk is worse than a loud gap.
        ArrayKind::Unsupported => {
            Err(DdbCoreError::Query("unsupported array element type".to_string()))
        }
    }
}

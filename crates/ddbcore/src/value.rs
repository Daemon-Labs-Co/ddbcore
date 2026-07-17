use chrono::{NaiveDate, NaiveDateTime, NaiveTime, DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single cell value, engine-agnostic. `stream_rows`, `bulk_write`, and
/// `execute_query` params all move data around as `Value`, never as
/// engine-native driver types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Decimal(Decimal),
    Real(f32),
    Double(f64),
    Text(String),
    Binary(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    /// A timestamp with an explicit UTC offset (`timestamptz` and
    /// friends). Only decode into this when the engine actually stored an
    /// offset-aware value.
    Timestamp(DateTime<Utc>),
    /// A wall-clock timestamp with NO timezone (`timestamp without time
    /// zone`, MySQL `DATETIME`). Kept distinct from `Timestamp` — stamping
    /// naive values as UTC silently changes the data's meaning.
    TimestampNaive(NaiveDateTime),
    Uuid(Uuid),
    Json(serde_json::Value),
    Array(Vec<Value>),
}

/// One row, positional to match the column order returned by
/// `reflect_schema` / a `stream_rows` query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Row(pub Vec<Value>);

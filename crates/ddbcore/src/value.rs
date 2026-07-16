use chrono::{NaiveDate, NaiveTime, DateTime, Utc};
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
    Timestamp(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Array(Vec<Value>),
}

/// One row, positional to match the column order returned by
/// `reflect_schema` / a `stream_rows` query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Row(pub Vec<Value>);

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single cell value, engine-agnostic. `stream_rows`, `bulk_write`, and
/// `execute_query` params all move data around as `Value`, never as
/// engine-native driver types.
///
/// Performance note: `Text`/`Binary`/`Json` heap-allocate per cell and
/// `Row` allocates per row. This is a deliberate v1 simplicity/throughput
/// tradeoff — network and decode dominate in practice — but it caps the
/// ceiling below the theoretical COPY maximum. A batch-level,
/// buffer-reusing transport may be added later alongside (not replacing)
/// this representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Equality is manual so floats compare BITWISE: a NaN round-tripped
/// through a database must compare equal to the NaN that went in — the
/// derived IEEE `NaN != NaN` semantics would make copy-verification
/// spuriously fail on any table containing a NaN. (Note: serde_json
/// cannot serialize non-finite floats to JSON; that's a serialization
/// concern, not an equality one.)
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Null, Null) => true,
            (Boolean(a), Boolean(b)) => a == b,
            (SmallInt(a), SmallInt(b)) => a == b,
            (Integer(a), Integer(b)) => a == b,
            (BigInt(a), BigInt(b)) => a == b,
            (Decimal(a), Decimal(b)) => a == b,
            (Real(a), Real(b)) => a.to_bits() == b.to_bits(),
            (Double(a), Double(b)) => a.to_bits() == b.to_bits(),
            (Text(a), Text(b)) => a == b,
            (Binary(a), Binary(b)) => a == b,
            (Date(a), Date(b)) => a == b,
            (Time(a), Time(b)) => a == b,
            (Timestamp(a), Timestamp(b)) => a == b,
            (TimestampNaive(a), TimestampNaive(b)) => a == b,
            (Uuid(a), Uuid(b)) => a == b,
            (Json(a), Json(b)) => a == b,
            (Array(a), Array(b)) => a == b,
            _ => false,
        }
    }
}

/// One row, positional to match the column order returned by
/// `reflect_schema` / a `stream_rows` query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Row(pub Vec<Value>);

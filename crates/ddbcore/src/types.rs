use serde::{Deserialize, Serialize};

/// Canonical type categories that every engine's native column types map into
/// and back out of. Detection, transformation, and any other Readactus-side
/// logic operates only against this enum — never against engine-specific
/// type names.
///
/// `Unsupported` is the escape hatch: schema reflection must never drop or
/// crash on a type it doesn't recognize. Capture the raw native type string
/// and let the caller decide what to do with it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypeCategory {
    Boolean,

    SmallInt,
    Integer,
    BigInt,
    Decimal { precision: Option<u32>, scale: Option<u32> },
    Real,
    Double,

    Char { length: Option<u32> },
    VarChar { length: Option<u32> },
    Text,

    Binary { length: Option<u32> },
    VarBinary { length: Option<u32> },
    Blob,

    Date,
    Time { precision: Option<u32> },
    Timestamp { precision: Option<u32>, with_timezone: bool },
    Interval,

    Uuid,
    Json,
    Xml,
    Bit { length: Option<u32> },

    Enum { values: Vec<String> },
    Array { element: Box<TypeCategory> },
    Geometry { subtype: Option<String> },

    /// A native type this version of DDBCore doesn't yet map to a canonical
    /// category. `native_type` preserves the engine's own type name so
    /// nothing is silently lost during reflection.
    Unsupported { native_type: String },
}

/// A column's full type information: the canonical category plus the raw
/// native type string as reported by the engine, kept for diagnostics,
/// round-tripping into `render_ddl`, and cases where the canonical mapping
/// is lossy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataType {
    pub category: TypeCategory,
    pub native_type: String,
}

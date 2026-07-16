use serde::{Deserialize, Serialize};

use crate::types::DataType;

/// The full reflected structure of a database: every schema (namespace) the
/// connected credential can see, and everything in it. This is meant to be
/// exhaustive — reflect_schema must walk the entire catalog, not a subset.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Catalog {
    pub schemas: Vec<Schema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub name: String,
    pub tables: Vec<Table>,
    pub views: Vec<View>,
    pub sequences: Vec<Sequence>,
    pub functions: Vec<Function>,
}

/// Fully-qualified reference to a table, used anywhere a caller needs to
/// name a table without carrying the whole reflected `Table` around
/// (stream_rows, bulk_write, create_index, ...).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<PrimaryKey>,
    pub foreign_keys: Vec<ForeignKey>,
    pub unique_constraints: Vec<UniqueConstraint>,
    pub check_constraints: Vec<CheckConstraint>,
    pub indexes: Vec<Index>,
    pub triggers: Vec<Trigger>,
    pub comment: Option<String>,
}

impl Table {
    pub fn table_ref(&self) -> TableRef {
        TableRef { schema: self.schema.clone(), name: self.name.clone() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ordinal_position: u32,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<String>,
    pub is_identity: bool,
    pub identity_generation: Option<IdentityGeneration>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdentityGeneration {
    Always,
    ByDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKey {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_schema: String,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueConstraint {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub name: String,
    pub expression: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexColumn {
    pub name: String,
    pub descending: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    /// Engine-specific access method (e.g. "btree", "gin", "hash"). Not
    /// normalized across engines — there's no shared concept here beyond
    /// the name.
    pub method: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Trigger metadata is normalized (name, timing, events); the body is not —
/// procedural trigger bodies are engine-specific (PL/pgSQL, T-SQL, PL/SQL,
/// ...) with no shared structure, so it's captured as an opaque string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trigger {
    pub name: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub function_name: Option<String>,
    pub body: Option<String>,
}

/// Function/procedure metadata, same caveat as `Trigger::body`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Function {
    pub schema: String,
    pub name: String,
    pub arguments: Vec<FunctionArgument>,
    pub return_type: Option<String>,
    pub language: String,
    pub body: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionArgument {
    pub name: Option<String>,
    pub native_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub schema: String,
    pub name: String,
    pub definition: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sequence {
    pub schema: String,
    pub name: String,
    pub data_type: DataType,
    pub start_value: i64,
    pub increment: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: bool,
}

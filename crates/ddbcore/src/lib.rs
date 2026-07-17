pub mod adapter;
pub mod error;
pub mod schema;
pub mod types;
pub mod value;

pub use adapter::{
    ColumnDefinition, ConnectionConfig, Connection, ConstraintDefinition, DatabaseAdapter,
    Dialect, EncryptionMode, IndexDefinition, KeyRange, ParamStyle, RowStream, StreamOptions,
    TableAlteration, TableDefinition,
};
pub use error::DdbCoreError;
pub use schema::{
    Catalog, CheckConstraint, Column, ExclusionConstraint, Function, FunctionArgument, ForeignKey,
    IdentityGeneration, Index, IndexColumn, PrimaryKey, ReferentialAction, Schema, Sequence,
    Table, TableRef, Trigger, TriggerEvent, TriggerTiming, UniqueConstraint, View,
};
pub use types::{DataType, TypeCategory};
pub use value::{Row, Value};

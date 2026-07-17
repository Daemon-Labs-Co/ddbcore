mod bulk;
mod connection;
mod ddl;
mod query;
mod reflect;
mod stream;
mod types;
mod util;

pub use connection::{PostgresAdapter, PostgresConnection};

/// Internal hooks for this crate's criterion benches. Not public API —
/// hidden, unstable, and subject to change without notice.
#[doc(hidden)]
pub mod bench_support {
    pub use crate::types::map_pg_type;

    pub fn write_copy_line(buffer: &mut String, row: &ddbcore::Row) {
        crate::bulk::write_copy_line(buffer, row);
    }
}

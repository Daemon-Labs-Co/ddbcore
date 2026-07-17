use ddbcore::{DdbCoreError, RowStream, TableRef};
use futures::TryStreamExt;

use crate::connection::MySqlConnection;
use crate::query::mysql_row_to_row;
use crate::util::quote_qualified;

/// MySQL's wire protocol doesn't support Postgres-style server-side
/// `DECLARE CURSOR`/`FETCH` — but `sqlx`'s MySQL driver already streams
/// rows off the socket one at a time as they arrive, so memory use is
/// bounded regardless. `batch_size` is accepted for API symmetry with
/// other adapters but is currently unused here.
///
/// `async_stream::try_stream!` (rather than a bare `.fetch()` call) is
/// needed because `sqlx::query(&sql)` borrows `sql`'s lifetime — the
/// generator owns both `sql` and the pool clone across the whole stream,
/// which a bare borrow can't do while still satisfying `RowStream`'s
/// `'static` bound.
pub(crate) async fn stream_rows(conn: &MySqlConnection, table: &TableRef, _batch_size: usize) -> Result<RowStream, DdbCoreError> {
    let sql = format!("SELECT * FROM {}", quote_qualified(&table.schema, &table.name));
    let pool = conn.pool.clone();

    let stream = async_stream::try_stream! {
        let mut rows = sqlx::query(&sql).fetch(&pool).map_err(|e| DdbCoreError::Query(e.to_string()));
        while let Some(row) = rows.try_next().await? {
            yield mysql_row_to_row(&row)?;
        }
    };

    Ok(Box::pin(stream))
}

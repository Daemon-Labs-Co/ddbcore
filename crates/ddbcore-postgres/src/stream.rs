use ddbcore::{DdbCoreError, RowStream, TableRef};
use futures::stream::{self, TryStreamExt};
use uuid::Uuid;

use crate::connection::PostgresConnection;
use crate::query::pg_row_to_row;
use crate::util::quote_qualified;

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::Query(e.to_string())
}

/// Reads `table` through a server-side cursor, `batch_size` rows at a
/// time, so arbitrarily large tables never load into memory at once. The
/// backing transaction lives for the lifetime of the stream and is rolled
/// back (a no-op for a read-only cursor) when the stream is dropped.
pub(crate) async fn stream_rows(conn: &PostgresConnection, table: &TableRef, batch_size: usize) -> Result<RowStream, DdbCoreError> {
    let sql = format!("SELECT * FROM {}", quote_qualified(&table.schema, &table.name));
    let cursor_name = format!("ddbcore_cursor_{}", Uuid::new_v4().simple());

    let mut tx = conn.pool.begin().await.map_err(db_err)?;
    sqlx::query(&format!("DECLARE {cursor_name} NO SCROLL CURSOR FOR {sql}"))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

    let fetch_sql = format!("FETCH FORWARD {batch_size} FROM {cursor_name}");

    let batches = stream::try_unfold((tx, false), move |(mut tx, done)| {
        let fetch_sql = fetch_sql.clone();
        async move {
            if done {
                return Ok(None);
            }
            let rows = sqlx::query(&fetch_sql).fetch_all(&mut *tx).await.map_err(db_err)?;
            let is_last = rows.len() < batch_size;
            let batch = rows.iter().map(pg_row_to_row).collect::<Result<Vec<_>, _>>()?;
            Ok(Some((batch, (tx, is_last))))
        }
    });

    let rows = batches.map_ok(|batch| stream::iter(batch.into_iter().map(Ok))).try_flatten();

    Ok(Box::pin(rows))
}

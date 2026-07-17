use ddbcore::{DdbCoreError, RowStream, StreamOptions, TableRef, Value};
use futures::TryStreamExt;
use sqlx::Connection as _;

use crate::connection::MySqlConnection;
use crate::query::{bind_value, RowDecoder};
use crate::util::{quote_ident, quote_qualified};

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::Query(e.to_string())
}

/// Streams `table` on a DEDICATED connection detached from the pool.
///
/// This is load-bearing for MySQL specifically: the wire protocol has no
/// way to abandon an in-flight resultset, so if a stream over a POOLED
/// connection is dropped mid-table, the next command on that connection
/// must first read and discard every remaining row — cancel a copy of a
/// 500M-row table and the next innocuous pool query silently downloads
/// the rest of the table. On a dedicated connection, dropping the stream
/// drops the socket: cancellation costs one reconnect, nothing more.
///
/// `batch_size` is unused here (the driver already streams rows off the
/// socket one at a time, so memory is bounded without cursor batching) —
/// see the trait docs: it is a fetch-granularity hint adapters may ignore.
pub(crate) async fn stream_rows(conn: &MySqlConnection, table: &TableRef, options: StreamOptions) -> Result<RowStream, DdbCoreError> {
    if options.batch_size == 0 {
        return Err(DdbCoreError::Query("batch_size must be >= 1".into()));
    }

    let select_list = match &options.columns {
        None => "*".to_string(),
        Some(cols) => {
            if cols.is_empty() {
                return Err(DdbCoreError::Query("columns projection must not be empty".into()));
            }
            cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        }
    };

    // Key-range bounds bind as normal parameters here (no cursor DECLARE
    // in the way, unlike Postgres).
    let mut binds: Vec<Value> = Vec::new();
    let mut where_clause = String::new();
    if let Some(range) = &options.key_range {
        let col = quote_ident(&range.column);
        let mut clauses = Vec::with_capacity(2);
        if let Some(lower) = &range.lower {
            clauses.push(format!("{col} >= ?"));
            binds.push(lower.clone());
        }
        if let Some(upper) = &range.upper {
            clauses.push(format!("{col} < ?"));
            binds.push(upper.clone());
        }
        if clauses.is_empty() {
            return Err(DdbCoreError::Query("key_range must set at least one bound".into()));
        }
        where_clause = format!(" WHERE {}", clauses.join(" AND "));
    }
    crate::query::reject_array_params(&binds)?;

    let sql = format!("SELECT {select_list} FROM {}{where_clause}", quote_qualified(&table.schema, &table.name));

    let mut db = conn.pool.acquire().await.map_err(db_err)?.detach();

    let stream = async_stream::try_stream! {
        {
            let mut query = sqlx::query(&sql);
            for value in &binds {
                query = bind_value(query, value);
            }
            let mut rows = query.fetch(&mut db).map_err(db_err);
            let mut decoders: Option<RowDecoder> = None;
            while let Some(row) = rows.try_next().await? {
                let d = decoders.get_or_insert_with(|| RowDecoder::for_row(&row));
                yield d.decode(&row)?;
            }
        }
        // Normal completion: close the dedicated connection gracefully.
        let _ = db.close().await;
    };

    Ok(Box::pin(stream))
}

# DDBCore

**Daemon DB Core** — an engine-agnostic database abstraction layer for Rust: schema reflection, streaming reads, bulk writes, DDL generation, and DDL mutation, behind one trait implemented once per database engine.

Built by [Daemon Labs](https://github.com/Daemon-Labs-Co) as the foundation of [Readactus](https://github.com/Daemon-Labs-Co) — but designed to stand on its own.

## Why

Rust has no equivalent of SQLAlchemy Core: a single API for reflecting a database's structure and moving data through it regardless of which engine is on the other end. DDBCore is that layer. Detection, transformation, or any other business logic built on top of it never needs to know or care whether it's talking to Postgres, MySQL, SQL Server, or Oracle.

## Design

Everything downstream of `Connection` operates only against DDBCore's own canonical model — never against an engine's native types or SQL dialect:

- **`TypeCategory`** (`crates/ddbcore/src/types.rs`) — a fixed set of type categories (integers, decimals, strings, temporal types, UUID, JSON, arrays, enums, ...) that every engine's native column types map into and back out of. `Unsupported { native_type }` is the escape hatch — reflection never silently drops a type it doesn't recognize.
- **Canonical schema model** (`crates/ddbcore/src/schema.rs`) — `Catalog` → `Schema` → `Table`, with full `Column`, `PrimaryKey`, `ForeignKey`, `UniqueConstraint`, `CheckConstraint`, `Index`, `Trigger`, `Function`, `View`, and `Sequence` coverage. Reflection is meant to be exhaustive, not a subset.
- **`Connection` trait** (`crates/ddbcore/src/adapter.rs`) — implemented once per engine:
  - `reflect_schema()` — walks the entire catalog visible to the connection's credentials
  - `stream_rows(table, batch_size)` — cursor-backed, batched reads; a multi-hundred-million-row table never loads into memory at once
  - `bulk_write(table, rows)` — the engine's fast bulk-load path (`COPY` for Postgres, `LOAD DATA` for MySQL, `BULK INSERT` for SQL Server, direct-path for Oracle), not row-by-row `INSERT`s
  - `execute_query(sql, params)` — an escape hatch for arbitrary SQL
  - `create_table` / `create_index` / `alter_table` — user-config-driven DDL mutation
  - `render_ddl(table)` — renders a reflected `Table` back into that engine's DDL

`ConnectionConfig::encryption` defaults to clear-text (`EncryptionMode::ClearText`); TLS/SSL is opt-in per connection.

## Workspace layout

```
crates/
  ddbcore/           canonical type system, schema model, Connection/DatabaseAdapter traits
  ddbcore-postgres/  Postgres adapter (first engine implemented, proves the trait design)
  ddbcore-testkit/   engine-agnostic contract tests — same test code runs against any adapter
```

## Status

Postgres adapter is implemented and verified end-to-end: reflect → render DDL → stream → bulk-write → verify, against a schema exercising foreign keys, unique/check constraints, indexes, a custom enum type, a trigger + function, a view, sequences, arrays, and JSONB.

**Known v1 gaps:**
- Index column sort direction (ASC/DESC) isn't resolved from the catalog yet — always reports `false`
- Enum-typed *array* columns error on decode rather than decoding (scalar enums work)
- Function arguments are captured as one opaque signature string, not parsed per-parameter
- `TypeCategory::Enum` / `Geometry` render as `text` in Postgres DDL (no `CREATE TYPE`/PostGIS assumption)
- Array parameter binding in `execute_query` isn't implemented yet
- Only Postgres exists so far — MySQL, SQL Server, and Oracle adapters are not yet started

## Testing

Two tiers:

1. **Contract tests** (`ddbcore-testkit`) — engine-agnostic; talk to the database only through the `Connection` trait, so the same test code will run unmodified against every future adapter.
2. **Integration tests** (`crates/ddbcore-postgres/tests/contract.rs`) — spin up a real, throwaway Postgres via [`testcontainers`](https://docs.rs/testcontainers) automatically and run the contract suite against it. No manually pre-started database required.

```sh
cargo test --workspace          # unit tests + Postgres integration tests (needs Docker)
cargo test --workspace --lib    # unit tests only, no Docker required
```

## License

MIT OR Apache-2.0

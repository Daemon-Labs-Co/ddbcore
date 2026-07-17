# DDBCore

**Daemon DB Core** — an engine-agnostic database abstraction layer for Rust: schema reflection, streaming reads, bulk writes, DDL generation, and DDL mutation, behind one trait implemented once per database engine.

Built by [Daemon Labs](https://github.com/Daemon-Labs-Co) as the foundation of [Readactus](https://github.com/Daemon-Labs-Co) — but designed to stand on its own.

## Why

Rust has no equivalent of SQLAlchemy Core: a single API for reflecting a database's structure and moving data through it regardless of which engine is on the other end. DDBCore is that layer. Detection, transformation, or any other business logic built on top of it never needs to know or care whether it's talking to Postgres, MySQL, SQL Server, or Oracle.

## Design

Everything downstream of `Connection` operates only against DDBCore's own canonical model — never against an engine's native types or SQL dialect:

- **`TypeCategory`** (`crates/ddbcore/src/types.rs`) — a fixed set of type categories (integers, decimals, strings, temporal types, UUID, JSON, arrays, enums, ...) that every engine's native column types map into and back out of. `Unsupported { native_type }` is the escape hatch — reflection never silently drops a type it doesn't recognize.
- **Canonical schema model** (`crates/ddbcore/src/schema.rs`) — `Catalog` → `Schema` → `Table`, with full `Column` (incl. identity), `PrimaryKey`, `ForeignKey`, `UniqueConstraint`, `CheckConstraint`, `Index`, `Trigger`, `Function`, `View`, `Sequence`, and partitioning coverage. Reflection is meant to be exhaustive, not a subset.
- **`Connection` trait** (`crates/ddbcore/src/adapter.rs`) — implemented once per engine:
  - `reflect_schema()` / `reflect_schema_named()` / `reflect_table()` — full-catalog or scoped reflection
  - `stream_rows(table, options)` — cursor-backed batched reads with optional column projection and half-open key ranges (for running parallel non-overlapping sub-streams over one huge table); runs on a dedicated connection so multi-hour scans never pin a pool slot, and cancellation costs one reconnect
  - `bulk_write(table, rows)` — the engine's fast bulk-load path (`COPY` for Postgres; batched, placeholder-capped, byte-budgeted multi-row `INSERT` for MySQL), never row-by-row
  - `execute_query(sql, params)` / `execute_query_stream(...)` — ad-hoc SQL, materialized (small results) or streamed (large)
  - `create_table` / `create_index` / `alter_table` — user-config-driven DDL mutation
  - `render_ddl(table)` — renders a reflected `Table` back into engine DDL, one statement per `Vec` element
  - `dialect()` — the engine's quoting characters, parameter style, and capability flags, so generic callers can compose portable SQL without hardcoding any one engine's syntax

`ConnectionConfig::encryption` defaults to clear-text (`EncryptionMode::ClearText`); TLS/SSL is opt-in per connection.

## Workspace layout

```
crates/
  ddbcore/           canonical type system, schema model, Connection/DatabaseAdapter traits, Dialect
  ddbcore-postgres/  PostgreSQL adapter (sqlx; cursor streaming, COPY bulk writes)
  ddbcore-mysql/     MySQL/MariaDB adapter (sqlx; socket streaming, batched-INSERT bulk writes)
  ddbcore-testkit/   engine-agnostic contract tests — same test code runs against any adapter
```

## Status

Postgres and MySQL/MariaDB adapters are implemented and verified end-to-end (reflect → render DDL → stream → bulk-write → verify) against real dockerized instances, including foreign keys, unique/check constraints, indexes, enums, triggers, views, sequences, identity columns, arrays (Postgres), JSON, column projection, and key-range streaming.

**Known v1 gaps:**
- Index column sort direction (ASC/DESC) isn't resolved from the catalog yet — always reports `false`
- Enum-typed *array* columns error on decode rather than decoding (scalar enums work)
- Function arguments are captured as one opaque signature string (Postgres) or not at all (MySQL)
- `TypeCategory::Enum` / `Geometry` render as `text` in Postgres DDL (no `CREATE TYPE`/PostGIS assumption)
- Array parameter binding in `execute_query` returns `Unsupported` (never silently NULLs)
- MySQL partitioning is not reflected (Postgres declarative partitioning is)
- SQL Server and Oracle adapters are not yet started

## Testing

Two tiers:

1. **Contract tests** (`ddbcore-testkit`) — engine-agnostic; talk to the database only through the `Connection` trait (composing any hand-written SQL via `dialect()`), so the same test code runs unmodified against every adapter.
2. **Integration tests** (each adapter's `tests/contract.rs`) — spin up a real, throwaway database via [`testcontainers`](https://docs.rs/testcontainers) automatically and run the contract suite against it. No manually pre-started database required.

Container image tags and credentials live in **`.env.testing`** at the repo root — one source of truth shared by the cargo tests and `docker-compose.testing.yml`. Real environment variables override the file. To pre-pull all pinned images (or run long-lived local test databases):

```sh
docker compose --env-file .env.testing -f docker-compose.testing.yml pull
docker compose --env-file .env.testing -f docker-compose.testing.yml up -d
```

Run the tests:

```sh
cargo test --workspace          # unit tests + integration tests (needs Docker)
cargo test --workspace --lib    # unit tests only, no Docker required
```

## License

MIT OR Apache-2.0

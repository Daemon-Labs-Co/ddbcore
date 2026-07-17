//! Benchmarks for the hottest paths in a multi-TB copy: the COPY TEXT
//! encoder and the native-type mapper. Run with `cargo bench -p
//! ddbcore-postgres`. Row decoding (`RowDecoder`) is not benched here —
//! it requires live `PgRow` values that can't be constructed without a
//! server.

use std::collections::HashMap;

use chrono::{NaiveDate, TimeZone, Utc};
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use ddbcore::{Row, Value};
use ddbcore_postgres::bench_support::{map_pg_type, write_copy_line};
use rust_decimal::Decimal;

fn representative_row() -> Row {
    Row(vec![
        Value::BigInt(9_223_372_036_854_775_807),
        Value::Text("Alice Example with a \t tab and a \\ backslash".into()),
        Value::Decimal(Decimal::new(1234567, 2)),
        Value::Boolean(true),
        Value::Timestamp(Utc.with_ymd_and_hms(2026, 7, 17, 12, 0, 0).unwrap()),
        Value::Date(NaiveDate::from_ymd_opt(2026, 7, 17).unwrap()),
        Value::Null,
        Value::Json(serde_json::json!({"plan": "pro", "count": 42})),
        Value::Array(vec![Value::Text("vip".into()), Value::Null, Value::Text("a,b{c}".into())]),
        Value::Binary(vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02, 0x03]),
    ])
}

fn bench_copy_encode(c: &mut Criterion) {
    let row = representative_row();
    c.bench_function("write_copy_line/10col-row", |b| {
        let mut buffer = String::with_capacity(4096);
        b.iter(|| {
            buffer.clear();
            write_copy_line(&mut buffer, black_box(&row));
            black_box(buffer.len());
        });
    });

    let blob_row = Row(vec![Value::BigInt(1), Value::Binary(vec![0xabu8; 64 * 1024])]);
    c.bench_function("write_copy_line/64KiB-bytea", |b| {
        let mut buffer = String::with_capacity(256 * 1024);
        b.iter(|| {
            buffer.clear();
            write_copy_line(&mut buffer, black_box(&blob_row));
            black_box(buffer.len());
        });
    });
}

fn bench_type_mapping(c: &mut Criterion) {
    let enums: HashMap<String, Vec<String>> = HashMap::new();
    let native_types = [
        "bigint",
        "character varying(255)",
        "numeric(10,2)",
        "timestamp(3) with time zone",
        "text[]",
        "uuid",
        "jsonb",
        "some_unknown_extension_type",
    ];
    c.bench_function("map_pg_type/8-types", |b| {
        b.iter(|| {
            for native in &native_types {
                black_box(map_pg_type(black_box(native), &enums));
            }
        });
    });
}

criterion_group!(benches, bench_copy_encode, bench_type_mapping);
criterion_main!(benches);

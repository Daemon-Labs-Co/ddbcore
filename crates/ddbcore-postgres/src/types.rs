use ddbcore::TypeCategory;

/// Maps a Postgres `format_type()` string (e.g. `"character varying(255)"`,
/// `"numeric(10,2)"`, `"timestamp(3) with time zone"`) into a canonical
/// `TypeCategory`. `enum_types` holds label sets for user-defined enum
/// types visible in the schema being reflected, keyed by type name.
pub fn map_pg_type(native: &str, enum_types: &std::collections::HashMap<String, Vec<String>>) -> TypeCategory {
    let native = native.trim();

    if let Some(base) = native.strip_suffix("[]") {
        return TypeCategory::Array { element: Box::new(map_pg_type(base, enum_types)) };
    }

    let (name, args) = split_name_args(native);

    if let Some(values) = enum_types.get(&name) {
        return TypeCategory::Enum { values: values.clone() };
    }

    match name.as_str() {
        "boolean" | "bool" => TypeCategory::Boolean,
        "smallint" | "int2" => TypeCategory::SmallInt,
        "integer" | "int" | "int4" => TypeCategory::Integer,
        "bigint" | "int8" => TypeCategory::BigInt,
        "numeric" | "decimal" => {
            let (p, s) = parse_precision_scale(&args);
            TypeCategory::Decimal { precision: p, scale: s }
        }
        "real" | "float4" => TypeCategory::Real,
        "double precision" | "float8" => TypeCategory::Double,
        "character" | "char" | "bpchar" => TypeCategory::Char { length: parse_single_len(&args) },
        "character varying" | "varchar" => TypeCategory::VarChar { length: parse_single_len(&args) },
        "text" => TypeCategory::Text,
        "bytea" => TypeCategory::Blob,
        "date" => TypeCategory::Date,
        s if s.starts_with("time") && !s.starts_with("timestamp") => {
            TypeCategory::Time { precision: parse_single_len(&args) }
        }
        s if s.starts_with("timestamp") => TypeCategory::Timestamp {
            precision: parse_single_len(&args),
            with_timezone: s.contains("with time zone"),
        },
        "interval" => TypeCategory::Interval,
        "uuid" => TypeCategory::Uuid,
        "json" | "jsonb" => TypeCategory::Json,
        "xml" => TypeCategory::Xml,
        s if s.starts_with("bit") => TypeCategory::Bit { length: parse_single_len(&args) },
        _ => TypeCategory::Unsupported { native_type: native.to_string() },
    }
}

/// Splits `"numeric(10,2)"` into `("numeric", "10,2")`, `"integer"` into
/// `("integer", "")`, and passes multi-word type names like
/// `"timestamp(3) with time zone"` through with the parenthetical stripped
/// out of the name but kept available as `args`.
fn split_name_args(native: &str) -> (String, String) {
    if let Some(open) = native.find('(') {
        if let Some(close) = native[open..].find(')') {
            let name_part = &native[..open];
            let args = native[open + 1..open + close].to_string();
            let rest = &native[open + close + 1..];
            let name = format!("{}{}", name_part.trim_end(), rest);
            return (name, args);
        }
    }
    (native.to_string(), String::new())
}

fn parse_single_len(args: &str) -> Option<u32> {
    args.split(',').next()?.trim().parse().ok()
}

fn parse_precision_scale(args: &str) -> (Option<u32>, Option<u32>) {
    let mut parts = args.split(',').map(|s| s.trim().parse().ok());
    (parts.next().flatten(), parts.next().flatten())
}

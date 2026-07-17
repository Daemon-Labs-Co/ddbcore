use ddbcore::TypeCategory;

/// Maps a MySQL/MariaDB `information_schema.COLUMNS.COLUMN_TYPE` string
/// (e.g. `"varchar(255)"`, `"decimal(10,2)"`, `"enum('a','b','c')"`,
/// `"int(10) unsigned"`) into a canonical `TypeCategory`. Unlike Postgres,
/// enum values are embedded directly in the type string — no separate
/// catalog lookup needed.
pub fn map_mysql_type(column_type: &str) -> TypeCategory {
    let s = strip_unsigned_zerofill(column_type.trim());

    if let Some(inner) = s.strip_prefix("enum(").and_then(|rest| rest.strip_suffix(')')) {
        return TypeCategory::Enum { values: parse_quoted_list(inner) };
    }
    // SET is a MySQL-specific multi-value column type with no canonical
    // equivalent yet — preserved via the Unsupported escape hatch rather
    // than guessed at.
    if s.starts_with("set(") {
        return TypeCategory::Unsupported { native_type: column_type.to_string() };
    }

    let (name, args) = split_name_args(s);

    match name.as_str() {
        // MySQL's boolean convention: `tinyint(1)` is a bool, any other
        // tinyint width is a small integer. There's no dedicated TinyInt
        // category, so non-boolean tinyints map to SmallInt as the
        // closest wider fit.
        "tinyint" if args.trim() == "1" => TypeCategory::Boolean,
        "tinyint" | "smallint" => TypeCategory::SmallInt,
        // No MediumInt category either (24-bit); Integer is the closest
        // wider fit.
        "mediumint" | "int" | "integer" => TypeCategory::Integer,
        "bigint" => TypeCategory::BigInt,
        "decimal" | "numeric" => {
            let (p, s) = parse_precision_scale(&args);
            TypeCategory::Decimal { precision: p, scale: s }
        }
        "float" => TypeCategory::Real,
        "double" | "double precision" => TypeCategory::Double,
        "char" => TypeCategory::Char { length: parse_single_len(&args) },
        "varchar" => TypeCategory::VarChar { length: parse_single_len(&args) },
        "text" | "tinytext" | "mediumtext" | "longtext" => TypeCategory::Text,
        "binary" => TypeCategory::Binary { length: parse_single_len(&args) },
        "varbinary" => TypeCategory::VarBinary { length: parse_single_len(&args) },
        "blob" | "tinyblob" | "mediumblob" | "longblob" => TypeCategory::Blob,
        "date" => TypeCategory::Date,
        // MySQL TIME never carries a timezone.
        "time" => TypeCategory::Time { precision: parse_single_len(&args), with_timezone: false },
        // MySQL DATETIME/TIMESTAMP never carry an explicit UTC offset the
        // way Postgres's `timestamptz` does (TIMESTAMP is silently
        // converted to/from the session time zone instead), so both map
        // to `with_timezone: false`.
        "datetime" | "timestamp" => TypeCategory::Timestamp { precision: parse_single_len(&args), with_timezone: false },
        // No Year category; Integer is the closest fit.
        "year" => TypeCategory::Integer,
        // MySQL JSON normalizes documents (duplicate keys removed, order
        // not preserved) — semantically jsonb-like, hence binary: true.
        "json" => TypeCategory::Json { binary: true },
        "bit" => TypeCategory::Bit { length: parse_single_len(&args) },
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring" | "multipolygon" | "geometrycollection" => {
            TypeCategory::Geometry { subtype: Some(name) }
        }
        _ => TypeCategory::Unsupported { native_type: column_type.to_string() },
    }
}

/// Strips trailing ` zerofill` / ` unsigned` modifiers (order matters —
/// `zerofill` always follows `unsigned` when both are present).
fn strip_unsigned_zerofill(s: &str) -> &str {
    s.trim_end().trim_end_matches("zerofill").trim_end().trim_end_matches("unsigned").trim_end()
}

fn split_name_args(native: &str) -> (String, String) {
    if let Some(open) = native.find('(') {
        if let Some(close) = native[open..].find(')') {
            let name_part = &native[..open];
            let args = native[open + 1..open + close].to_string();
            return (name_part.trim().to_string(), args);
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

/// Splits `'happy','sad','neutral'` into `["happy", "sad", "neutral"]`.
/// Simple comma-split — values containing an embedded comma inside their
/// quoted literal are not handled correctly yet.
fn parse_quoted_list(inner: &str) -> Vec<String> {
    inner.split(',').map(|s| s.trim().trim_matches('\'').replace("''", "'")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_simple_scalars() {
        assert_eq!(map_mysql_type("int(11)"), TypeCategory::Integer);
        assert_eq!(map_mysql_type("bigint(20)"), TypeCategory::BigInt);
        assert_eq!(map_mysql_type("text"), TypeCategory::Text);
        assert_eq!(map_mysql_type("json"), TypeCategory::Json { binary: true });
    }

    #[test]
    fn maps_tinyint_one_as_boolean() {
        assert_eq!(map_mysql_type("tinyint(1)"), TypeCategory::Boolean);
        assert_eq!(map_mysql_type("tinyint(4)"), TypeCategory::SmallInt);
    }

    #[test]
    fn strips_unsigned_and_zerofill() {
        assert_eq!(map_mysql_type("int(10) unsigned"), TypeCategory::Integer);
        assert_eq!(map_mysql_type("int(10) unsigned zerofill"), TypeCategory::Integer);
    }

    #[test]
    fn maps_decimal_with_precision_and_scale() {
        assert_eq!(map_mysql_type("decimal(10,2)"), TypeCategory::Decimal { precision: Some(10), scale: Some(2) });
    }

    #[test]
    fn maps_varchar_with_length() {
        assert_eq!(map_mysql_type("varchar(255)"), TypeCategory::VarChar { length: Some(255) });
    }

    #[test]
    fn maps_enum_values() {
        assert_eq!(
            map_mysql_type("enum('happy','sad','neutral')"),
            TypeCategory::Enum { values: vec!["happy".to_string(), "sad".to_string(), "neutral".to_string()] }
        );
    }

    #[test]
    fn maps_set_to_unsupported() {
        assert_eq!(map_mysql_type("set('a','b')"), TypeCategory::Unsupported { native_type: "set('a','b')".to_string() });
    }

    #[test]
    fn falls_back_to_unsupported_for_unknown_types() {
        assert_eq!(map_mysql_type("inet6"), TypeCategory::Unsupported { native_type: "inet6".to_string() });
    }
}

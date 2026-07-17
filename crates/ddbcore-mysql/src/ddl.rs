use ddbcore::{
    ColumnDefinition, ConstraintDefinition, DdbCoreError, IdentityGeneration, IndexDefinition,
    ReferentialAction, Table, TableAlteration, TableDefinition, TypeCategory,
};

use crate::connection::MySqlConnection;
use crate::util::{quote_ident, quote_qualified};

fn db_err(e: sqlx::Error) -> DdbCoreError {
    DdbCoreError::Ddl(e.to_string())
}

/// Maps a canonical `TypeCategory` to MySQL/MariaDB DDL syntax. Like the
/// Postgres adapter, this ignores `DataType::native_type` and works only
/// from the category, so a table reflected from a different engine
/// renders correctly here too.
fn category_to_mysql_type(category: &TypeCategory) -> String {
    match category {
        TypeCategory::Boolean => "tinyint(1)".to_string(),
        TypeCategory::SmallInt => "smallint".to_string(),
        TypeCategory::Integer => "int".to_string(),
        TypeCategory::BigInt => "bigint".to_string(),
        TypeCategory::Decimal { precision, scale } => match (precision, scale) {
            (Some(p), Some(s)) => format!("decimal({p},{s})"),
            (Some(p), None) => format!("decimal({p})"),
            _ => "decimal".to_string(),
        },
        TypeCategory::Real => "float".to_string(),
        TypeCategory::Double => "double".to_string(),
        TypeCategory::Char { length } => format!("char({})", length.unwrap_or(1)),
        // MySQL requires an explicit length for VARCHAR; 255 is the
        // conventional default when none was captured.
        TypeCategory::VarChar { length } => format!("varchar({})", length.unwrap_or(255)),
        TypeCategory::Text => "text".to_string(),
        TypeCategory::Binary { length } => format!("binary({})", length.unwrap_or(1)),
        TypeCategory::VarBinary { length } => format!("varbinary({})", length.unwrap_or(255)),
        TypeCategory::Blob => "blob".to_string(),
        TypeCategory::Date => "date".to_string(),
        TypeCategory::Time { precision } => match precision {
            Some(p) if *p > 0 => format!("time({p})"),
            _ => "time".to_string(),
        },
        // Always DATETIME, never TIMESTAMP — TIMESTAMP has MySQL-specific
        // auto-update/timezone-conversion side effects that would silently
        // change behavior on a recreated table.
        TypeCategory::Timestamp { precision, .. } => match precision {
            Some(p) if *p > 0 => format!("datetime({p})"),
            _ => "datetime".to_string(),
        },
        // No native INTERVAL type in MySQL.
        TypeCategory::Interval => "varchar(255)".to_string(),
        // No native UUID type; char(36) is the common convention.
        TypeCategory::Uuid => "char(36)".to_string(),
        TypeCategory::Json => "json".to_string(),
        // No native XML type.
        TypeCategory::Xml => "text".to_string(),
        TypeCategory::Bit { length } => format!("bit({})", length.unwrap_or(1)),
        TypeCategory::Enum { values } => {
            let quoted = values.iter().map(|v| format!("'{}'", v.replace('\'', "''"))).collect::<Vec<_>>().join(", ");
            format!("enum({quoted})")
        }
        // No native array type; JSON is the pragmatic fallback for
        // structure, not a claim of equivalent query semantics.
        TypeCategory::Array { .. } => "json".to_string(),
        TypeCategory::Geometry { .. } => "geometry".to_string(),
        TypeCategory::Unsupported { native_type } => native_type.clone(),
    }
}

fn referential_action_sql(action: &ReferentialAction) -> &'static str {
    match action {
        ReferentialAction::NoAction => "NO ACTION",
        ReferentialAction::Restrict => "RESTRICT",
        ReferentialAction::Cascade => "CASCADE",
        ReferentialAction::SetNull => "SET NULL",
        ReferentialAction::SetDefault => "SET DEFAULT",
    }
}

fn column_def_sql(
    name: &str,
    native_sql_type: &str,
    nullable: bool,
    default: Option<&str>,
    identity: Option<IdentityGeneration>,
) -> String {
    let mut sql = format!("{} {}", quote_ident(name), native_sql_type);
    if !nullable {
        sql.push_str(" NOT NULL");
    }
    // MySQL has one flavor of generated keys regardless of the canonical
    // Always/ByDefault distinction. AUTO_INCREMENT and DEFAULT are
    // mutually exclusive; identity wins.
    if identity.is_some() {
        sql.push_str(" AUTO_INCREMENT");
    } else if let Some(default) = default {
        sql.push_str(&format!(" DEFAULT {default}"));
    }
    sql
}

/// Renders a reflected `Table` back into MySQL/MariaDB DDL, one statement
/// per element (no trailing semicolons). See the Postgres adapter's
/// `render_ddl` for the overall shape — this mirrors it, swapping
/// identifier quoting to backticks and using MySQL's `ALTER TABLE ADD
/// CONSTRAINT` / `CREATE INDEX` syntax.
pub(crate) fn render_ddl(table: &Table) -> Result<Vec<String>, DdbCoreError> {
    let qualified = quote_qualified(&table.schema, &table.name);
    let mut statements = Vec::new();

    let mut column_lines: Vec<String> = table
        .columns
        .iter()
        .map(|c| column_def_sql(&c.name, &category_to_mysql_type(&c.data_type.category), c.nullable, c.default.as_deref(), c.identity_generation))
        .collect();

    if let Some(pk) = &table.primary_key {
        let cols = pk.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        column_lines.push(format!("PRIMARY KEY ({cols})"));
    }

    statements.push(format!("CREATE TABLE {qualified} (\n  {}\n)", column_lines.join(",\n  ")));

    for uc in &table.unique_constraints {
        let cols = uc.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        statements.push(format!("ALTER TABLE {qualified} ADD CONSTRAINT {} UNIQUE ({cols})", quote_ident(&uc.name)));
    }

    for cc in &table.check_constraints {
        statements.push(format!("ALTER TABLE {qualified} ADD CONSTRAINT {} CHECK ({})", quote_ident(&cc.name), cc.expression));
    }

    for fk in &table.foreign_keys {
        let cols = fk.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let ref_qualified = quote_qualified(&fk.referenced_schema, &fk.referenced_table);
        let ref_cols = fk.referenced_columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        statements.push(format!(
            "ALTER TABLE {qualified} ADD CONSTRAINT {} FOREIGN KEY ({cols}) REFERENCES {ref_qualified} ({ref_cols}) ON UPDATE {} ON DELETE {}",
            quote_ident(&fk.name),
            referential_action_sql(&fk.on_update),
            referential_action_sql(&fk.on_delete),
        ));
    }

    for idx in &table.indexes {
        let unique = if idx.unique { "UNIQUE " } else { "" };
        let cols = idx.columns.iter().map(|c| quote_ident(&c.name)).collect::<Vec<_>>().join(", ");
        statements.push(format!("CREATE {unique}INDEX {} ON {qualified} ({cols})", quote_ident(&idx.name)));
    }

    Ok(statements)
}

pub(crate) async fn create_table(conn: &MySqlConnection, def: &TableDefinition) -> Result<(), DdbCoreError> {
    let qualified = quote_qualified(&def.schema, &def.name);
    let mut column_lines: Vec<String> = def
        .columns
        .iter()
        .map(|c: &ColumnDefinition| column_def_sql(&c.name, &category_to_mysql_type(&c.data_type.category), c.nullable, c.default.as_deref(), c.identity))
        .collect();

    if let Some(pk) = &def.primary_key {
        let cols = pk.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        column_lines.push(format!("PRIMARY KEY ({cols})"));
    }

    let sql = format!("CREATE TABLE {qualified} (\n  {}\n)", column_lines.join(",\n  "));
    sqlx::query(&sql).execute(&conn.pool).await.map_err(db_err)?;
    Ok(())
}

pub(crate) async fn create_index(conn: &MySqlConnection, def: &IndexDefinition) -> Result<(), DdbCoreError> {
    let qualified = quote_qualified(&def.table.schema, &def.table.name);
    let unique = if def.unique { "UNIQUE " } else { "" };
    let cols = def.columns.iter().map(|c| quote_ident(&c.name)).collect::<Vec<_>>().join(", ");
    let sql = format!("CREATE {unique}INDEX {} ON {qualified} ({cols})", quote_ident(&def.name));
    sqlx::query(&sql).execute(&conn.pool).await.map_err(db_err)?;
    Ok(())
}

pub(crate) async fn alter_table(conn: &MySqlConnection, alteration: &TableAlteration) -> Result<(), DdbCoreError> {
    let sql = match alteration {
        TableAlteration::AddColumn { table, column } => format!(
            "ALTER TABLE {} ADD COLUMN {}",
            quote_qualified(&table.schema, &table.name),
            column_def_sql(&column.name, &category_to_mysql_type(&column.data_type.category), column.nullable, column.default.as_deref(), column.identity)
        ),
        TableAlteration::DropColumn { table, column } => {
            format!("ALTER TABLE {} DROP COLUMN {}", quote_qualified(&table.schema, &table.name), quote_ident(column))
        }
        TableAlteration::AlterColumnType { table, column, data_type } => format!(
            "ALTER TABLE {} MODIFY COLUMN {} {}",
            quote_qualified(&table.schema, &table.name),
            quote_ident(column),
            category_to_mysql_type(&data_type.category)
        ),
        TableAlteration::AddConstraint { table, constraint } => {
            let qualified = quote_qualified(&table.schema, &table.name);
            match constraint {
                ConstraintDefinition::PrimaryKey(pk) => {
                    let cols = pk.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
                    format!("ALTER TABLE {qualified} ADD PRIMARY KEY ({cols})")
                }
                ConstraintDefinition::ForeignKey(fk) => {
                    let cols = fk.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
                    let ref_qualified = quote_qualified(&fk.referenced_schema, &fk.referenced_table);
                    let ref_cols = fk.referenced_columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
                    format!(
                        "ALTER TABLE {qualified} ADD CONSTRAINT {} FOREIGN KEY ({cols}) REFERENCES {ref_qualified} ({ref_cols}) ON UPDATE {} ON DELETE {}",
                        quote_ident(&fk.name),
                        referential_action_sql(&fk.on_update),
                        referential_action_sql(&fk.on_delete),
                    )
                }
                ConstraintDefinition::Unique(uc) => {
                    let cols = uc.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
                    format!("ALTER TABLE {qualified} ADD CONSTRAINT {} UNIQUE ({cols})", quote_ident(&uc.name))
                }
                ConstraintDefinition::Check(cc) => {
                    format!("ALTER TABLE {qualified} ADD CONSTRAINT {} CHECK ({})", quote_ident(&cc.name), cc.expression)
                }
            }
        }
        TableAlteration::DropConstraint { table, name } => {
            format!("ALTER TABLE {} DROP CONSTRAINT {}", quote_qualified(&table.schema, &table.name), quote_ident(name))
        }
        // Unlike Postgres, MySQL requires naming the owning table when
        // dropping an index.
        TableAlteration::DropIndex { table, name } => {
            format!("DROP INDEX {} ON {}", quote_ident(name), quote_qualified(&table.schema, &table.name))
        }
    };

    sqlx::query(&sql).execute(&conn.pool).await.map_err(db_err)?;
    Ok(())
}

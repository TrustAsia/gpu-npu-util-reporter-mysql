//! # sql_gen 模块
//!
//! 建表 SQL 生成层（仅 `--init` 模式使用）。
//!
//! 固定列基线 + mapping 列按 `position` 插入排序，输出到 `./init/<table>.sql`。
//! 生成的 SQL 含每列 COMMENT、主键、3 个索引；**不含** DROP TABLE、**不含**
//! CREATE DATABASE（重复执行因 `IF NOT EXISTS` 而安全跳过）。
//!
//! ## 列顺序
//! - 固定列按 [`FIXED_COLUMNS`](crate::config::FIXED_COLUMNS) 声明顺序排列。
//! - mapping 列按各自 `position`（after/before + 锚点列名）插入到锚点前后。
//!   锚点不存在时追加到末尾（防御，配置校验已保证 anchor 合法）。

use crate::config::{mapping_final_name, Config, MappingColumn, FIXED_COLUMNS};
use crate::models::ColumnDef;
use std::path::Path;

/// 生成建表 SQL 全文。
pub fn generate(cfg: &Config) -> String {
    let columns = build_column_list(cfg);
    let has_mapping = cfg.mapping.enabled || !cfg.mapping.sources.is_empty();

    let mut lines: Vec<String> = Vec::new();
    lines.push("-- 由 gpu-npu-util-reporter --init 自动生成".into());
    lines.push(format!("-- 配置文件对应表: {}", cfg.database.table));
    lines.push(format!("-- 含 mapping 列: {}", has_mapping));
    lines.push("-- 注意: 本文件不含 DROP TABLE，重复执行会因表已存在而跳过(IF NOT EXISTS)。".into());
    lines.push(format!("CREATE TABLE IF NOT EXISTS {} (", cfg.database.table));

    let col_lines: Vec<String> = columns
        .iter()
        .map(|c| format!("    {:<16} {}{}", c.name, type_decl(c), comment_clause(c)))
        .collect();
    lines.extend(col_lines);

    lines.push("    PRIMARY KEY (id),".into());
    lines.push("    INDEX idx_ts_ip_card (ts, ip, card_id),".into());
    lines.push("    INDEX idx_ip_card (ip, card_id),".into());
    lines.push("    INDEX idx_ts (ts)".into());
    lines.push(
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='计算卡利用率采集记录';".into(),
    );

    lines.join("\n") + "\n"
}

/// 生成单列的类型声明（含 NULL/NOT NULL）。
///
/// `id` 列特殊：带 `AUTO_INCREMENT`。其余列按 nullable 决定 NULL/NOT NULL。
fn type_decl(c: &ColumnDef) -> String {
    if c.name == "id" {
        return "BIGINT NOT NULL AUTO_INCREMENT".into();
    }
    let null_part = if c.nullable { "NULL" } else { "NOT NULL" };
    format!("{} {}", c.sql_type, null_part)
}

/// 生成列的 COMMENT 子句（转义内部单引号）。
fn comment_clause(c: &ColumnDef) -> String {
    format!("COMMENT '{}'", c.comment.replace('\'', "''"))
}

/// 构建最终列列表（固定列 + mapping 列按 position 插入）。
///
/// 供 sql_gen 生成 SQL，也供 schema 校验取期望列名集合。
pub fn build_column_list(cfg: &Config) -> Vec<ColumnDef> {
    // 固定列基线：直接从 FIXED_COLUMNS 取（其 sql_type 已是纯类型，无修饰）。
    let mut result: Vec<ColumnDef> = FIXED_COLUMNS
        .iter()
        .map(|(n, t, nullable, comment)| ColumnDef {
            name: n.to_string(),
            sql_type: t.to_string(),
            nullable: *nullable,
            comment: comment.to_string(),
        })
        .collect();

    // mapping 列按 position 插入。
    for ms in &cfg.mapping.sources {
        for col in &ms.columns {
            insert_by_position(&mut result, col);
        }
    }
    result
}

/// 按 `position` 把 mapping 列插入 result。
///
/// `after anchor` → 插在锚点列之后；`before anchor` → 插在锚点列之前；
/// 锚点不存在 → 追加到末尾（防御，配置校验已保证 anchor 合法）。
fn insert_by_position(result: &mut Vec<ColumnDef>, col: &MappingColumn) {
    let new_col = ColumnDef {
        name: mapping_final_name(col),
        sql_type: col_type_to_sql(&col.col_type),
        nullable: true,
        comment: col.comment.clone(),
    };
    let anchor_idx = result.iter().position(|c| c.name == col.position.anchor);
    if let Some(idx) = anchor_idx {
        let insert_at = if col.position.direction == "before" {
            idx
        } else {
            idx + 1
        };
        result.insert(insert_at, new_col);
    } else {
        // 锚点不存在：追加到末尾（防御性，配置校验应已拦截）。
        result.push(new_col);
    }
}

/// 配置的列类型(如 "varchar(255)") 转 SQL 类型字符串。
///
/// int/bigint → INT/BIGINT；double/float → DOUBLE；varchar 透传；
/// 其余(含带长度的 decimal/text 等)原样透传。
fn col_type_to_sql(t: &str) -> String {
    let lower = t.to_lowercase();
    let lower = lower.trim();
    if lower.starts_with("varchar") {
        return t.to_string();
    }
    if lower.starts_with("bigint") {
        return "BIGINT".into();
    }
    if lower.starts_with("int") {
        return "INT".into();
    }
    if lower.starts_with("double") || lower.starts_with("float") {
        return "DOUBLE".into();
    }
    t.to_string()
}

/// 生成 SQL 并写入 `./init/<table>.sql`（目录自动创建）。
pub fn write_init_sql(cfg: &Config, dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}.sql", cfg.database.table));
    std::fs::write(path, generate(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造一份带 mapping(location 列 after namespace) 的合法配置。
    fn cfg_with_mapping() -> Config {
        let yaml = r#"
interval: 60
retention_days: 30
retention_interval: 3600
timezone: "Asia/Shanghai"
database: { host: "h", port: 3306, user: "u", password: "p", database: "db", table: "gpu_usage", max_connections: 10 }
logging: { level: "info", dir: "./logs", all_file: "all.log", error_file: "error.log", rotation: "daily", archive_after_days: 7, archive_prefix: "logs", stdout: true }
mapping:
  enabled: true
  sources:
    - source_path: "./a.csv"
      src_key: "namespace"
      dest_key: "Namespace"
      columns:
        - source_field: "机房位置"
          rename: "location"
          type: "varchar(255)"
          comment: "机房位置"
          position: { direction: after, anchor: "namespace" }
sources:
  - name: "s1"
    ip: "1.1.1.1"
    url: "http://1.1.1.1:9090"
    primary: { metric: "m1", card_label: "gpu" }
"#;
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn mapping_column_inserted_after_namespace() {
        let cfg = cfg_with_mapping();
        let cols = build_column_list(&cfg);
        let ns_idx = cols.iter().position(|c| c.name == "namespace").unwrap();
        let loc_idx = cols.iter().position(|c| c.name == "location").unwrap();
        assert_eq!(loc_idx, ns_idx + 1);
        // pod 原本紧跟 namespace，现应在 location 之后。
        let pod_idx = cols.iter().position(|c| c.name == "pod").unwrap();
        assert_eq!(pod_idx, loc_idx + 1);
    }

    #[test]
    fn generated_sql_has_no_drop() {
        let cfg = cfg_with_mapping();
        let sql = generate(&cfg);
        // 去掉注释行后再断言，避免注释里的说明文字误伤。
        let no_comments: String = sql
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let lower = no_comments.to_lowercase();
        assert!(!lower.contains("drop table"));
        assert!(!lower.contains("create database"));
    }

    #[test]
    fn generated_sql_has_comment_and_indexes() {
        let cfg = cfg_with_mapping();
        let sql = generate(&cfg);
        assert!(sql.contains("COMMENT '机房位置'"));
        assert!(sql.contains("idx_ts_ip_card"));
        assert!(sql.contains("PRIMARY KEY (id)"));
        assert!(sql.contains("IF NOT EXISTS"));
        assert!(sql.contains("AUTO_INCREMENT"));
    }

    #[test]
    fn disabled_mapping_still_has_column() {
        // mapping.enabled=false 时列仍建立（--init 不看 enabled），只是采集不填值。
        let mut cfg = cfg_with_mapping();
        cfg.mapping.enabled = false;
        let cols = build_column_list(&cfg);
        assert!(cols.iter().any(|c| c.name == "location"));
    }

    #[test]
    fn before_position_inserts_before_anchor() {
        let mut cfg = cfg_with_mapping();
        // 把 location 改为 before gpu_util，应插在 gpu_util 之前。
        cfg.mapping.sources[0].columns[0].position.direction = "before".into();
        cfg.mapping.sources[0].columns[0].position.anchor = "gpu_util".into();
        let cols = build_column_list(&cfg);
        let gu_idx = cols.iter().position(|c| c.name == "gpu_util").unwrap();
        let loc_idx = cols.iter().position(|c| c.name == "location").unwrap();
        assert_eq!(loc_idx, gu_idx - 1);
    }

    #[test]
    fn col_type_normalization() {
        assert_eq!(col_type_to_sql("int"), "INT");
        assert_eq!(col_type_to_sql("bigint"), "BIGINT");
        assert_eq!(col_type_to_sql("double"), "DOUBLE");
        assert_eq!(col_type_to_sql("float"), "DOUBLE");
        assert_eq!(col_type_to_sql("varchar(255)"), "varchar(255)");
        assert_eq!(col_type_to_sql("text"), "text");
    }
}

//! schema 校验与维护的纯函数 + SQL 字符串构造。
//!
//! 把"对比期望列与实际列"和"拼各类 SQL"都做成纯函数，便于单元测试；
//! 真正执行（连库、fetch、execute）在 [`super::Sink`] 中。

use std::collections::HashSet;

/// schema 校验结果。
#[derive(Debug, PartialEq)]
pub enum SchemaCheck {
    /// 期望列与实际列完全匹配。
    Match,
    /// 实际表缺少这些列（缺列 → 调用方报错退出，不应继续）。
    Missing(Vec<String>),
    /// 实际表多出这些列（多列 → 调用方按 on_extra_columns 策略告警询问）。
    Extra(Vec<String>),
}

/// 对比期望列与实际列。
///
/// 期望列来自配置（固定列 + mapping 列）。缺列优先于多列返回
/// （缺列是必须修复的硬错误）。
pub fn compare(expected: &HashSet<String>, actual: &HashSet<String>) -> SchemaCheck {
    let missing: Vec<String> = expected.difference(actual).cloned().collect();
    let extra: Vec<String> = actual.difference(expected).cloned().collect();
    if !missing.is_empty() {
        SchemaCheck::Missing(missing)
    } else if !extra.is_empty() {
        SchemaCheck::Extra(extra)
    } else {
        SchemaCheck::Match
    }
}

/// 连接级时区 SET 语句（程序/连接/清理三方须同一时区）。
pub fn set_timezone_sql(tz: &str) -> String {
    format!("SET time_zone = '{}'", tz)
}

/// 保留期清理 SQL。`?` 绑定保留天数；用 `NOW()`（连接已 SET time_zone）作基准。
pub fn retention_delete_sql(table: &str) -> String {
    format!(
        "DELETE FROM {} WHERE ts < DATE_SUB(NOW(), INTERVAL ? DAY)",
        table
    )
}

/// 读取表列的 SQL（查 INFORMATION_SCHEMA）。
pub fn list_columns_sql(table: &str) -> String {
    format!(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'",
        table
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn match_when_identical() {
        assert_eq!(compare(&set(&["a", "b"]), &set(&["a", "b"])), SchemaCheck::Match);
    }

    #[test]
    fn missing_when_actual_lacks() {
        assert_eq!(
            compare(&set(&["a", "b", "c"]), &set(&["a", "b"])),
            SchemaCheck::Missing(vec!["c".into()])
        );
    }

    #[test]
    fn extra_when_actual_has_more() {
        assert_eq!(
            compare(&set(&["a"]), &set(&["a", "x"])),
            SchemaCheck::Extra(vec!["x".into()])
        );
    }

    #[test]
    fn missing_takes_precedence_over_extra() {
        // 同时缺 c 又多 x：缺列优先返回。
        assert_eq!(
            compare(&set(&["a", "c"]), &set(&["a", "x"])),
            SchemaCheck::Missing(vec!["c".into()])
        );
    }

    #[test]
    fn sql_builders_format() {
        assert_eq!(set_timezone_sql("Asia/Shanghai"), "SET time_zone = 'Asia/Shanghai'");
        assert_eq!(retention_delete_sql("gpu_usage"), "DELETE FROM gpu_usage WHERE ts < DATE_SUB(NOW(), INTERVAL ? DAY)");
        assert!(list_columns_sql("t").contains("TABLE_NAME = 't'"));
    }
}

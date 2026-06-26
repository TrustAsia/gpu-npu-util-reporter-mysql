//! schema 校验与维护的纯函数 + SQL 字符串构造。
//!
//! 把"对比期望列与实际列"和"拼各类 SQL"都做成纯函数，便于单元测试；
//! 真正执行（连库、fetch、execute）在 [`super::Sink`] 中。

use chrono::Offset;
use std::collections::{HashMap, HashSet};

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
/// （缺列是必须修复的硬错误）。返回的列名已排序，使错误信息可复现
/// （`HashSet` 迭代序不确定，否则同一问题两次运行列序可能不同）。
///
/// **大小写不敏感**：MySQL 列名解析本身大小写不敏感，且 `INFORMATION_SCHEMA.
/// COLUMN_NAME` 在不同 `lower_case_table_names` 下返回的存储大小写不同（Linux 默认
/// 区分，Windows/macOS 默认小写）。若严格按字符串比较，建表时用了 `Namespace` 而
/// 配置期望 `namespace` 会误报 Missing/Extra——本程序自生成表全用小写，但手动
/// ALTER 或历史库可能大小写不一。故比较前双方统一转小写，与 MySQL 解析语义一致。
/// 返回的列名用期望侧（配置）的原大小写，便于运维对照配置排查。
pub fn compare(expected: &HashSet<String>, actual: &HashSet<String>) -> SchemaCheck {
    // 双方转小写后做差集；保留期望侧原字符串用于错误消息。
    let expected_lower: HashMap<String, String> =
        expected.iter().map(|s| (s.to_lowercase(), s.clone())).collect();
    let actual_lower: HashSet<String> = actual.iter().map(|s| s.to_lowercase()).collect();
    // expected 的键（小写）转成拥有的 HashSet，便于与 actual_lower 同类型做 difference。
    let expected_keys: HashSet<String> = expected_lower.keys().cloned().collect();

    let mut missing: Vec<String> = expected_keys
        .difference(&actual_lower)
        .map(|k| expected_lower[k].clone())
        .collect();
    missing.sort();
    let mut extra: Vec<String> = actual_lower
        .difference(&expected_keys)
        .cloned()
        .collect();
    extra.sort();
    if !missing.is_empty() {
        SchemaCheck::Missing(missing)
    } else if !extra.is_empty() {
        SchemaCheck::Extra(extra)
    } else {
        SchemaCheck::Match
    }
}

/// 连接级时区 SET 语句（程序/连接/清理三方须同一时区）。
///
/// `offset=true` 时将 IANA 时区名（如 `Asia/Shanghai`）转换为 UTC 偏移格式
/// （如 `+08:00`），MySQL **始终支持**，无需加载时区表（Windows 默认无时区表，
/// `SET time_zone = 'Asia/Shanghai'` 会报 `Unknown or incorrect time zone`）。
///
/// `offset=false` 时直接使用 IANA 时区名，需 MySQL 已加载时区表（Linux 通常已加载），
/// 但能正确处理夏令时切换。
///
/// **安全前提**：
/// - 偏移格式 `+HH:MM` / `-HH:MM` 字符集为 `[0-9:+-]`，不含单引号/反斜杠/分号，
///   无法突破 `'...'` 字面量边界。
/// - IANA 名经 `chrono_tz::Tz` 解析校验，字符集为 `[A-Za-z0-9_/+~-]`，同样安全。
pub fn set_timezone_sql(tz: chrono_tz::Tz, offset: bool) -> String {
    if offset {
        let utc_offset = chrono::Utc::now().with_timezone(&tz).offset().fix();
        format!("SET time_zone = '{}'", utc_offset)
    } else {
        format!("SET time_zone = '{}'", tz.name())
    }
}

/// 保留期清理 SQL。`?` 绑定保留天数；用 `NOW()`（连接已 SET time_zone）作基准。
pub fn retention_delete_sql(table: &str) -> String {
    format!(
        "DELETE FROM {} WHERE ts < DATE_SUB(NOW(), INTERVAL ? DAY)",
        table
    )
}

/// 读取表列的 SQL（查 INFORMATION_SCHEMA）。
///
/// **必须限定 TABLE_SCHEMA**：仅按 TABLE_NAME 过滤会匹配服务器上所有库的同名表，
/// 返回的列是跨库并集，可能掩盖真正的缺列（另一库恰好有同名列）或制造虚假多列，
/// 使 schema 校验（缺列即退出的硬门槛）失效。`DATABASE()` 解析为当前连接库
/// （URL 里指定的 database），精确限定到目标库。
///
/// **过滤生成列**：`GENERATION_EXPRESSION <> ''` 排除 GENERATED/VIRTUAL 列。
/// 这类列由 MySQL 自动维护，不算"用户声明的列"，否则会触发 on_extra_columns 的
/// ask/abort 分支造成误报（用户并未多建列）。
pub fn list_columns_sql(table: &str) -> String {
    format!(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}' AND GENERATION_EXPRESSION = ''",
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
        // offset=true: Asia/Shanghai = UTC+8，SET time_zone 应输出偏移格式 '+08:00'。
        let tz_sql_offset = set_timezone_sql(chrono_tz::Asia::Shanghai, true);
        assert!(tz_sql_offset.starts_with("SET time_zone = '+08:00'"), "期望 '+08:00' 偏移格式，实际: {}", tz_sql_offset);
        // offset=false: 应输出 IANA 名 'Asia/Shanghai'。
        let tz_sql_iana = set_timezone_sql(chrono_tz::Asia::Shanghai, false);
        assert_eq!(tz_sql_iana, "SET time_zone = 'Asia/Shanghai'");
        assert_eq!(retention_delete_sql("gpu_usage"), "DELETE FROM gpu_usage WHERE ts < DATE_SUB(NOW(), INTERVAL ? DAY)");
        // 必须限定 TABLE_SCHEMA，否则跨库同名表会污染列集合。
        let cols_sql = list_columns_sql("t");
        assert!(cols_sql.contains("TABLE_SCHEMA = DATABASE()"), "缺 TABLE_SCHEMA 限定: {}", cols_sql);
        assert!(cols_sql.contains("TABLE_NAME = 't'"));
    }

    /// 守护（R-minor）：列对比须大小写不敏感——MySQL 列名解析本就大小写不敏感，
    /// 且 INFORMATION_SCHEMA.COLUMN_NAME 在不同 lower_case_table_names 下返回的
    /// 存储大小写不同。建表用了 Namespace 而配置期望 namespace 不应误报差异。
    #[test]
    fn compare_is_case_insensitive() {
        // 期望小写 namespace，实际大写 Namespace → 应判为 Match。
        assert_eq!(
            compare(&set(&["namespace"]), &set(&["Namespace"])),
            SchemaCheck::Match
        );
        // 期望 GPU_UTIL，实际 gpu_util → Match。
        assert_eq!(
            compare(&set(&["GPU_UTIL"]), &set(&["gpu_util"])),
            SchemaCheck::Match
        );
        // 实际多出列但仅大小写不同 → 不算多列（同列）。
        assert_eq!(
            compare(&set(&["a"]), &set(&["A"])),
            SchemaCheck::Match
        );
    }

    /// 守护（R-minor）：缺列仍须能被检出（大小写不敏感不能掩盖真正的缺列）。
    #[test]
    fn compare_case_insensitive_still_detects_real_missing() {
        assert_eq!(
            compare(&set(&["namespace", "pod"]), &set(&["Namespace"])),
            SchemaCheck::Missing(vec!["pod".into()])
        );
    }

    /// 守护（R-minor）：list_columns_sql 须过滤生成列，避免 GENERATED/VIRTUAL 列
    /// 触发 on_extra_columns 的 ask/abort 误报。
    #[test]
    fn list_columns_sql_filters_generated_columns() {
        let sql = list_columns_sql("t");
        assert!(
            sql.contains("GENERATION_EXPRESSION = ''"),
            "应过滤生成列（GENERATION_EXPRESSION = ''），实际: {}",
            sql
        );
    }
}

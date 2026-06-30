//! # mapping 模块
//!
//! 资产表关联层（纯内存查找，无运行期 I/O）。
//!
//! 启动时把 CSV/Excel 资产表加载进内存，按 [`dest_key`](crate::config::MappingSource::dest_key)
//! 列建索引。采集时用行内 [`src_key`](crate::config::MappingSource::src_key) 值查找匹配行，
//! 把该匹配行声明的 [`columns`](crate::config::MappingColumn) 字段补进采集行。
//!
//! ## 规则（见 spec）
//! - 无匹配 → 该列写 NULL。
//! - 多匹配（同一 dest_key 多行）→ 取首条（其余在加载期忽略，调用方可记 WARN）。
//! - 数值类型解析失败 → NULL（并产生一条 warning 由调用方记日志）。
//! - `enabled: false` → 不补值（列仍由 --init 建立，只是采集期留 NULL）。
//!
//! ## 与配置的关系
//! - 列的"最终名"由 [`crate::config::mapping_final_name`] 决定（rename 或 source_field）。
//! - 列的"是否数值"由配置的 `type`（如 int/double/float 视为数值）决定。

use crate::config::{mapping_final_name, MappingConfig, MappingSource};
use crate::models::Row as CollectorRow;
use std::collections::HashMap;

/// 加载后的资产索引：`dest_key` 值 → 该行声明的列值。
///
/// 一个 [`MappingSource`] 对应一个 [`AssetIndex`]。
pub struct AssetIndex {
    /// key = dest_key 的值，value = Map<最终列名, 字符串原值>。
    map: HashMap<String, HashMap<String, String>>,
    /// 该资产源要补的列：(最终列名, 配置的列类型)。保持声明顺序。
    columns: Vec<(String, String)>,
}

/// 加载错误（携带可读描述）。
#[derive(Debug)]
pub struct MappingError(pub String);

impl AssetIndex {
    /// 用行内 src_key 的值做 join，返回该匹配行所有声明列的字符串原值。
    /// 无匹配返回 None。
    pub fn lookup(&self, key: &str) -> Option<&HashMap<String, String>> {
        self.map.get(key)
    }

    /// 该索引负责补充的列名列表（最终名）。保持配置中的声明顺序。
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|(n, _)| n.clone()).collect()
    }
}

/// 行 = HashMap<列名, 字符串值>。CSV 与 Excel 统一转成这种结构。
type RowMap = HashMap<String, String>;

/// 规范化关联键：去 UTF-8 BOM、去首尾空白。
///
/// 资产表常由 Excel 导出（带 BOM、单元格前后残留空格），采集行内键来自 Prometheus
/// 标签（无 BOM/通常无首尾空格）。若不规范化，`" prod "`≠`"prod"`、
/// `"\u{FEFF}default"`≠`"default"` 会导致整列静默 NULL——违背"无静默数据损坏"原则。
/// 索引侧（load_source）与行内键侧（join_row）都用本函数，保持对称。
fn normalize_key(v: &str) -> String {
    v.strip_prefix('\u{FEFF}')
        .unwrap_or(v)
        .trim()
        .to_string()
}

/// 去掉首个表头单元格可能的 UTF-8 BOM（Excel/Windows 默认带 BOM，
/// 会导致首个表头名变成 `"\u{FEFF}Namespace"`，与配置的 dest_key 不匹配 → 静默全 NULL）。
fn strip_bom(s: &str) -> &str {
    s.strip_prefix('\u{FEFF}').unwrap_or(s)
}

/// 从单个 [`MappingSource`] 加载为 [`AssetIndex`]。
///
/// 读取 CSV 或 Excel（按扩展名判断），取表头列名，逐行构造 RowMap，
/// 然后按 `dest_key` 建索引：同一 key 仅保留首条（去重）。
pub fn load_source(ms: &MappingSource) -> Result<AssetIndex, MappingError> {
    let columns: Vec<(String, String)> = ms
        .columns
        .iter()
        .map(|c| (mapping_final_name(c), c.col_type.clone()))
        .collect();
    let rows = if ms.source_path.ends_with(".xlsx") {
        read_xlsx(ms)?
    } else {
        read_csv(ms)?
    };

    // 建 dest_key 索引：同一 key 仅保留首条（多匹配取首条规则）。
    // key 经规范化（去 BOM/首尾空白）后再建索引，与 join_row 侧的行内键规范化对称，
    // 避免 " prod "≠"prod"、"\u{FEFF}Namespace"≠"Namespace" 等导致的静默全 NULL。
    let mut map: HashMap<String, HashMap<String, String>> = HashMap::new();
    for row in &rows {
        let key = row
            .get(&ms.dest_key)
            .map(|v| normalize_key(v))
            .unwrap_or_default();
        if key.is_empty() {
            continue;
        }
        if map.contains_key(&key) {
            // 已存在：跳过（首条优先），调用方若需告警可另行处理。
            continue;
        }
        // 只保留 columns 声明的列（按最终名）。
        let filtered: HashMap<String, String> = ms
            .columns
            .iter()
            .filter_map(|c| row.get(&c.source_field).map(|v| (mapping_final_name(c), v.clone())))
            .collect();
        map.insert(key, filtered);
    }
    Ok(AssetIndex { map, columns })
}

/// 用 csv crate 读取 CSV。首行为表头列名。
fn read_csv(ms: &MappingSource) -> Result<Vec<RowMap>, MappingError> {
    let mut rdr = csv::Reader::from_path(&ms.source_path)
        .map_err(|e| MappingError(format!("打开 CSV 失败: {}", e)))?;
    let headers = rdr
        .headers()
        .map_err(|e| MappingError(format!("读 CSV 表头失败: {}", e)))?;
    // 首个表头单元格去 BOM（Excel/Windows 导出的 CSV 默认带 UTF-8 BOM，
    // 否则首个表头名形如 "\u{FEFF}Namespace"，与配置 dest_key 不匹配 → 全 NULL）。
    let headers: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(i, s)| if i == 0 { strip_bom(s) } else { s }.to_string())
        .collect();
    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec.map_err(|e| MappingError(format!("读 CSV 行失败: {}", e)))?;
        let mut row: RowMap = HashMap::new();
        for (i, h) in headers.iter().enumerate() {
            if let Some(v) = rec.get(i) {
                row.insert(h.clone(), v.to_string());
            }
        }
        out.push(row);
    }
    Ok(out)
}

/// 用 calamine 读取 Excel（.xlsx）。首行为表头列名。
fn read_xlsx(ms: &MappingSource) -> Result<Vec<RowMap>, MappingError> {
    use calamine::{open_workbook, Reader, Xlsx};
    let path = &ms.source_path;
    let mut wb: Xlsx<_> = open_workbook(path)
        .map_err(|e| MappingError(format!("打开 Excel 失败: {}", e)))?;
    let sheet_name = ms.source_sheet.clone().unwrap_or_else(|| "Sheet1".into());
    let range = wb
        .worksheet_range(&sheet_name)
        .map_err(|e| MappingError(format!("读工作表 {} 失败: {}", sheet_name, e)))?;
    let mut rows_iter = range.rows();
    let header = rows_iter
        .next()
        .ok_or_else(|| MappingError("Excel 表头为空".into()))?;
    // 首个表头去 BOM（与 read_csv 对称；xlsx 理论上少有 BOM，但统一处理无害）。
    let mut headers: Vec<String> = Vec::with_capacity(header.len());
    for (i, c) in header.iter().enumerate() {
        let s = c.to_string();
        headers.push(if i == 0 { strip_bom(&s).to_string() } else { s });
    }
    let mut out = Vec::new();
    for row in rows_iter {
        let mut r: RowMap = HashMap::new();
        for (i, h) in headers.iter().enumerate() {
            if let Some(cell) = row.get(i) {
                r.insert(h.clone(), cell.to_string());
            }
        }
        out.push(r);
    }
    Ok(out)
}

/// 加载所有配置的资产源，返回索引列表（顺序与配置一致）。
pub fn load_all(cfg: &MappingConfig) -> Result<Vec<AssetIndex>, MappingError> {
    cfg.sources.iter().map(load_source).collect()
}

/// 判断配置的列类型是否为数值类型（影响 join 时是否尝试解析为数字）。
///
/// 需与 [`crate::sql_gen`] 的类型识别保持一致：`col_type_to_sql` 把 `int`/`bigint`/
/// `double`/`float` 归为数值，故此处也覆盖 MySQL 的全部整数类型
/// （`tinyint`/`smallint`/`mediumint`/`int`/`bigint`）与浮点/定点（`float`/`double`/
/// `decimal`）。否则一个声明为 `bigint` 的列若值脏（非数字），不会被此处的解析检查
/// 拦下（→ 列级 NULL + warning），而是被原样透传给 MySQL，在 `STRICT_TRANS_TABLES` 下
/// 导致**整行** INSERT 失败被丢弃——与 `int` 列的"列级软失败"语义不一致。
fn is_numeric_type(col_type: &str) -> bool {
    let lower = col_type.trim().to_lowercase();
    lower.starts_with("int")
        || lower.starts_with("bigint")
        || lower.starts_with("tinyint")
        || lower.starts_with("smallint")
        || lower.starts_with("mediumint")
        || lower.starts_with("double")
        || lower.starts_with("float")
        || lower.starts_with("decimal")
}

/// 从采集行中解析关联键值。
///
/// `src_key` 可能指向行内的结构性字段（ip/card_id/source，存在 Row 的具名字段中）
/// 或标签字段（namespace/pod 等，存在 `row.strings` 中）。本函数统一处理两种情况，
/// 避免 `src_key: "ip"` 时因只查 `row.strings` 而得到 None → 关联键空串 → 全列静默 NULL。
fn resolve_row_key(row: &CollectorRow, src_key: &str) -> Option<String> {
    match src_key {
        "ip" => Some(row.ip.clone()),
        "card_id" => Some(row.card_id.clone()),
        "source" => Some(row.source.clone()),
        _ => row.strings.get(src_key).cloned().flatten(),
    }
}

/// 把一个采集行与所有资产索引 join，补 mapping 列到 `row.strings`。
///
/// 每个资产索引用其对应 `mapping_sources[idx].src_key` 作为行内关联键
/// （不同资产源可用不同行内键，如一个 join namespace、另一个 join ip）。
/// `indices` 与 `mapping_sources` 须按相同顺序一一对应。
///
/// 返回 warnings 列表（当前主要是"数值类型解析失败"提示），由调用方记 WARN。
/// 无匹配的列直接置 NULL，不产生 warning（属于正常情况）。
pub fn join_row(
    row: &mut CollectorRow,
    indices: &[AssetIndex],
    mapping_sources: &[MappingSource],
) -> Vec<String> {
    let mut warnings = Vec::new();

    for (idx, index) in indices.iter().enumerate() {
        // 每个资产源用自己的 src_key 从行内取关联值。
        // src_key 可能指向结构性字段（ip/card_id/source）或 strings 中的标签字段。
        let src_key = mapping_sources
            .get(idx)
            .map(|ms| ms.src_key.as_str())
            .unwrap_or("");
        let key_value = resolve_row_key(row, src_key)
            .map(|v| normalize_key(&v))
            .unwrap_or_default();
        let matched = index.lookup(&key_value);
        for col_name in index.column_names() {
            // 查该列配置的类型，判断是否需数值解析。
            let col_type = mapping_sources
                .get(idx)
                .and_then(|ms| {
                    ms.columns
                        .iter()
                        .find(|c| mapping_final_name(c) == col_name)
                        .map(|c| c.col_type.to_lowercase())
                })
                .unwrap_or_default();
            let is_numeric = is_numeric_type(&col_type);

            let value = matched.and_then(|m| m.get(&col_name)).cloned();
            match value {
                None => {
                    row.strings.insert(col_name, None);
                }
                Some(v) => {
                    if is_numeric && v.parse::<f64>().is_err() {
                        warnings.push(format!("{} 类型解析失败: '{}'", col_name, v));
                        row.strings.insert(col_name, None);
                    } else {
                        row.strings.insert(col_name, Some(v));
                    }
                }
            }
        }
    }
    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ColumnPosition, MappingColumn};
    use chrono_tz::Asia::Shanghai;

    /// 构造一个指向 CSV fixture 的 MappingSource，含 location/owner 两列。
    fn sample_mapping() -> MappingSource {
        MappingSource {
            source_path: "tests/fixtures/assets.csv".into(),
            src_key: "namespace".into(),
            dest_key: "Namespace".into(),
            source_sheet: None,
            columns: vec![
                MappingColumn {
                    source_field: "机房位置".into(),
                    rename: Some("location".into()),
                    col_type: "varchar(255)".into(),
                    comment: "机房".into(),
                    position: ColumnPosition {
                        direction: "after".into(),
                        anchor: "namespace".into(),
                    },
                },
                MappingColumn {
                    source_field: "负责人".into(),
                    rename: Some("owner".into()),
                    col_type: "varchar(64)".into(),
                    comment: "负责人".into(),
                    position: ColumnPosition {
                        direction: "after".into(),
                        anchor: "namespace".into(),
                    },
                },
            ],
        }
    }

    #[test]
    fn loads_csv_and_dedups_by_first() {
        let ms = sample_mapping();
        let index = load_source(&ms).unwrap();
        // default 出现两次，取首条（机房A, 张三）。
        let m = index.lookup("default").unwrap();
        assert_eq!(m.get("location").unwrap(), "机房A");
        assert_eq!(m.get("owner").unwrap(), "张三");
        // prod 单条。
        let p = index.lookup("prod").unwrap();
        assert_eq!(p.get("location").unwrap(), "机房B");
    }

    #[test]
    fn no_match_returns_none() {
        let ms = sample_mapping();
        let index = load_source(&ms).unwrap();
        assert!(index.lookup("nonexistent").is_none());
    }

    #[test]
    fn join_row_fills_columns() {
        let ms = sample_mapping();
        let cfg = MappingConfig {
            enabled: true,
            sources: vec![ms.clone()],
        };
        let indices = load_all(&cfg).unwrap();
        let mut row = CollectorRow {
            ts: chrono::Utc::now().with_timezone(&Shanghai),
            ip: "1.1.1.1".into(),
            card_id: "0".into(),
            fields: Default::default(),
            strings: HashMap::from([("namespace".into(), Some("default".into()))]),
            source: "s1".into(),
        };
        let warnings = join_row(&mut row, &indices, &cfg.sources);
        assert!(warnings.is_empty());
        assert_eq!(row.strings.get("location").unwrap().as_deref(), Some("机房A"));
    }

    #[test]
    fn join_no_match_fills_null() {
        let ms = sample_mapping();
        let cfg = MappingConfig {
            enabled: true,
            sources: vec![ms],
        };
        let indices = load_all(&cfg).unwrap();
        let mut row = CollectorRow {
            ts: chrono::Utc::now().with_timezone(&Shanghai),
            ip: "1.1.1.1".into(),
            card_id: "0".into(),
            fields: Default::default(),
            strings: HashMap::from([("namespace".into(), Some("zzz".into()))]),
            source: "s1".into(),
        };
        join_row(&mut row, &indices, &cfg.sources);
        assert_eq!(row.strings.get("location").unwrap(), &None);
    }

    #[test]
    fn numeric_parse_failure_warns_and_nulls() {
        // 构造一个声明为 int 但 CSV 值非数字的列，应产生 warning 并写 NULL。
        let ms = MappingSource {
            source_path: "tests/fixtures/assets.csv".into(),
            src_key: "namespace".into(),
            dest_key: "Namespace".into(),
            source_sheet: None,
            columns: vec![MappingColumn {
                source_field: "机房位置".into(), // 值如"机房A"非数字
                rename: Some("loc_int".into()),
                col_type: "int".into(),
                comment: "c".into(),
                position: ColumnPosition {
                    direction: "after".into(),
                    anchor: "namespace".into(),
                },
            }],
        };
        let cfg = MappingConfig {
            enabled: true,
            sources: vec![ms],
        };
        let indices = load_all(&cfg).unwrap();
        let mut row = CollectorRow {
            ts: chrono::Utc::now().with_timezone(&Shanghai),
            ip: "1.1.1.1".into(),
            card_id: "0".into(),
            fields: Default::default(),
            strings: HashMap::from([("namespace".into(), Some("default".into()))]),
            source: "s1".into(),
        };
        let warnings = join_row(&mut row, &indices, &cfg.sources);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("loc_int"));
        assert_eq!(row.strings.get("loc_int").unwrap(), &None);
    }

    /// 守护 F5：is_numeric_type 须覆盖 MySQL 全部整数类型（bigint/tinyint/smallint/
    /// mediumint），与 sql_gen::col_type_to_sql 保持一致。否则 bigint 列的脏值不会被
    /// 列级解析拦截（→ 整行 INSERT 失败被丢），与 int 列的"列级 NULL+warning"语义分裂。
    #[test]
    fn is_numeric_type_covers_all_mysql_integers() {
        // 整数族（F5 重点）。
        assert!(is_numeric_type("int"));
        assert!(is_numeric_type("bigint"));
        assert!(is_numeric_type("tinyint"));
        assert!(is_numeric_type("smallint"));
        assert!(is_numeric_type("mediumint"));
        // 浮点/定点（原有覆盖，不应回归）。
        assert!(is_numeric_type("double"));
        assert!(is_numeric_type("float"));
        assert!(is_numeric_type("decimal(10,2)"));
        assert!(is_numeric_type("INT"));
        assert!(is_numeric_type(" BIGINT ")); // 含空白与大小写
        // 非数值类型不应误判。
        assert!(!is_numeric_type("varchar(255)"));
        assert!(!is_numeric_type("text"));
        assert!(!is_numeric_type("datetime"));
    }

    /// 守护：resolve_row_key 须从结构性字段取值（ip/card_id/source），
    /// 也能从 strings 取标签字段（namespace）。这是 src_key: "ip" 不静默 NULL 的关键。
    #[test]
    fn resolve_row_key_finds_structural_fields() {
        let row = CollectorRow {
            ts: chrono::Utc::now().with_timezone(&chrono_tz::Asia::Shanghai),
            ip: "10.0.0.1".into(),
            card_id: "3".into(),
            fields: Default::default(),
            strings: HashMap::from([("namespace".into(), Some("default".into()))]),
            source: "gpu".into(),
        };
        // 结构性字段
        assert_eq!(resolve_row_key(&row, "ip"), Some("10.0.0.1".into()));
        assert_eq!(resolve_row_key(&row, "card_id"), Some("3".into()));
        assert_eq!(resolve_row_key(&row, "source"), Some("gpu".into()));
        // strings 中的标签字段
        assert_eq!(resolve_row_key(&row, "namespace"), Some("default".into()));
        // 不存在的 key → None
        assert_eq!(resolve_row_key(&row, "nonexistent"), None);
    }
}

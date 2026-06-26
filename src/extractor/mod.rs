//! # extractor 模块
//!
//! 提取对齐层（核心业务逻辑）。介于 source 与 sink 之间，
//! 是整个程序里唯一持有"指标如何变成一行"业务规则的层。
//!
//! ## 流程（collect_source）
//! 1. 查主指标 → 枚举所有卡片序列（决定每轮行数）。
//! 2. 批量查询该 source 用到的所有 metric（fields 的来源 + expressions 的变量），
//!    缓存为 `metric名 -> Vec<MetricSample>`。
//! 3. 对每个主指标序列（每张卡）：
//!    - 用 `card_label` 取卡号，构造行骨架。
//!    - `from: metric` 字段：按对齐键查该卡对应 metric 的值。
//!    - `from: label` 字段：按对齐键查该卡对应 metric 的标签集，再取指定标签。
//!    - expressions：构建 `变量名->值`（取该卡对应 metric 值），用 expr 求值。
//!    - host_fields：整主机单值，复制到该主机每张卡的行。
//!
//! ## 可测试性
//! [`collect_source`] 对查询抽象为泛型 `Q: SourceQuerier`，测试用 mock 替换真实
//! [`PrometheusClient`](crate::source::PrometheusClient)，无需联网。

mod align;

use crate::config::SourceConfig;
use crate::expr;
use crate::models::{MetricSample, Row};
use crate::source::PrometheusClient;
use async_trait::async_trait;
use chrono::Utc;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

/// 查询接口抽象，便于测试用 mock 替换真实 PrometheusClient。
///
/// 与 [`PrometheusClient::query`] 同名同签名，故真实实现可直接转发。
#[async_trait]
pub trait SourceQuerier {
    /// 查询一个 metric（或完整 PromQL），返回其瞬时向量样本列表。
    async fn query(&self, metric: &str) -> Result<Vec<MetricSample>, crate::source::SourceError>;
}

/// 真实实现：直接转发到 [`PrometheusClient::query`]。
#[async_trait]
impl SourceQuerier for PrometheusClient {
    async fn query(&self, metric: &str) -> Result<Vec<MetricSample>, crate::source::SourceError> {
        PrometheusClient::query(self, metric).await
    }
}

/// 采集一个 source 的所有行（每张卡一行）。失败返回 Err，由 scheduler 隔离。
///
/// `Q` 为查询器类型（真实 client 或测试 mock），要求 `Sync`（async 跨 await 复用引用）。
pub async fn collect_source<Q: SourceQuerier + Sync>(
    cfg: &SourceConfig,
    client: &Q,
    tz: chrono_tz::Tz,
) -> Result<Vec<Row>, crate::source::SourceError> {
    // 1. 主指标 → 行骨架来源（枚举所有卡片序列，决定行数）。
    let primary_samples = client.query(&cfg.primary.metric).await?;
    let card_label = &cfg.primary.card_label;

    // 主指标为空（exporter 宕机/无卡）：提前返回，避免后续无谓的查询。
    if primary_samples.is_empty() {
        return Ok(Vec::new());
    }

    // 2. 批量查询用到的所有 metric（fields 来源 + expressions 变量），缓存。
    let needed_metrics = collect_needed_metrics(cfg);
    let mut metric_cache: HashMap<String, Vec<MetricSample>> = HashMap::new();
    for m in &needed_metrics {
        let samples = client.query(m).await?;
        metric_cache.insert(m.clone(), samples);
    }

    let align_labels = vec![card_label.clone()];

    // 预建索引：每个 metric 的"对齐键->值"与"对齐键->标签集"只算一次，
    // 避免在逐卡循环里反复重建 HashMap（原本是 O(卡数×字段数×样本数)）。
    let value_idx: HashMap<String, HashMap<String, f64>> = metric_cache
        .iter()
        .map(|(m, s)| (m.clone(), align::index_by_key(s, &align_labels)))
        .collect();
    let label_idx: HashMap<String, HashMap<String, HashMap<String, String>>> = metric_cache
        .iter()
        .map(|(m, s)| (m.clone(), align::index_labels_by_key(s, &align_labels)))
        .collect();

    // 可观测性（R-minor）：主指标枚举的卡集 与 某所需 metric 实际返回的卡集，
    // 若二者**零交集**，则该 metric 的逐卡对齐查找会全部落空 → 该字段对所有卡永远
    // 写 NULL，且无任何提示。这是"静默数据丢失"的典型（如 exporter 对次指标用 "00"
    // 零填充而主指标用 "0"，或 metric 名拼错导致查到无关序列）。逐 metric 比对键集，
    // 零交集时记 WARN 让运维可定位。键集非空但部分缺失属正常（某张卡缺温度），不告警。
    let primary_keys: HashSet<String> = primary_samples
        .iter()
        .map(|s| align::make_key(&s.labels, &align_labels))
        .collect();
    for (m, keys) in &value_idx {
        if keys.is_empty() {
            continue;
        }
        let any_overlap = keys.keys().any(|k| primary_keys.contains(k));
        if !any_overlap {
            tracing::warn!(
                metric = %m,
                "该 metric 的卡片键集与主指标零交集，其字段将全部写 NULL（检查 card_label 值是否一致，如 \"0\" vs \"00\"，或 metric 名是否拼错）"
            );
        }
    }

    // 3. 主机级字段（整主机单值）：每个 host_field 查一次，取首条序列的值。
    //    单个 host_field 查询失败只让该字段本轮写 NULL（WARN），不连累整个 source
    //    （与逐卡字段缺失的软失败语义一致；否则抖动的 host_cpu 查询会丢掉全部卡数据）。
    let mut host_values: HashMap<String, f64> = HashMap::new();
    for hf in &cfg.host_fields {
        match client.query(&hf.expr).await {
            Ok(samples) => {
                if let Some(first) = samples.first() {
                    // 非有限值（NaN/±Inf）规范化为缺席 → 后续填 NULL（见 finite_or_none 文档）。
                    if first.value.is_finite() {
                        host_values.insert(hf.name.clone(), first.value);
                    }
                }
                // 无结果或非有限值 → 该字段缺席（后续填 NULL）。
            }
            Err(e) => {
                tracing::warn!(
                    field = %hf.name,
                    error = %e.0,
                    "host_field 查询失败，该字段本轮写 NULL"
                );
            }
        }
    }

    let now = Utc::now().with_timezone(&tz);

    // 按 card_id 去重：同一张卡只保留首个序列的行。
    // Prometheus 主指标可能返回多个序列共享同一 card_id（如不同 namespace/pod 的
    // DCGM_FI_DEV_GPU_UTIL），若每个序列都生成一行，同一张卡会重复写入多行。
    // 语义上"取一次当前瞬时完整值"：每个 (ip, card_id) 只写一行。
    let mut seen_cards: HashSet<String> = HashSet::new();
    let mut rows = Vec::with_capacity(primary_samples.len());
    for ps in &primary_samples {
        let card_id = ps.labels.get(card_label).cloned().unwrap_or_default();
        if !seen_cards.insert(card_id.clone()) {
            continue; // 同一张卡的后续序列跳过
        }
        let key = align::make_key(&ps.labels, &align_labels);

        let mut row = Row {
            ts: now,
            ip: cfg.ip.clone(),
            card_id: card_id.clone(),
            fields: HashMap::new(),
            strings: HashMap::new(),
            source: cfg.name.clone(),
        };

        // 各字段对齐（用预建索引，O(1) 查找）
        for fc in &cfg.fields {
            match fc.from.as_str() {
                "metric" => {
                    let v = value_idx
                        .get(&fc.metric)
                        .and_then(|m| m.get(&key))
                        .copied()
                        .and_then(finite_or_none);
                    row.fields.insert(fc.name.clone(), v);
                }
                "label" => {
                    let label = fc.label.as_deref().unwrap_or("");
                    let v = label_idx
                        .get(&fc.metric)
                        .and_then(|m| m.get(&key))
                        .and_then(|m| m.get(label))
                        .cloned();
                    row.strings.insert(fc.name.clone(), v);
                }
                _ => {
                    // 未知 from 值：跳过（配置校验已限定，这里防御）。
                }
            }
        }

        // expressions：构建变量值表（变量名=metric 名，用预建索引取该卡的值），求值。
        for ec in &cfg.expressions {
            let vars = build_vars_for_expr(&ec.expr, &key, &value_idx);
            // 用 expr::eval 一步求值，避免跨模块命名私有的 Ast。
            let val = expr::eval(&ec.expr, &vars).and_then(finite_or_none);
            row.fields.insert(ec.name.clone(), val);
        }

        // 主机级字段复制到该行（按 ip，整主机一个值，每张卡相同）。
        for (name, v) in &host_values {
            row.fields.insert(name.clone(), Some(*v));
        }

        rows.push(row);
    }
    Ok(rows)
}

/// 提取表达式中的变量名（metric 名）。
///
/// 变量名模式 `[A-Za-z_][A-Za-z0-9_]*`，过滤掉纯数字（避免把数值字面量当变量）。
/// 用正则而非遍历 expr 的 AST（AST 是私有的，且这里只需变量名列表）。
fn extract_var_names(expr_str: &str) -> Vec<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"[A-Za-z_][A-Za-z0-9_]*").unwrap());
    re.find_iter(expr_str)
        .map(|m| m.as_str().to_string())
        .filter(|s| s.parse::<f64>().is_err()) // 排除纯数字
        .collect()
}

/// 收集该 source 需要查询的所有 metric 名（fields 来源 + expressions 变量）。
fn collect_needed_metrics(cfg: &SourceConfig) -> Vec<String> {
    let mut set: HashSet<String> = HashSet::new();
    for fc in &cfg.fields {
        set.insert(fc.metric.clone());
    }
    for ec in &cfg.expressions {
        for v in extract_var_names(&ec.expr) {
            set.insert(v);
        }
    }
    set.into_iter().collect()
}

/// 为表达式构建变量值表：`变量名(metric) -> 该卡的值`。
///
/// 直接查预建好的 `value_idx`（对齐键->值），避免重复建索引。
fn build_vars_for_expr(
    expr_str: &str,
    key: &str,
    value_idx: &HashMap<String, HashMap<String, f64>>,
) -> HashMap<String, f64> {
    let mut vars = HashMap::new();
    for var in extract_var_names(expr_str) {
        if let Some(v) = value_idx.get(&var).and_then(|m| m.get(key)) {
            vars.insert(var, *v);
        }
    }
    vars
}

/// 把非有限浮点（NaN / +Inf / -Inf）规范化为 `None`。
///
/// Prometheus exporter 在卡空闲/异常时确实会发出 `"NaN"`（如除零派生指标）、
/// `"+Inf"`/`"-Inf"`（如空闲卡的某些比率），而 Rust 的 `f64::parse` **成功接受**
/// 这些字符串（与解析失败不同）。这些值一旦流入 `Row.fields` 再被 sink 绑定到
/// MySQL 的 DOUBLE 列，会触发两类静默问题：
///
/// - MySQL 不支持 NaN/Inf 表示：非严格 `sql_mode` 下存成截断/零值（**数据失真**），
///   严格模式下整行 INSERT 失败（**本轮该源全部丢数**）。
/// - sqlx-mysql 0.7 的 `Encode<f64>` 实现是 `buf.extend(&self.to_le_bytes())`，
///   **无 `is_finite()` 守卫**，原样写入 IEEE-754 位模式，问题不会被提前拦下。
///
/// 与缺字段写 NULL 的"软失败"语义一致：单张卡某指标无效只让该字段写 NULL，
/// 不污染整行、不丢失本轮其它字段。返回 `None` 后 sink 绑定 `Option<f64> = None`
/// → 落库为 NULL。
fn finite_or_none(v: f64) -> Option<f64> {
    if v.is_finite() {
        Some(v)
    } else {
        tracing::warn!(
            value = v,
            "采集值非有限(NaN/Inf)，该字段写 NULL 而非落入 MySQL DOUBLE 列"
        );
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_metric_vars() {
        let vars = extract_var_names(
            "DCGM_FI_DEV_FB_USED / (DCGM_FI_DEV_FB_USED + DCGM_FI_DEV_FB_FREE)",
        );
        assert!(vars.contains(&"DCGM_FI_DEV_FB_USED".to_string()));
        assert!(vars.contains(&"DCGM_FI_DEV_FB_FREE".to_string()));
    }

    #[test]
    fn filters_out_numbers() {
        let vars = extract_var_names("100 - A / B");
        assert!(!vars.iter().any(|v| v == "100"));
        assert!(vars.contains(&"A".to_string()));
        assert!(vars.contains(&"B".to_string()));
    }

    /// 守护 R11：finite_or_none 把非有限值（NaN/±Inf）转 None（→ NULL），
    /// 有限值原样保留。即使 source 层已拦截 NaN/Inf，表达式运算仍可能产生
    /// 非有限结果（如 inf - inf = NaN），故此处仍需兜底。
    #[test]
    fn finite_or_none_normalizes_non_finite() {
        assert_eq!(finite_or_none(0.0), Some(0.0));
        assert_eq!(finite_or_none(-1.5), Some(-1.5));
        assert_eq!(finite_or_none(1e308), Some(1e308)); // 仍有限
        assert_eq!(finite_or_none(f64::NAN), None);
        assert_eq!(finite_or_none(f64::INFINITY), None);
        assert_eq!(finite_or_none(f64::NEG_INFINITY), None);
    }
}

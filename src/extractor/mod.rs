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

    // 3. 主机级字段（整主机单值）：每个 host_field 查一次，取首条序列的值。
    //    单个 host_field 查询失败只让该字段本轮写 NULL（WARN），不连累整个 source
    //    （与逐卡字段缺失的软失败语义一致；否则抖动的 host_cpu 查询会丢掉全部卡数据）。
    let mut host_values: HashMap<String, f64> = HashMap::new();
    for hf in &cfg.host_fields {
        match client.query(&hf.expr).await {
            Ok(samples) => {
                if let Some(first) = samples.first() {
                    host_values.insert(hf.name.clone(), first.value);
                }
                // 无结果则该字段缺席（后续填 NULL）。
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

    let mut rows = Vec::with_capacity(primary_samples.len());
    for ps in &primary_samples {
        let card_id = ps.labels.get(card_label).cloned().unwrap_or_default();
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
                    let v = value_idx.get(&fc.metric).and_then(|m| m.get(&key)).copied();
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
            let val = expr::eval(&ec.expr, &vars);
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
}

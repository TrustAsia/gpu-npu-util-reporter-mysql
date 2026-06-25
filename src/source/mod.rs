//! # source 模块
//!
//! 数据源层（纯 I/O 边界）。只负责"查 Prometheus"，不知道业务含义。
//! 提供 [`PrometheusClient`] 查询瞬时向量，以及 [`parse_vector`] 把 JSON
//! 响应解析成 [`MetricSample`](crate::models::MetricSample) 列表。
//!
//! ## 设计
//! - 业务逻辑全在 extractor 层；本模块只做"发请求 + 解析响应"。
//! - 可单独替换（换库 / 换协议）而不影响上层：只要同样返回
//!   `Vec<MetricSample>` 即可。
//! - 解析逻辑 [`parse_vector`] 是纯函数，不联网，便于单元测试。
//!
//! ## 解析的 Prometheus 响应格式
//! `/api/v1/query` 成功响应形如：
//! ```json
//! { "status": "success", "data": { "resultType": "vector", "result": [
//!     { "metric": { "__name__": "M", "gpu": "0", ... }, "value": [1234, "55"] },
//!     ...
//! ] } }
//! ```
//! 本模块提取 `result[]` 中每项的 `metric`（标签集）与 `value[1]`（数值）。

use crate::models::MetricSample;
use std::collections::HashMap;
use std::time::Duration;

/// Prometheus HTTP 客户端。封装 reqwest，带查询超时。
///
/// 一个实例对应一个 Prometheus 服务器（一个 source）。复用底层连接池。
pub struct PrometheusClient {
    client: reqwest::Client,
    base_url: String,
}

/// 查询或解析错误（携带可读描述）。
#[derive(Debug)]
pub struct SourceError(pub String);

impl PrometheusClient {
    /// 创建客户端。`base_url` 为 Prometheus 根地址（如 `http://10.0.0.1:9400`），
    /// 末尾的 `/` 会被去掉。`timeout` 为单次查询超时秒数。
    pub fn new(base_url: &str, timeout: u64) -> Result<Self, SourceError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()
            .map_err(|e| SourceError(format!("构建 HTTP 客户端失败: {}", e)))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    /// 查询瞬时向量。
    ///
    /// `metric` 可为完整 PromQL（用于 host_fields 这类算好的单值）或纯指标名
    /// （用于 fields 这类逐卡取值）。返回该查询的所有序列样本。
    ///
    /// 失败（网络错误、非 2xx、响应不可解析）统一返回 [`SourceError`]。
    pub async fn query(&self, metric: &str) -> Result<Vec<MetricSample>, SourceError> {
        let url = format!("{}/api/v1/query", self.base_url);
        let resp = self
            .client
            .post(&url)
            .form(&[("query", metric)])
            .send()
            .await
            .map_err(|e| SourceError(format!("查询 Prometheus 失败: {}", e)))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| SourceError(format!("读取响应失败: {}", e)))?;
        if !status.is_success() {
            return Err(SourceError(format!("Prometheus 返回 {}: {}", status, text)));
        }
        parse_vector(&text)
    }
}

/// 解析 Prometheus `/api/v1/query` 响应为 [`MetricSample`] 列表。
///
/// 提取 `data.result[]` 中每项的 `metric`（标签集）与 `value[1]`（数值）。
/// 纯函数，不联网，便于用 fixture 单元测试。
///
/// # 错误
/// - JSON 解析失败。
/// - 缺少 `data.result` 或其不是数组。
/// - 某项缺少 `metric`、缺少 `value`、或 `value[1]` 不是合法浮点字符串。
pub fn parse_vector(body: &str) -> Result<Vec<MetricSample>, SourceError> {
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| SourceError(format!("JSON 解析失败: {}", e)))?;
    let result = v
        .get("data")
        .and_then(|d| d.get("result"))
        .ok_or_else(|| SourceError("响应缺少 data.result".into()))?;
    let arr = result
        .as_array()
        .ok_or_else(|| SourceError("data.result 不是数组".into()))?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        // metric：标签集，跳过非字符串值（理论上有，健壮处理）
        let metric = item
            .get("metric")
            .and_then(|m| m.as_object())
            .ok_or_else(|| SourceError("缺少 metric".into()))?;
        let mut labels: HashMap<String, String> = HashMap::new();
        for (k, val) in metric {
            if let Some(s) = val.as_str() {
                labels.insert(k.clone(), s.to_string());
            }
        }
        // value: [timestamp, "数值字符串"]，取第二元素解析为 f64。
        // 拒绝 NaN / ±Inf：Rust 的 f64::parse 会**成功**接受 "NaN"/"+Inf"/"-Inf"
        // （Prometheus exporter 对空闲/异常卡确会发出这些），但 MySQL 的 DOUBLE 列
        // 无法表示它们（非严格模式存成截断值/0 = 数据失真；严格模式整行 INSERT 失败）。
        // 解析失败或非有限的样本**跳过**（而非整响应报错）：单条坏样本不应连累
        // 同批其它正常样本被丢弃，与 extractor 缺字段写 NULL 的软失败语义一致。
        // extractor 层另有 finite_or_none 兜底表达式运算可能产生的非有限结果。
        let Some(value) = item
            .get("value")
            .and_then(|x| x.as_array())
            .and_then(|a| a.get(1))
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite())
        else {
            continue;
        };
        out.push(MetricSample { labels, value });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_two_samples() {
        let body = include_str!("../../tests/fixtures/prom_gpu_util.json");
        let samples = parse_vector(body).unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels.get("gpu").unwrap(), "0");
        assert_eq!(samples[0].labels.get("namespace").unwrap(), "default");
        assert_eq!(samples[0].value, 55.0);
        assert_eq!(samples[1].value, 77.0);
    }

    #[test]
    fn rejects_missing_data_result() {
        let body = r#"{"status":"success"}"#;
        assert!(parse_vector(body).is_err());
    }

    #[test]
    fn skips_unparseable_value_sample() {
        // 无法解析为 f64 的 value：该样本被跳过（而非整响应报错），
        // 与跳过 NaN/Inf 的语义一致——单条坏样本不连累同批正常样本。
        let body = r#"{"data":{"result":[{"metric":{},"value":[1,"NaN-xxx"]}]}}"#;
        let samples = parse_vector(body).unwrap();
        assert!(samples.is_empty());
    }

    /// 守护 R11：裸 "NaN" 会被 Rust 的 f64::parse **成功**接受（与 "NaN-xxx" 不同），
    /// 但 MySQL DOUBLE 无法表示。解析边界须将其跳过，保证进程序的值恒有限。
    /// 同理覆盖 "+Inf"/"-Inf"/"Infinity"。该样本被丢弃，但同批正常样本保留。
    #[test]
    fn skips_nan_and_inf_samples_keeps_finite() {
        let body = r#"{"data":{"result":[
            {"metric":{"gpu":"0"},"value":[1,"NaN"]},
            {"metric":{"gpu":"1"},"value":[1,"+Inf"]},
            {"metric":{"gpu":"2"},"value":[1,"-Infinity"]},
            {"metric":{"gpu":"3"},"value":[1,"42.0"]}
        ]}}"#;
        let samples = parse_vector(body).unwrap();
        // 仅卡3（有限值）保留；NaN/Inf 三条被跳过。
        assert_eq!(samples.len(), 1, "NaN/Inf 样本应被跳过，仅保留有限值");
        assert_eq!(samples[0].labels.get("gpu").unwrap(), "3");
        assert_eq!(samples[0].value, 42.0);
    }

    #[test]
    fn parses_empty_result() {
        // 合法的空结果：result 为空数组，应返回空 Vec 而非报错。
        let body = r#"{"data":{"resultType":"vector","result":[]}}"#;
        let samples = parse_vector(body).unwrap();
        assert!(samples.is_empty());
    }

    #[test]
    fn skips_non_string_label_values() {
        // 健壮性：理论上 metric 值都是字符串，但若出现非字符串值应跳过而非崩溃。
        let body = r#"{"data":{"result":[{"metric":{"gpu":"0","bad":123},"value":[1,"1"]}]}}"#;
        let samples = parse_vector(body).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].labels.get("gpu").unwrap(), "0");
        assert!(!samples[0].labels.contains_key("bad"));
    }
}

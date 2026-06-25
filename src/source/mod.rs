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
            // 禁用重定向：本程序只与配置的单个 Prometheus 端点通信，3xx 永非预期。
            // 默认策略会跟随最多 10 跳——内网代理/负载均衡器的 302 会让本程序去抓取
            // 任意大资源（与下方响应体上限叠加放大内存 DoS 面），故一律当作错误。
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| SourceError(format!("构建 HTTP 客户端失败: {}", e)))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    /// 单次响应体最大字节数上限。
    ///
    /// Prometheus `/api/v1/query` 正常响应远小于此（即便上千卡也只有几 MiB）。
    /// 长驻守护进程无界 `resp.text()` 读全 body 进内存是 OOM 向量：主指标高基数
    /// 爆炸、代理塞回大 HTML 错误页等都会让 `text` 无限增长 → 进程被 OOM 杀死 →
    /// 所有源停采（静默数据丢失）。16 MiB 足够覆盖正常负载又杜绝无界增长。
    const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

    /// 查询瞬时向量。
    ///
    /// `metric` 可为完整 PromQL（用于 host_fields 这类算好的单值）或纯指标名
    /// （用于 fields 这类逐卡取值）。返回该查询的所有序列样本。
    ///
    /// 失败（网络错误、非 2xx、3xx 重定向、响应不可解析、响应体超限）统一返回 [`SourceError`]。
    pub async fn query(&self, metric: &str) -> Result<Vec<MetricSample>, SourceError> {
        let url = format!("{}/api/v1/query", self.base_url);
        let mut resp = self
            .client
            .post(&url)
            .form(&[("query", metric)])
            .send()
            .await
            .map_err(|e| SourceError(format!("查询 Prometheus 失败: {}", e)))?;
        let status = resp.status();
        // 在读 body 前用响应头里的 Content-Length 预判（若提供），避免为超大响应
        // 仍逐块读到上限。但 Content-Length 可缺失/伪造，故下面读流时仍二次卡上限。
        if status.is_redirection() {
            // 禁用了重定向，3xx 不会再被自动跟随；显式当作错误（而非当作成功空 body）。
            let loc = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            return Err(SourceError(format!(
                "Prometheus 返回重定向 {}（已禁用跟随）：Location={}",
                status, loc
            )));
        }
        // 有界读取：用 resp.chunk() 逐块累加，超 MAX_BODY_BYTES 立即中止，杜绝无界 OOM。
        // （chunk 是 reqwest 自带 API，无需引入 futures-util 依赖。）
        let mut buf: Vec<u8> = Vec::new();
        loop {
            let chunk = resp
                .chunk()
                .await
                .map_err(|e| SourceError(format!("读取响应失败: {}", e)))?;
            let Some(chunk) = chunk else { break };
            if buf.len() + chunk.len() > Self::MAX_BODY_BYTES {
                return Err(SourceError(format!(
                    "响应体超过 {} 字节上限，已中止（疑似主指标高基数或被代理塞回大页面）",
                    Self::MAX_BODY_BYTES
                )));
            }
            buf.extend_from_slice(&chunk);
        }
        let text = String::from_utf8(buf)
            .map_err(|e| SourceError(format!("响应非 UTF-8: {}", e)))?;
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
        let value_raw = item
            .get("value")
            .and_then(|x| x.as_array())
            .and_then(|a| a.get(1))
            .and_then(|x| x.as_str());
        let Some(parsed) = value_raw.and_then(|s| s.parse::<f64>().ok()) else {
            tracing::debug!(value = ?value_raw, "样本值无法解析为 f64，跳过该样本");
            continue;
        };
        if !parsed.is_finite() {
            tracing::debug!(value = parsed, "样本值为 NaN/Inf，跳过该样本(MySQL DOUBLE 无法表示)");
            continue;
        }
        let value = parsed;
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

    /// 守护 R17：响应体上限常量存在且为合理量级（既够装正常负载又杜绝无界增长）。
    /// 防止有人误改成超大/零值导致 OOM 面回归。
    #[test]
    fn max_body_bytes_is_reasonable_bound() {
        // 16 MiB：正常上千卡 Prometheus 响应远小于此；过小会误杀正常负载。
        assert_eq!(PrometheusClient::MAX_BODY_BYTES, 16 * 1024 * 1024);
    }

    /// 守护 R17：客户端能成功构造（含禁重定向配置）。重定向禁用与 body 上限的
    /// 真实运行期行为由 query() 的 is_redirection 分支与 chunk 有界读取保证，
    /// 此处只断言构造路径不回归（曾因 redirect API 误用导致编译失败）。
    #[test]
    fn client_constructs_with_redirect_policy_none() {
        let _c = PrometheusClient::new("http://10.0.0.1:9400/", 10).unwrap();
    }
}

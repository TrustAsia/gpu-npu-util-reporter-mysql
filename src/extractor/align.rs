//! 字段对齐辅助（纯函数）。
//!
//! 把一组 Prometheus 样本按"对齐键"组织成 map，便于在组装每张卡的行时，
//! 用 (ip, card_id) 这种对齐键快速查到该卡对应字段值或标签。
//!
//! 对齐键由若干标签值拼接而成（用 `'\x1f'` 单元分隔符避免与正常值碰撞）。
//! 对 DCGM/NPU 等"单维度卡号"场景，对齐标签通常只有一个（gpu 或 id）。

use crate::models::MetricSample;
use std::collections::HashMap;

/// 把一组样本按"对齐键"组织成 `对齐键 -> 样本值` 的 map。
///
/// `align_labels` 为用于拼对齐键的标签名列表（如 `["gpu"]`）。
/// 同一对齐键出现多次时后者覆盖前者（Prometheus 正常情况下不会）。
pub fn index_by_key(samples: &[MetricSample], align_labels: &[String]) -> HashMap<String, f64> {
    let mut m = HashMap::new();
    for s in samples {
        let key = make_key(&s.labels, align_labels);
        m.insert(key, s.value);
    }
    m
}

/// 把一组样本按"对齐键"组织成 `对齐键 -> (标签名->值)` 的 map。
///
/// 用于 `from: label` 字段：拿到对齐键对应的整组标签后，再取具体某个标签值。
pub fn index_labels_by_key(
    samples: &[MetricSample],
    align_labels: &[String],
) -> HashMap<String, HashMap<String, String>> {
    let mut m = HashMap::new();
    for s in samples {
        let key = make_key(&s.labels, align_labels);
        m.insert(key, s.labels.clone());
    }
    m
}

/// 用 `align_labels` 对应的标签值拼成对齐键（以 `'\x1f'` 分隔）。
///
/// 缺失的标签按空串处理。用单元分隔符避免与正常标签值（可能含逗号等）碰撞。
pub fn make_key(labels: &HashMap<String, String>, align_labels: &[String]) -> String {
    align_labels
        .iter()
        .map(|l| labels.get(l).cloned().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\x1f")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(gpu: &str, v: f64) -> MetricSample {
        MetricSample {
            labels: HashMap::from([("gpu".into(), gpu.into())]),
            value: v,
        }
    }

    #[test]
    fn index_by_gpu() {
        let samples = vec![sample("0", 10.0), sample("1", 20.0)];
        let idx = index_by_key(&samples, &["gpu".into()]);
        assert_eq!(idx.get("0"), Some(&10.0));
        assert_eq!(idx.get("1"), Some(&20.0));
    }

    #[test]
    fn missing_card_absent() {
        let samples = vec![sample("0", 10.0)];
        let idx = index_by_key(&samples, &["gpu".into()]);
        assert!(!idx.contains_key("9"));
    }

    #[test]
    fn label_lookup_by_key() {
        let s = MetricSample {
            labels: HashMap::from([("gpu".into(), "0".into()), ("namespace".into(), "default".into())]),
            value: 1.0,
        };
        let idx = index_labels_by_key(&[s], &["gpu".into()]);
        assert_eq!(idx.get("0").unwrap().get("namespace").unwrap(), "default");
    }
}

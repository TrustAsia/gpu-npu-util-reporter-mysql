//! # models 模块
//!
//! 全程序共享的数据结构。无业务逻辑，无 I/O。
//! 被几乎所有其它模块依赖，故置于依赖图最底层，必须最先实现。
//!
//! 本模块对外暴露三个核心类型：
//! - [`MetricSample`]：从 Prometheus 返回的一条瞬时向量样本（标签 + 数值）。
//! - [`Row`]：采集并按卡片维度对齐后、待写入 MySQL 的一行业务记录。
//! - [`ColumnDef`]：建表 SQL 中的一列定义（供 sql_gen 模块生成建表语句）。

use std::collections::HashMap;

/// 从 Prometheus 查询返回的一条瞬时向量样本。
///
/// 对应 Prometheus `/api/v1/query` 响应中 `data.result[]` 的一项：
/// 即一个时间序列在某个时刻的值，连同其全部标签。
///
/// # 字段
/// - `labels`: 该序列的标签集合（如 `gpu="0"`, `namespace="default"`）。
///   key 为标签名，value 为标签值（均为字符串）。
/// - `value`: 该序列当前值（Prometheus 的样本值，浮点）。
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSample {
    pub labels: HashMap<String, String>,
    pub value: f64,
}

/// 一行采集结果中对齐后的单个数值字段值。
///
/// `None` 表示该字段本轮未取到（例如某张卡缺少温度指标），
/// 落库时写 NULL，不污染整行其它字段。
pub type FieldValue = Option<f64>;

/// 组装完成、待写入 MySQL 的一行。
///
/// 设计上把列按"承载类型"分成两类，便于写入时分别绑定到 SQL：
/// - 数值列放在 `fields`（按列名索引，如 `gpu_util`/`temp`/`power`/`host_*`/`mem_util`）。
/// - 字符串/维度列放在 `strings`（如 `namespace`、`pod`，以及资产表 join 来的 varchar 列）。
///
/// 维度列 `ip`、`card_id`、`source` 与时间戳 `ts` 因在每行都固定且常用于对齐，
/// 单独作为具名字段，而不混入 map，避免遗漏与键名拼写错误。
///
/// 注意：未派生 `Default`，因为 [`chrono::DateTime`] 仅对 `Utc`/`Local`/`FixedOffset`
/// 实现 `Default`，不对通用时区 `chrono_tz::Tz` 实现。各调用方均显式构造 `Row`。
#[derive(Debug, Clone)]
pub struct Row {
    /// 采集时间（已转换为配置时区）。落库时取其 naive_local 写入 DATETIME(3)。
    pub ts: chrono::DateTime<chrono_tz::Tz>,
    /// 主机 IP（来自 source 配置的 ip 字段）。
    pub ip: String,
    /// GPU/NPU 卡号（来自配置的 card_label，如 DCGM 的 gpu、NPU 的 id）。
    pub card_id: String,
    /// 数值列：gpu_util/temp/power/host_cpu/host_mem/host_fds/mem_util 等。
    pub fields: HashMap<String, FieldValue>,
    /// 字符串列：namespace/pod 以及资产表 mapping 的 varchar 列。
    pub strings: HashMap<String, Option<String>>,
    /// 数据源名（配置中的 source.name），用于区分不同 Prometheus 源的行。
    pub source: String,
}

/// 建表 SQL 中的一列定义（供 sql_gen 模块使用）。
///
/// 由配置（固定列基线 + mapping 列）推导而来，最终拼进 `CREATE TABLE` 语句。
///
/// # 字段
/// - `name`: 列名。
/// - `sql_type`: SQL 类型声明（如 `"DOUBLE"`、`"VARCHAR(255)"`，不含 NULL/修饰）。
/// - `nullable`: 是否允许 NULL（决定 `NULL` / `NOT NULL`）。
/// - `comment`: 列备注（写入 SQL 的 `COMMENT '...'`）。
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub sql_type: String,
    pub nullable: bool,
    pub comment: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono_tz::Asia::Shanghai;

    #[test]
    fn row_can_be_constructed() {
        let now = chrono::Utc::now().with_timezone(&Shanghai);
        let row = Row {
            ts: now,
            ip: "10.0.0.1".into(),
            card_id: "0".into(),
            fields: HashMap::from([("gpu_util".into(), Some(50.0))]),
            strings: HashMap::from([("namespace".into(), Some("default".into()))]),
            source: "gpu-a".into(),
        };
        assert_eq!(row.ip, "10.0.0.1");
        assert_eq!(row.fields.get("gpu_util"), Some(&Some(50.0)));
    }

    #[test]
    fn metric_sample_labels() {
        let s = MetricSample {
            labels: HashMap::from([("gpu".into(), "0".into())]),
            value: 99.0,
        };
        assert_eq!(s.labels.get("gpu").unwrap(), "0");
        assert_eq!(s.value, 99.0);
    }
}

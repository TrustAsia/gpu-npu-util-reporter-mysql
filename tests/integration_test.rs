//! 端到端集成测试：用 mock [`SourceQuerier`] 验证"采集→对齐→表达式→join"的整条链路，
//! 不连真实 Prometheus / MySQL。
//!
//! 这些测试放在 `tests/` 目录，通过库 crate（`gpu_npu_util_reporter`）访问内部模块，
//! 故要求各业务模块以 `pub` 暴露（见 `src/lib.rs`）。

use async_trait::async_trait;
use gpu_npu_util_reporter::config::{FieldConfig, PrimaryConfig, SourceConfig};
use gpu_npu_util_reporter::extractor::{collect_source, SourceQuerier};
use gpu_npu_util_reporter::mapping::{join_row, AssetIndex};
use gpu_npu_util_reporter::models::MetricSample;
use gpu_npu_util_reporter::source::SourceError;
use std::collections::HashMap;

/// mock 查询器：按预设的 metric 名 → 样本列表返回。未预设的 metric 返回空（缺字段 → NULL）。
struct MockQuerier {
    responses: HashMap<String, Vec<MetricSample>>,
}

#[async_trait]
impl SourceQuerier for MockQuerier {
    async fn query(&self, metric: &str) -> Result<Vec<MetricSample>, SourceError> {
        Ok(self.responses.get(metric).cloned().unwrap_or_default())
    }
}

/// 构造一个带 gpu 卡号 + namespace 标签的样本。
fn sample(gpu: &str, val: f64, ns: &str) -> MetricSample {
    MetricSample {
        labels: HashMap::from([
            ("gpu".into(), gpu.into()),
            ("namespace".into(), ns.into()),
            ("pod".into(), format!("pod-{}", gpu)),
        ]),
        value: val,
    }
}

/// 端到端：两张卡，温度指标只对卡0有数据 → 卡1 温度应为 NULL。
#[tokio::test]
async fn collect_two_cards_with_partial_field() {
    let responses = HashMap::from([
        // 主指标：两张卡 → 决定两行。
        ("m_primary".to_string(), vec![sample("0", 50.0, "default"), sample("1", 75.0, "prod")]),
        // 温度：只有卡0，卡1 缺失 → None。
        ("m_temp".to_string(), vec![sample("0", 40.0, "default")]),
    ]);
    let q = MockQuerier { responses };
    let src = SourceConfig {
        name: "test".into(),
        ip: "10.0.0.1".into(),
        url: "http://x".into(),
        timeout: 10,
        interval: None,
        primary: PrimaryConfig {
            metric: "m_primary".into(),
            card_label: "gpu".into(),
        },
        fields: vec![
            FieldConfig {
                name: "gpu_util".into(),
                from: "metric".into(),
                metric: "m_primary".into(),
                label: None,
            },
            FieldConfig {
                name: "temp".into(),
                from: "metric".into(),
                metric: "m_temp".into(),
                label: None,
            },
            // label 来源：取 namespace 标签
            FieldConfig {
                name: "namespace".into(),
                from: "label".into(),
                metric: "m_primary".into(),
                label: Some("namespace".into()),
            },
        ],
        expressions: vec![],
        host_fields: vec![],
    };
    let tz = chrono_tz::Asia::Shanghai;
    let mut rows = collect_source(&src, &q, tz).await.unwrap();

    // 行数 = 主指标序列数（两张卡）。
    assert_eq!(rows.len(), 2);

    // 按 card_id 排序使断言稳定（HashMap 顺序不定）。
    rows.sort_by(|a, b| a.card_id.cmp(&b.card_id));

    // 卡0：gpu_util=50, temp=40, namespace=default。
    assert_eq!(rows[0].card_id, "0");
    assert_eq!(rows[0].fields.get("gpu_util"), Some(&Some(50.0)));
    assert_eq!(rows[0].fields.get("temp"), Some(&Some(40.0)));
    assert_eq!(rows[0].strings.get("namespace").unwrap().as_deref(), Some("default"));
    assert_eq!(rows[0].ip, "10.0.0.1");
    assert_eq!(rows[0].source, "test");

    // 卡1：gpu_util=75, temp=None（缺失字段填 NULL）, namespace=prod。
    assert_eq!(rows[1].card_id, "1");
    assert_eq!(rows[1].fields.get("gpu_util"), Some(&Some(75.0)));
    assert_eq!(rows[1].fields.get("temp"), Some(&None));
    assert_eq!(rows[1].strings.get("namespace").unwrap().as_deref(), Some("prod"));
}

/// 端到端：表达式计算（显存占用率 = used / (used+free)）。
/// 卡0: used=4 free=4 → 0.5；卡1: used=6 free=0 → 1.0；卡缺 used → None。
#[tokio::test]
async fn expression_evaluates_per_card() {
    let responses = HashMap::from([
        ("m_primary".to_string(), vec![sample("0", 1.0, "ns"), sample("1", 1.0, "ns")]),
        ("FB_USED".to_string(), vec![sample("0", 4.0, "ns"), sample("1", 6.0, "ns")]),
        ("FB_FREE".to_string(), vec![sample("0", 4.0, "ns"), sample("1", 0.0, "ns")]),
    ]);
    let q = MockQuerier { responses };
    let src = SourceConfig {
        name: "t".into(),
        ip: "1.1.1.1".into(),
        url: "http://x".into(),
        timeout: 10,
        interval: None,
        primary: PrimaryConfig {
            metric: "m_primary".into(),
            card_label: "gpu".into(),
        },
        fields: vec![],
        expressions: vec![gpu_npu_util_reporter::config::ExprConfig {
            name: "mem_util".into(),
            expr: "FB_USED / (FB_USED + FB_FREE)".into(),
            unit: Some("%".into()),
        }],
        host_fields: vec![],
    };
    let rows = collect_source(&src, &q, chrono_tz::Asia::Shanghai)
        .await
        .unwrap();
    let by_card: HashMap<&str, f64> = rows
        .iter()
        .filter_map(|r| r.fields.get("mem_util").and_then(|v| *v).map(|v| (r.card_id.as_str(), v)))
        .collect();
    assert_eq!(by_card.get("0"), Some(&0.5));
    assert_eq!(by_card.get("1"), Some(&1.0));
}

/// 端到端：mapping join —— 用行内 namespace 查资产索引补 location/owner。
/// 直接构造 AssetIndex（绕过文件加载），验证 join_row 补值与无匹配置 NULL。
#[tokio::test]
async fn mapping_join_after_collect() {
    // 先采集得到带 namespace 的行。
    let responses = HashMap::from([(
        "m_primary".to_string(),
        vec![sample("0", 1.0, "default"), sample("1", 1.0, "zzz-no-match")],
    )]);
    let q = MockQuerier { responses };
    let src = SourceConfig {
        name: "t".into(),
        ip: "1.1.1.1".into(),
        url: "http://x".into(),
        timeout: 10,
        interval: None,
        primary: PrimaryConfig {
            metric: "m_primary".into(),
            card_label: "gpu".into(),
        },
        fields: vec![FieldConfig {
            name: "namespace".into(),
            from: "label".into(),
            metric: "m_primary".into(),
            label: Some("namespace".into()),
        }],
        expressions: vec![],
        host_fields: vec![],
    };
    let mut rows = collect_source(&src, &q, chrono_tz::Asia::Shanghai)
        .await
        .unwrap();

    // 构造一个资产索引：namespace=default → location=机房A。
    // 用 mapping::load_source 从 CSV fixture 加载更真实，但这里直接复用 fixture。
    let ms = gpu_npu_util_reporter::config::MappingSource {
        source_path: "tests/fixtures/assets.csv".into(),
        src_key: "namespace".into(),
        dest_key: "Namespace".into(),
        source_sheet: None,
        columns: vec![
            gpu_npu_util_reporter::config::MappingColumn {
                source_field: "机房位置".into(),
                rename: Some("location".into()),
                col_type: "varchar(255)".into(),
                comment: "机房".into(),
                position: gpu_npu_util_reporter::config::ColumnPosition {
                    direction: "after".into(),
                    anchor: "namespace".into(),
                },
            },
            gpu_npu_util_reporter::config::MappingColumn {
                source_field: "负责人".into(),
                rename: Some("owner".into()),
                col_type: "varchar(64)".into(),
                comment: "负责人".into(),
                position: gpu_npu_util_reporter::config::ColumnPosition {
                    direction: "after".into(),
                    anchor: "namespace".into(),
                },
            },
        ],
    };
    let index: Vec<AssetIndex> = vec![gpu_npu_util_reporter::mapping::load_source(&ms).unwrap()];
    let msrcs = vec![ms];

    rows.sort_by(|a, b| a.card_id.cmp(&b.card_id));
    for row in rows.iter_mut() {
        let warnings = join_row(row, &index, &msrcs);
        assert!(warnings.is_empty(), "不应有 warning: {:?}", warnings);
    }

    // 卡0 namespace=default → location=机房A。
    assert_eq!(rows[0].strings.get("location").unwrap().as_deref(), Some("机房A"));
    assert_eq!(rows[0].strings.get("owner").unwrap().as_deref(), Some("张三"));
    // 卡1 namespace=zzz-no-match → 无匹配置 NULL。
    assert_eq!(rows[1].strings.get("location").unwrap(), &None);
    assert_eq!(rows[1].strings.get("owner").unwrap(), &None);
}

/// 验证 mapping 各资产源用自己的 src_key（issue #3 回归守护）：
/// 构造一个 src_key=ip 的资产源，行内按 ip 关联，确认 join_row 用 ip 而非 namespace。
#[test]
fn mapping_join_uses_per_source_src_key() {
    // 行内：ip=10.0.0.1。资产源声明 src_key=ip。
    let ms = gpu_npu_util_reporter::config::MappingSource {
        source_path: "tests/fixtures/assets.csv".into(),
        src_key: "ip".into(), // 故意用 ip 而非 namespace，验证按源取键
        dest_key: "Namespace".into(),
        source_sheet: None,
        columns: vec![gpu_npu_util_reporter::config::MappingColumn {
            source_field: "机房位置".into(),
            rename: Some("location".into()),
            col_type: "varchar(255)".into(),
            comment: "机房".into(),
            position: gpu_npu_util_reporter::config::ColumnPosition {
                direction: "after".into(),
                anchor: "namespace".into(),
            },
        }],
    };
    let index = vec![gpu_npu_util_reporter::mapping::load_source(&ms).unwrap()];
    let msrcs = vec![ms];

    // 行内同时有 namespace(default) 和 ip；但本源 src_key=ip，故应按 ip 关联。
    // assets.csv 的 dest_key 是 Namespace 列，值不可能是 IP → 必然无匹配 → NULL。
    let mut row = gpu_npu_util_reporter::models::Row {
        ts: chrono::Utc::now().with_timezone(&chrono_tz::Asia::Shanghai),
        ip: "10.0.0.1".into(),
        card_id: "0".into(),
        fields: Default::default(),
        strings: HashMap::from([
            ("namespace".into(), Some("default".into())),
            ("ip".into(), Some("10.0.0.1".into())),
        ]),
        source: "s1".into(),
    };
    let warnings = join_row(&mut row, &index, &msrcs);
    assert!(warnings.is_empty());
    // src_key=ip，而资产表无 IP 值的 Namespace 列 → 无匹配 → location 应为 NULL。
    // 若错误地用 namespace 关联，则会命中 default → location=机房A（本测试即排除该 bug）。
    assert_eq!(
        row.strings.get("location").unwrap(),
        &None,
        "join_row 应按 src_key=ip 关联，而非 namespace"
    );
}

/// 守护：配置示例 config.example.yaml 能完整解析 + 校验（防止文档与代码漂移）。
#[test]
fn example_config_loads_and_validates() {
    let text = gpu_npu_util_reporter::config::EXAMPLE_CONFIG;
    let cfg: gpu_npu_util_reporter::config::Config =
        serde_yaml::from_str(text).expect("config.example.yaml 解析失败");
    gpu_npu_util_reporter::config::validate(&cfg).expect("config.example.yaml 校验失败");
    assert!(!cfg.sources.is_empty(), "示例配置应至少含一个 source");
}

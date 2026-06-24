//! # scheduler 模块
//!
//! 调度层（编排各层）。每个 source 一个 tokio 任务，按自身 interval 循环采集；
//! 另有一个独立的保留期清理任务。单源/单轮失败隔离：任何错误只记日志，
//! 永不向上传播，不影响其他源与其他轮次。
//!
//! ## 失败隔离边界
//! - source 采集失败（Prometheus 不可达）→ 跳过本轮，下一轮重试。
//! - 写入失败（MySQL 抖动）→ 跳过本轮写入，下一轮重试。
//! - 保留期清理失败 → 跳过本次，下次重试。
//!
//! 以上均只记日志，不 abort 任务、不传播给其他任务。
//!
//! ## 可测试性
//! 调度本身涉及 spawn + sleep + tokio 运行时，难以单元测试；
//! 但采集间隔的选择逻辑 [`effective_interval`] 是纯函数，单独覆盖。

use crate::extractor::{collect_source, SourceQuerier};
use crate::mapping::{join_row, AssetIndex};
use crate::sink::Sink;
use std::sync::Arc;
use tokio::time::Duration;

/// 计算某 source 的实际采集间隔：source 自身 interval 优先，缺省取全局 interval。
///
/// 抽成纯函数便于单测（避免为测试构造完整 `Config`）。
pub fn effective_interval(src_interval: Option<u64>, global_interval: u64) -> u64 {
    src_interval.unwrap_or(global_interval)
}

/// 启动所有 source 的采集任务 + 保留期清理任务，返回各任务 `JoinHandle`，
/// 交由 main 统一管理生命周期（优雅退出时 abort）。
///
/// `client_factory` 接收 `(url, timeout)`，每调用一次为对应 source 创建一个
/// 实现了 [`SourceQuerier`] 的客户端。
///
/// `mapping_src_key` 为行内关联键列名（如 `namespace`）；`None` 表示不启用 mapping。
pub fn run<Q>(
    cfg: Arc<crate::config::Config>,
    sink: Arc<Sink>,
    client_factory: impl Fn(&str, u64) -> Q,
    asset_indices: Arc<Vec<AssetIndex>>,
    mapping_sources: Arc<Vec<crate::config::MappingSource>>,
    mapping_src_key: Option<String>,
) -> Vec<tokio::task::JoinHandle<()>>
where
    Q: SourceQuerier + Send + Sync + 'static,
{
    // 时区：配置加载阶段已校验合法，此处的 unwrap_or 仅为防御（不影响正确性）。
    let tz: chrono_tz::Tz = cfg.timezone.parse().unwrap_or(chrono_tz::Asia::Shanghai);

    // mapping 列名：从所有资产索引收集最终列名，供 INSERT 动态拼列。
    // （每个 source 都用同一套 mapping 列；未匹配的列在 join 阶段已置 NULL。）
    let mapping_cols: Vec<String> = asset_indices
        .iter()
        .flat_map(|i| i.column_names())
        .collect();

    let mut handles = Vec::new();

    // —— 保留期清理任务（独立循环，按 retention_interval 周期执行）——
    {
        let sink2 = sink.clone();
        let days = cfg.retention_days;
        let interval = cfg.retention_interval;
        handles.push(tokio::spawn(async move {
            loop {
                match sink2.run_retention(days).await {
                    Ok(n) if n > 0 => {
                        tracing::info!(target: "retention", deleted = n, "保留期清理完成");
                    }
                    Ok(_) => {}
                    Err(e) => tracing::error!(target: "retention", "保留期清理失败: {}", e.0),
                }
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }
        }));
    }

    // —— 每个 source 一个采集任务 ——
    for src in &cfg.sources {
        let interval = effective_interval(src.interval, cfg.interval);
        // 在 spawn 前创建 client（factory 可能 panic/出错，应在本线程暴露）。
        let client = client_factory(&src.url, src.timeout);
        let sink2 = sink.clone();
        let name = src.name.clone();
        let src_cfg = Arc::new(src.clone());
        let indices = asset_indices.clone();
        let msrcs = mapping_sources.clone();
        let msk = mapping_src_key.clone();
        let mapping_cols = mapping_cols.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let started = std::time::Instant::now();
                match collect_source(&src_cfg, &client, tz).await {
                    Ok(mut rows) => {
                        // mapping join（仅当配置启用且有资产索引）。
                        if let Some(key) = msk.as_ref() {
                            if !indices.is_empty() {
                                for row in rows.iter_mut() {
                                    for w in join_row(row, key, &indices, &msrcs) {
                                        tracing::warn!(source = %name, "mapping: {}", w);
                                    }
                                }
                            }
                        }
                        match sink2.insert_rows(&rows, &mapping_cols).await {
                            Ok(n) => tracing::info!(
                                source = %name,
                                rows = n,
                                elapsed_ms = started.elapsed().as_millis() as u64,
                                "采集完成"
                            ),
                            Err(e) => tracing::error!(source = %name, "写入失败: {}", e.0),
                        }
                    }
                    Err(e) => {
                        tracing::warn!(source = %name, "采集失败，跳过本轮: {}", e.0);
                    }
                }
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }
        }));
    }

    handles
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefers_source_interval_when_set() {
        assert_eq!(effective_interval(Some(30), 60), 30);
    }

    #[test]
    fn falls_back_to_global_when_source_unset() {
        assert_eq!(effective_interval(None, 60), 60);
    }
}

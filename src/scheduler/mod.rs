//! # scheduler 模块
//!
//! 调度层（编排各层）。统一采集循环按固定间隔触发，每轮并发采集所有 source，
//! 全部完成后统一排序写入。另有一个独立的保留期清理任务。
//! 单源/单轮失败隔离：任何错误只记日志，永不向上传播，不影响其他源与其他轮次。
//!
//! ## 调度策略
//! - **固定间隔**：每轮采集结束后，计算到下一个对齐时间点的等待时间，
//!   确保"采集完成时刻"始终落在 `interval` 的整数倍上（如 60 秒间隔时
//!   在 :00, :01:00, :02:00 采集），不受采集耗时长短影响。
//! - **统一时间戳**：所有 source 共享同一个 `query_ts`，确保多源数据时间一致。
//! - **统一写入**：所有 source 采集完成后，合并排序后一次性写入数据库。
//!
//! ## 失败隔离边界
//! - source 采集失败（Prometheus 不可达）→ 该源跳过本轮，下一轮重试；
//!   其他源不受影响。
//! - 写入失败（MySQL 抖动）→ 跳过本轮写入，下一轮重试。
//! - 保留期清理失败 → 跳过本次，下次重试。
//!
//! 以上均只记日志，不 abort 任务、不传播给其他任务。
//!
//! ## 可测试性
//! 调度本身涉及 spawn + sleep + tokio 运行时，难以单元测试；
//! 但采集间隔的选择逻辑 [`effective_interval`] 与对齐计算 [`next_aligned_time`]
//! 是纯函数，单独覆盖。

use crate::extractor::{collect_source_at, SourceQuerier};
use crate::mapping::{join_row, AssetIndex};
use crate::sink::Sink;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::Duration;

/// 计算某 source 的实际采集间隔：source 自身 interval 优先，缺省取全局 interval。
///
/// 抽成纯函数便于单测（避免为测试构造完整 `Config`）。
pub fn effective_interval(src_interval: Option<u64>, global_interval: u64) -> u64 {
    src_interval.unwrap_or(global_interval)
}

/// 计算下一个对齐到 `interval` 整数倍的时间点。
///
/// 以 Unix 纪元为起点，`interval` 为步长。例如 interval=60 时：
/// - 当前 10:01:23 → 下一个对齐点 10:02:00
/// - 当前 10:02:00 → 下一个对齐点 10:03:00（当前时刻已对齐则取下一个）
///
/// 若 `now_secs` 已经对齐（即 `now_secs % interval == 0`），返回
/// `now_secs + interval`（确保至少等一个完整间隔）。
pub fn next_aligned_time(now_secs: u64, interval: u64) -> u64 {
    let remainder = now_secs % interval;
    if remainder == 0 {
        now_secs + interval
    } else {
        now_secs + (interval - remainder)
    }
}

/// 在循环间隙睡眠到目标时刻，但若收到 `shutdown` 信号则提前返回 `false`。
/// 用于优雅退出：不在采集轮次中途打断，而在两轮之间的 sleep 处响应退出。
///
/// 与简单 `sleep(interval)` 不同，本函数睡眠到 `target_secs`（绝对时刻），
/// 消除采集耗时对间隔的影响——无论采集花了 2 秒还是 30 秒，下一轮总是
/// 在下一个对齐时间点开始。
async fn sleep_until_or_shutdown(target_secs: u64, shutdown: &AtomicBool) -> bool {
    if shutdown.load(Ordering::Acquire) {
        return false;
    }
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let remaining = target_secs.saturating_sub(now_secs);
    // 分片睡眠（每秒检查一次信号），避免长间隔时退出延迟过大。
    let mut left = remaining;
    while left > 0 {
        let step = left.min(1);
        tokio::time::sleep(Duration::from_secs(step)).await;
        left -= step;
        if shutdown.load(Ordering::Acquire) {
            return false;
        }
        // 重新检查：若系统时钟跳变导致剩余时间变大，重新计算
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now_secs >= target_secs {
            break;
        }
        left = left.min(target_secs.saturating_sub(now_secs));
    }
    true
}

/// 启动统一采集循环 + 保留期清理任务，返回各任务 `JoinHandle`。
///
/// `shutdown` 为优雅退出信号：main 收到 SIGINT/SIGTERM 后置位，各任务在
/// **下一轮循环开始前**（当前轮的 collect+insert 已完成）检查并退出，
/// 避免 abort 打断正在写入的批次（spec §9：等当前轮完成再退出）。
/// 故调用方应：置位 `shutdown` → `await` 各 handle（而非 `abort`）。
///
/// `client_factory` 接收 `(url, timeout)`，每调用一次为对应 source 创建一个
/// 实现了 [`SourceQuerier`] 的客户端。
///
/// `asset_indices` 为空时跳过 mapping join；非空时 `join_row` 用各
/// `mapping_sources[i].src_key` 取行内关联值，支持不同资产源用不同行内键。
pub fn run<Q>(
    cfg: Arc<crate::config::Config>,
    sink: Arc<Sink>,
    client_factory: impl Fn(&str, u64) -> Q,
    asset_indices: Arc<Vec<AssetIndex>>,
    mapping_sources: Arc<Vec<crate::config::MappingSource>>,
    shutdown: Arc<AtomicBool>,
) -> Vec<tokio::task::JoinHandle<()>>
where
    Q: SourceQuerier + Send + Sync + 'static,
{
    // 时区：配置加载阶段已校验合法（validate 会拒绝非法时区），此处用 expect
    // 表明这是不变量；一旦校验逻辑被绕过应立即暴露，而非静默改用默认时区
    // （静默改时区会让采集时间/清理基准错位，远比 panic 危险）。
    let tz: chrono_tz::Tz = cfg
        .timezone
        .parse()
        .expect("时区已在 config::validate 中校验为合法 IANA 名");

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
        let shutdown2 = shutdown.clone();
        handles.push(tokio::spawn(async move {
            loop {
                match sink2.run_retention(days).await {
                    Ok(n) if n > 0 => {
                        tracing::info!(target: "retention", deleted = n, "保留期清理完成");
                    }
                    Ok(_) => {}
                    Err(e) => tracing::error!(target: "retention", "保留期清理失败: {}", e.0),
                }
                if !sleep_or_shutdown(interval, &shutdown2).await {
                    break;
                }
            }
        }));
    }

    // —— 统一采集循环 ——
    {
        let interval = cfg.interval;
        let clients: Arc<Vec<Q>> = Arc::new(
            cfg.sources
                .iter()
                .map(|src| client_factory(&src.url, src.timeout))
                .collect(),
        );
        let sink2 = sink.clone();
        let sources = cfg.sources.clone();
        let indices = asset_indices.clone();
        let msrcs = mapping_sources.clone();
        let mapping_cols = mapping_cols.clone();
        let shutdown2 = shutdown.clone();

        handles.push(tokio::spawn(async move {
            loop {
                if shutdown2.load(Ordering::Acquire) {
                    break;
                }

                let started = std::time::Instant::now();

                // 计算本轮查询时间戳：2 分钟前 00 秒时刻，与 extractor 逻辑一致。
                // 使用统一的 query_ts，确保所有 source 获取同一时间点的数据。
                let now = chrono::Utc::now().with_timezone(&tz);
                let query_ts = crate::extractor::compute_query_ts(&now);

                // 并发采集所有 source，统一使用 query_ts
                let mut all_rows: Vec<crate::models::Row> = Vec::new();
                let mut tasks = tokio::task::JoinSet::new();

                for (i, src) in sources.iter().enumerate() {
                    let src_cfg = Arc::new(src.clone());
                    let client = clients.clone(); // Arc clone（廉价）
                    let name = src.name.clone();
                    tasks.spawn(async move {
                        let result =
                            collect_source_at(&src_cfg, &client[i], tz, Some(query_ts)).await;
                        (name, result)
                    });
                }

                // 等待所有采集完成
                while let Some(res) = tasks.join_next().await {
                    match res {
                        Ok((name, result)) => match result {
                            Ok(rows) => all_rows.extend(rows),
                            Err(e) => {
                                tracing::warn!(source = %name, "采集失败，跳过该源: {}", e.0);
                            }
                        },
                        Err(e) => {
                            tracing::error!("采集任务异常退出: {}", e);
                        }
                    }
                }

                // mapping join（仅当配置启用且有资产索引）
                if !indices.is_empty() {
                    for row in all_rows.iter_mut() {
                        for w in join_row(row, &indices, &msrcs) {
                            tracing::warn!(source = %row.source, "mapping: {}", w);
                        }
                    }
                }

                // 全局排序：按 source → ip → card_id 排序
                all_rows.sort_by(|a, b| {
                    match a.source.cmp(&b.source) {
                        std::cmp::Ordering::Equal => match compare_ip(&a.ip, &b.ip) {
                            std::cmp::Ordering::Equal => {
                                let a_num = a.card_id.parse::<u32>();
                                let b_num = b.card_id.parse::<u32>();
                                match (a_num, b_num) {
                                    (Ok(an), Ok(bn)) => an.cmp(&bn),
                                    _ => a.card_id.cmp(&b.card_id),
                                }
                            }
                            other => other,
                        },
                        other => other,
                    }
                });

                // 统一写入所有行
                if !all_rows.is_empty() {
                    match sink2.insert_rows(&all_rows, &mapping_cols).await {
                        Ok(n) => tracing::info!(
                            rows = n,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            query_ts = query_ts,
                            "采集完成"
                        ),
                        Err(e) => tracing::error!("写入失败: {}", e.0),
                    }
                } else {
                    tracing::warn!("本轮所有源均未采集到数据");
                }

                // 计算下一个对齐时间点，睡眠到该时刻
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let next = next_aligned_time(now_secs, interval);
                if !sleep_until_or_shutdown(next, &shutdown2).await {
                    break;
                }
            }
        }));
    }

    handles
}

/// 按点分十进制逐段数值比较两个 IP 地址。
///
/// 字典序会导致 "192.168.1.10" 排在 "192.168.1.2" 之前，
/// 逐段数值比较保证 "192.168.1.2" < "192.168.1.10"。
/// 非点分格式（如主机名）回退为字典序。
fn compare_ip(a: &str, b: &str) -> std::cmp::Ordering {
    let a_parts: Vec<&str> = a.split('.').collect();
    let b_parts: Vec<&str> = b.split('.').collect();
    // 若两边都是 4 段纯数字，按段数值比较（IPv4）。
    if a_parts.len() == 4 && b_parts.len() == 4 {
        let a_nums: Vec<u64> = match a_parts.iter().map(|s| s.parse::<u64>()).collect() {
            Ok(v) => v,
            Err(_) => return a.cmp(b),
        };
        let b_nums: Vec<u64> = match b_parts.iter().map(|s| s.parse::<u64>()).collect() {
            Ok(v) => v,
            Err(_) => return a.cmp(b),
        };
        for (an, bn) in a_nums.iter().zip(b_nums.iter()) {
            match an.cmp(bn) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            }
        }
        return a_nums.len().cmp(&b_nums.len());
    }
    // 非 IPv4 格式（主机名、IPv6 等）回退字典序。
    a.cmp(b)
}

/// 在循环间隙睡眠 `secs`，但若收到 `shutdown` 信号则提前返回 `false`。
/// 用于保留期清理等非对齐场景。统一采集循环使用 `sleep_until_or_shutdown`。
async fn sleep_or_shutdown(secs: u64, shutdown: &AtomicBool) -> bool {
    if shutdown.load(Ordering::Acquire) {
        return false;
    }
    // 分片睡眠（每秒检查一次信号），避免长间隔时退出延迟过大。
    let mut remaining = secs;
    while remaining > 0 {
        let step = remaining.min(1);
        tokio::time::sleep(Duration::from_secs(step)).await;
        remaining -= step;
        if shutdown.load(Ordering::Acquire) {
            return false;
        }
    }
    true
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

    // —— next_aligned_time 测试 ——

    #[test]
    fn next_aligned_time_at_boundary() {
        // 正好在 60 的整数倍上 → 下一个是 +60
        assert_eq!(next_aligned_time(120, 60), 180);
    }

    #[test]
    fn next_aligned_time_mid_interval() {
        // 61 秒时，下一个对齐点是 120
        assert_eq!(next_aligned_time(61, 60), 120);
    }

    #[test]
    fn next_aligned_time_near_boundary() {
        // 119 秒时，下一个对齐点是 120
        assert_eq!(next_aligned_time(119, 60), 120);
    }

    #[test]
    fn next_aligned_time_small_interval() {
        // interval=10, 当前 23 → 下一个对齐点是 30
        assert_eq!(next_aligned_time(23, 10), 30);
    }

    #[test]
    fn next_aligned_time_epoch() {
        // 纪元起点 0 是 60 的整数倍 → 下一个是 60
        assert_eq!(next_aligned_time(0, 60), 60);
    }

    // —— 旧 sleep 函数测试 ——

    /// shutdown 已置位时，sleep_or_shutdown 立即返回 false（不等满睡眠）。
    #[tokio::test]
    async fn sleep_returns_false_when_shutdown_set() {
        let shutdown = AtomicBool::new(true);
        let started = std::time::Instant::now();
        let cont = sleep_or_shutdown(60, &shutdown).await;
        assert!(!cont);
        // 应几乎立即返回，不应等 60 秒。
        assert!(started.elapsed().as_secs() < 5);
    }

    /// shutdown 未置位时，sleep_or_shutdown 睡满后返回 true。
    #[tokio::test]
    async fn sleep_returns_true_when_not_shutdown() {
        let shutdown = AtomicBool::new(false);
        let cont = sleep_or_shutdown(2, &shutdown).await;
        assert!(cont);
    }

    /// 睡眠中途置位 shutdown，应提前返回 false（分片睡眠每秒检查一次）。
    #[tokio::test]
    async fn sleep_interruptible_midway() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let s2 = shutdown.clone();
        // 1 秒后置位。
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            s2.store(true, Ordering::Release);
        });
        let started = std::time::Instant::now();
        let cont = sleep_or_shutdown(60, &shutdown).await;
        assert!(!cont);
        // 应在约 2 秒内返回（1 秒等待置位 + 一次分片检查），远小于 60。
        assert!(started.elapsed().as_secs() < 5);
    }

    // —— compare_ip 测试 ——

    #[test]
    fn compare_ip_sorts_numerically_per_segment() {
        use std::cmp::Ordering;
        assert_eq!(compare_ip("192.168.1.2", "192.168.1.10"), Ordering::Less);
        assert_eq!(compare_ip("192.168.1.10", "192.168.1.2"), Ordering::Greater);
        assert_eq!(compare_ip("192.168.1.1", "192.168.1.1"), Ordering::Equal);
        assert_eq!(compare_ip("192.168.2.1", "192.168.1.10"), Ordering::Greater);
        assert_eq!(compare_ip("10.0.0.1", "10.0.0.2"), Ordering::Less);
        assert_eq!(compare_ip("host-a", "host-b"), Ordering::Less);
    }

    /// 守护：全局排序按 source → ip → card_id，IP 逐段数值排序，card_id 纯数字时数值排序。
    #[test]
    fn rows_sorted_by_source_then_ip_then_numeric_card_id() {
        use crate::models::Row;
        let tz = chrono_tz::Asia::Shanghai;
        let now = chrono::Utc::now().with_timezone(&tz);
        let make_row = |ip: &str, card_id: &str, source: &str| Row {
            ts: now.clone(),
            ip: ip.into(),
            card_id: card_id.into(),
            fields: Default::default(),
            strings: Default::default(),
            source: source.into(),
        };
        let mut rows = vec![
            make_row("10.0.0.2", "1", "gpu"),
            make_row("10.0.0.1", "10", "gpu"),
            make_row("10.0.0.1", "2", "npu"),
            make_row("10.0.0.2", "0", "gpu"),
            make_row("10.0.0.1", "1", "gpu"),
            make_row("10.0.0.1", "2", "gpu"),
        ];
        rows.sort_by(|a, b| {
            match a.source.cmp(&b.source) {
                std::cmp::Ordering::Equal => match compare_ip(&a.ip, &b.ip) {
                    std::cmp::Ordering::Equal => {
                        let a_num = a.card_id.parse::<u32>();
                        let b_num = b.card_id.parse::<u32>();
                        match (a_num, b_num) {
                            (Ok(an), Ok(bn)) => an.cmp(&bn),
                            _ => a.card_id.cmp(&b.card_id),
                        }
                    }
                    other => other,
                },
                other => other,
            }
        });
        // gpu 在前（字典序 gpu < npu），gpu 内按 IP 逐段数值排序，同 IP 按 card_id 数值排序
        assert_eq!(rows[0].source, "gpu");
        assert_eq!(rows[0].ip, "10.0.0.1");
        assert_eq!(rows[0].card_id, "1");
        assert_eq!(rows[1].source, "gpu");
        assert_eq!(rows[1].ip, "10.0.0.1");
        assert_eq!(rows[1].card_id, "2");
        assert_eq!(rows[2].source, "gpu");
        assert_eq!(rows[2].ip, "10.0.0.1");
        assert_eq!(rows[2].card_id, "10");
        assert_eq!(rows[3].source, "gpu");
        assert_eq!(rows[3].ip, "10.0.0.2");
        assert_eq!(rows[3].card_id, "0");
        assert_eq!(rows[4].source, "gpu");
        assert_eq!(rows[4].ip, "10.0.0.2");
        assert_eq!(rows[4].card_id, "1");
        // npu 行
        assert_eq!(rows[5].source, "npu");
        assert_eq!(rows[5].ip, "10.0.0.1");
        assert_eq!(rows[5].card_id, "2");
    }
}

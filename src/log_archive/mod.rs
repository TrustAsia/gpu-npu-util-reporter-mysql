//! # log_archive 模块
//!
//! 日志归档后台任务。扫描日志目录，对超期散日志
//! (`<base>-YYYY-MM-DD.log`，含 all 与 error 两份) 打包成单个 `tar.gz`，
//! 原始散文件删除，压缩包永不删除。
//!
//! ## 为什么不用 tracing-appender 的删除
//! tracing-appender 的 `max_log_files` 只能"删除"，无法"重命名归档"。
//! 业务要求是把超期日志归档（压缩留存）而非直接丢弃，故由本模块自定义归档。
//!
//! ## 时区一致性
//! 归档的"今天"基准用程序配置时区（与采集时间、保留期清理同一时区），
//! 而非 `chrono::Local`（系统时区），避免部署在 UTC 容器里时归档边界偏移一天。

use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::time::Duration;

/// 归档错误（携带可读描述）。
#[derive(Debug)]
pub struct ArchiveError(pub String);

/// 扫描日志目录，归档所有早于 `archive_after_days` 的散日志对。
///
/// - `today`：归档的"今天"基准日期（应由调用方按配置时区算出后传入）。
/// - `prefix`：归档包前缀，生成 `<prefix>-YYYY-MM-DD.tar.gz`。
/// - `all_file` / `error_file`：散文件基名（如 `all.log`，去 `.log` 后缀拼日期）。
///
/// 返回本次归档的日期数量。已存在同名压缩包的日期会跳过（不重复归档、不删散文件）。
pub fn archive_old_logs(
    dir: &Path,
    archive_after_days: u32,
    today: chrono::NaiveDate,
    prefix: &str,
    all_file: &str,
    error_file: &str,
) -> Result<usize, ArchiveError> {
    // cutoff = today - archive_after_days；早于等于该日期的散日志才归档。
    // 用 checked_sub_days 而非 `today - Duration::days(...)`：后者在 archive_after_days
    // 极大（u32 上限 ~96 million 天）时会下溢到 NaiveDate::MIN 之下 → chrono 的
    // `Sub<TimeDelta>` 实现 `expect()` 会 **panic**，而本函数在独立 tokio 任务里运行，
    // panic 会杀掉归档任务且其余任务不受影响地继续跑——归档从此静默永久失效。
    // checked_sub_days 返回 Option，None 时返回明确错误而非 panic。
    let cutoff = today
        .checked_sub_days(chrono::Days::new(archive_after_days as u64))
        .ok_or_else(|| ArchiveError(format!("archive_after_days={} 过大，cutoff 下溢", archive_after_days)))?;

    // 收集所有形如 <base>-YYYY-MM-DD.log 且日期 <= cutoff 的散文件日期（去重升序）。
    let mut dates: std::collections::BTreeSet<chrono::NaiveDate> = std::collections::BTreeSet::new();
    let entries = fs::read_dir(dir).map_err(|e| ArchiveError(format!("读日志目录失败: {}", e)))?;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let date = parse_log_date(&name, all_file).or_else(|| parse_log_date(&name, error_file));
        if let Some(d) = date {
            if d <= cutoff {
                dates.insert(d);
            }
        }
    }

    // F3：清理残留的临时归档文件（<prefix>-<date>.tar.gz.tmp）。
    // 归档采用 temp+rename 原子写：临时文件写到 <prefix>-<date>.tar.gz.tmp，sync 后
    // rename 成最终包。但本函数运行在独立 tokio 任务里，main 退出时对其调 abort()
    // 会在下一次 .await 处取消——若此刻正在 create_tar_gz 写临时文件，任务被直接 drop，
    // 留下的 .tmp 文件**永不会被清理**（下次 run_loop 用相同日期名，但仅当散文件仍存在
    // 才会重写；散文件已删的孤儿 .tmp 会无限滞留）。此外进程崩溃/SIGKILL 也会留下孤儿。
    // 这里在每轮归档开头扫描并删除所有 <prefix>-*.tar.gz.tmp，既清当前要归档日期的残留，
    // 也顺带清掉历史孤儿（删除失败只记 WARN，不阻断归档）。
    cleanup_stale_tmp(dir, prefix);

    let mut archived = 0;
    for date in dates {
        let date_str = date.format("%Y-%m-%d").to_string();
        let all_path = scatter_path(dir, all_file, &date_str);
        let err_path = scatter_path(dir, error_file, &date_str);

        // 至少有一个散文件存在才值得归档。
        if !all_path.exists() && !err_path.exists() {
            continue;
        }
        let archive_path = dir.join(format!("{}-{}.tar.gz", prefix, date_str));
        // 已有归档包则跳过（幂等：不覆盖、不删散文件，避免丢数据）。
        if archive_path.exists() {
            continue;
        }
        // 原子写入：先写临时文件，sync 后再 rename 到最终名。直接 File::create 最终名
        // 在写中途崩溃会留下**损坏**的 tar.gz；由于上方"已存在则跳过"的幂等检查，
        // 该损坏包永远不会被重写、那一天的散日志也永远不会再被归档（"压缩包永不删除"
        // 规则下永久滞留）——这是不可恢复的日志完整性问题。rename 在同一文件系统上原子。
        let tmp_path = dir.join(format!("{}-{}.tar.gz.tmp", prefix, date_str));
        create_tar_gz(&tmp_path, &[("all", &all_path), ("error", &err_path)])?;
        // 落盘后再 rename：确保崩溃后要么看到完整旧状态（散文件在、无包），
        // 要么看到完整新状态（包在、散文件删）。
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(&tmp_path)
            .map_err(|e| ArchiveError(format!("重开临时归档失败: {}", e)))?;
        let _ = file.sync_all();
        drop(file);
        if let Err(e) = fs::rename(&tmp_path, &archive_path) {
            // rename 失败：清理临时文件，散文件保留（下次重试）。
            let _ = fs::remove_file(&tmp_path);
            return Err(ArchiveError(format!("重命名归档失败: {}", e)));
        }
        // 归档成功（rename 完成）后才删除散文件（压缩包永不删）。
        // 删除失败只记日志不视作归档失败：散文件残留不会丢数据（只是占空间）。
        if let Err(e) = fs::remove_file(&all_path) {
            tracing::warn!(target: "log_archive", path = %all_path.display(), error = %e, "删除散日志失败(归档已完成，文件残留)");
        }
        if let Err(e) = fs::remove_file(&err_path) {
            tracing::warn!(target: "log_archive", path = %err_path.display(), error = %e, "删除散日志失败(归档已完成，文件残留)");
        }
        archived += 1;
        tracing::info!(date = %date_str, "日志已归档为 {}", archive_path.display());
    }
    Ok(archived)
}

/// 拼出某 base 某日期的散文件路径：`<dir>/<base 去后缀>-<date>.log`。
fn scatter_path(dir: &Path, base: &str, date_str: &str) -> PathBuf {
    let stem = base.trim_end_matches(".log");
    dir.join(format!("{}-{}.log", stem, date_str))
}

/// 清理残留的临时归档文件 `<prefix>-*.tar.gz.tmp`（F3）。
///
/// 见 [`archive_old_logs`] 中的说明：temp+rename 原子写在被 abort/崩溃打断时会留下
/// 孤儿 `.tmp`，该文件永不被正常流程清理（散文件删后不会再触发该日期的归档）。
/// 本函数扫描目录删除所有 `<prefix>-` 开头、`.tar.gz.tmp` 结尾的文件；删除失败只记
/// WARN（可能是并发归档正在写，不视作错误）。函数无副作用要求，目录读失败时静默返回。
fn cleanup_stale_tmp(dir: &Path, prefix: &str) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let tmp_suffix = ".tar.gz.tmp";
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(prefix) && name.ends_with(tmp_suffix) {
            let path = entry.path();
            if let Err(e) = fs::remove_file(&path) {
                tracing::warn!(
                    target: "log_archive",
                    path = %path.display(),
                    error = %e,
                    "删除残留临时归档失败(可能并发写入中)"
                );
            }
        }
    }
}

/// 从散文件名解析日期。
///
/// `name` 形如 `all-2026-06-24.log`，`base` 形如 `all.log`（去 `.log` 得 `all`）。
/// 不匹配（其它前缀、非 `.log`、日期格式不符）返回 `None`。
fn parse_log_date(name: &str, base: &str) -> Option<chrono::NaiveDate> {
    let prefix = base.trim_end_matches(".log");
    let date_str = name.strip_prefix(&format!("{}-", prefix))?.strip_suffix(".log")?;
    chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()
}

/// 创建 tar.gz，包含给定文件（不存在的跳过）。
///
/// `files` 为 `(归档内别名, 源路径)` 列表。归档内统一用 `all.log`/`error.log` 等
/// 固定别名，避免随日期变化。
///
/// 注意：`tar::Builder::new(enc)` 会消费 `enc`；`tar.finish()` 在收尾时会 drop
/// 内部的 `GzEncoder`（触发其尾部 gzip 校验写入），故无需也不能再单独 finish `enc`。
fn create_tar_gz(out: &Path, files: &[(&str, &PathBuf)]) -> Result<(), ArchiveError> {
    let tar_gz = File::create(out).map_err(|e| ArchiveError(format!("创建归档文件失败: {}", e)))?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = tar::Builder::new(enc);
    for (alias, path) in files {
        if path.exists() {
            // tar 0.4 的 append_file 以第一个参数作为归档内名称，第二个是已打开的文件。
            let mut f = File::open(path)
                .map_err(|e| ArchiveError(format!("打开 {} 失败: {}", path.display(), e)))?;
            tar.append_file(alias, &mut f)
                .map_err(|e| ArchiveError(format!("写入 tar 失败: {}", e)))?;
        }
    }
    tar.finish()
        .map_err(|e| ArchiveError(format!("完成 tar 失败: {}", e)))?;
    Ok(())
}

/// 后台归档循环的参数（`shutdown` 信号语义独立，单独传入 [`run_loop`]）。
///
/// 把原本 8 个并列参数收进结构体，既满足"函数参数不过多"的可读性要求，
/// 也让调用方（main）以字段赋值的形式表达，减少位置参数错位风险。
pub struct LoopConfig {
    pub dir: PathBuf,
    pub archive_after_days: u32,
    pub interval: u64,
    pub prefix: String,
    pub all_file: String,
    pub error_file: String,
    pub tz: chrono_tz::Tz,
}

/// 后台循环：每 `cfg.interval` 秒扫描归档一次。
///
/// `cfg.tz` 为配置时区，用于算"今天"基准。失败只记日志，不退出循环。
/// `shutdown` 置位时在下次循环开始前退出（与采集任务同模式优雅退出）。
pub async fn run_loop(cfg: LoopConfig, shutdown: Arc<std::sync::atomic::AtomicBool>) {
    use std::sync::atomic::Ordering;
    loop {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let today = chrono::Utc::now().with_timezone(&cfg.tz).date_naive();
        if let Err(e) = archive_old_logs(
            &cfg.dir,
            cfg.archive_after_days,
            today,
            &cfg.prefix,
            &cfg.all_file,
            &cfg.error_file,
        ) {
            tracing::error!(target: "log_archive", "归档失败: {}", e.0);
        }
        // 分片睡眠以及时响应退出信号。
        let mut remaining = cfg.interval;
        while remaining > 0 {
            let step = remaining.min(1);
            tokio::time::sleep(Duration::from_secs(step)).await;
            remaining -= step;
            if shutdown.load(Ordering::Acquire) {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_date_from_log_name() {
        let d = parse_log_date("all-2026-06-24.log", "all.log");
        assert_eq!(d, Some(chrono::NaiveDate::from_ymd_opt(2026, 6, 24).unwrap()));
    }

    #[test]
    fn ignores_non_log_file() {
        assert!(parse_log_date("random.txt", "all.log").is_none());
        // 归档包本身（.tar.gz）不应被当成散日志
        assert!(parse_log_date("logs-2026-06-24.tar.gz", "all.log").is_none());
    }

    #[test]
    fn ignores_wrong_prefix() {
        // error 的散文件不会被 all 的前缀匹配
        assert!(parse_log_date("error-2026-06-24.log", "all.log").is_none());
        // 但能被 error 的前缀匹配
        assert!(parse_log_date("error-2026-06-24.log", "error.log").is_some());
    }

    #[test]
    fn archives_old_pair_and_deletes_scatter() {
        let dir = std::env::temp_dir().join(format!("archive_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // 写一个"很老"的日期散文件（保证 <= cutoff）。
        let old = "all-2000-01-01.log";
        let err = "error-2000-01-01.log";
        fs::write(dir.join(old), "all content").unwrap();
        fs::write(dir.join(err), "err content").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2026, 6, 25).unwrap();
        let n = archive_old_logs(&dir, 1, today, "logs", "all.log", "error.log").unwrap();
        assert_eq!(n, 1);
        // 散文件应已删除。
        assert!(!dir.join(old).exists());
        assert!(!dir.join(err).exists());
        // 压缩包应存在。
        assert!(dir.join("logs-2000-01-01.tar.gz").exists());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn skips_recent_logs_within_retention() {
        let dir = std::env::temp_dir().join(format!("archive_recent_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // 2 天前的散文件，保留期 7 天 → 不应归档。
        fs::write(dir.join("all-2026-06-23.log"), "x").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2026, 6, 25).unwrap();
        let n = archive_old_logs(&dir, 7, today, "logs", "all.log", "error.log").unwrap();
        assert_eq!(n, 0);
        // 散文件应保留。
        assert!(dir.join("all-2026-06-23.log").exists());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn idempotent_when_archive_exists() {
        let dir = std::env::temp_dir().join(format!("archive_idem_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // 预置一个"已归档"的压缩包 + 同日散文件。
        fs::write(dir.join("logs-2000-01-01.tar.gz"), "fake").unwrap();
        fs::write(dir.join("all-2000-01-01.log"), "x").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2026, 6, 25).unwrap();
        let n = archive_old_logs(&dir, 1, today, "logs", "all.log", "error.log").unwrap();
        assert_eq!(n, 0);
        // 不应删除散文件（避免覆盖既有归档丢数据）。
        assert!(dir.join("all-2000-01-01.log").exists());
        fs::remove_dir_all(&dir).ok();
    }

    /// 守护 R15：archive_after_days 极大时，旧的 `today - Duration::days(...)` 会
    /// 下溢 NaiveDate::MIN 而 panic（且 panic 在独立任务里静默杀掉归档）。
    /// 改用 checked_sub_days 后应返回 Err 而非 panic。
    #[test]
    fn huge_archive_after_days_returns_err_not_panic() {
        let dir = std::env::temp_dir().join(format!("archive_overflow_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("all-2000-01-01.log"), "x").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2026, 6, 25).unwrap();
        // u32::MAX 远超 NaiveDate 可表示范围 → 必须返回 Err 而非 panic。
        let res = archive_old_logs(&dir, u32::MAX, today, "logs", "all.log", "error.log");
        assert!(res.is_err(), "archive_after_days 极大应返回 Err 而非 panic");
        fs::remove_dir_all(&dir).ok();
    }

    /// 守护 R16：归档成功后不应残留 .tmp 临时文件（旧实现直接写最终路径，
    /// 改为 temp+rename 后须确认临时文件被清理、最终包存在）。
    #[test]
    fn archive_leaves_no_tmp_and_produces_final() {
        let dir = std::env::temp_dir().join(format!("archive_notmp_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("all-2000-01-01.log"), "all content").unwrap();
        fs::write(dir.join("error-2000-01-01.log"), "err content").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2026, 6, 25).unwrap();
        let n = archive_old_logs(&dir, 1, today, "logs", "all.log", "error.log").unwrap();
        assert_eq!(n, 1);
        assert!(dir.join("logs-2000-01-01.tar.gz").exists(), "最终归档包应存在");
        // 不应残留任何 .tmp 文件。
        let tmp_count = fs::read_dir(&dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .map(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(tmp_count, 0, "不应残留 .tmp 临时文件，实际残留 {}", tmp_count);
        fs::remove_dir_all(&dir).ok();
    }

    /// 守护 F3：残留的孤儿临时归档文件（<prefix>-<date>.tar.gz.tmp）应在本轮归档开头
    /// 被清理。这模拟 abort/崩溃打断 create_tar_gz 后留下孤儿 .tmp 的场景——正常流程
    /// 永不会清理它（散文件删后不再触发该日期归档）。本轮归档会先删除孤儿 .tmp，
    /// 再正常归档生成最终包。
    #[test]
    fn cleans_stale_tmp_before_archiving() {
        let dir = std::env::temp_dir().join(format!("archive_stale_tmp_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // 预置散日志（保证该日期会进入归档流程）+ 一个孤儿 .tmp（模拟被打断的旧归档）。
        fs::write(dir.join("all-2000-01-01.log"), "all content").unwrap();
        fs::write(dir.join("logs-2000-01-01.tar.gz.tmp"), "half-written orphan").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2026, 6, 25).unwrap();
        let n = archive_old_logs(&dir, 1, today, "logs", "all.log", "error.log").unwrap();
        // 正常归档完成。
        assert_eq!(n, 1);
        assert!(dir.join("logs-2000-01-01.tar.gz").exists(), "最终归档包应存在");
        // 孤儿 .tmp 应被清理（不存在）。
        assert!(
            !dir.join("logs-2000-01-01.tar.gz.tmp").exists(),
            "孤儿 .tmp 应被清理"
        );
        fs::remove_dir_all(&dir).ok();
    }

    /// 守护 F3：即使散文件已被删除（孤儿场景：散文件删了但 .tmp 残留），cleanup_stale_tmp
    /// 仍应清理该孤儿 .tmp。此场景下 archive_old_logs 本身可能不归档（无散文件），
    /// 但 cleanup 在收集 dates 之后、归档循环之前无条件执行。
    #[test]
    fn cleans_orphan_tmp_without_scatter_files() {
        let dir = std::env::temp_dir().join(format!("archive_orphan_tmp_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // 只有孤儿 .tmp，无散文件（模拟散文件已删但 .tmp 残留）。
        fs::write(dir.join("logs-1999-01-01.tar.gz.tmp"), "orphan").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2026, 6, 25).unwrap();
        // 调用 archive_old_logs（内部会先 cleanup）。无散文件 → 归档数为 0。
        let n = archive_old_logs(&dir, 1, today, "logs", "all.log", "error.log").unwrap();
        assert_eq!(n, 0, "无散文件不应归档");
        // 孤儿 .tmp 仍应被清理。
        assert!(
            !dir.join("logs-1999-01-01.tar.gz.tmp").exists(),
            "无散文件时孤儿 .tmp 也应被清理"
        );
        fs::remove_dir_all(&dir).ok();
    }
}

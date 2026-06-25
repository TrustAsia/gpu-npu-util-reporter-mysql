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
    let cutoff = today - chrono::Duration::days(archive_after_days as i64);

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
        create_tar_gz(&archive_path, &[("all", &all_path), ("error", &err_path)])?;
        // 归档成功后删除散文件（压缩包永不删）。
        let _ = fs::remove_file(&all_path);
        let _ = fs::remove_file(&err_path);
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
}

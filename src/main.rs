//! # gpu-npu-util-reporter-mysql 入口
//!
//! 命令行解析 → 加载配置 → 初始化日志 → 按模式分支：
//! - `--init`：仅生成 `./init/<table>.sql` 后退出（不连任何外部服务、不写日志文件）。
//! - 正常：连 MySQL → schema 校验 → 加载资产表 → 启动采集调度 + 保留期清理 +
//!   日志归档任务 → 等待 Ctrl+C 优雅退出。
//!
//! ## 启动失败策略
//! 配置错误、MySQL 连不上、schema 缺列、资产表加载失败 → 立即退出（确定性错误，
//! 重试无意义）。运行期错误由 scheduler/log_archive 各自隔离，不退出。

// 业务模块来自库 crate（src/lib.rs），二进制仅做编排/CLI/日志/退出。
use gpu_npu_util_reporter_mysql::{config, log_archive, mapping, scheduler, sink, source, sql_gen};

use clap::Parser;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;

/// 命令行参数。
#[derive(Parser, Debug)]
#[command(
    name = "gpu-npu-util-reporter-mysql",
    about = "定时从多个 Prometheus 读取计算卡/主机指标，对齐后写入 MySQL"
)]
struct Args {
    /// 仅生成建表 SQL（不连库、不采集、不写日志）。
    #[arg(long)]
    init: bool,
    /// 配置文件路径。不存在时自动生成示例后退出。
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // 配置文件不存在 → 生成示例并退出（提示用户编辑后重试）。
    if !args.config.exists() {
        let example_path = PathBuf::from("config.example.yaml");
        match config::write_example(&example_path) {
            Ok(()) => {
                eprintln!(
                    "配置文件 {} 不存在，已生成示例: {}",
                    args.config.display(),
                    example_path.display()
                );
                eprintln!("请编辑后重试。");
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("生成示例配置失败: {}", e.0);
                std::process::exit(1);
            }
        }
    }

    let cfg = match config::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("配置错误: {}", e.0);
            std::process::exit(1);
        }
    };

    // 时区：配置阶段已校验合法，此处直接解析。
    let tz: chrono_tz::Tz = cfg.timezone.parse().expect("时区已校验");

    // --init 模式：生成 SQL 后退出（不初始化日志、不连任何服务）。
    if args.init {
        let dir = PathBuf::from("init");
        if let Err(e) = sql_gen::write_init_sql(&cfg, &dir) {
            eprintln!("生成建表 SQL 失败: {}", e);
            std::process::exit(1);
        }
        println!(
            "已生成 ./init/{}.sql，请执行建表后以正常模式运行。",
            cfg.database.table
        );
        return;
    }

    // 正常模式：先初始化日志，后续错误可用 tracing 记录。
    init_logging(&cfg, tz);
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        sources = cfg.sources.len(),
        interval = cfg.interval,
        "gpu-npu-util-reporter-mysql 启动"
    );

    // 建立 MySQL 连接池。
    let sink = match sink::Sink::connect(&cfg).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            // 密码脱敏：错误信息来自 sqlx，不含密码字段，原样输出。
            tracing::error!("连接 MySQL 失败: {}", e.0);
            std::process::exit(1);
        }
    };

    // schema 校验（连上后、采集前）。
    let expected = sink::expected_columns(&cfg);
    match sink.check_schema(&expected).await {
        Ok(sink::schema::SchemaCheck::Match) => {
            tracing::info!("表结构校验通过");
        }
        Ok(sink::schema::SchemaCheck::Missing(cols)) => {
            tracing::error!(
                missing = ?cols,
                "表缺少列，请用 --init 重新生成 SQL 或手动 ALTER 后重启"
            );
            std::process::exit(1);
        }
        Ok(sink::schema::SchemaCheck::Extra(cols)) => {
            tracing::warn!(extra = ?cols, "表多出列");
            match cfg.database.on_extra_columns.as_str() {
                "abort" => {
                    tracing::error!("on_extra_columns=abort，因表多列退出");
                    std::process::exit(1);
                }
                "continue" => {
                    tracing::warn!("on_extra_columns=continue，忽略多列继续");
                }
                _ => {
                    // ask：TTY 时交互确认；非 TTY 回退 continue。
                    if is_tty() {
                        if !confirm_continue(&cols) {
                            std::process::exit(1);
                        }
                    } else {
                        tracing::warn!(
                            "on_extra_columns=ask，非交互环境按 continue 处理"
                        );
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!("schema 校验失败: {}", e.0);
            std::process::exit(1);
        }
    }

    // 加载资产表（mapping.enabled=false 时跳过，asset_indices 为空）。
    let asset_indices = if cfg.mapping.enabled {
        match mapping::load_all(&cfg.mapping) {
            Ok(v) => {
                tracing::info!(
                    sources = v.len(),
                    "资产表加载完成"
                );
                Arc::new(v)
            }
            Err(e) => {
                tracing::error!("资产表加载失败: {}", e.0);
                std::process::exit(1);
            }
        }
    } else {
        Arc::new(Vec::new())
    };
    // 资产源配置：join_row 内部按各 mapping_sources[i].src_key 取行内关联值，
    // 故不同资产源可用不同行内键（如一个 join namespace、另一个 join ip）。
    let mapping_sources = Arc::new(cfg.mapping.sources.clone());

    // 启动采集调度（含每源采集任务 + 保留期清理任务）。
    let cfg_arc = Arc::new(cfg.clone());
    let client_factory = |url: &str, timeout: u64| {
        source::PrometheusClient::new(url, timeout)
            .unwrap_or_else(|e| panic!("构建 HTTP 客户端失败 ({}): {}", url, e.0))
    };
    // 优雅退出信号：置位后各任务在下一轮开始前退出（当前轮已完整写入）。
    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handles = scheduler::run(
        cfg_arc.clone(),
        sink.clone(),
        client_factory,
        asset_indices,
        mapping_sources,
        shutdown.clone(),
    );

    // 启动日志归档后台任务。
    let log_cfg = cfg_arc.logging.clone();
    let shutdown_for_log = shutdown.clone();
    let log_handle = tokio::spawn(async move {
        log_archive::run_loop(
            log_archive::LoopConfig {
                dir: PathBuf::from(&log_cfg.dir),
                archive_after_days: log_cfg.archive_after_days,
                interval: 3600, // 归档扫描间隔（秒）
                prefix: log_cfg.archive_prefix.clone(),
                all_file: log_cfg.all_file.clone(),
                error_file: log_cfg.error_file.clone(),
                tz,
            },
            shutdown_for_log,
        )
        .await;
    });

    // 优雅退出：捕获中断信号(Ctrl+C / 容器停止)，等当前轮完成再退出。
    // Unix 监听 SIGINT/SIGTERM；Windows 监听 Ctrl+C / 控制台关闭
    // （见 wait_for_shutdown_signal 的平台分支）。
    tracing::info!("已启动所有采集与维护任务，等待中断信号退出");
    wait_for_shutdown_signal().await;
    tracing::info!("收到退出信号，等待当前轮采集/写入完成...");
    shutdown.store(true, std::sync::atomic::Ordering::Release);
    // 等待各任务在轮次边界自行退出（而非 abort 打断写入）。
    for h in handles {
        // 等待结果，忽略 JoinError（任务已正常返回）。
        let _ = h.await;
    }
    log_handle.abort();
    tracing::info!("已优雅退出");
}

/// 等待退出信号（任一到达即返回），用于优雅退出。
///
/// 平台差异由条件编译隔离：
/// - **Unix**(Linux/macOS)：监听 SIGINT(Ctrl+C) 与 SIGTERM（Kubernetes/Docker 停止
///   容器的信号）。二者等价处理，置位 shutdown 后等当前轮完成再退出。
/// - **Windows**：无 SIGTERM 概念。监听 Ctrl+C 与"控制台关闭事件"（用户关控制台
///   窗口或 `Stop-Process`，对应 docker stop 的近似场景）。
#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigint = signal(SignalKind::interrupt()).expect("注册 SIGINT 失败");
    let mut sigterm = signal(SignalKind::terminate()).expect("注册 SIGTERM 失败");
    tokio::select! {
        _ = sigint.recv() => tracing::info!("收到 SIGINT"),
        _ = sigterm.recv() => tracing::info!("收到 SIGTERM"),
    }
}

#[cfg(windows)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::windows;
    // ctrl_c 对应 Ctrl+C；ctrl_close 对应关闭控制台窗口（容器停止的近似场景）。
    let mut ctrl_c = windows::ctrl_c().expect("注册 Ctrl+C 失败");
    let mut ctrl_close = windows::ctrl_close().expect("注册 Ctrl+Close 失败");
    tokio::select! {
        _ = ctrl_c.recv() => tracing::info!("收到 Ctrl+C"),
        _ = ctrl_close.recv() => tracing::info!("收到 Ctrl+Close"),
    }
}

// =====================================================================
// 日志初始化：双文件(完整 INFO+ / 错误 ERROR) + stdout + 按日文件名
//
// 不用 tracing-appender 的 rolling（其文件名格式与归档模块期望的
// <base>-YYYY-MM-DD.log 不一致，且其 max_log_files 只删不归档）。
// 改用自定义 MakeWriter：每次写日志时按"当前配置时区的日期"算文件路径，
// 这样跨天自动切到新文件，且文件名正好被 log_archive 识别归档。
// =====================================================================

/// 按"配置时区当天日期"拼日志文件路径。
fn daily_log_path(dir: &str, base: &str, tz: chrono_tz::Tz) -> PathBuf {
    let today = chrono::Utc::now().with_timezone(&tz).date_naive();
    let stem = base.trim_end_matches(".log");
    PathBuf::from(dir).join(format!("{}-{}.log", stem, today.format("%Y-%m-%d")))
}

/// 按日期缓存复用的追加写文件句柄。
///
/// 同一天内复用同一 `Arc<Mutex<File>>`，跨天（日期变化）时重新打开新文件，
/// 旧句柄随缓存替换被 drop（关闭）。避免每条日志都 open/close 系统调用。
struct CachedAppendFile {
    dir: String,
    base: String,
    tz: chrono_tz::Tz,
    /// (日期, 句柄)；日期变了就重开。
    cached: Mutex<Option<(chrono::NaiveDate, Arc<Mutex<std::fs::File>>)>>,
}

impl CachedAppendFile {
    fn new(dir: String, base: String, tz: chrono_tz::Tz) -> Self {
        Self {
            dir,
            base,
            tz,
            cached: Mutex::new(None),
        }
    }

    /// 返回当前日期对应文件句柄的 Arc 克隆（必要时重开）。
    /// 文件打不开时返回 None（调用方退化为丢弃日志，避免日志拖垮采集）。
    fn handle(&self) -> Option<Arc<Mutex<std::fs::File>>> {
        let today = chrono::Utc::now().with_timezone(&self.tz).date_naive();
        let mut slot = self.cached.lock().unwrap();
        let needs_open = match slot.as_ref() {
            None => true,
            Some((d, _)) => *d != today,
        };
        if needs_open {
            let path = daily_log_path(&self.dir, &self.base, self.tz);
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
            {
                Ok(f) => *slot = Some((today, Arc::new(Mutex::new(f)))),
                Err(_) => return None,
            }
        }
        slot.as_ref().map(|(_, h)| h.clone())
    }
}

/// 一个按日切分、可写 all 与 error 两文件(按级别)的 MakeWriter。
///
/// 文件句柄按日期缓存复用（同一天内复用，跨天重开）。`make_writer_for` 按
/// metadata 级别决定是否同时写 error 文件。
struct DailyFileWriter {
    dir: String,
    /// 已确认目录存在的标记，避免每条日志都 stat。
    dir_ready: Mutex<bool>,
    /// all 文件缓存句柄。
    all_file: CachedAppendFile,
    /// error 文件缓存句柄。
    err_file: CachedAppendFile,
}

impl DailyFileWriter {
    /// 确保日志目录存在（惰性创建一次）。
    fn ensure_dir(&self) {
        let mut ready = self.dir_ready.lock().unwrap();
        if *ready {
            return;
        }
        let _ = std::fs::create_dir_all(&self.dir);
        *ready = true;
    }
}

impl<'a> MakeWriter<'a> for DailyFileWriter {
    type Writer = Box<dyn Write + Send + 'a>;

    fn make_writer(&'a self) -> Self::Writer {
        self.ensure_dir();
        match self.all_file.handle() {
            Some(a) => Box::new(TeeWriter::new(a, None)),
            None => Box::new(io::sink()),
        }
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        self.ensure_dir();
        let all = self.all_file.handle();
        let err = if *meta.level() == tracing::Level::ERROR {
            self.err_file.handle()
        } else {
            None
        };
        match (all, err) {
            (Some(a), Some(e)) => Box::new(TeeWriter::new(a, Some(e))),
            (Some(a), None) => Box::new(TeeWriter::new(a, None)),
            // 主文件都打不开时退化为丢弃（不 panic，避免日志拖垮采集）。
            (None, _) => Box::new(io::sink()),
        }
    }
}

/// 把输出同时写到 all 与(可选的)error 文件（均为 Arc<Mutex<File>> 句柄）。
/// 写时短暂加锁；error 文件写失败不阻塞主文件。
struct TeeWriter {
    all: Arc<Mutex<std::fs::File>>,
    err: Option<Arc<Mutex<std::fs::File>>>,
}

impl TeeWriter {
    fn new(all: Arc<Mutex<std::fs::File>>, err: Option<Arc<Mutex<std::fs::File>>>) -> Self {
        Self { all, err }
    }
}

impl Write for TeeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.all.lock().unwrap().write(buf)?;
        if let Some(e) = self.err.as_ref() {
            let _ = e.lock().unwrap().write_all(buf);
        }
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.all.lock().unwrap().flush()?;
        if let Some(e) = self.err.as_ref() {
            let _ = e.lock().unwrap().flush();
        }
        Ok(())
    }
}


/// 初始化 tracing：stdout(可选) + 双文件(all INFO+ / error ERROR)，按级别过滤。
fn init_logging(cfg: &config::Config, tz: chrono_tz::Tz) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let file_writer = DailyFileWriter {
        dir: cfg.logging.dir.clone(),
        dir_ready: Mutex::new(false),
        all_file: CachedAppendFile::new(
            cfg.logging.dir.clone(),
            cfg.logging.all_file.clone(),
            tz,
        ),
        err_file: CachedAppendFile::new(
            cfg.logging.dir.clone(),
            cfg.logging.error_file.clone(),
            tz,
        ),
    };

    // 级别过滤：解析为 LevelFilter（订阅层共用，决定哪些事件进入写入层）。
    let level: tracing::Level = match cfg.logging.level.as_str() {
        "error" => tracing::Level::ERROR,
        "warn" => tracing::Level::WARN,
        "info" => tracing::Level::INFO,
        "debug" => tracing::Level::DEBUG,
        _ => tracing::Level::TRACE,
    };
    let level_filter =
        tracing_subscriber::filter::LevelFilter::from_level(level);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false) // 文件里不要 ANSI 颜色码
        .with_target(true);

    if cfg.logging.stdout {
        let stdout_layer = tracing_subscriber::fmt::layer()
            .with_writer(io::stdout)
            .with_ansi(is_tty());
        tracing_subscriber::registry()
            .with(level_filter)
            .with(file_layer)
            .with(stdout_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(level_filter)
            .with(file_layer)
            .init();
    }
}

/// 判断标准输出是否为 TTY（决定是否着色、ask 模式是否交互）。
fn is_tty() -> bool {
    use std::io::IsTerminal;
    std::io::stdout().is_terminal()
}

/// TTY 交互：询问用户表多列时是否继续。
fn confirm_continue(cols: &[String]) -> bool {
    print!(
        "表多出列 {:?}，是否继续？[y/N] ",
        cols
    );
    let _ = io::stdout().flush();
    let mut input = String::new();
    if io::stdin().read_line(&mut input).is_err() {
        return false;
    }
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 日志文件名格式必须与 log_archive 期望的 `<base>-YYYY-MM-DD.log` 一致，
    /// 否则归档扫描无法识别散日志。这是 main 与 log_archive 的契约点。
    #[test]
    fn daily_log_path_format_matches_archive() {
        let p = daily_log_path("./logs", "all.log", chrono_tz::Asia::Shanghai);
        let name = p.file_name().unwrap().to_string_lossy();
        // 应形如 all-YYYY-MM-DD.log。
        assert!(
            name.starts_with("all-"),
            "日志文件名 {} 不以 all- 开头",
            name
        );
        assert!(name.ends_with(".log"), "日志文件名 {} 不以 .log 结尾", name);
    }

    /// 验证 all 与 error 两个文件名的 base 去后缀逻辑一致。
    #[test]
    fn daily_log_path_strips_dotlog_suffix() {
        let p = daily_log_path("./logs", "error.log", chrono_tz::UTC);
        let name = p.file_name().unwrap().to_string_lossy();
        assert!(
            name.starts_with("error-") && !name.contains("error.log-"),
            "error 文件名 base 去后缀错误: {}",
            name
        );
    }
}


//! # gpu-npu-util-reporter 入口
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
use gpu_npu_util_reporter::{config, log_archive, mapping, scheduler, sink, source, sql_gen};

use clap::Parser;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;

/// 命令行参数。
#[derive(Parser, Debug)]
#[command(
    name = "gpu-npu-util-reporter",
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
        "gpu-npu-util-reporter 启动"
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
    // 行内关联键：取首个 mapping source 的 src_key（多 source 共用一个 key 的简化）。
    let mapping_src_key = cfg.mapping.sources.first().map(|m| m.src_key.clone());
    let mapping_sources = Arc::new(cfg.mapping.sources.clone());

    // 启动采集调度（含每源采集任务 + 保留期清理任务）。
    let cfg_arc = Arc::new(cfg.clone());
    let client_factory = |url: &str, timeout: u64| {
        source::PrometheusClient::new(url, timeout)
            .unwrap_or_else(|e| panic!("构建 HTTP 客户端失败 ({}): {}", url, e.0))
    };
    let handles = scheduler::run(
        cfg_arc.clone(),
        sink.clone(),
        client_factory,
        asset_indices,
        mapping_sources,
        mapping_src_key,
    );

    // 启动日志归档后台任务。
    let log_cfg = cfg_arc.logging.clone();
    let log_handle = tokio::spawn(log_archive::run_loop(
        PathBuf::from(&log_cfg.dir),
        log_cfg.archive_after_days,
        3600, // 归档扫描间隔（秒）
        log_cfg.archive_prefix.clone(),
        log_cfg.all_file.clone(),
        log_cfg.error_file.clone(),
        tz,
    ));

    // 优雅退出：等待 Ctrl+C。
    tracing::info!("已启动所有采集与维护任务，等待 Ctrl+C 退出");
    tokio::signal::ctrl_c().await.ok();
    tracing::info!("收到退出信号，正在停止任务...");
    for h in handles {
        h.abort();
    }
    log_handle.abort();
    tracing::info!("已停止，退出");
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

/// 一个按日切分、可写 all 与 error 两文件(按级别)的 MakeWriter。
///
/// 每次 make_writer 用一个按当前日期算路径的 writer；为避免持有过多句柄，
/// 这里用 `Box<dyn Write>` 让 tracing 接管生命周期。
struct DailyFileWriter {
    dir: String,
    all_base: String,
    err_base: String,
    tz: chrono_tz::Tz,
    /// 已确认目录存在的标记，避免每条日志都 stat。
    dir_ready: Arc<Mutex<bool>>,
}

impl<'a> MakeWriter<'a> for DailyFileWriter {
    // 用 Box 让 tracing 在自己的缓冲生命周期里持有 writer。
    // 注意：这里不能加 'a 约束（Writer 须 owned/Send），故返回独立分配的 writer。
    type Writer = Box<dyn Write + Send + 'a>;

    fn make_writer(&'a self) -> Self::Writer {
        self.ensure_dir();
        let all = daily_log_path(&self.dir, &self.all_base, self.tz);
        match AppendWriter::open(&all) {
            Ok(a) => Box::new(TeeWriter::new(a, None::<AppendWriter>)),
            Err(_) => Box::new(io::sink()),
        }
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        self.ensure_dir();
        let all = daily_log_path(&self.dir, &self.all_base, self.tz);
        let err = daily_log_path(&self.dir, &self.err_base, self.tz);
        // 完整文件始终写；错误文件仅 ERROR 写。
        let all_w = AppendWriter::open(&all);
        let err_w = if *meta.level() == tracing::Level::ERROR {
            AppendWriter::open(&err)
        } else {
            Err(io::Error::other("not error level"))
        };
        match (all_w, err_w) {
            (Ok(a), Ok(e)) => Box::new(TeeWriter::new(a, Some(e))),
            (Ok(a), Err(_)) => Box::new(TeeWriter::new(a, None::<AppendWriter>)),
            // 主文件都打不开时退化为丢弃（不 panic，避免日志拖垮采集）。
            (Err(_), _) => Box::new(io::sink()),
        }
    }
}

impl DailyFileWriter {
    fn ensure_dir(&self) {
        let mut ready = self.dir_ready.lock().unwrap();
        if *ready {
            return;
        }
        let _ = std::fs::create_dir_all(&self.dir);
        *ready = true;
    }
}

/// 以 append 方式打开文件（不存在则创建）。
struct AppendWriter(std::fs::File);

impl AppendWriter {
    fn open(path: &PathBuf) -> io::Result<Self> {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self(f))
    }
}

impl Write for AppendWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

/// 把输出同时写到 all 与(可选的)error 文件。
struct TeeWriter<A: Write, B: Write> {
    a: A,
    b: Option<B>,
}

impl<A: Write, B: Write> TeeWriter<A, B> {
    fn new(a: A, b: Option<B>) -> Self {
        Self { a, b }
    }
}

impl<A: Write, B: Write> Write for TeeWriter<A, B> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.a.write(buf)?;
        if let Some(b) = self.b.as_mut() {
            let _ = b.write_all(buf); // error 文件写失败不阻塞主文件
        }
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.a.flush()?;
        if let Some(b) = self.b.as_mut() {
            let _ = b.flush();
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
        all_base: cfg.logging.all_file.clone(),
        err_base: cfg.logging.error_file.clone(),
        tz,
        dir_ready: Arc::new(Mutex::new(false)),
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
    // libc::isatty(1)：1 = stdout。避免引入额外 isatty 依赖。
    unsafe { libc::isatty(1) == 1 }
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


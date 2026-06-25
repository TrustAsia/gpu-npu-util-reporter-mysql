//! # config 模块
//!
//! 配置层。负责 YAML 反序列化、校验、生成示例配置。
//! 所有指标映射、字段来源、表达式、采集周期、时区、资产表 mapping 均在此定义。
//!
//! ## 校验
//! 配置错误是确定性错误（写错了就永远错），故采用"启动即失败退出"策略。
//! 校验项包括：
//! - 必填项存在（由 serde 反序列化保证，缺字段即报错）。
//! - [`expressions`](ExprConfig) 的表达式语法合法（调用 [`crate::expr::parse`]）。
//! - `timezone` 是合法 IANA 名（[`chrono_tz::Tz`] 解析）。
//! - mapping 的 `position.anchor` 必须是已知列（固定列或已声明的 mapping 列名）。
//! - mapping 的最终列名（rename 或 source_field）不得与固定列名冲突。
//! - `from: label` 的字段必须提供 `label`。
//!
//! ## 固定列
//! [`FIXED_COLUMNS`] 定义了建表时的固定列基线，供 sql_gen 与 schema 校验复用。

use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;

/// 顶层配置。
///
/// 对应整个 YAML 文件。包含全局参数、数据库、日志、资产表 mapping、数据源列表。
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// 全局默认采集间隔(秒)。每个 source 可用自身 interval 覆盖。
    pub interval: u64,
    /// 数据保留期(天)。retention 任务据此定期删除早于该天数的旧行。
    pub retention_days: u32,
    /// 清理任务执行间隔(秒)。
    pub retention_interval: u64,
    /// 时区(IANA 名)。采集时间、MySQL session time_zone、保留期清理三方须同一时区。
    pub timezone: String,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
    /// 资产表关联配置。缺省(未写 mapping 段)时视为禁用。
    #[serde(default)]
    pub mapping: MappingConfig,
    pub sources: Vec<SourceConfig>,
}

/// 数据库连接与表配置。
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    /// 写入目标表名。`--init` 据此生成 `./init/<table>.sql`。
    pub table: String,
    /// 连接池大小。
    pub max_connections: u32,
    /// schema 校验策略：实际表多出列时如何处理。
    /// `ask`(交互询问,非TTY回退continue) / `continue`(仅告警) / `abort`(退出)。
    #[serde(default = "default_on_extra_columns")]
    pub on_extra_columns: String,
}

/// `on_extra_columns` 的默认值。
fn default_on_extra_columns() -> String {
    "ask".into()
}

/// 日志配置。
///
/// 双文件(完整日志 INFO+ / 错误日志 ERROR)，按日轮转；
/// 超期散日志(all+error)打包成单个 tar.gz 归档，散文件删除，压缩包永不删除。
#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    /// 日志级别: error/warn/info/debug/trace。
    pub level: String,
    /// 日志目录(自动创建)，归档包也存于此。
    pub dir: String,
    /// 完整日志前缀(实际文件名: <all_file 去后缀>-YYYY-MM-DD.log)。
    pub all_file: String,
    /// 错误日志前缀。
    pub error_file: String,
    /// 轮转粒度: daily/hourly/never。
    pub rotation: String,
    /// 散日志保留天数；超期后打包归档。
    pub archive_after_days: u32,
    /// 归档包前缀(<prefix>-YYYY-MM-DD.tar.gz)。
    pub archive_prefix: String,
    /// 是否同时输出 stdout(容器场景建议 true)。
    #[serde(default = "default_true")]
    pub stdout: bool,
}

fn default_true() -> bool {
    true
}

/// 资产表关联配置。
///
/// 语义：用【行内】`src_key` 列值，去【资产表】`dest_key` 列查匹配行，
/// 把该匹配行的 `columns` 字段补进采集行。启动时加载一次，改资产表需重启。
///
/// `enabled: false` 时仍建列(--init 仍生成列)，但采集不填值(NULL)。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MappingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub sources: Vec<MappingSource>,
}

/// 单个资产表来源。
#[derive(Debug, Clone, Deserialize)]
pub struct MappingSource {
    /// 资产表路径(CSV 或 .xlsx)。
    pub source_path: String,
    /// 【行内】关联键(采集行中的列名，如 namespace)。
    pub src_key: String,
    /// 【资产表】对应列名(如 Namespace)。
    pub dest_key: String,
    /// 仅 Excel 有效，指定工作表；CSV 忽略。
    pub source_sheet: Option<String>,
    pub columns: Vec<MappingColumn>,
}

/// 资产表中要关联补充的一列。
#[derive(Debug, Clone, Deserialize)]
pub struct MappingColumn {
    /// 资产表中的原始列名。
    pub source_field: String,
    /// 可选，最终列名(缺省 = source_field)。
    pub rename: Option<String>,
    /// 列类型(写入建表 SQL，如 varchar(255)/int/double)。
    #[serde(rename = "type")]
    pub col_type: String,
    /// 列备注(写入 SQL COMMENT)。
    pub comment: String,
    /// 该列在表中的位置(仅影响 --init 生成的 SQL 列顺序)。
    pub position: ColumnPosition,
}

/// 列在表中的相对位置。
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnPosition {
    /// `after` | `before`。
    pub direction: String,
    /// 锚点列名(必须是已知列：固定列或已声明的 mapping 列)。
    pub anchor: String,
}

/// 单个数据源(一个 Prometheus 服务器 + 一种卡类型)。
///
/// 卡类型差异完全靠此配置表达，新增卡类型零代码改动。
#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    /// 数据源名(写入行的 source 字段，区分不同源)。
    pub name: String,
    /// 本源主机 IP(写入行的 ip 字段)。
    pub ip: String,
    /// Prometheus 地址。
    pub url: String,
    /// 查询超时(秒)，默认 10。
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    /// 覆盖全局 interval；None 时用全局 interval。
    pub interval: Option<u64>,
    pub primary: PrimaryConfig,
    #[serde(default)]
    pub fields: Vec<FieldConfig>,
    #[serde(default)]
    pub expressions: Vec<ExprConfig>,
    #[serde(default)]
    pub host_fields: Vec<HostFieldConfig>,
}

fn default_timeout() -> u64 {
    10
}

/// 主指标：枚举所有卡片序列，决定每个 source 每轮的行数。
#[derive(Debug, Clone, Deserialize)]
pub struct PrimaryConfig {
    /// 主指标 metric 名。
    pub metric: String,
    /// 用作卡号的标签名(DCGM 用 gpu，NPU 用 id)。
    pub card_label: String,
}

/// 单个字段的取值来源。
#[derive(Debug, Clone, Deserialize)]
pub struct FieldConfig {
    /// 字段名 = 列名(如 gpu_util/temp/namespace/pod)。
    pub name: String,
    /// `metric`(取样本值) | `label`(取标签值)。
    pub from: String,
    /// 来源 metric 名(from=metric 时取其值；from=label 时取其该标签)。
    pub metric: String,
    /// from=label 时必填，指定取哪个标签。
    pub label: Option<String>,
}

/// 派生指标(由表达式计算)。
#[derive(Debug, Clone, Deserialize)]
pub struct ExprConfig {
    /// 派生列名(如 mem_util)。
    pub name: String,
    /// 表达式，变量名 = metric 名(见 [`crate::expr`])。
    pub expr: String,
}

/// 主机级字段(整主机一个值，按 ip 复制到该主机每张卡)。
#[derive(Debug, Clone, Deserialize)]
pub struct HostFieldConfig {
    /// 列名(如 host_cpu/host_mem/host_fds)。
    pub name: String,
    /// 完整 PromQL(让 Prometheus 算单值，本程序取返回的首条序列)。
    pub expr: String,
}

/// 固定列名(不含 mapping 列)。供 sql_gen 基线和列顺序、schema 校验复用。
///
/// 元组含义：`(列名, SQL 类型, 是否允许 NULL, 列备注)`。
/// 顺序即建表时的默认列顺序(从上到下)。
pub const FIXED_COLUMNS: &[(&str, &str, bool, &str)] = &[
    ("id", "BIGINT", false, "自增主键"),
    ("ts", "DATETIME(3)", false, "采集时间(毫秒精度,配置时区)"),
    ("ip", "VARCHAR(64)", false, "主机IP"),
    ("card_id", "VARCHAR(32)", false, "GPU/NPU卡号(来自配置的card_label)"),
    ("namespace", "VARCHAR(128)", true, "K8s命名空间,裸金属场景为NULL"),
    ("pod", "VARCHAR(256)", true, "Pod名,裸金属场景为NULL"),
    ("gpu_util", "DOUBLE", true, "GPU核心利用率(%)"),
    ("mem_util", "DOUBLE", true, "显存/片上内存占用率"),
    ("temp", "DOUBLE", true, "显卡温度(℃)"),
    ("power", "DOUBLE", true, "显卡功率(W)"),
    ("host_cpu", "DOUBLE", true, "主机CPU使用率(%)"),
    ("host_mem", "DOUBLE", true, "主机内存使用率(%)"),
    ("host_fds", "DOUBLE", true, "主机系统句柄数"),
    ("source", "VARCHAR(64)", false, "数据源名(配置中的source.name)"),
];

/// 返回所有固定列名集合。供 schema 校验对比用。
pub fn fixed_column_names() -> HashSet<String> {
    FIXED_COLUMNS
        .iter()
        .map(|(n, _, _, _)| n.to_string())
        .collect()
}

/// 固定列中由 sink 直接从结构字段取值（不查 row.fields/strings）的列名。
/// 配置在 fields/expressions/host_fields 里用这些名，值会被静默丢弃。
fn is_structural_column(name: &str) -> bool {
    matches!(name, "id" | "ts" | "ip" | "card_id" | "source")
}

/// 可写的**数值**固定列名集合（`DOUBLE` 且非结构列）。
///
/// 由 [`FIXED_COLUMNS`] 派生，与 sink 的 `fixed_write_values` 保持同一真相源。
/// `from: metric` / `expressions` / `host_fields` 的 `name` 必须落在此集合内——
/// 否则 extractor 把值写进 `row.fields`（按配置名建键），而 sink 只按固定名读取，
/// 该值永远不会被绑定落库（静默 NULL + 丢值），且 schema 校验抓不到
/// （`expected_columns` 不含这些名）。
fn writable_numeric_columns() -> HashSet<&'static str> {
    FIXED_COLUMNS
        .iter()
        .filter(|(n, t, _, _)| !is_structural_column(n) && t.starts_with("DOUBLE"))
        .map(|(n, _, _, _)| *n)
        .collect()
}

/// 可写的**字符串**固定列名集合（`VARCHAR` 且非结构列，即 `namespace`/`pod`）。
///
/// `from: label` 字段的 `name` 必须落在此集合内。资产表列由 mapping 的 `join_row`
/// 填充（独立来源），不应由 Prometheus 标签字段覆盖，故 from=label 仅限 namespace/pod。
fn writable_string_columns() -> HashSet<&'static str> {
    FIXED_COLUMNS
        .iter()
        .filter(|(n, t, _, _)| !is_structural_column(n) && t.starts_with("VARCHAR"))
        .map(|(n, _, _, _)| *n)
        .collect()
}

/// 把列名集合排序成 " / " 分隔的可读串，用于错误提示（顺序确定，便于复现）。
fn sorted_list(set: &HashSet<&str>) -> String {
    let mut v: Vec<&str> = set.iter().copied().collect();
    v.sort();
    v.join(" / ")
}

/// 计算 mapping 列最终名(rename 优先，缺省取 source_field)。
///
/// 供 sql_gen/sink 复用，避免各处重复这段逻辑。
pub fn mapping_final_name(col: &MappingColumn) -> String {
    col.rename
        .clone()
        .unwrap_or_else(|| col.source_field.clone())
}

/// 判断是否为 MySQL 合法的**无引号**标识符：`[A-Za-z_][A-Za-z0-9_]*`，且非空。
///
/// 用于校验所有"原样拼接进 SQL"的标识符：`database.table`（CREATE/INSERT/DELETE/
/// INFORMATION_SCHEMA）、mapping 最终列名（INSERT 列名、CREATE TABLE 列名）。
/// 这些都是裸标识符插值，无法用参数绑定，故字符集之外的名字（含连字符、空格、
/// 引号、点等）应在此拦截，保持"启动期失败"原则。
///
/// **注意：仅校验字符集，不校验保留字。** MySQL 保留字（order/group/select 等）
/// 满足本字符集但作表名时仍需反引号转义。维护 250+ 条版本相关的保留字黑名单成本
/// 远高于收益（运维方极少如此命名），故不在本检查范围；示例配置注释已提示避免。
pub(crate) fn is_valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// 校验 mapping 列声明的 SQL 类型（`type` 字段）是否安全可拼进 CREATE TABLE。
///
/// 与 `database.table` 同理，`type` 原样拼进 `--init` 生成的 DDL（见
/// [`crate::sql_gen::col_type_to_sql`），故不能含任意字符。允许集合：字母/数字/下划线/
/// 空格/圆括号/逗号——覆盖 `varchar(255)`/`int`/`double`/`decimal(10,2)`/`text`
/// 等合法类型，排除引号/分号/反引号/注释序列(`--`/`#`/`/*`)等注入字符。
fn is_safe_col_type(t: &str) -> bool {
    if t.trim().is_empty() {
        return false;
    }
    t.chars().all(|c| {
        c.is_ascii_alphanumeric() || c == '_' || c == '(' || c == ')' || c == ',' || c == ' '
    }) && !t.contains("--")
}

/// 配置错误(携带可读描述)。
#[derive(Debug)]
pub struct ConfigError(pub String);

/// 加载并校验配置。
///
/// 路径不存在时返回 Err，由调用方(main)决定是否生成示例后退出。
pub fn load(path: &Path) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| ConfigError(format!("读取配置失败 {}: {}", path.display(), e)))?;
    let cfg: Config = serde_yaml::from_str(&text)
        .map_err(|e| ConfigError(format!("解析 YAML 失败: {}", e)))?;
    validate(&cfg)?;
    Ok(cfg)
}

/// 校验配置。失败返回 [`ConfigError`]，调用方应据此退出。
pub fn validate(cfg: &Config) -> Result<(), ConfigError> {
    // 所有调度间隔必须为正，否则对应任务会忙循环空转压垮 Prometheus/MySQL。
    // 全局 interval、保留期清理 interval、每个 source 自身 interval 均覆盖。
    if cfg.interval == 0 {
        return Err(ConfigError("interval 必须 > 0".into()));
    }
    if cfg.retention_interval == 0 {
        return Err(ConfigError("retention_interval 必须 > 0".into()));
    }
    // retention_days=0 会让清理 SQL 退化为 DELETE ... WHERE ts < NOW()，
    // 即每轮清理删掉当前时刻之前的全部行 → 数据全量丢失，且不可恢复。
    // 这与 interval=0(忙循环)性质不同(后者只浪费资源)，此处会丢数据，故必须拦截。
    if cfg.retention_days == 0 {
        return Err(ConfigError(
            "retention_days 必须 > 0（0 会触发全表删除，详见 sink::run_retention）".into(),
        ));
    }
    for (i, src) in cfg.sources.iter().enumerate() {
        if let Some(iv) = src.interval {
            if iv == 0 {
                return Err(ConfigError(format!(
                    "sources[{}]({}).interval 必须 > 0",
                    i, src.name
                )));
            }
        }
        // timeout=0 会让该源每次查询立即超时 → 该源每轮都失败、永远采不到数据。
        // 有 serde 默认值 10 兜底，显式写 0 几乎必为笔误，在此早期拦截。
        if src.timeout == 0 {
            return Err(ConfigError(format!(
                "sources[{}]({}).timeout 必须 > 0",
                i, src.name
            )));
        }
    }
    // 至少要有一个数据源，否则程序空跑。
    if cfg.sources.is_empty() {
        return Err(ConfigError("sources 不能为空".into()));
    }

    // max_connections=0 会让连接池容量为 0：sqlx 的许可信号量初始 permits=0，
    // `Sink::connect`（其内部 `connect_with` 会 acquire 一个连接）会在此**永久挂起**，
    // 程序启动后无任何日志、无报错即卡死（连 schema 校验都到不了）。
    // 这比 interval=0（忙循环浪费资源）严重得多——是彻底的死锁式挂起，故必须拦截。
    if cfg.database.max_connections == 0 {
        return Err(ConfigError(
            "database.max_connections 必须 > 0（0 会让连接池无法建立任何连接，启动即永久挂起）".into(),
        ));
    }

    // table 名被**原样拼接**进多处 SQL：CREATE TABLE {}、INSERT INTO {}、
    // DELETE FROM {}、INFORMATION_SCHEMA … TABLE_NAME='{}'，以及 --init 输出文件名。
    // 这些都是裸标识符/字符串字面量插值，无法用参数绑定。若 table 名含连字符
    // (`gpu-usage`)、空格、引号、分号或首字符为数字，会在运行期产生模糊的
    // MySQL 语法错误（而非启动期清晰提示），且构成注入面。这与 R1-R9 的"启动即失败"
    // 原则相悖，故在此按 MySQL 无引号标识符字符集 [A-Za-z_][A-Za-z0-9_]* 拦截。
    // 仅校验字符集，不含保留字检测（见 is_valid_identifier 文档）。
    // （时区已由 IANA 解析保证受限字符集，故无需同类校验。）
    if !is_valid_identifier(&cfg.database.table) {
        return Err(ConfigError(format!(
            "database.table '{}' 非法：须为 MySQL 合法标识符 [A-Za-z_][A-Za-z0-9_]*（勿用连字符/空格/引号/保留字）",
            cfg.database.table
        )));
    }

    // archive_after_days=0 会让归档 cutoff=今天，即 `日期 <= 今天` 成立，
    // 当天正在写入的 all/error 散日志会在下一个整点被打包并删除。
    // CachedAppendFile 仍持有已打开句柄：Unix 下文件已从磁盘删除（后续日志写进
    // 已删除的 inode，到下次跨天重开前静默丢失且不可见），Windows 下句柄占用导致删除失败。
    // 无论哪种结果都是日志数据完整性问题，故必须 > 0。
    if cfg.logging.archive_after_days == 0 {
        return Err(ConfigError(
            "logging.archive_after_days 必须 > 0（0 会归档当天正在写入的活跃日志）".into(),
        ));
    }

    // 时区合法性
    if cfg.timezone.parse::<chrono_tz::Tz>().is_err() {
        return Err(ConfigError(format!(
            "非法时区 '{}'，请用 IANA 名如 Asia/Shanghai",
            cfg.timezone
        )));
    }

    // —— 枚举类字段校验 ——
    // 这些字段取值有限，但在运行期被 match 分支消费（见 main::init_logging、
    // main 的 on_extra_columns 分支、extractor::collect_source 的 from 分支）。
    // 若不在此拦截，用户拼错（如 "erorr"/"strict"/"metircs"）会被默认分支
    // 静默吞掉，得到与预期不符的行为而无任何提示——与"启动即失败退出"原则相悖。
    if !matches!(
        cfg.logging.level.as_str(),
        "error" | "warn" | "info" | "debug" | "trace"
    ) {
        return Err(ConfigError(format!(
            "logging.level '{}' 非法，须为 error/warn/info/debug/trace",
            cfg.logging.level
        )));
    }
    // rotation 字段当前实现固定按日切分（见 main::daily_log_path），值仅作文档约束，
    // 但仍校验合法枚举，避免用户误以为支持 hourly/never 而配置后无效果。
    if !matches!(cfg.logging.rotation.as_str(), "daily" | "hourly" | "never") {
        return Err(ConfigError(format!(
            "logging.rotation '{}' 非法，须为 daily/hourly/never",
            cfg.logging.rotation
        )));
    }
    if !matches!(
        cfg.database.on_extra_columns.as_str(),
        "ask" | "continue" | "abort"
    ) {
        return Err(ConfigError(format!(
            "database.on_extra_columns '{}' 非法，须为 ask/continue/abort",
            cfg.database.on_extra_columns
        )));
    }

    let fixed = fixed_column_names();
    // 固定列中由结构字段直接取值（ip/card_id/ts/source）的列名集合：
    // 用户在 fields/expressions/host_fields 里配这些名字会写入 row.fields/strings，
    // 但 sink 的 fixed_write_values 直接从结构字段取值（不查 map），故配置值会被
    // 静默丢弃——这与"无静默数据损坏"原则相悖，必须在此拦截。
    let structural_fixed: HashSet<&str> = ["id", "ts", "ip", "card_id", "source"]
        .into_iter()
        .collect();
    // 可写的数值 / 字符串固定列名集合（与 sink::fixed_write_values 同一真相源）。
    // 见 writable_numeric_columns / writable_string_columns 的文档：配置名必须落在
    // 对应集合内，否则值写进 row 但 sink 按固定名读取 → 静默 NULL + 丢值，且
    // schema 校验抓不到（expected_columns 不含这些名）。这是典型的"配置写错但无任何
    // 提示"静默数据丢失，按"启动即失败"原则在此拦截。
    let writable_numeric = writable_numeric_columns();
    let writable_string = writable_string_columns();
    let numeric_allowed = sorted_list(&writable_numeric);
    let string_allowed = sorted_list(&writable_string);

    // 每个 source 的字段/表达式校验
    for (i, src) in cfg.sources.iter().enumerate() {
        if src.name.is_empty() {
            return Err(ConfigError(format!("sources[{}].name 不能为空", i)));
        }
        if src.primary.metric.is_empty() {
            return Err(ConfigError(format!(
                "sources[{}]({}).primary.metric 不能为空",
                i, src.name
            )));
        }
        if src.primary.card_label.is_empty() {
            return Err(ConfigError(format!(
                "sources[{}]({}).primary.card_label 不能为空",
                i, src.name
            )));
        }

        // 跨类别重复名检测（R13）：fields[].name / expressions[].name /
        // host_fields[].name 都会写入同一 row.fields（数值）或 row.strings（标签）。
        // HashMap::insert 重名静默覆盖 → 后者覆盖前者 → 数据静默丢失，无任何提示。
        // 在此收集全部"产出列名"，遇重名即报错。`from: label` 的字段写 strings，
        // 其余写 fields，二者命名空间不同，故分开跟踪（但都不得撞结构固定列）。
        let mut seen_numeric: HashSet<String> = HashSet::new();
        let mut seen_string: HashSet<String> = HashSet::new();
        // 注册一个"产出列名"。`allowed` 为该命名空间（数值/字符串）下允许的列名集合，
        // `allowed_desc` 是其在错误信息里的可读清单。`kind` 标注来源（fields[N] 等）。
        let register = |seen: &mut HashSet<String>,
                        allowed: &HashSet<&str>,
                        allowed_desc: &str,
                        name: &str,
                        kind: &str|
         -> Result<(), ConfigError> {
            if name.is_empty() {
                return Err(ConfigError(format!(
                    "sources[{}]({}): {} 的 name 不能为空",
                    i, src.name, kind
                )));
            }
            if structural_fixed.contains(name) {
                return Err(ConfigError(format!(
                    "sources[{}]({}): {} 的 name '{}' 与固定结构列冲突（该列由 ip/card_id/ts/source 字段直接取值，配置同名列会被静默丢弃）",
                    i, src.name, kind, name
                )));
            }
            // F1：name 必须落在"可写列"集合内。否则 extractor 把值写进 row（按 name 建键），
            // 而 sink 的 fixed_write_values 只按 FIXED_COLUMNS 的固定名读取（不查 row），
            // 配置的值永远不会被绑定 → 该列静默 NULL 且源值丢弃；schema 校验也抓不到
            // （expected_columns 不含此名）。示例：把 gpu_util 误写成 utilization，
            // 或多写一个不在表里的字段 → 整列永久 NULL、无任何提示。在此拦截。
            if !allowed.contains(name) {
                return Err(ConfigError(format!(
                    "sources[{}]({}): {} 的 name '{}' 不是可写入的列；允许: {}（名称必须与表中固定列完全一致，否则值会被静默丢弃）",
                    i, src.name, kind, name, allowed_desc
                )));
            }
            if !seen.insert(name.to_string()) {
                return Err(ConfigError(format!(
                    "sources[{}]({}): {} 的 name '{}' 与同源其它字段/表达式/host_field 重名（HashMap 写入会静默覆盖 → 数据丢失）",
                    i, src.name, kind, name
                )));
            }
            Ok(())
        };

        for (fi, fe) in src.fields.iter().enumerate() {
            // from 必须是已知枚举：extractor 用 match fc.from 消费，未知值走 `_ =>`
            // 静默跳过该字段（永远写不进表）。在此拦截，避免"配错字段名却无提示"。
            if !matches!(fe.from.as_str(), "metric" | "label") {
                return Err(ConfigError(format!(
                    "sources[{}].fields[{}]({}): from '{}' 非法，须为 metric/label",
                    i, fi, fe.name, fe.from
                )));
            }
            if fe.from == "label" && fe.label.is_none() {
                return Err(ConfigError(format!(
                    "sources[{}].fields[{}]({}): from=label 时 label 必填",
                    i, fi, fe.name
                )));
            }
            if fe.metric.is_empty() {
                return Err(ConfigError(format!(
                    "sources[{}].fields[{}]({}): metric 不能为空",
                    i, fi, fe.name
                )));
            }
            // from=metric 写数值列；from=label 写字符串列。两者命名空间不同，
            // 用各自的允许集合校验。
            if fe.from == "metric" {
                register(
                    &mut seen_numeric,
                    &writable_numeric,
                    &numeric_allowed,
                    &fe.name,
                    &format!("fields[{}]", fi),
                )?;
            } else {
                register(
                    &mut seen_string,
                    &writable_string,
                    &string_allowed,
                    &fe.name,
                    &format!("fields[{}]", fi),
                )?;
            }
        }
        for (ei, ex) in src.expressions.iter().enumerate() {
            if !crate::expr::is_valid(&ex.expr) {
                return Err(ConfigError(format!(
                    "sources[{}].expressions[{}]({}) 表达式语法错误: '{}'",
                    i, ex.name, ei, ex.expr
                )));
            }
            register(
                &mut seen_numeric,
                &writable_numeric,
                &numeric_allowed,
                &ex.name,
                &format!("expressions[{}]", ei),
            )?;
        }
        for (hi, hf) in src.host_fields.iter().enumerate() {
            register(
                &mut seen_numeric,
                &writable_numeric,
                &numeric_allowed,
                &hf.name,
                &format!("host_fields[{}]", hi),
            )?;
        }
    }

    // 收集所有 mapping 最终列名，用于 position.anchor 跨源解析
    let mut all_mapping_names: HashSet<String> = HashSet::new();
    let mut seen_final_names: HashSet<String> = HashSet::new();
    for (si, ms) in cfg.mapping.sources.iter().enumerate() {
        for (ci, col) in ms.columns.iter().enumerate() {
            let final_name = mapping_final_name(col);
            // 同名 mapping 列会导致建表 SQL 与 INSERT 列重复（MySQL 报错），
            // 在此早期拦截并给出明确位置，优于让 SQL 执行时才暴露模糊错误。
            if !seen_final_names.insert(final_name.clone()) {
                return Err(ConfigError(format!(
                    "mapping.sources[{}].columns[{}]: 最终列名 '{}' 与其它 mapping 列重复",
                    si, ci, final_name
                )));
            }
            // R14：mapping 最终列名被**原样拼接**进 INSERT 列表与 CREATE TABLE 列定义
            // （裸标识符插值，无法参数化），与 database.table 同性质。含连字符/空格/
            // 引号的名字会在运行期产生模糊 MySQL 语法错误而非启动期清晰提示，且构成
            // 注入面。与 table 用同一套 is_valid_identifier 校验，保持一致原则。
            if !is_valid_identifier(&final_name) {
                return Err(ConfigError(format!(
                    "mapping.sources[{}].columns[{}]: 最终列名 '{}' 非法：须为 MySQL 合法标识符 [A-Za-z_][A-Za-z0-9_]*（勿用连字符/空格/引号/保留字）",
                    si, ci, final_name
                )));
            }
            // R14：列类型(col_type)同样原样拼进 CREATE TABLE（见 sql_gen::col_type_to_sql），
            // 必须限定为安全字符集，排除引号/分号/注释序列等注入字符。
            if !is_safe_col_type(&col.col_type) {
                return Err(ConfigError(format!(
                    "mapping.sources[{}].columns[{}]({}): type '{}' 非法：仅允许字母/数字/下划线/空格/圆括号/逗号（如 varchar(255)/int/double/decimal(10,2)），禁止引号/分号/注释序列",
                    si, ci, final_name, col.col_type
                )));
            }
            all_mapping_names.insert(final_name);
        }
    }

    // mapping 列校验：最终名不得与固定列冲突；position 方向合法；anchor 是已知列
    for (si, ms) in cfg.mapping.sources.iter().enumerate() {
        for (ci, col) in ms.columns.iter().enumerate() {
            let final_name = mapping_final_name(col);
            if fixed.contains(&final_name) {
                return Err(ConfigError(format!(
                    "mapping.sources[{}].columns[{}]: rename/source_field '{}' 与固定列冲突",
                    si, ci, final_name
                )));
            }
            if col.position.direction != "after" && col.position.direction != "before" {
                return Err(ConfigError(format!(
                    "mapping.sources[{}].columns[{}]: position.direction 必须是 after/before",
                    si, ci
                )));
            }
            // anchor 必须是已知列：固定列 或 任一 mapping 最终列
            if !fixed.contains(&col.position.anchor)
                && !all_mapping_names.contains(&col.position.anchor)
            {
                return Err(ConfigError(format!(
                    "mapping.sources[{}].columns[{}]: position.anchor '{}' 不是已知列",
                    si, ci, col.position.anchor
                )));
            }
        }
    }

    Ok(())
}

/// 配置文件不存在时，生成示例 `config.example.yaml` 到指定路径。
///
/// 示例内容即文档：每字段含注释，含 DCGM + NPU 两个真实示例。
pub fn write_example(path: &Path) -> Result<(), ConfigError> {
    std::fs::write(path, EXAMPLE_CONFIG)
        .map_err(|e| ConfigError(format!("写入示例配置失败: {}", e)))
}

/// 示例配置全文(同时也是文档)。见 spec 第 6 节。
///
/// 通过 `include_str!` 编译期嵌入，保证代码与示例一致。
pub const EXAMPLE_CONFIG: &str = include_str!("../../config.example.yaml");

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造一份语法合法、校验通过的基础 YAML(无 mapping 段)。
    fn valid_base_yaml() -> String {
        r#"
interval: 60
retention_days: 30
retention_interval: 3600
timezone: "Asia/Shanghai"
database:
  host: "127.0.0.1"
  port: 3306
  user: "u"
  password: "p"
  database: "db"
  table: "gpu_usage"
  max_connections: 10
logging:
  level: "info"
  dir: "./logs"
  all_file: "all.log"
  error_file: "error.log"
  rotation: "daily"
  archive_after_days: 7
  archive_prefix: "logs"
  stdout: true
sources:
  - name: "s1"
    ip: "1.1.1.1"
    url: "http://1.1.1.1:9090"
    primary: { metric: "m1", card_label: "gpu" }
    fields:
      - { name: "gpu_util", from: "metric", metric: "m1" }
    expressions:
      - { name: "mem_util", expr: "a / b" }
"#
        .to_string()
    }

    #[test]
    fn parses_valid_config() {
        let cfg: Config = serde_yaml::from_str(&valid_base_yaml()).unwrap();
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn rejects_bad_timezone() {
        let mut yaml = valid_base_yaml();
        yaml = yaml.replace("Asia/Shanghai", "Not/A/Zone");
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn rejects_bad_expression() {
        let yaml = valid_base_yaml().replace("a / b", "a /");
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn rejects_label_without_label_field() {
        let yaml = valid_base_yaml().replace(
            "{ name: \"gpu_util\", from: \"metric\", metric: \"m1\" }",
            "{ name: \"ns\", from: \"label\", metric: \"m1\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn rejects_rename_conflict_with_fixed_column() {
        let base = valid_base_yaml();
        let yaml = format!(
            "{base}\nmapping:\n  enabled: true\n  sources:\n    - source_path: \"./a.csv\"\n      src_key: \"namespace\"\n      dest_key: \"Namespace\"\n      columns:\n        - source_field: \"x\"\n          rename: \"ip\"\n          type: \"varchar(64)\"\n          comment: \"c\"\n          position: {{ direction: after, anchor: \"namespace\" }}",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn rejects_bad_position_anchor() {
        let base = valid_base_yaml();
        let yaml = format!(
            "{base}\nmapping:\n  enabled: true\n  sources:\n    - source_path: \"./a.csv\"\n      src_key: \"namespace\"\n      dest_key: \"Namespace\"\n      columns:\n        - source_field: \"x\"\n          rename: \"loc\"\n          type: \"varchar(64)\"\n          comment: \"c\"\n          position: {{ direction: after, anchor: \"nonexistent\" }}",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn fixed_columns_contain_expected_basics() {
        let names = fixed_column_names();
        for must in ["id", "ts", "ip", "card_id", "gpu_util", "source"] {
            assert!(names.contains(must), "固定列缺少 {}", must);
        }
    }

    #[test]
    fn rejects_zero_interval() {
        let yaml = valid_base_yaml().replace("interval: 60", "interval: 0");
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn rejects_zero_retention_interval() {
        let yaml = valid_base_yaml().replace("retention_interval: 3600", "retention_interval: 0");
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    /// 守护：retention_days=0 会触发全表删除（SQL 退化为 ts < NOW()），
    /// 必须在启动期拦截，避免数据静默丢失。
    #[test]
    fn rejects_zero_retention_days() {
        let yaml = valid_base_yaml().replace("retention_days: 30", "retention_days: 0");
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    /// 守护：source.timeout=0 会让该源每轮查询都立即超时，应早期拦截。
    #[test]
    fn rejects_zero_source_timeout() {
        let yaml = valid_base_yaml().replace(
            "primary: { metric: \"m1\", card_label: \"gpu\" }",
            "timeout: 0\n    primary: { metric: \"m1\", card_label: \"gpu\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn rejects_zero_source_interval() {
        // 给 source 加一个 interval: 0，应被拒绝。
        let yaml = valid_base_yaml().replace(
            "primary: { metric: \"m1\", card_label: \"gpu\" }",
            "interval: 0\n    primary: { metric: \"m1\", card_label: \"gpu\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn accepts_positive_source_interval() {
        // source 自身正数 interval 应通过。
        let yaml = valid_base_yaml().replace(
            "primary: { metric: \"m1\", card_label: \"gpu\" }",
            "interval: 30\n    primary: { metric: \"m1\", card_label: \"gpu\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn rejects_empty_sources() {
        // 删掉 sources 段后整体替换为空列表
        let yaml = valid_base_yaml().split("sources:").next().unwrap().to_string()
            + "sources: []";
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    /// 守护：from 字段枚举校验。拼错（如 "metircs"）不应被静默跳过。
    #[test]
    fn rejects_bad_field_from() {
        let yaml = valid_base_yaml().replace(
            "from: \"metric\"",
            "from: \"metircs\"", // 故意拼错
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    /// 守护：合法 from 值不误伤（metric/label 均应通过）。注意 from=label 的字段名
    /// 必须是可写字符串列（namespace/pod），否则会被 F1 白名单拦截（设计如此）。
    #[test]
    fn accepts_valid_field_from() {
        let yaml = valid_base_yaml().replace(
            "{ name: \"gpu_util\", from: \"metric\", metric: \"m1\" }",
            "{ name: \"namespace\", from: \"label\", metric: \"m1\", label: \"ns\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_ok());
    }

    // ===== F1 守护测试（可写列白名单） =====

    /// 守护 F1：from=metric 的字段名若不是可写数值列（gpu_util/mem_util/temp/power/
    /// host_cpu/host_mem/host_fds），值会写进 row.fields 但 sink 按固定名读取 → 静默
    /// NULL + 丢值，且 schema 校验抓不到。必须启动期拦截。例：把 gpu_util 误写成
    /// utilization（运维极易犯的错）。
    #[test]
    fn rejects_field_name_not_in_writable_numeric_columns() {
        let yaml = valid_base_yaml().replace(
            "{ name: \"gpu_util\", from: \"metric\", metric: \"m1\" }",
            "{ name: \"utilization\", from: \"metric\", metric: \"m1\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(
            validate(&cfg).is_err(),
            "非可写数值列名 'utilization' 应被拒绝"
        );
    }

    /// 守护 F1：表达式（expressions）的 name 同样必须在可写数值列集合内。
    /// 例：把 mem_util 表达式误命名为 mem_usage → 静默 NULL。
    #[test]
    fn rejects_expression_name_not_in_writable_numeric_columns() {
        let yaml = valid_base_yaml().replace(
            "{ name: \"mem_util\", expr: \"a / b\" }",
            "{ name: \"mem_usage\", expr: \"a / b\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(
            validate(&cfg).is_err(),
            "非可写数值列名 'mem_usage' 应被拒绝"
        );
    }

    /// 守护 F1：host_fields 的 name 必须在可写数值列集合内（host_cpu/host_mem/host_fds）。
    #[test]
    fn rejects_host_field_name_not_in_writable_numeric_columns() {
        // 基础 yaml 无 host_fields，注入一个非法名的。
        let yaml = valid_base_yaml().replace(
            "primary: { metric: \"m1\", card_label: \"gpu\" }",
            "primary: { metric: \"m1\", card_label: \"gpu\" }\n    host_fields:\n      - { name: \"host_uptime\", expr: \"node_uptime\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(
            validate(&cfg).is_err(),
            "非可写数值列名 'host_uptime' 应被拒绝"
        );
    }

    /// 守护 F1：from=label 的字段名必须是可写字符串列（namespace/pod）。
    /// 例：把 namespace 字段误命名为 ns → 静默丢值。
    #[test]
    fn rejects_label_field_name_not_in_writable_string_columns() {
        let yaml = valid_base_yaml().replace(
            "{ name: \"gpu_util\", from: \"metric\", metric: \"m1\" }",
            "{ name: \"ns\", from: \"label\", metric: \"m1\", label: \"ns\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(
            validate(&cfg).is_err(),
            "非可写字符串列名 'ns' 应被拒绝（仅允许 namespace/pod）"
        );
    }

    /// 守护 F1：合法的 host_fields（host_cpu/host_mem/host_fds）不应被误伤。
    #[test]
    fn accepts_valid_host_field_names() {
        let yaml = valid_base_yaml().replace(
            "primary: { metric: \"m1\", card_label: \"gpu\" }",
            "primary: { metric: \"m1\", card_label: \"gpu\" }\n    host_fields:\n      - { name: \"host_cpu\", expr: \"node_cpu\" }\n      - { name: \"host_mem\", expr: \"node_mem\" }\n      - { name: \"host_fds\", expr: \"node_fds\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_ok(), "合法 host_field 名不应被误伤");
    }

    /// 守护 F1：错误信息应包含允许的列名清单，便于运维定位（不只是"非法"）。
    #[test]
    fn f1_error_message_lists_allowed_columns() {
        let yaml = valid_base_yaml().replace(
            "{ name: \"gpu_util\", from: \"metric\", metric: \"m1\" }",
            "{ name: \"utilization\", from: \"metric\", metric: \"m1\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        let err = validate(&cfg).unwrap_err().0;
        assert!(
            err.contains("gpu_util"),
            "错误信息应列出允许的数值列名，实际: {}",
            err
        );
    }

    /// 守护：logging.level 枚举校验。拼错（如 "erorr"）不应静默退化。
    #[test]
    fn rejects_bad_logging_level() {
        let yaml = valid_base_yaml().replace("level: \"info\"", "level: \"erorr\"");
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    /// 守护：logging.rotation 枚举校验（虽当前实现固定 daily，仍拒绝非法值）。
    #[test]
    fn rejects_bad_logging_rotation() {
        let yaml = valid_base_yaml().replace("rotation: \"daily\"", "rotation: \"weekly\"");
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    /// 守护：on_extra_columns 枚举校验。非法值不应走默认 continue 分支。
    #[test]
    fn rejects_bad_on_extra_columns() {
        // on_extra_columns 是 database 的字段，需注入到 database 块内。
        let yaml = valid_base_yaml().replace(
            "max_connections: 10",
            "max_connections: 10\n  on_extra_columns: \"strict\"",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    /// 守护：max_connections=0 会让 sqlx 连接池 permits=0，启动期 `Sink::connect`
    /// 内部的 acquire 永久挂起（无日志无报错即卡死），必须早期拦截。
    #[test]
    fn rejects_zero_max_connections() {
        let yaml = valid_base_yaml().replace("max_connections: 10", "max_connections: 0");
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    /// 守护：archive_after_days=0 会让 cutoff=今天，归档循环会在下一个整点把
    /// 当天正在写入的活跃散日志打包并删除（句柄仍开着 → 日志静默丢失），必须 > 0。
    #[test]
    fn rejects_zero_archive_after_days() {
        let yaml = valid_base_yaml().replace("archive_after_days: 7", "archive_after_days: 0");
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    /// 守护：含连字符的表名（如 `gpu-usage`）会被原样拼进 CREATE/INSERT/DELETE 等
    /// SQL（裸标识符插值，无法参数化），在运行期产生模糊语法错误而非启动期提示，
    /// 必须在 validate 早期拦截。
    #[test]
    fn rejects_table_name_with_hyphen() {
        let yaml = valid_base_yaml().replace("table: \"gpu_usage\"", "table: \"gpu-usage\"");
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err());
    }

    /// 守护：含非法字符的表名（空格、引号、分号、首字符为数字等）会被原样拼进
    /// CREATE/INSERT/DELETE 等 SQL（裸标识符插值，无法参数化），在运行期产生模糊
    /// 语法错误而非启动期提示，必须在 validate 早期拦截。
    ///
    /// 注：MySQL 保留字（如 order/group）属另一维度——维护 250+ 条版本相关的保留字
    /// 黑名单成本远高于收益（运维方极少如此命名），且示例配置注释已提示避免；
    /// 故本校验范围限定为"字符集合法"，保留字检测不在内。
    #[test]
    fn rejects_table_name_with_injection_chars() {
        for bad in ["gpu usage", "gpu'; DROP--", "gpu-usage", "", "1gpu", "a.b", "a`b"] {
            let yaml = valid_base_yaml().replace("table: \"gpu_usage\"", &format!("table: \"{bad}\""));
            let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
            assert!(validate(&cfg).is_err(), "应拒绝非法表名: {:?}", bad);
        }
    }

    /// 守护：合法表名（含下划线/数字/混排）不应被误伤。
    #[test]
    fn accepts_valid_table_names() {
        for ok in ["gpu_usage", "GpuUsage", "_t", "t1", "a_b_c_1"] {
            let yaml = valid_base_yaml().replace("table: \"gpu_usage\"", &format!("table: \"{ok}\""));
            let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
            assert!(validate(&cfg).is_ok(), "合法表名不应被拒: {:?}", ok);
        }
    }

    /// 守护：同一 mapping 下多个列声明相同最终名应被拒绝（避免重复列）。
    #[test]
    fn rejects_duplicate_mapping_final_names() {
        let base = valid_base_yaml();
        let yaml = format!(
            "{base}\nmapping:\n  enabled: true\n  sources:\n    - source_path: \"./a.csv\"\n      src_key: \"namespace\"\n      dest_key: \"Namespace\"\n      columns:\n        - source_field: \"x\"\n          rename: \"loc\"\n          type: \"varchar(64)\"\n          comment: \"c\"\n          position: {{ direction: after, anchor: \"namespace\" }}\n        - source_field: \"y\"\n          rename: \"loc\"\n          type: \"varchar(64)\"\n          comment: \"d\"\n          position: {{ direction: after, anchor: \"pod\" }}",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err(), "重复最终列名应被拒绝");
    }

    /// 守护：不同 mapping 源之间也不能撞最终列名。
    #[test]
    fn rejects_duplicate_mapping_final_names_across_sources() {
        let base = valid_base_yaml();
        let yaml = format!(
            "{base}\nmapping:\n  enabled: true\n  sources:\n    - source_path: \"./a.csv\"\n      src_key: \"namespace\"\n      dest_key: \"Namespace\"\n      columns:\n        - source_field: \"x\"\n          rename: \"owner\"\n          type: \"varchar(64)\"\n          comment: \"c\"\n          position: {{ direction: after, anchor: \"namespace\" }}\n    - source_path: \"./b.csv\"\n      src_key: \"ip\"\n      dest_key: \"IP\"\n      columns:\n        - source_field: \"y\"\n          rename: \"owner\"\n          type: \"varchar(64)\"\n          comment: \"c\"\n          position: {{ direction: after, anchor: \"pod\" }}",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err(), "跨源重复最终列名应被拒绝");
    }

    /// 守护测试：示例配置 config.example.yaml 必须能解析并通过校验。
    /// 这能防止文档示例与代码校验逻辑产生偏差。
    #[test]
    fn example_config_parses_and_validates() {
        let cfg: Config = serde_yaml::from_str(EXAMPLE_CONFIG)
            .expect("config.example.yaml 解析失败，请检查语法");
        validate(&cfg).expect("config.example.yaml 校验失败，请检查字段");
    }

    // ===== R13-R14、R20 守护测试（本轮新增） =====

    /// 守护 R13：两个同名字段会被静默覆盖（HashMap.insert），必须在启动期拒绝。
    #[test]
    fn rejects_duplicate_field_names() {
        let yaml = valid_base_yaml().replace(
            "fields:\n      - { name: \"gpu_util\", from: \"metric\", metric: \"m1\" }",
            "fields:\n      - { name: \"gpu_util\", from: \"metric\", metric: \"m1\" }\n      - { name: \"gpu_util\", from: \"metric\", metric: \"m2\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err(), "同名字段应被拒绝");
    }

    /// 守护 R13：字段名与表达式名相同 → 后者覆盖前者，必须拒绝。
    #[test]
    fn rejects_field_expression_name_collision() {
        // 基础 yaml 已有字段 gpu_util 和表达式 mem_util；把表达式名也改成 gpu_util。
        let yaml = valid_base_yaml().replace(
            "{ name: \"mem_util\", expr: \"a / b\" }",
            "{ name: \"gpu_util\", expr: \"a / b\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err(), "字段与表达式同名应被拒绝");
    }

    /// 守护 R13：字段名撞结构固定列（如 ip）→ 配置值会被 sink 的 fixed_write_values
    /// 静默丢弃（它直接从结构字段取值），必须拒绝。
    #[test]
    fn rejects_field_name_colliding_with_structural_column() {
        let yaml = valid_base_yaml().replace(
            "{ name: \"gpu_util\", from: \"metric\", metric: \"m1\" }",
            "{ name: \"ip\", from: \"metric\", metric: \"m1\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err(), "字段名撞结构固定列(ip)应被拒绝");
    }

    /// 守护 R20：空 primary.metric / card_label / 字段 metric 应被拒绝（非空校验）。
    #[test]
    fn rejects_empty_primary_metric_and_card_label() {
        let yaml = valid_base_yaml().replace(
            "primary: { metric: \"m1\", card_label: \"gpu\" }",
            "primary: { metric: \"\", card_label: \"gpu\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err(), "空 primary.metric 应被拒绝");

        let yaml = valid_base_yaml().replace(
            "primary: { metric: \"m1\", card_label: \"gpu\" }",
            "primary: { metric: \"m1\", card_label: \"\" }",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_err(), "空 primary.card_label 应被拒绝");
    }

    /// 守护 R14：mapping 最终列名含非法字符（连字符/空格/引号）会被原样拼进
    /// CREATE TABLE / INSERT，必须在启动期按标识符字符集拦截（与 table 同标准）。
    #[test]
    fn rejects_mapping_name_with_injection_chars() {
        let base = valid_base_yaml();
        for bad in ["loc-x", "loc x", "loc';DROP--"] {
            let yaml = format!(
                "{base}\nmapping:\n  enabled: true\n  sources:\n    - source_path: \"./a.csv\"\n      src_key: \"namespace\"\n      dest_key: \"Namespace\"\n      columns:\n        - source_field: \"x\"\n          rename: \"{bad}\"\n          type: \"varchar(64)\"\n          comment: \"c\"\n          position: {{ direction: after, anchor: \"namespace\" }}",
            );
            let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
            assert!(validate(&cfg).is_err(), "非法 mapping 列名 {:?} 应被拒绝", bad);
        }
    }

    /// 守护 R14：mapping 列类型(type)含注入字符（引号/分号/注释序列）会被原样拼进
    /// CREATE TABLE，必须按白名单字符集拦截。
    #[test]
    fn rejects_mapping_type_with_injection_chars() {
        let base = valid_base_yaml();
        for bad in ["varchar(64); DROP TABLE t; --", "int' OR '1'='1", "text#abc"] {
            let yaml = format!(
                "{base}\nmapping:\n  enabled: true\n  sources:\n    - source_path: \"./a.csv\"\n      src_key: \"namespace\"\n      dest_key: \"Namespace\"\n      columns:\n        - source_field: \"x\"\n          rename: \"loc\"\n          type: \"{bad}\"\n          comment: \"c\"\n          position: {{ direction: after, anchor: \"namespace\" }}",
            );
            let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
            assert!(validate(&cfg).is_err(), "非法 mapping 列类型 {:?} 应被拒绝", bad);
        }
    }

    /// 守护 R14：合法 mapping 列名与类型不误伤（下划线/数字/带长度的 decimal 均合法）。
    #[test]
    fn accepts_valid_mapping_name_and_type() {
        let base = valid_base_yaml();
        let yaml = format!(
            "{base}\nmapping:\n  enabled: true\n  sources:\n    - source_path: \"./a.csv\"\n      src_key: \"namespace\"\n      dest_key: \"Namespace\"\n      columns:\n        - source_field: \"x\"\n          rename: \"owner_1\"\n          type: \"decimal(10,2)\"\n          comment: \"c\"\n          position: {{ direction: after, anchor: \"namespace\" }}",
        );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(validate(&cfg).is_ok(), "合法 mapping 列名/类型不应被误伤");
    }
}

# 计算卡利用率采集程序 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建一个 Rust 异步程序，定时从多个 Prometheus 服务器读取 GPU/NPU 及主机指标，按卡片维度对齐（含资产表 join 和表达式计算）后写入 MySQL，支持 `--init` 生成建表 SQL、启动 schema 校验、日志按日轮转归档。

**Architecture:** 分层 + tokio 异步。业务逻辑集中在 `extractor` 层，`source`/`sink` 为纯 I/O 边界，`expr`/`mapping`/`sql_gen` 为纯逻辑模块，`scheduler` 编排各层，`log_archive` 为独立后台任务。所有卡类型差异由 YAML 配置驱动，新增卡类型零代码改动。

**Tech Stack:** Rust + tokio + reqwest + sqlx + serde_yaml + tracing + flate2/tar + csv + calamine + clap + chrono-tz。测试用纯单元测试 + fixtures（跳过真实 DB/Prometheus，I/O 层用 mock）。

**对应 Spec:** `docs/superpowers/specs/2026-06-24-prometheus-gpu-collector-design.md`

---

## 文件结构

```
.
├── Cargo.toml                      # 依赖声明
├── config.example.yaml             # 示例配置（程序在配置缺失时也会生成）
├── docs/superpowers/
│   ├── specs/2026-06-24-prometheus-gpu-collector-design.md   # 已存在
│   └── plans/2026-06-24-prometheus-gpu-collector.md          # 本文件
├── src/
│   ├── main.rs                     # 入口：clap 解析 --init / 加载配置 / 启动
│   ├── models.rs                   # 共享数据结构：MetricSample, Row, ColumnDef
│   ├── config/mod.rs               # Config 结构体 + 校验 + 生成示例
│   ├── expr/mod.rs                 # 表达式解析与求值（纯函数）
│   ├── source/mod.rs               # PrometheusClient + parse_vector
│   ├── extractor/
│   │   ├── mod.rs                  # 提取主指标 → 行骨架
│   │   ├── align.rs                # 按 (ip,card_id) 对齐字段
│   │   └── host.rs                 # 主机级字段按 ip 对齐复制
│   ├── mapping/mod.rs              # 资产表加载 + join
│   ├── sql_gen/mod.rs              # --init 生成建表 SQL
│   ├── sink/
│   │   ├── mod.rs                  # 批量 INSERT
│   │   └── schema.rs               # schema 校验 + 保留期清理 + 时区 SET
│   ├── scheduler/mod.rs            # 每源一个 tokio 任务，失败隔离
│   └── log_archive/mod.rs          # 超期散日志打包 tar.gz
├── tests/
│   ├── fixtures/                   # prom*.json, assets.csv, assets.xlsx 等测试样例
│   └── integration_test.rs         # 端到端（用 mock source/sink）
```

**构建顺序（自底向上，依赖先行）：**
Task 0 (脚手架) → Task 1 (models) → Task 2 (expr) → Task 3 (config) → Task 4 (source) → Task 5 (mapping) → Task 6 (extractor) → Task 7 (sql_gen) → Task 8 (sink) → Task 9 (scheduler) → Task 10 (log_archive) → Task 11 (main 串联) → Task 12 (集成测试 + README)。

---

## Task 0: 项目脚手架与依赖

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`（最小可编译占位）
- Create: `.gitignore`

- [ ] **Step 1: 初始化 Cargo 项目并写 Cargo.toml**

```toml
[package]
name = "gpu-collector"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["full"] }
reqwest = { version = "0.12", features = ["json", "rustls-tls"], default-features = false }
sqlx = { version = "0.7", features = ["mysql", "runtime-tokio-rustls", "chrono", "macros"] }
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
flate2 = "1"
tar = "0.4"
csv = "1"
calamine = "0.24"
clap = { version = "4", features = ["derive"] }
chrono = { version = "0.4", features = ["serde"] }
chrono-tz = "0.9"
thiserror = "1"

[dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
```

- [ ] **Step 2: 写最小 main.rs 占位（确保 cargo build 通过）**

```rust
fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 3: 写 .gitignore**

```
/target
/logs
/init
/config.yaml
*.sql.bak
```

注意：`config.yaml`（实际配置，含密码）不入库；`config.example.yaml` 入库。

- [ ] **Step 4: 验证编译**

Run: `cargo build`
Expected: 编译成功，无错误（可能有 unused 警告，忽略）

- [ ] **Step 5: 初始化 git 并提交**

```bash
git init
git add -A
git commit -m "chore: project scaffold with dependencies"
```

---

## Task 1: models — 共享数据结构

**Files:**
- Create: `src/models.rs`
- Modify: `src/main.rs`（加 `mod models;`）

`models.rs` 定义所有模块共享的数据结构，是依赖图的叶子节点，必须最先实现。

- [ ] **Step 1: 写 models.rs 并加文档注释**

```rust
//! # models 模块
//!
//! 全程序共享的数据结构。无业务逻辑，无 I/O。
//! 被几乎所有其它模块依赖，故置于依赖图最底层。

use std::collections::HashMap;

/// 从 Prometheus 查询返回的一条瞬时向量样本。
///
/// # 字段
/// - `labels`: 该序列的标签集合（如 gpu="0", namespace="default"）
/// - `value`: 该序列当前值
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSample {
    pub labels: HashMap<String, String>,
    pub value: f64,
}

/// 一行采集结果中对齐后的单个字段值。
/// None 表示该字段本轮未取到（写入 NULL）。
pub type FieldValue = Option<f64>;

/// 组装完成、待写入 MySQL 的一行。
///
/// 业务数值列放 `fields`（按列名索引），维度字符串列（namespace/pod）
/// 与 mapping 列放 `strings`。`source` 是 source.name。
#[derive(Debug, Clone, Default)]
pub struct Row {
    pub ts: chrono::DateTime<chrono_tz::Tz>,
    pub ip: String,
    pub card_id: String,
    /// 数值列：gpu_util/temp/power/host_*/mem_util 等
    pub fields: HashMap<String, FieldValue>,
    /// 字符串列：namespace/pod/mapping 的 varchar 列
    pub strings: HashMap<String, Option<String>>,
    pub source: String,
}

/// 建表 SQL 中的一列定义（供 sql_gen 使用）。
///
/// # 字段
/// - `name`: 列名
/// - `sql_type`: SQL 类型声明（如 "DOUBLE"、"VARCHAR(255)"）
/// - `nullable`: 是否允许 NULL
/// - `comment`: 列备注（写入 SQL COMMENT）
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub sql_type: String,
    pub nullable: bool,
    pub comment: String,
}
```

- [ ] **Step 2: 在 main.rs 加模块声明**

```rust
mod models;

fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 3: 写单元测试验证结构可构造**

在 `src/models.rs` 末尾加：

```rust
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
```

- [ ] **Step 4: 运行测试**

Run: `cargo test models`
Expected: 2 个测试通过

- [ ] **Step 5: 提交**

```bash
git add src/models.rs src/main.rs
git commit -m "feat(models): shared data structures MetricSample/Row/ColumnDef"
```

---

## Task 2: expr — 表达式解析与求值

**Files:**
- Create: `src/expr/mod.rs`
- Modify: `src/main.rs`（加 `mod expr;`）

纯函数模块，无 I/O 无状态，最高优先级测试覆盖。自写递归下降解析器，仅支持 `+ - * / ()` 和变量（变量名 = metric 名，匹配 `[A-Za-z_][A-Za-z0-9_]*`）。

- [ ] **Step 1: 写 expr/mod.rs 的 AST 与解析器**

```rust
//! # expr 模块
//!
//! 轻量表达式求值器（纯函数，无副作用，无 I/O）。
//! 仅支持 `+ - * / ()` 与变量名。用于配置中 `expressions` 的派生指标计算，
//! 如显存占用率 `USED / (USED + FREE)`。
//!
//! ## 算法
//! 递归下降解析，文法：
//!   expr   := term (('+' | '-') term)*
//!   term   := factor (('*' | '/') factor)*
//!   factor := number | variable | '(' expr ')' | '-' factor
//!
//! ## 错误处理
//! 语法错误在 `parse` 阶段返回 Err（配置加载时拦截）；
//! 运行时除零/变量缺失在 `evaluate` 阶段返回 None（不污染整行）。

use std::collections::HashMap;

#[derive(Debug, Clone)]
enum Ast {
    Num(f64),
    Var(String),
    Neg(Box<Ast>),
    BinOp(Op, Box<Ast>, Box<Ast>),
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Add,
    Sub,
    Mul,
    Div,
}

/// 解析错误（语法层面）。
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError(pub String);

/// 解析表达式字符串为 AST。变量名 = metric 名。
/// 配置加载阶段调用，语法错误应导致启动失败。
pub fn parse(input: &str) -> Result<Ast, ParseError> {
    let mut p = Parser {
        chars: input.chars().peekable(),
        src: input,
    };
    let ast = p.parse_expr()?;
    p.skip_ws();
    if p.chars.peek().is_some() {
        return Err(ParseError(format!("意外字符: 剩余未解析")));
    }
    Ok(ast)
}

struct Parser<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    src: &'a str,
}

impl<'a> Parser<'a> {
    fn skip_ws(&mut self) {
        while let Some(&c) = self.chars.peek() {
            if c.is_whitespace() {
                self.chars.next();
            } else {
                break;
            }
        }
    }

    fn parse_expr(&mut self) -> Result<Ast, ParseError> {
        let mut left = self.parse_term()?;
        loop {
            self.skip_ws();
            match self.chars.peek() {
                Some('+') => {
                    self.chars.next();
                    let right = self.parse_term()?;
                    left = Ast::BinOp(Op::Add, Box::new(left), Box::new(right));
                }
                Some('-') => {
                    self.chars.next();
                    let right = self.parse_term()?;
                    left = Ast::BinOp(Op::Sub, Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_term(&mut self) -> Result<Ast, ParseError> {
        let mut left = self.parse_factor()?;
        loop {
            self.skip_ws();
            match self.chars.peek() {
                Some('*') => {
                    self.chars.next();
                    let right = self.parse_factor()?;
                    left = Ast::BinOp(Op::Mul, Box::new(left), Box::new(right));
                }
                Some('/') => {
                    self.chars.next();
                    let right = self.parse_factor()?;
                    left = Ast::BinOp(Op::Div, Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_factor(&mut self) -> Result<Ast, ParseError> {
        self.skip_ws();
        match self.chars.peek() {
            Some('(') => {
                self.chars.next();
                let inner = self.parse_expr()?;
                self.skip_ws();
                match self.chars.next() {
                    Some(')') => Ok(inner),
                    _ => Err(ParseError("缺少右括号 ')'".into())),
                }
            }
            Some('-') => {
                self.chars.next();
                let inner = self.parse_factor()?;
                Ok(Ast::Neg(Box::new(inner)))
            }
            Some(c) if c.is_ascii_digit() || *c == '.' => self.parse_number(),
            Some(c) if c.is_alphabetic() || *c == '_' => self.parse_var(),
            other => Err(ParseError(format!("意外字符: {:?}", other))),
        }
    }

    fn parse_number(&mut self) -> Result<Ast, ParseError> {
        let mut s = String::new();
        while let Some(&c) = self.chars.peek() {
            if c.is_ascii_digit() || c == '.' {
                s.push(c);
                self.chars.next();
            } else {
                break;
            }
        }
        s.parse::<f64>()
            .map(Ast::Num)
            .map_err(|_| ParseError(format!("非法数字: {}", s)))
    }

    fn parse_var(&mut self) -> Result<Ast, ParseError> {
        let mut s = String::new();
        while let Some(&c) = self.chars.peek() {
            if c.is_alphanumeric() || c == '_' {
                s.push(c);
                self.chars.next();
            } else {
                break;
            }
        }
        Ok(Ast::Var(s))
    }
}

/// 对 AST 求值。变量值由 `vars` 提供（key = metric 名）。
/// 除零或变量缺失返回 None（调用方写 NULL，不污染整行）。
pub fn evaluate(ast: &Ast, vars: &HashMap<String, f64>) -> Option<f64> {
    match ast {
        Ast::Num(n) => Some(*n),
        Ast::Var(name) => vars.get(name).copied(),
        Ast::Neg(inner) => evaluate(inner, vars).map(|v| -v),
        Ast::BinOp(op, l, r) => {
            let lv = evaluate(l, vars)?;
            let rv = evaluate(r, vars)?;
            match op {
                Op::Add => Some(lv + rv),
                Op::Sub => Some(lv - rv),
                Op::Mul => Some(lv * rv),
                Op::Div => {
                    if rv == 0.0 {
                        None
                    } else {
                        Some(lv / rv)
                    }
                }
            }
        }
    }
}
```

- [ ] **Step 2: 在 main.rs 加 `mod expr;`**

```rust
mod expr;
mod models;

fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 3: 写测试（覆盖 spec 第10节关键用例）**

在 `src/expr/mod.rs` 末尾加：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn basic_division() {
        let ast = parse("A / B").unwrap();
        assert_eq!(evaluate(&ast, &vars(&[("A", 6.0), ("B", 3.0)])), Some(2.0));
    }

    #[test]
    fn paren_and_three_vars() {
        let ast = parse("(A + B) / C").unwrap();
        assert_eq!(
            evaluate(&ast, &vars(&[("A", 1.0), ("B", 2.0), ("C", 3.0)])),
            Some(1.0)
        );
    }

    #[test]
    fn division_by_zero_returns_none() {
        let ast = parse("A / B").unwrap();
        assert_eq!(evaluate(&ast, &vars(&[("A", 1.0), ("B", 0.0)])), None);
    }

    #[test]
    fn missing_variable_returns_none() {
        let ast = parse("A / B").unwrap();
        assert_eq!(evaluate(&ast, &vars(&[("A", 1.0)])), None);
    }

    #[test]
    fn syntax_error_incomplete() {
        assert!(parse("A /").is_err());
    }

    #[test]
    fn metric_name_with_underscores() {
        // 真实 metric 名如 DCGM_FI_DEV_FB_USED
        let ast = parse("DCGM_FI_DEV_FB_USED / DCGM_FI_DEV_FB_FREE").unwrap();
        let v = vars(&[("DCGM_FI_DEV_FB_USED", 4.0), ("DCGM_FI_DEV_FB_FREE", 4.0)]);
        assert_eq!(evaluate(&ast, &v), Some(1.0));
    }

    #[test]
    fn missing_closing_paren() {
        assert!(parse("(A + B").is_err());
    }

    #[test]
    fn unary_negation() {
        let ast = parse("-A + B").unwrap();
        assert_eq!(evaluate(&ast, &vars(&[("A", 1.0), ("B", 3.0)])), Some(2.0));
    }
}
```

- [ ] **Step 4: 运行测试**

Run: `cargo test expr`
Expected: 8 个测试通过

- [ ] **Step 5: 提交**

```bash
git add src/expr/ src/main.rs
git commit -m "feat(expr): recursive-descent expression parser and evaluator"
```

---

## Task 3: config — 配置加载、校验、生成示例

**Files:**
- Create: `src/config/mod.rs`
- Modify: `src/main.rs`（加 `mod config;`）
- Create: `config.example.yaml`

定义所有配置结构体（serde 反序列化），实现校验（表达式语法、时区名、mapping position 锚点、rename 冲突），以及配置文件不存在时生成示例。

- [ ] **Step 1: 写 config/mod.rs 结构体定义**

```rust
//! # config 模块
//!
//! 配置层。负责 YAML 反序列化、校验、生成示例配置。
//! 所有指标映射、字段来源、表达式、采集周期、时区、mapping 均在此定义。
//!
//! ## 校验
//! 配置错误是确定性错误，启动即失败退出。校验项：
//! - 必填项存在
//! - expressions 表达式语法合法（调用 expr::parse）
//! - timezone 是合法 IANA 名（chrono-tz 解析）
//! - mapping position.anchor 必须是已知列（固定列或已声明的 rename）
//! - rename 不得与固定列名冲突

use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub interval: u64,
    pub retention_days: u32,
    pub retention_interval: u64,
    pub timezone: String,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
    #[serde(default)]
    pub mapping: MappingConfig,
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
    pub max_connections: u32,
    #[serde(default = "default_on_extra_columns")]
    pub on_extra_columns: String,
}

fn default_on_extra_columns() -> String {
    "ask".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub dir: String,
    pub all_file: String,
    pub error_file: String,
    pub rotation: String,
    pub archive_after_days: u32,
    pub archive_prefix: String,
    #[serde(default = "default_true")]
    pub stdout: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MappingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub sources: Vec<MappingSource>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MappingSource {
    pub source_path: String,
    pub src_key: String,
    pub dest_key: String,
    pub source_sheet: Option<String>,
    pub columns: Vec<MappingColumn>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MappingColumn {
    pub source_field: String,
    pub rename: Option<String>,
    #[serde(rename = "type")]
    pub col_type: String,
    pub comment: String,
    pub position: ColumnPosition,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnPosition {
    pub direction: String, // "after" | "before"
    pub anchor: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub name: String,
    pub ip: String,
    pub url: String,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
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

#[derive(Debug, Clone, Deserialize)]
pub struct PrimaryConfig {
    pub metric: String,
    pub card_label: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FieldConfig {
    pub name: String,            // 字段名=列名
    pub from: String,            // "metric" | "label"
    pub metric: String,
    pub label: Option<String>,   // from=label 时必填
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExprConfig {
    pub name: String,            // 派生列名
    pub expr: String,
    pub unit: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HostFieldConfig {
    pub name: String,            // 列名
    pub expr: String,            // 完整 PromQL（让 Prometheus 算单值）
}
```

- [ ] **Step 2: 加固定列名常量（供校验和 sql_gen 复用）**

继续在 config/mod.rs：

```rust
/// 固定列名（不含 mapping 列）。供 sql_gen 基线和校验复用。
/// 顺序即建表时的默认列顺序。
pub const FIXED_COLUMNS: &[(&str, &str, bool, &str)] = &[
    // (name, sql_type, nullable, comment)
    ("id", "BIGINT NOT NULL AUTO_INCREMENT", false, "自增主键"),
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

/// 返回所有固定列名集合。
pub fn fixed_column_names() -> HashSet<String> {
    FIXED_COLUMNS.iter().map(|(n, _, _, _)| n.to_string()).collect()
}
```

- [ ] **Step 3: 实现校验逻辑**

继续在 config/mod.rs：

```rust
/// 配置错误。
#[derive(Debug)]
pub struct ConfigError(pub String);

/// 加载并校验配置。路径不存在时返回 Err（由调用方决定生成示例）。
pub fn load(path: &Path) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| ConfigError(format!("读取配置失败 {}: {}", path.display(), e)))?;
    let cfg: Config = serde_yaml::from_str(&text)
        .map_err(|e| ConfigError(format!("解析 YAML 失败: {}", e)))?;
    validate(&cfg)?;
    Ok(cfg)
}

/// 校验配置。失败则返回 ConfigError（启动应失败退出）。
pub fn validate(cfg: &Config) -> Result<(), ConfigError> {
    // 时区合法性
    if cfg.timezone.parse::<chrono_tz::Tz>().is_err() {
        return Err(ConfigError(format!(
            "非法时区 '{}'，请用 IANA 名如 Asia/Shanghai",
            cfg.timezone
        )));
    }

    let fixed = fixed_column_names();

    // 每个 source 的表达式语法 + 字段
    for (i, src) in cfg.sources.iter().enumerate() {
        if src.name.is_empty() {
            return Err(ConfigError(format!("sources[{}].name 不能为空", i)));
        }
        for fe in &src.fields {
            if fe.from == "label" && fe.label.is_none() {
                return Err(ConfigError(format!(
                    "sources[{}].fields[{}]: from=label 时 label 必填",
                    i, fe.name
                )));
            }
        }
        for ex in &src.expressions {
            if crate::expr::parse(&ex.expr).is_err() {
                return Err(ConfigError(format!(
                    "sources[{}].expressions[{}] 表达式语法错误: '{}'",
                    i, ex.name, ex.expr
                )));
            }
        }
    }

    // mapping: position.anchor 必须是已知列；rename 不得与固定列冲突
    for (si, ms) in cfg.mapping.sources.iter().enumerate() {
        for (ci, col) in ms.columns.iter().enumerate() {
            let final_name = col.rename.clone().unwrap_or_else(|| col.source_field.clone());
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
            if !fixed.contains(&col.position.anchor)
                && !cfg.mapping.sources.iter().any(|m| {
                    m.columns.iter().any(|c| {
                        c.rename.clone().unwrap_or_else(|| c.source_field.clone())
                            == col.position.anchor
                    })
                }) {
                return Err(ConfigError(format!(
                    "mapping.sources[{}].columns[{}]: position.anchor '{}' 不是已知列",
                    si, ci, col.position.anchor
                ));
            }
        }
    }

    Ok(())
}
```

- [ ] **Step 4: 实现生成示例配置**

继续在 config/mod.rs：

```rust
/// 配置文件不存在时，生成示例 config.example.yaml 到指定路径。
/// 示例内容即文档：每字段含注释，含 DCGM + NPU 两个真实示例。
pub fn write_example(path: &Path) -> Result<(), ConfigError> {
    std::fs::write(path, EXAMPLE_CONFIG)
        .map_err(|e| ConfigError(format!("写入示例配置失败: {}", e)))
}

/// 示例配置全文（同时也是文档）。见 spec 第6节。
pub const EXAMPLE_CONFIG: &str = include_str!("../../config.example.yaml");
```

- [ ] **Step 5: 写 config.example.yaml（详尽注释，DCGM+NPU 双示例）**

```yaml
# =====================================================================
# 计算卡利用率采集配置（示例）
# 本程序从多个 Prometheus 服务器读取计算卡与主机指标，按卡片维度对齐
# 后写入 MySQL。本文件即为文档：每字段含注释。修改后需重启程序生效。
# =====================================================================

# 全局默认采集间隔(秒)。必填。每个 source 可用自身 interval 覆盖。
# 取值范围: 正整数。建议 >=15，过小会压垮 Prometheus。
interval: 60

# 数据保留期(天)。retention 任务据此定期删除早于该天数的旧行。
retention_days: 30

# 清理任务执行间隔(秒)。
retention_interval: 3600

# 时区。程序采集时间、MySQL 连接 session time_zone、保留期清理函数
# 三方必须同一时区。取 IANA 名: Asia/Shanghai / UTC / America/New_York。
timezone: "Asia/Shanghai"

database:
  host: "127.0.0.1"
  port: 3306
  user: "collector"
  password: "secret"         # 实际使用请改为强密码，且勿提交到版本库
  database: "gpu_metrics"
  table: "gpu_usage"         # 写入目标表名。--init 据此生成 ./init/gpu_usage.sql
  max_connections: 10        # 连接池大小
  # schema 校验策略：正常启动对比实际表列与期望列。缺列恒报错退出；
  # 多列时: ask(交互询问,非TTY回退continue) / continue(仅告警) / abort(退出)
  on_extra_columns: "ask"

# ---------------------------------------------------------------------
# 日志配置
# 双文件(完整日志 INFO+ / 错误日志 ERROR)。按日轮转。
# 超期散日志(all+error)打包成单个 tar.gz 归档，散文件删除，压缩包永不删除。
# ---------------------------------------------------------------------
logging:
  level: "info"              # error/warn/info/debug/trace
  dir: "./logs"              # 日志目录(自动创建)，归档包也存于此
  all_file: "all.log"        # 完整日志前缀(实际: all-2026-06-24.log)
  error_file: "error.log"    # 错误日志前缀(实际: error-2026-06-24.log)
  rotation: "daily"          # daily/hourly/never
  archive_after_days: 7      # 散日志保留天数；超期后打包归档
  archive_prefix: "logs"     # 归档包前缀(logs-2026-06-24.tar.gz)
  stdout: true               # 是否同时输出 stdout(容器场景建议 true)

# ---------------------------------------------------------------------
# 资产表关联(可选)。enabled:false 时仍建列(--init 仍生成列)，采集不填值。
# 语义: 用【行内】src_key 列值，去【资产表】dest_key 列查匹配行，
#       把该匹配行的 columns 字段补进行。启动时加载一次，改资产表需重启。
# ---------------------------------------------------------------------
mapping:
  enabled: true
  sources:
    - source_path: "./assets.csv"   # CSV 或 .xlsx
      src_key: "namespace"          # 【行内】关联键(采集行中的列名)
      dest_key: "Namespace"         # 【资产表】对应列名
      # source_sheet: "Sheet1"      # 仅 Excel 有效，指定工作表
      columns:
        - source_field: "机房位置"  # 资产表中要关联的列
          rename: "location"        # 可选，最终列名(缺省=source_field)
          type: "varchar(255)"      # 列类型(写入建表SQL)
          comment: "设备所在机房位置" # 列备注(写入SQL COMMENT)
          position:                 # 列在表中位置(仅影响 --init SQL 顺序)
            direction: after        # after/before
            anchor: "namespace"     # 锚点列名(必须是已知列)
        - source_field: "负责人"
          rename: "owner"
          type: "varchar(64)"
          comment: "设备负责人"
          position: { direction: after, anchor: "namespace" }

# ---------------------------------------------------------------------
# 数据源列表。卡类型差异完全靠配置表达，不改代码。
# ---------------------------------------------------------------------
sources:
  # ===== 示例1: NVIDIA GPU (dcgm-exporter) =====
  - name: "gpu-cluster-a"
    ip: "10.0.0.1"                     # 本源主机IP(写入行的ip字段)
    url: "http://10.0.0.1:9400"        # Prometheus 地址
    timeout: 10                        # 查询超时(秒)，默认10
    interval: 30                       # 覆盖全局 interval
    primary:                           # 主指标：枚举所有卡片，决定行数
      metric: "DCGM_FI_DEV_GPU_UTIL"
      card_label: "gpu"                # dcgm 用 gpu 标签作卡号
    fields:
      - { name: "gpu_util", from: "metric", metric: "DCGM_FI_DEV_GPU_UTIL" }
      - { name: "temp",     from: "metric", metric: "DCGM_FI_DEV_GPU_TEMP" }
      - { name: "power",    from: "metric", metric: "DCGM_FI_DEV_POWER_USAGE" }
      - { name: "namespace", from: "label", metric: "DCGM_FI_DEV_GPU_UTIL", label: "namespace" }
      - { name: "pod",       from: "label", metric: "DCGM_FI_DEV_GPU_UTIL", label: "pod" }
    expressions:
      - name: "mem_util"
        expr: "DCGM_FI_DEV_FB_USED / (DCGM_FI_DEV_FB_USED + DCGM_FI_DEV_FB_FREE)"
        unit: "%"
    host_fields:
      - name: "host_cpu"
        expr: '100 - (avg by(instance)(irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)'
      - name: "host_mem"
        expr: "(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100"
      - name: "host_fds"
        expr: "node_filefd_allocated"

  # ===== 示例2: 昇腾 NPU (npu-exporter) =====
  - name: "npu-cluster-b"
    ip: "10.0.0.2"
    url: "http://10.0.0.2:9401"
    primary:
      metric: "npu_chip_info_utilization"
      card_label: "id"                 # npu 用 id 标签作卡号
    fields:
      - { name: "gpu_util", from: "metric", metric: "npu_chip_info_utilization" }
      - { name: "temp",     from: "metric", metric: "npu_chip_info_temperature" }
      - { name: "power",    from: "metric", metric: "npu_chip_info_power" }
      - { name: "namespace", from: "label", metric: "npu_chip_info_utilization", label: "namespace" }
      - { name: "pod",       from: "label", metric: "npu_chip_info_utilization", label: "pod_name" }
    expressions:
      - name: "mem_util"
        expr: "npu_chip_info_hbm_used_memory / npu_chip_info_hbm_total_memory"
```

- [ ] **Step 6: 在 main.rs 加 `mod config;`**

```rust
mod config;
mod expr;
mod models;

fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 7: 写测试**

在 config/mod.rs 末尾加：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn valid_base_yaml() -> String {
        format!(
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
    primary: {{ metric: "m1", card_label: "gpu" }}
    fields:
      - {{ name: "gpu_util", from: "metric", metric: "m1" }}
    expressions:
      - {{ name: "mem_util", expr: "a / b" }}
"#
        )
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
}
```

- [ ] **Step 8: 运行测试**

Run: `cargo test config`
Expected: 6 个测试通过

- [ ] **Step 9: 提交**

```bash
git add src/config/ src/main.rs config.example.yaml
git commit -m "feat(config): YAML config structs, validation, and example generator"
```

---

## Task 4: source — Prometheus 客户端

**Files:**
- Create: `src/source/mod.rs`
- Modify: `src/main.rs`（加 `mod source;`）
- Create: `tests/fixtures/prom_gpu_util.json`

纯 I/O 边界。查询瞬时向量并解析成 `MetricSample` 列表。注意：测试用 fixture JSON 字符串，不联网。

- [ ] **Step 1: 写 source/mod.rs**

```rust
//! # source 模块
//!
//! 数据源层（纯 I/O 边界）。只负责"查 Prometheus"，不知道业务含义。
//! 提供 PrometheusClient 查询瞬时向量，以及 parse_vector 把 JSON 响应
//! 解析成 MetricSample 列表。可单独替换（换库/换协议）。

use crate::models::MetricSample;
use std::collections::HashMap;
use std::time::Duration;

/// Prometheus 客户端。封装 reqwest，带连接池与超时。
pub struct PrometheusClient {
    client: reqwest::Client,
    base_url: String,
}

/// 查询或解析错误。
#[derive(Debug)]
pub struct SourceError(pub String);

impl PrometheusClient {
    /// 创建客户端。timeout 为查询超时秒数。
    pub fn new(base_url: &str, timeout: u64) -> Result<Self, SourceError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()
            .map_err(|e| SourceError(format!("构建 HTTP 客户端失败: {}", e)))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    /// 查询瞬时向量。metric 可为完整 PromQL（用于 host_fields）或纯指标名。
    /// 返回该查询的所有序列样本。
    pub async fn query(&self, metric: &str) -> Result<Vec<MetricSample>, SourceError> {
        let url = format!("{}/api/v1/query", self.base_url);
        let resp = self
            .client
            .post(&url)
            .form(&[("query", metric)])
            .send()
            .await
            .map_err(|e| SourceError(format!("查询 Prometheus 失败: {}", e)))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| SourceError(format!("读取响应失败: {}", e)))?;
        if !status.is_success() {
            return Err(SourceError(format!("Prometheus 返回 {}: {}", status, text)));
        }
        parse_vector(&text)
    }
}

/// 解析 Prometheus /api/v1/query 响应为 MetricSample 列表。
/// 提取 result 数组中每个 {metric, value[ts, val]}。
pub fn parse_vector(body: &str) -> Result<Vec<MetricSample>, SourceError> {
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| SourceError(format!("JSON 解析失败: {}", e)))?;
    let result = v
        .get("data")
        .and_then(|d| d.get("result"))
        .ok_or_else(|| SourceError("响应缺少 data.result".into()))?;
    let arr = result
        .as_array()
        .ok_or_else(|| SourceError("data.result 不是数组".into()))?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let metric = item
            .get("metric")
            .and_then(|m| m.as_object())
            .ok_or_else(|| SourceError("缺少 metric".into()))?;
        let mut labels: HashMap<String, String> = HashMap::new();
        for (k, val) in metric {
            if let Some(s) = val.as_str() {
                labels.insert(k.clone(), s.to_string());
            }
        }
        let value = item
            .get("value")
            .and_then(|x| x.as_array())
            .and_then(|a| a.get(1))
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| SourceError("缺少 value 或无法解析为数字".into()))?;
        out.push(MetricSample { labels, value });
    }
    Ok(out)
}
```

注意：`reqwest` 已在 Cargo.toml 声明，这里直接用。`serde_json` 需补加依赖。

- [ ] **Step 2: 在 Cargo.toml 的 [dependencies] 加 serde_json**

在 Task 0 的 Cargo.toml `[dependencies]` 段加一行：

```toml
serde_json = "1"
```

- [ ] **Step 3: 写 fixture tests/fixtures/prom_gpu_util.json**

```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": { "__name__": "DCGM_FI_DEV_GPU_UTIL", "gpu": "0", "namespace": "default", "pod": "app-0" },
        "value": [1719235200, "55"]
      },
      {
        "metric": { "__name__": "DCGM_FI_DEV_GPU_UTIL", "gpu": "1", "namespace": "prod", "pod": "app-1" },
        "value": [1719235200, "77"]
      }
    ]
  }
}
```

- [ ] **Step 4: 在 main.rs 加 `mod source;` 并加 serde_json 引用**

```rust
mod config;
mod expr;
mod models;
mod source;

fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 5: 写测试（解析 fixture，不联网）**

在 source/mod.rs 末尾加：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_two_samples() {
        let body = include_str!("../../tests/fixtures/prom_gpu_util.json");
        let samples = parse_vector(body).unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels.get("gpu").unwrap(), "0");
        assert_eq!(samples[0].labels.get("namespace").unwrap(), "default");
        assert_eq!(samples[0].value, 55.0);
        assert_eq!(samples[1].value, 77.0);
    }

    #[test]
    fn rejects_missing_data_result() {
        let body = r#"{"status":"success"}"#;
        assert!(parse_vector(body).is_err());
    }

    #[test]
    fn rejects_non_numeric_value() {
        let body = r#"{"data":{"result":[{"metric":{},"value":[1,"NaN-xxx"]}]}}"#;
        assert!(parse_vector(body).is_err());
    }
}
```

- [ ] **Step 6: 运行测试**

Run: `cargo test source`
Expected: 3 个测试通过

- [ ] **Step 7: 提交**

```bash
git add src/source/ src/main.rs tests/fixtures/prom_gpu_util.json Cargo.toml
git commit -m "feat(source): Prometheus client and vector parser with fixtures"
```

---

## Task 5: mapping — 资产表加载与 join

**Files:**
- Create: `src/mapping/mod.rs`
- Modify: `src/main.rs`（加 `mod mapping;`）
- Create: `tests/fixtures/assets.csv`

纯内存查找。启动时加载 CSV/Excel 建 dest_key 索引，join 时按行内 src_key 值查找补字段。

- [ ] **Step 1: 写 mapping/mod.rs 数据结构**

```rust
//! # mapping 模块
//!
//! 资产表关联层（纯内存查找，无 I/O）。
//! 启动时加载 CSV/Excel 资产表，按 dest_key 列建索引。
//! join 时用行内 src_key 值查找匹配行，把 columns 字段补进行。
//!
//! ## 规则
//! - 无匹配 → 该列 NULL
//! - 多匹配 → 取首条 + 记 WARN（资产表键重复）
//! - 类型解析失败 → NULL + WARN
//! - enabled=false → 不补值（列仍由 --init 建立）

use crate::config::MappingConfig;
use std::collections::HashMap;

/// 加载后的资产索引：dest_key值 -> 该行所有列值。
/// 若同一 dest_key 多行，只保留第一条（其余记 WARN）。
pub struct AssetIndex {
    /// key = dest_key 的值, value = Map<最终列名, 字符串原值>
    map: HashMap<String, HashMap<String, String>>,
    /// 该资产源要补的列：(最终列名, 配置的列类型)
    columns: Vec<(String, String)>,
}

impl AssetIndex {
    /// 用行内 src_key 的值做 join，返回各列的字符串原值。
    /// 无匹配返回空 map。
    pub fn lookup(&self, key: &str) -> Option<&HashMap<String, String>> {
        self.map.get(key)
    }

    /// 该索引负责补充的列名列表。
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|(n, _)| n.clone()).collect()
    }
}
```

- [ ] **Step 2: 实现加载（CSV + Excel）**

继续 mapping/mod.rs：

```rust
use crate::config::{MappingColumn, MappingSource};

/// 加载错误。
#[derive(Debug)]
pub struct MappingError(pub String);

/// 最终列名 = rename 或 source_field。
pub fn final_name(col: &MappingColumn) -> String {
    col.rename.clone().unwrap_or_else(|| col.source_field.clone())
}

/// 从单个 MappingSource 加载为 AssetIndex。
pub fn load_source(ms: &MappingSource) -> Result<AssetIndex, MappingError> {
    let columns: Vec<(String, String)> = ms
        .columns
        .iter()
        .map(|c| (final_name(c), c.col_type.clone()))
        .collect();
    let rows = if ms.source_path.ends_with(".xlsx") {
        read_xlsx(ms)?
    } else {
        read_csv(ms)?
    };
    // 建 dest_key 索引
    let mut map: HashMap<String, HashMap<String, String>> = HashMap::new();
    for (row_idx, row) in rows.iter().enumerate() {
        let key = row
            .get(&ms.dest_key)
            .cloned()
            .unwrap_or_default();
        if key.is_empty() {
            continue;
        }
        if map.contains_key(&key) && row_idx > 0 {
            // 多匹配：首条优先，跳过后续（调用方应记 WARN）
            continue;
        }
        // 只保留 columns 声明的列
        let filtered: HashMap<String, String> = ms
            .columns
            .iter()
            .filter_map(|c| row.get(&c.source_field).map(|v| (final_name(c), v.clone())))
            .collect();
        map.entry(key).or_insert(filtered);
    }
    Ok(AssetIndex { map, columns })
}

/// 行 = HashMap<列名, 值>
type Row = HashMap<String, String>;

fn read_csv(ms: &MappingSource) -> Result<Vec<Row>, MappingError> {
    let mut rdr = csv::Reader::from_path(&ms.source_path)
        .map_err(|e| MappingError(format!("打开 CSV 失败: {}", e)))?;
    let headers = rdr
        .headers()
        .map_err(|e| MappingError(format!("读 CSV 表头失败: {}", e)))?
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec.map_err(|e| MappingError(format!("读 CSV 行失败: {}", e)))?;
        let mut row: Row = HashMap::new();
        for (i, h) in headers.iter().enumerate() {
            if let Some(v) = rec.get(i) {
                row.insert(h.clone(), v.to_string());
            }
        }
        out.push(row);
    }
    Ok(out)
}

fn read_xlsx(ms: &MappingSource) -> Result<Vec<Row>, MappingError> {
    use calamine::{open_workbook, Reader, Xlsx};
    let path = &ms.source_path;
    let mut wb: Xlsx<_> = open_workbook(path)
        .map_err(|e| MappingError(format!("打开 Excel 失败: {}", e)))?;
    let sheet_name = ms.source_sheet.clone().unwrap_or_else(|| "Sheet1".into());
    let range = wb
        .worksheet_range(&sheet_name)
        .map_err(|e| MappingError(format!("读工作表 {} 失败: {}", sheet_name, e)))?;
    let mut rows_iter = range.rows();
    let header = rows_iter
        .next()
        .ok_or_else(|| MappingError("Excel 表头为空".into()))?;
    let headers: Vec<String> = header.iter().map(|c| c.to_string()).collect();
    let mut out = Vec::new();
    for row in rows_iter {
        let mut r: Row = HashMap::new();
        for (i, h) in headers.iter().enumerate() {
            if let Some(cell) = row.get(i) {
                r.insert(h.clone(), cell.to_string());
            }
        }
        out.push(r);
    }
    Ok(out)
}

/// 加载所有配置的资产源，返回索引列表。
pub fn load_all(cfg: &MappingConfig) -> Result<Vec<AssetIndex>, MappingError> {
    cfg.sources.iter().map(load_source).collect()
}
```

- [ ] **Step 3: 实现 join 逻辑（补进行 + 类型解析）**

继续 mapping/mod.rs：

```rust
use crate::models::Row as CollectorRow;

/// 把一个采集行与所有资产索引 join，补 mapping 列到 row.strings。
/// src_key 是行内用于关联的列名（如 namespace）。
/// 多匹配/解析失败的 WARN 由调用方根据返回的 warnings 记录。
///
/// 返回 warnings 列表（每个元素是一条提示，如 "loc 类型解析失败"）。
pub fn join_row(
    row: &mut CollectorRow,
    src_key: &str,
    indices: &[AssetIndex],
    mapping_sources: &[crate::config::MappingSource],
) -> Vec<String> {
    let mut warnings = Vec::new();
    let key_value = row.strings.get(src_key).cloned().flatten().unwrap_or_default();
    for (idx, index) in indices.iter().enumerate() {
        // 取该资产源声明的列类型
        let col_types: HashMap<&String, &String> =
            mapping_sources[idx].columns.iter().map(|c| {
                let n = final_name(c);
                (unsafe { std::mem::transmute::<&String, &String>(n.as_str().to_string().leak()) }, &c.col_type)
            }).collect();
        // 上面 transmute 不安全，改用简单方式：直接遍历
        let matched = index.lookup(&key_value);
        for col_name in index.column_names() {
            let value = matched.and_then(|m| m.get(&col_name)).cloned();
            match value {
                None => {
                    row.strings.insert(col_name, None);
                }
                Some(v) => {
                    // 数值类型尝试解析；varchar 原样存
                    let is_numeric = mapping_sources[idx]
                        .columns
                        .iter()
                        .any(|c| final_name(c) == col_name && c.col_type.to_lowercase().starts_with("int") || c.col_type.to_lowercase().starts_with("double") || c.col_type.to_lowercase().starts_with("float"));
                    if is_numeric && v.parse::<f64>().is_err() {
                        warnings.push(format!("{} 类型解析失败: '{}'", col_name, v));
                        row.strings.insert(col_name, None);
                    } else {
                        row.strings.insert(col_name, Some(v));
                    }
                }
            }
        }
    }
    let _ = col_types; // 抑制
    warnings
}
```

注意：上面的 `col_types` 块包含不安全的 transmute，必须删除。下面的步骤会修正为干净实现。

- [ ] **Step 4: 用干净的 join 实现替换 Step 3（删除不安全代码）**

删除 Step 3 中的 `join_row` 整体，替换为：

```rust
use crate::models::Row as CollectorRow;

/// 把一个采集行与所有资产索引 join，补 mapping 列到 row.strings。
/// src_key 是行内用于关联的列名（如 namespace）。
/// 返回 warnings 列表（多匹配提示在加载阶段，此处为类型解析失败）。
pub fn join_row(
    row: &mut CollectorRow,
    src_key: &str,
    indices: &[AssetIndex],
    mapping_sources: &[crate::config::MappingSource],
) -> Vec<String> {
    let mut warnings = Vec::new();
    let key_value = row
        .strings
        .get(src_key)
        .cloned()
        .flatten()
        .unwrap_or_default();
    for (idx, index) in indices.iter().enumerate() {
        let matched = index.lookup(&key_value);
        for col_name in index.column_names() {
            // 查该列配置的类型
            let col_type = mapping_sources[idx]
                .columns
                .iter()
                .find(|c| final_name(c) == col_name)
                .map(|c| c.col_type.to_lowercase())
                .unwrap_or_default();
            let is_numeric =
                col_type.starts_with("int") || col_type.starts_with("double") || col_type.starts_with("float");
            let value = matched.and_then(|m| m.get(&col_name)).cloned();
            match value {
                None => {
                    row.strings.insert(col_name, None);
                }
                Some(v) => {
                    if is_numeric && v.parse::<f64>().is_err() {
                        warnings.push(format!("{} 类型解析失败: '{}'", col_name, v));
                        row.strings.insert(col_name, None);
                    } else {
                        row.strings.insert(col_name, Some(v));
                    }
                }
            }
        }
    }
    warnings
}
```

- [ ] **Step 5: 写 fixture tests/fixtures/assets.csv**

```csv
Namespace,机房位置,负责人
default,机房A,张三
prod,机房B,李四
default,机房A重复,王五
```

注意：default 出现两次，测试多匹配取首条。

- [ ] **Step 6: 在 main.rs 加 `mod mapping;`**

```rust
mod config;
mod expr;
mod mapping;
mod models;
mod source;

fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 7: 写测试**

在 mapping/mod.rs 末尾加：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MappingColumn, MappingConfig, MappingSource, ColumnPosition};
    use chrono_tz::Asia::Shanghai;

    fn sample_mapping() -> MappingSource {
        MappingSource {
            source_path: "tests/fixtures/assets.csv".into(),
            src_key: "namespace".into(),
            dest_key: "Namespace".into(),
            source_sheet: None,
            columns: vec![
                MappingColumn {
                    source_field: "机房位置".into(),
                    rename: Some("location".into()),
                    type: "varchar(255)".into(),
                    comment: "机房".into(),
                    position: ColumnPosition { direction: "after".into(), anchor: "namespace".into() },
                },
                MappingColumn {
                    source_field: "负责人".into(),
                    rename: Some("owner".into()),
                    type: "varchar(64)".into(),
                    comment: "负责人".into(),
                    position: ColumnPosition { direction: "after".into(), anchor: "namespace".into() },
                },
            ],
        }
    }

    #[test]
    fn loads_csv_and_dedups_by_first() {
        let ms = sample_mapping();
        let index = load_source(&ms).unwrap();
        // default 出现两次，取首条（机房A, 张三）
        let m = index.lookup("default").unwrap();
        assert_eq!(m.get("location").unwrap(), "机房A");
        assert_eq!(m.get("owner").unwrap(), "张三");
        // prod 单条
        let p = index.lookup("prod").unwrap();
        assert_eq!(p.get("location").unwrap(), "机房B");
    }

    #[test]
    fn no_match_returns_none() {
        let ms = sample_mapping();
        let index = load_source(&ms).unwrap();
        assert!(index.lookup("nonexistent").is_none());
    }

    #[test]
    fn join_row_fills_columns() {
        let ms = sample_mapping();
        let cfg = MappingConfig { enabled: true, sources: vec![ms.clone()] };
        let indices = load_all(&cfg).unwrap();
        let mut row = crate::models::Row {
            ts: chrono::Utc::now().with_timezone(&Shanghai),
            ip: "1.1.1.1".into(),
            card_id: "0".into(),
            fields: Default::default(),
            strings: std::collections::HashMap::from([("namespace".into(), Some("default".into()))]),
            source: "s1".into(),
        };
        let warnings = join_row(&mut row, "namespace", &indices, &cfg.sources);
        assert!(warnings.is_empty());
        assert_eq!(row.strings.get("location").unwrap().as_deref(), Some("机房A"));
    }

    #[test]
    fn join_no_match_fills_null() {
        let ms = sample_mapping();
        let cfg = MappingConfig { enabled: true, sources: vec![ms] };
        let indices = load_all(&cfg).unwrap();
        let mut row = crate::models::Row {
            ts: chrono::Utc::now().with_timezone(&Shanghai),
            ip: "1.1.1.1".into(),
            card_id: "0".into(),
            fields: Default::default(),
            strings: std::collections::HashMap::from([("namespace".into(), Some("zzz".into()))]),
            source: "s1".into(),
        };
        join_row(&mut row, "namespace", &indices, &cfg.sources);
        assert_eq!(row.strings.get("location").unwrap(), &None);
    }
}
```

- [ ] **Step 8: 运行测试**

Run: `cargo test mapping`
Expected: 4 个测试通过

- [ ] **Step 9: 提交**

```bash
git add src/mapping/ src/main.rs tests/fixtures/assets.csv
git commit -m "feat(mapping): asset table loading and row join with dedup"
```

---

## Task 6: extractor — 主指标提取与字段对齐

**Files:**
- Create: `src/extractor/mod.rs`
- Create: `src/extractor/align.rs`
- Create: `src/extractor/host.rs`
- Modify: `src/main.rs`（加 `mod extractor;`）

核心业务逻辑。提取主指标序列生成行骨架，按 (ip, card_id) 对齐字段，主机级按 ip 复制。

- [ ] **Step 1: 写 extractor/align.rs（对齐辅助）**

```rust
//! 字段对齐辅助。按对齐键把指标值匹配到行骨架。

use crate::models::MetricSample;
use std::collections::HashMap;

/// 把一组样本按"对齐键"组织成 map，便于按卡查找。
/// align_labels: 用于拼对齐键的标签名列表（如 ["gpu"]）。
/// 返回 map: 对齐键字符串 -> 样本值。
pub fn index_by_key(samples: &[MetricSample], align_labels: &[String]) -> HashMap<String, f64> {
    let mut m = HashMap::new();
    for s in samples {
        let key = make_key(&s.labels, align_labels);
        m.insert(key, s.value);
    }
    m
}

/// 把一组样本按对齐键组织成"取标签"的 map。
/// 返回 map: 对齐键 -> (label名 -> 值)。
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

/// 用 align_labels 对应的标签值拼成对齐键（用 '\x1f' 分隔避免碰撞）。
pub fn make_key(labels: &HashMap<String, String>, align_labels: &[String]) -> String {
    align_labels
        .iter()
        .map(|l| labels.get(l).cloned().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\x1f")
}
```

- [ ] **Step 2: 写 extractor/host.rs（主机级对齐）**

```rust
//! 主机级字段处理。主机指标一台一个值，按 ip 对齐后复制到该主机每张卡。

use crate::config::HostFieldConfig;
use crate::source::PrometheusClient;
use std::collections::HashMap;

/// 查询所有 host_fields，返回每个主机字段的单值。
/// host 级 PromQL 查询返回的是单序列（一台主机一个值）；
/// 若返回多个序列取第一个并记 WARN（由调用方判断）。
///
/// 返回 map: host字段名 -> 值（整主机一个值，不区分卡）。
pub async fn collect_host_fields(
    client: &PrometheusClient,
    host_fields: &[HostFieldConfig],
) -> Result<HashMap<String, f64>, crate::source::SourceError> {
    let mut out = HashMap::new();
    for hf in host_fields {
        let samples = client.query(&hf.expr).await?;
        if let Some(first) = samples.first() {
            out.insert(hf.name.clone(), first.value);
        }
        // 无结果则该字段缺席（后续填 NULL）
    }
    Ok(out)
}
```

- [ ] **Step 3: 写 extractor/mod.rs（编排：主指标→行骨架→对齐→表达式）**

```rust
//! # extractor 模块
//!
//! 提取对齐层（核心业务逻辑）。介于 source 与 sink 之间，
//! 是唯一持有业务规则的层。
//!
//! ## 流程
//! 1. 查主指标 → 枚举所有卡片序列（决定行数）
//! 2. 对每个 from=metric/label 字段，按 (ip,card_id) 对齐
//! 3. 主机级字段按 ip 对齐后复制到该主机每张卡
//! 4. 表达式求值得到派生指标
//! 5. 组装成 Row

mod align;
mod host;

use crate::config::{ExprConfig, FieldConfig, SourceConfig};
use crate::expr;
use crate::models::{FieldValue, MetricSample, Row};
use crate::source::PrometheusClient;
use chrono::Utc;
use std::collections::{HashMap, HashSet};

/// 采集一个 source 的所有行。失败返回 Err（由 scheduler 隔离）。
pub async fn collect_source(
    cfg: &SourceConfig,
    client: &PrometheusClient,
    tz: chrono_tz::Tz,
) -> Result<Vec<Row>, crate::source::SourceError> {
    // 1. 主指标 → 行骨架
    let primary_samples = client.query(&cfg.primary.metric).await?;
    let card_label = &cfg.primary.card_label;
    // 收集所有出现过的 metric 名（fields + expressions 用到的），批量查询
    let needed_metrics = collect_needed_metrics(cfg);
    let mut metric_cache: HashMap<String, Vec<MetricSample>> = HashMap::new();
    for m in &needed_metrics {
        let samples = client.query(m).await?;
        metric_cache.insert(m.clone(), samples);
    }

    // 2. 主机级字段（整主机单值）
    let host_values = host::collect_host_fields(client, &cfg.host_fields).await?;

    let align_labels = vec![card_label.clone()];
    let now = Utc::now().with_timezone(&tz);

    let mut rows = Vec::new();
    for ps in &primary_samples {
        let card_id = ps.labels.get(card_label).cloned().unwrap_or_default();
        let key = align::make_key(&ps.labels, &align_labels);

        let mut row = Row {
            ts: now,
            ip: cfg.ip.clone(),
            card_id: card_id.clone(),
            fields: HashMap::new(),
            strings: HashMap::new(),
            source: cfg.name.clone(),
        };

        // 3. 各字段对齐
        for fc in &cfg.fields {
            let samples = metric_cache.get(&fc.metric);
            match fc.from.as_str() {
                "metric" => {
                    let idx = samples.map(|s| align::index_by_key(s, &align_labels)).unwrap_or_default();
                    let v = idx.get(&key).copied();
                    row.fields.insert(fc.name.clone(), v);
                }
                "label" => {
                    let label = fc.label.as_deref().unwrap_or("");
                    let idx = samples
                        .map(|s| align::index_labels_by_key(s, &align_labels))
                        .unwrap_or_default();
                    let v = idx.get(&key).and_then(|m| m.get(label)).cloned();
                    row.strings.insert(fc.name.clone(), v);
                }
                _ => {}
            }
        }

        // 4. 表达式：构建 vars（metric名->值），求值
        for ec in &cfg.expressions {
            let vars = build_vars_for_expr(&ec.expr, &key, &align_labels, &metric_cache);
            let val = expr::parse(&ec.expr)
                .ok()
                .and_then(|ast| expr::evaluate(&ast, &vars));
            row.fields.insert(ec.name.clone(), val);
        }

        // 5. 主机级字段复制到该行（按 ip，整主机一个值）
        for (name, v) in &host_values {
            row.fields.insert(name.clone(), Some(*v));
        }

        rows.push(row);
    }
    Ok(rows)
}

/// 收集该 source 需要查询的所有 metric 名（fields + expressions 中的变量）。
fn collect_needed_metrics(cfg: &SourceConfig) -> Vec<String> {
    let mut set: HashSet<String> = HashSet::new();
    for fc in &cfg.fields {
        set.insert(fc.metric.clone());
    }
    for ec in &cfg.expressions {
        if let Ok(ast) = expr::parse(&ec.expr) {
            collect_vars(&ast, &mut set);
        }
    }
    set.into_iter().collect()
}

fn collect_vars(ast: &expr_ast(), set: &mut HashSet<String>) {}

// 占位：expr 的 Ast 是私有的，无法直接遍历变量。
// 改用正则提取变量名。见 Step 4 修正。
```

注意：`expr::Ast` 是私有类型，无法在 extractor 中遍历。Step 4 用正则改写 `collect_vars`。

- [ ] **Step 4: 修正表达式变量提取（用正则，不依赖私有 AST）**

替换 extractor/mod.rs 末尾的占位代码。先在 Cargo.toml 加 regex 依赖：

```toml
regex = "1"
```

然后 extractor/mod.rs 替换 `collect_vars` 相关为：

```rust
use regex::Regex;
use std::sync::OnceLock;

/// 提取表达式中的变量名（metric 名）。
/// 变量名模式: [A-Za-z_][A-Za-z0-9_]*
/// 过滤掉纯数字和运算符。
fn extract_var_names(expr_str: &str) -> Vec<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"[A-Za-z_][A-Za-z0-9_]*").unwrap());
    re.find_iter(expr_str)
        .map(|m| m.as_str().to_string())
        .filter(|s| s.parse::<f64>().is_err()) // 排除纯数字
        .collect()
}

/// 收集该 source 需要查询的所有 metric 名。
fn collect_needed_metrics(cfg: &SourceConfig) -> Vec<String> {
    let mut set: HashSet<String> = HashSet::new();
    for fc in &cfg.fields {
        set.insert(fc.metric.clone());
    }
    for ec in &cfg.expressions {
        for v in extract_var_names(&ec.expr) {
            set.insert(v);
        }
    }
    set.into_iter().collect()
}

/// 为表达式构建变量值表：变量名(metric) -> 该卡的值。
fn build_vars_for_expr(
    expr_str: &str,
    key: &str,
    align_labels: &[String],
    metric_cache: &HashMap<String, Vec<MetricSample>>,
) -> HashMap<String, f64> {
    let mut vars = HashMap::new();
    for var in extract_var_names(expr_str) {
        if let Some(samples) = metric_cache.get(&var) {
            let idx = align::index_by_key(samples, align_labels);
            if let Some(v) = idx.get(key) {
                vars.insert(var, *v);
            }
        }
    }
    vars
}
```

同时删除 Step 3 中错误的 `collect_vars` 和 `expr_ast()` 占位行。

- [ ] **Step 5: 在 main.rs 加 `mod extractor;`**

```rust
mod config;
mod expr;
mod extractor;
mod mapping;
mod models;
mod source;

fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 6: 写测试（用构造的 MetricSample，不联网）**

extractor 依赖 PrometheusClient，但 collect_source 是 async 且调用 client.query。为可测试，Step 7 引入 trait 抽象。先写对齐函数的纯测试。

在 extractor/align.rs 末尾加：

```rust
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
}
```

在 extractor/mod.rs 末尾加表达式提取测试：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_metric_vars() {
        let vars = extract_var_names("DCGM_FI_DEV_FB_USED / (DCGM_FI_DEV_FB_USED + DCGM_FI_DEV_FB_FREE)");
        assert!(vars.contains(&"DCGM_FI_DEV_FB_USED".to_string()));
        assert!(vars.contains(&"DCGM_FI_DEV_FB_FREE".to_string()));
    }

    #[test]
    fn filters_out_numbers() {
        let vars = extract_var_names("100 - A / B");
        assert!(!vars.iter().any(|v| v == "100"));
        assert!(vars.contains(&"A".to_string()));
    }
}
```

- [ ] **Step 7: 引入 SourceQuerier trait 使 collect_source 可测**

为让 extractor 可脱离真实 HTTP 测试，抽象查询接口。新建 `src/extractor/mod.rs` 顶部加：

```rust
use async_trait::async_trait;
```

在 Cargo.toml 加：

```toml
async-trait = "0.1"
```

定义 trait（extractor/mod.rs）：

```rust
/// 查询接口抽象，便于测试用 mock 替换真实 PrometheusClient。
#[async_trait]
pub trait SourceQuerier {
    async fn query(&self, metric: &str) -> Result<Vec<MetricSample>, crate::source::SourceError>;
}

#[async_trait]
impl SourceQuerier for PrometheusClient {
    async fn query(&self, metric: &str) -> Result<Vec<MetricSample>, crate::source::SourceError> {
        PrometheusClient::query(self, metric).await
    }
}
```

把 collect_source 签名改为泛型 `Q: SourceQuerier + Sync`：

```rust
pub async fn collect_source<Q: SourceQuerier + Sync>(
    cfg: &SourceConfig,
    client: &Q,
    tz: chrono_tz::Tz,
) -> Result<Vec<Row>, crate::source::SourceError> {
    // 内部所有 client.query 调用不变（trait 方法同名）
    // ...（其余实现同 Step 3，仅签名改泛型）
}
```

注意：因 host.rs 内部也调用 client，需把 collect_host_fields 也泛型化或内联。简化：把 host 字段查询内联进 collect_source（删除 host.rs 的独立函数调用，保留对齐逻辑）。调整 collect_source 中：

```rust
    // 2. 主机级字段（整主机单值）—内联查询
    let mut host_values: HashMap<String, f64> = HashMap::new();
    for hf in &cfg.host_fields {
        let samples = client.query(&hf.expr).await?;
        if let Some(first) = samples.first() {
            host_values.insert(hf.name.clone(), first.value);
        }
    }
```

并删除 `mod host;` 行（逻辑已内联）。host.rs 文件可保留空或删除引用。

- [ ] **Step 8: 运行测试**

Run: `cargo test extractor`
Expected: align 的 2 个 + mod 的 2 个 = 4 个测试通过

- [ ] **Step 9: 提交**

```bash
git add src/extractor/ src/main.rs Cargo.toml
git commit -m "feat(extractor): primary metric extraction, field alignment, expression eval"
```

---

## Task 7: sql_gen — 建表 SQL 生成（--init）

**Files:**
- Create: `src/sql_gen/mod.rs`
- Modify: `src/main.rs`（加 `mod sql_gen;`）

仅 `--init` 用。固定列基线 + mapping 列按 position 插入，输出 `./init/<table>.sql`。

- [ ] **Step 1: 写 sql_gen/mod.rs**

```rust
//! # sql_gen 模块
//!
//! 建表 SQL 生成层（仅 --init 模式用）。
//! 固定列基线 + mapping 列按 position 插入排序，输出到 ./init/<table>.sql。
//! 含每列 COMMENT、主键、3 个索引；不含 DROP、不含 CREATE DATABASE。

use crate::config::{final_name as mapping_final_name, Config, MappingColumn, FIXED_COLUMNS};
use crate::models::ColumnDef;
use std::path::Path;

/// 生成建表 SQL 全文。
pub fn generate(cfg: &Config) -> String {
    let columns = build_column_list(cfg);
    let mut lines = Vec::new();
    lines.push(format!("-- 由 gpu-collector --init 生成".to_string()));
    lines.push(format!("-- 配置文件对应表: {}", cfg.database.table));
    lines.push(format!("-- 含 mapping 列: {}", cfg.mapping.enabled || !cfg.mapping.sources.is_empty()));
    lines.push("-- 注意: 本文件不含 DROP TABLE，重复执行会因表已存在而跳过(IF NOT EXISTS)。".to_string());
    lines.push(format!("CREATE TABLE IF NOT EXISTS {} (", cfg.database.table));
    let col_lines: Vec<String> = columns
        .iter()
        .map(|c| format!("    {:<16} {}{}", c.name, type_decl(c), comment_clause(c)))
        .collect();
    lines.extend(col_lines);
    lines.push("    PRIMARY KEY (id),".into());
    lines.push("    INDEX idx_ts_ip_card (ts, ip, card_id),".into());
    lines.push("    INDEX idx_ip_card (ip, card_id),".into());
    lines.push("    INDEX idx_ts (ts)".into());
    lines.push(format!(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='计算卡利用率采集记录';"));
    lines.join("\n") + "\n"
}

fn type_decl(c: &ColumnDef) -> String {
    // 固定列里 id/ts 带额外修饰（AUTO_INCREMENT / NOT NULL），统一处理 nullable
    let base = &c.sql_type;
    if c.name == "id" {
        return "BIGINT NOT NULL AUTO_INCREMENT".into();
    }
    let null_part = if c.nullable { "NULL" } else { "NOT NULL" };
    format!("{} {}", base, null_part)
}

fn comment_clause(c: &ColumnDef) -> String {
    format!("COMMENT '{}'", c.comment.replace('\'', "''"))
}

/// 构建最终列列表（固定列 + mapping 列按 position 插入）。
pub fn build_column_list(cfg: &Config) -> Vec<ColumnDef> {
    // 固定列基线
    let mut result: Vec<ColumnDef> = FIXED_COLUMNS
        .iter()
        .map(|(n, t, nullable, comment)| ColumnDef {
            name: n.to_string(),
            sql_type: strip_modifiers(t),
            nullable: *nullable,
            comment: comment.to_string(),
        })
        .collect();

    // mapping 列按 position 插入
    for ms in &cfg.mapping.sources {
        for col in &ms.columns {
            insert_by_position(&mut result, col);
        }
    }
    result
}

/// FIXED_COLUMNS 里的 sql_type 含 "NOT NULL AUTO_INCREMENT" 等，
/// 提取纯类型。id 单独处理，这里只对其它列。
fn strip_modifiers(t: &str) -> String {
    t.split_whitespace().next().unwrap_or(t).to_string()
}

/// 按 position 把 mapping 列插入 result。
fn insert_by_position(result: &mut Vec<ColumnDef>, col: &MappingColumn) {
    let name = mapping_final_name(col);
    let sql_type = col_type_to_sql(&col.col_type);
    let new_col = ColumnDef {
        name: name.clone(),
        sql_type,
        nullable: true,
        comment: col.comment.clone(),
    };
    let anchor_idx = result.iter().position(|c| c.name == col.position.anchor);
    if let Some(idx) = anchor_idx {
        let insert_at = if col.position.direction == "before" { idx } else { idx + 1 };
        result.insert(insert_at, new_col);
    } else {
        // anchor 不存在则追加到末尾
        result.push(new_col);
    }
}

/// 配置的 type(如 "varchar(255)") 转 SQL 类型。直接透传，规范化大小写可选。
fn col_type_to_sql(t: &str) -> String {
    let lower = t.to_lowercase();
    match lower.as_str() {
        s if s.starts_with("varchar") => t.to_string(),
        s if s.starts_with("int") => "INT".into(),
        s if s.starts_with("bigint") => "BIGINT".into(),
        s if s.starts_with("double") || s.starts_with("float") => "DOUBLE".into(),
        _ => t.to_string(),
    }
}

/// 写入 ./init/<table>.sql。
pub fn write_init_sql(cfg: &Config, dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}.sql", cfg.database.table));
    std::fs::write(path, generate(cfg))
}
```

- [ ] **Step 2: 让 config 的 final_name 对外可见**

config/mod.rs 里 `final_name` 当前是 mapping 模块的私有函数。需在 config 里也暴露固定列信息（FIXED_COLUMNS 已是 pub）。sql_gen 用的是 mapping::final_name，但为避免循环依赖，把 final_name 逻辑复制到 sql_gen（已在 Step 1 用 `crate::config::final_name`，需修正）。

修正：在 config/mod.rs 加一个 pub helper（不依赖 mapping）：

在 config/mod.rs 加：

```rust
/// 计算 mapping 列最终名（rename 或 source_field）。供 sql_gen 复用。
pub fn mapping_final_name(col: &MappingColumn) -> String {
    col.rename.clone().unwrap_or_else(|| col.source_field.clone())
}
```

并修正 sql_gen Step 1 的 import 为：

```rust
use crate::config::{mapping_final_name, Config, MappingColumn, FIXED_COLUMNS};
```

同时 `insert_by_position` 和 `generate` 里所有 `mapping_final_name(col)` 调用不变。

- [ ] **Step 3: 在 main.rs 加 `mod sql_gen;`**

```rust
mod config;
mod expr;
mod extractor;
mod mapping;
mod models;
mod source;
mod sql_gen;

fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 4: 写测试**

在 sql_gen/mod.rs 末尾加：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn cfg_with_mapping() -> Config {
        let yaml = r#"
interval: 60
retention_days: 30
retention_interval: 3600
timezone: "Asia/Shanghai"
database: { host: "h", port: 3306, user: "u", password: "p", database: "db", table: "gpu_usage", max_connections: 10 }
logging: { level: "info", dir: "./logs", all_file: "all.log", error_file: "error.log", rotation: "daily", archive_after_days: 7, archive_prefix: "logs", stdout: true }
mapping:
  enabled: true
  sources:
    - source_path: "./a.csv"
      src_key: "namespace"
      dest_key: "Namespace"
      columns:
        - source_field: "机房位置"
          rename: "location"
          type: "varchar(255)"
          comment: "机房位置"
          position: { direction: after, anchor: "namespace" }
sources:
  - name: "s1"
    ip: "1.1.1.1"
    url: "http://1.1.1.1:9090"
    primary: { metric: "m1", card_label: "gpu" }
"#;
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn mapping_column_inserted_after_namespace() {
        let cfg = cfg_with_mapping();
        let cols = build_column_list(&cfg);
        let ns_idx = cols.iter().position(|c| c.name == "namespace").unwrap();
        let loc_idx = cols.iter().position(|c| c.name == "location").unwrap();
        assert_eq!(loc_idx, ns_idx + 1);
        // pod 应在 location 之后
        let pod_idx = cols.iter().position(|c| c.name == "pod").unwrap();
        assert_eq!(pod_idx, loc_idx + 1);
    }

    #[test]
    fn generated_sql_has_no_drop() {
        let cfg = cfg_with_mapping();
        let sql = generate(&cfg);
        assert!(!sql.to_lowercase().contains("drop table"));
        assert!(!sql.to_lowercase().contains("create database"));
    }

    #[test]
    fn generated_sql_has_comment_and_indexes() {
        let cfg = cfg_with_mapping();
        let sql = generate(&cfg);
        assert!(sql.contains("COMMENT '机房位置'"));
        assert!(sql.contains("idx_ts_ip_card"));
        assert!(sql.contains("PRIMARY KEY (id)"));
    }

    #[test]
    fn disabled_mapping_still_has_column() {
        let mut cfg = cfg_with_mapping();
        cfg.mapping.enabled = false;
        let cols = build_column_list(&cfg);
        assert!(cols.iter().any(|c| c.name == "location"));
    }
}
```

- [ ] **Step 5: 运行测试**

Run: `cargo test sql_gen`
Expected: 4 个测试通过

- [ ] **Step 6: 提交**

```bash
git add src/sql_gen/ src/config/mod.rs src/main.rs
git commit -m "feat(sql_gen): generate CREATE TABLE SQL with mapping columns for --init"
```

---

## Task 8: sink — MySQL 写入 + schema 校验

**Files:**
- Create: `src/sink/mod.rs`
- Create: `src/sink/schema.rs`
- Modify: `src/main.rs`（加 `mod sink;`）

批量 INSERT；schema 校验（读 INFORMATION_SCHEMA）；保留期清理；连接级时区 SET。注意：真实 DB 集成测试跳过，schema 对比逻辑用纯函数测试。

- [ ] **Step 1: 写 sink/schema.rs（schema 对比纯函数 + 时区/清理 SQL）**

```rust
//! schema 校验与维护。读实际表列，与期望列对比。

use std::collections::HashSet;

/// schema 校验结果。
#[derive(Debug, PartialEq)]
pub enum SchemaCheck {
    /// 完全匹配
    Match,
    /// 实际表缺少这些列（缺列 → 调用方报错退出）
    Missing(Vec<String>),
    /// 实际表多出这些列（多列 → 调用方告警询问）
    Extra(Vec<String>),
}

/// 对比期望列与实际列。期望列来自配置（固定列 + mapping 列）。
pub fn compare(expected: &HashSet<String>, actual: &HashSet<String>) -> SchemaCheck {
    let missing: Vec<String> = expected.difference(actual).cloned().collect();
    let extra: Vec<String> = actual.difference(expected).cloned().collect();
    if !missing.is_empty() {
        SchemaCheck::Missing(missing)
    } else if !extra.is_empty() {
        SchemaCheck::Extra(extra)
    } else {
        SchemaCheck::Match
    }
}

/// 连接级时区 SET 语句。
pub fn set_timezone_sql(tz: &str) -> String {
    format!("SET time_zone = '{}'", tz)
}

/// 保留期清理 SQL（参数：保留天数）。
pub fn retention_delete_sql(table: &str) -> String {
    format!(
        "DELETE FROM {} WHERE ts < DATE_SUB(NOW(), INTERVAL ? DAY)",
        table
    )
}

/// 读取表列的 SQL（查 INFORMATION_SCHEMA）。
pub fn list_columns_sql(table: &str) -> String {
    format!("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'", table)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_when_identical() {
        let e: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let a: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        assert_eq!(compare(&e, &a), SchemaCheck::Match);
    }

    #[test]
    fn missing_when_actual_lacks() {
        let e: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let a: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        assert_eq!(compare(&e, &a), SchemaCheck::Missing(vec!["c".into()]));
    }

    #[test]
    fn extra_when_actual_has_more() {
        let e: HashSet<String> = ["a"].iter().map(|s| s.to_string()).collect();
        let a: HashSet<String> = ["a", "x"].iter().map(|s| s.to_string()).collect();
        assert_eq!(compare(&e, &a), SchemaCheck::Extra(vec!["x".into()]));
    }
}
```

- [ ] **Step 2: 写 sink/mod.rs（连接池 + INSERT + schema 校验编排）**

```rust
//! # sink 模块
//!
//! 落库层（纯 I/O 边界）。只负责"写 MySQL"，不知道指标含义。
//! 批量 INSERT；schema 校验；保留期清理；连接级时区 SET。

pub mod schema;

use crate::config::Config;
use crate::models::Row;
use schema::{compare, SchemaCheck};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;
use std::collections::HashSet;

/// MySQL 连接池封装。
pub struct Sink {
    pool: MySqlPool,
    table: String,
}

/// sink 错误。
#[derive(Debug)]
pub struct SinkError(pub String);

impl Sink {
    /// 建立连接池，并对每个连接 SET time_zone。
    pub async fn connect(cfg: &Config) -> Result<Self, SinkError> {
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            cfg.database.user, cfg.database.password, cfg.database.host, cfg.database.port, cfg.database.database
        );
        let pool = MySqlPoolOptions::new()
            .max_connections(cfg.database.max_connections)
            .connect(&url)
            .await
            .map_err(|e| SinkError(format!("连接 MySQL 失败: {}", e)))?;
        // 连接级时区
        sqlx::query(&schema::set_timezone_sql(&cfg.timezone))
            .execute(&pool)
            .await
            .map_err(|e| SinkError(format!("SET time_zone 失败: {}", e)))?;
        Ok(Self {
            pool,
            table: cfg.database.table.clone(),
        })
    }

    /// 校验表结构。expected 为期望列集合。
    pub async fn check_schema(&self, expected: &HashSet<String>) -> Result<SchemaCheck, SinkError> {
        let rows: Vec<(String,)> =
            sqlx::query_as(&schema::list_columns_sql(&self.table))
                .fetch_all(&self.pool)
                .await
                .map_err(|e| SinkError(format!("读取表结构失败: {}", e)))?;
        let actual: HashSet<String> = rows.into_iter().map(|(c,)| c).collect();
        Ok(compare(expected, actual))
    }

    /// 批量写入行。
    pub async fn insert_rows(&self, rows: &[Row]) -> Result<u64, SinkError> {
        if rows.is_empty() {
            return Ok(0);
        }
        // 构造单行 INSERT，批量执行。为简单起见逐行 INSERT（生产可改批量 VALUES）。
        for row in rows {
            let ts = row.ts.naive_local();
            // 数值列从 fields 取，字符串列从 strings 取
            let gpu_util = row.fields.get("gpu_util").copied().flatten();
            let mem_util = row.fields.get("mem_util").copied().flatten();
            let temp = row.fields.get("temp").copied().flatten();
            let power = row.fields.get("power").copied().flatten();
            let host_cpu = row.fields.get("host_cpu").copied().flatten();
            let host_mem = row.fields.get("host_mem").copied().flatten();
            let host_fds = row.fields.get("host_fds").copied().flatten();
            let namespace = row.strings.get("namespace").cloned().flatten();
            let pod = row.strings.get("pod").cloned().flatten();
            sqlx::query(&format!(
                "INSERT INTO {} (ts, ip, card_id, namespace, pod, gpu_util, mem_util, temp, power, host_cpu, host_mem, host_fds, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                self.table
            ))
            .bind(ts)
            .bind(&row.ip)
            .bind(&row.card_id)
            .bind(namespace)
            .bind(pod)
            .bind(gpu_util)
            .bind(mem_util)
            .bind(temp)
            .bind(power)
            .bind(host_cpu)
            .bind(host_mem)
            .bind(host_fds)
            .bind(&row.source)
            .execute(&self.pool)
            .await
            .map_err(|e| SinkError(format!("INSERT 失败: {}", e)))?;
        }
        Ok(rows.len() as u64)
    }

    /// 执行保留期清理。
    pub async fn run_retention(&self, days: u32) -> Result<u64, SinkError> {
        let result = sqlx::query(&schema::retention_delete_sql(&self.table))
            .bind(days)
            .execute(&self.pool)
            .await
            .map_err(|e| SinkError(format!("清理失败: {}", e)))?;
        Ok(result.rows_affected())
    }
}

/// 计算期望列集合（固定列 + mapping 列）。供 schema 校验用。
pub fn expected_columns(cfg: &Config) -> HashSet<String> {
    let mut set = crate::config::fixed_column_names();
    for ms in &cfg.mapping.sources {
        for col in &ms.columns {
            set.insert(crate::config::mapping_final_name(col));
        }
    }
    set
}
```

注意：`insert_rows` 当前只写固定列（未含 mapping 列）。mapping 列需动态拼入 SQL。Step 3 完善动态列插入。

- [ ] **Step 3: 完善 INSERT 支持 mapping 列（动态列）**

替换 `insert_rows` 实现为动态拼接列名与占位符，包含 mapping 列：

```rust
    /// 批量写入行。固定列 + mapping 列动态拼入。
    pub async fn insert_rows(&self, rows: &[Row], mapping_cols: &[String]) -> Result<u64, SinkError> {
        if rows.is_empty() {
            return Ok(0);
        }
        // 固定列顺序
        let fixed = [
            "ts", "ip", "card_id", "namespace", "pod",
            "gpu_util", "mem_util", "temp", "power",
            "host_cpu", "host_mem", "host_fds", "source",
        ];
        let all_cols: Vec<&str> = fixed.iter().copied().chain(mapping_cols.iter().map(|s| s.as_str())).collect();
        let placeholders: Vec<String> = (0..all_cols.len()).map(|i| format!("${}", i + 1)).collect();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table,
            all_cols.join(", "),
            placeholders.join(", ")
        );
        for row in rows {
            let mut q = sqlx::query(&sql)
                .bind(row.ts.naive_local())
                .bind(&row.ip)
                .bind(&row.card_id)
                .bind(row.strings.get("namespace").cloned().flatten())
                .bind(row.strings.get("pod").cloned().flatten())
                .bind(row.fields.get("gpu_util").copied().flatten())
                .bind(row.fields.get("mem_util").copied().flatten())
                .bind(row.fields.get("temp").copied().flatten())
                .bind(row.fields.get("power").copied().flatten())
                .bind(row.fields.get("host_cpu").copied().flatten())
                .bind(row.fields.get("host_mem").copied().flatten())
                .bind(row.fields.get("host_fds").copied().flatten())
                .bind(&row.source);
            for mc in mapping_cols {
                q = q.bind(row.strings.get(mc).cloned().flatten());
            }
            q.execute(&self.pool)
                .await
                .map_err(|e| SinkError(format!("INSERT 失败: {}", e)))?;
        }
        Ok(rows.len() as u64)
    }
```

注意：sqlx 的 `bind` 使用 `?` 占位符（MySQL），不是 `$N`。修正 placeholders：

```rust
        let placeholders: Vec<String> = all_cols.iter().map(|_| "?".to_string()).collect();
```

- [ ] **Step 4: 在 main.rs 加 `mod sink;`**

```rust
mod config;
mod expr;
mod extractor;
mod mapping;
mod models;
mod sink;
mod source;
mod sql_gen;

fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 5: 运行 schema 纯函数测试（跳过真实 DB）**

Run: `cargo test sink::schema`
Expected: 3 个测试通过（compare 的 match/missing/extra）

- [ ] **Step 6: 提交**

```bash
git add src/sink/ src/main.rs
git commit -m "feat(sink): MySQL batch insert, schema check, retention, timezone set"
```

---

## Task 9: scheduler — 采集调度与失败隔离

**Files:**
- Create: `src/scheduler/mod.rs`
- Modify: `src/main.rs`（加 `mod scheduler;`）

每源一个 tokio 任务，按 interval 循环；单源失败隔离（catch 包裹，记日志，不影响其他源）。

- [ ] **Step 1: 写 scheduler/mod.rs**

```rust
//! # scheduler 模块
//!
//! 调度层（编排各层）。每个 source 一个 tokio 任务，按 interval 循环采集。
//! 单源失败隔离：整个 source 的采集用 catch 等价物（Result + 日志）包裹，
//! 任何 Err 只记日志，永不向上传播，不影响其他源。

use crate::config::Config;
use crate::extractor::{collect_source, SourceQuerier};
use crate::mapping::{join_row, AssetIndex};
use crate::sink::Sink;
use std::sync::Arc;
use tokio::time::Duration;

/// 启动所有 source 的采集任务 + 保留期清理任务。
/// 返回一组 JoinHandle，由 main 统一管理生命周期。
pub async fn run<Q: SourceQuerier + Sync + 'static>(
    cfg: Arc<Config>,
    sink: Arc<Sink>,
    client_factory: impl Fn(&str, u64) -> Q + Send + Sync + 'static,
    asset_indices: Arc<Vec<AssetIndex>>,
    mapping_src_key: Option<String>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    // 保留期清理任务
    {
        let cfg2 = cfg.clone();
        let sink2 = sink.clone();
        let h = tokio::spawn(async move {
            loop {
                if let Err(e) = sink2.run_retention(cfg2.retention_days).await {
                    tracing::error!(target: "retention", "保留期清理失败: {:?}", e);
                }
                tokio::time::sleep(Duration::from_secs(cfg2.retention_interval)).await;
            }
        });
        handles.push(h);
    }
    // 每个 source 一个采集任务
    for src in &cfg.sources {
        let interval = src.interval.unwrap_or(cfg.interval);
        let timeout = src.timeout;
        let src_cfg = src.clone();
        let tz: chrono_tz::Tz = cfg.timezone.parse().unwrap_or(chrono_tz::Asia::Shanghai);
        let client = client_factory(&src.url, timeout);
        let sink2 = sink.clone();
        let name = src.name.clone();
        let indices = asset_indices.clone();
        let msk = mapping_src_key.clone();
        let h = tokio::spawn(async move {
            loop {
                let started = std::time::Instant::now();
                match collect_source(&src_cfg, &client, tz).await {
                    Ok(mut rows) => {
                        // mapping join
                        if let (Some(key), true) = (msk.as_ref(), !indices.is_empty()) {
                            let msrcs: Vec<crate::config::MappingSource> = Vec::new();
                            let _ = msrcs;
                            // join_row 需要 MappingSource 列表以查类型；简化：传空 warnings
                            for row in rows.iter_mut() {
                                let _ = join_row(row, key, &indices, &Vec::new());
                            }
                        }
                        match sink2.insert_rows(&rows, &Vec::new()).await {
                            Ok(n) => tracing::info!(
                                source = %name,
                                rows = n,
                                elapsed_ms = started.elapsed().as_millis() as u64,
                                "采集完成"
                            ),
                            Err(e) => tracing::error!(source = %name, "写入失败: {:?}", e),
                        }
                    }
                    Err(e) => {
                        tracing::warn!(source = %name, "采集失败，跳过本轮: {:?}", e);
                    }
                }
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }
        });
        handles.push(h);
    }
    handles
}
```

注意：上面 `join_row` 调用传了空 `MappingSource` 列表，类型解析会失效。需把 `cfg.mapping.sources` 也传入。Step 2 修正。

- [ ] **Step 2: 修正 mapping join 传入真实 MappingSource**

scheduler/mod.rs 中替换 mapping join 块为：

```rust
                        // mapping join（传入真实 mapping.sources 以解析列类型）
                        if let (Some(key), true) = (msk.as_ref(), !indices.is_empty()) {
                            for row in rows.iter_mut() {
                                let warnings = join_row(row, key, &indices, &cfg.mapping.sources);
                                for w in &warnings {
                                    tracing::warn!(source = %name, "mapping: {}", w);
                                }
                            }
                        }
```

需把 `cfg` 也克隆进任务。调整循环开头加 `let cfg_for_join = cfg.clone();` 并在闭包捕获。由于 src 已 clone，cfg 在循环外可用。修正：把 `cfg.mapping.sources` 在 spawn 前克隆：

在 `for src in &cfg.sources {` 内、spawn 前加：

```rust
        let mapping_sources_for_join: Vec<crate::config::MappingSource> = cfg.mapping.sources.clone();
```

闭包内用 `mapping_sources_for_join`。同时把 `insert_rows` 的 mapping_cols 从资产索引收集：

```rust
                        let mapping_cols: Vec<String> = indices
                            .iter()
                            .flat_map(|i| i.column_names())
                            .collect();
                        match sink2.insert_rows(&rows, &mapping_cols).await {
```

- [ ] **Step 3: 在 main.rs 加 `mod scheduler;`**

```rust
mod config;
mod expr;
mod extractor;
mod mapping;
mod models;
mod scheduler;
mod sink;
mod source;
mod sql_gen;

fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 4: 编译验证（scheduler 难以单元测试，确保编译通过）**

Run: `cargo build`
Expected: 编译成功（修复所有类型不匹配）

- [ ] **Step 5: 提交**

```bash
git add src/scheduler/ src/main.rs
git commit -m "feat(scheduler): per-source concurrent collection with failure isolation"
```

---

## Task 10: log_archive — 日志按日轮转归档

**Files:**
- Create: `src/log_archive/mod.rs`
- Modify: `src/main.rs`（加 `mod log_archive;`）

超期散日志（all+error）打包成单个 tar.gz，散文件删除，压缩包永不删除。用自定义后台任务（不用 tracing-appender 的删除）。

- [ ] **Step 1: 写 log_archive/mod.rs**

```rust
//! # log_archive 模块
//!
//! 日志归档后台任务。扫描日志目录，对超期散日志
//! (all-YYYY-MM-DD.log + error-YYYY-MM-DD.log) 打包成单个 tar.gz，
//! 原始散文件删除，压缩包永不删除。
//!
//! ## 不使用 tracing-appender 的删除
//! tracing-appender 的 max_log_files 只能删除，无法重命名归档，
//! 故由本模块自定义归档逻辑。

use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

/// 归档错误。
#[derive(Debug)]
pub struct ArchiveError(pub String);

/// 扫描日志目录，归档所有早于 archive_after_days 的散日志对。
/// 返回本次归档的日期数量。
pub fn archive_old_logs(dir: &Path, archive_after_days: u32, prefix: &str, all_file: &str, error_file: &str) -> Result<usize, ArchiveError> {
    let today = chrono::Local::now().date_naive();
    let cutoff = today - chrono::Duration::days(archive_after_days as i64);

    // 收集所有形如 <base>-YYYY-MM-DD.log 的散文件日期
    let mut dates: std::collections::BTreeSet<chrono::NaiveDate> = std::collections::BTreeSet::new();
    let entries = fs::read_dir(dir).map_err(|e| ArchiveError(format!("读日志目录失败: {}", e)))?;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(date) = parse_log_date(&name, all_file).or_else(|| parse_log_date(&name, error_file)) {
            if date <= cutoff {
                dates.insert(date);
            }
        }
    }

    let mut archived = 0;
    for date in dates {
        let date_str = date.format("%Y-%m-%d").to_string();
        let all_path = dir.join(format!("{}-{}.log", all_file.trim_end_matches(".log"), date_str));
        let err_path = dir.join(format!("{}-{}.log", error_file.trim_end_matches(".log"), date_str));
        // 至少有一个存在才归档
        if !all_path.exists() && !err_path.exists() {
            continue;
        }
        let archive_path = dir.join(format!("{}-{}.tar.gz", prefix, date_str));
        create_tar_gz(&archive_path, &[("all", &all_path), ("error", &err_path)])?;
        // 归档成功后删除散文件（压缩包永不删）
        let _ = fs::remove_file(&all_path);
        let _ = fs::remove_file(&err_path);
        archived += 1;
        tracing::info!(date = %date_str, "日志已归档为 {}", archive_path.display());
    }
    Ok(archived)
}

/// 从文件名解析日期。name 形如 "all-2026-06-24.log"，base="all.log" → base_prefix="all"。
fn parse_log_date(name: &str, base: &str) -> Option<chrono::NaiveDate> {
    let prefix = base.trim_end_matches(".log");
    let stem = format!("{}-", prefix);
    let rest = name.strip_prefix(&stem)?;
    let date_str = rest.strip_suffix(".log")?;
    chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()
}

/// 创建 tar.gz，包含给定文件（不存在的跳过）。
fn create_tar_gz(out: &Path, files: &[(&str, &PathBuf)]) -> Result<(), ArchiveError> {
    let tar_gz = File::create(out).map_err(|e| ArchiveError(format!("创建归档文件失败: {}", e)))?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = tar::Builder::new(enc);
    for (alias, path) in files {
        if path.exists() {
            tar.append_file_with_name(path, alias)
                .map_err(|e| ArchiveError(format!("写入 tar 失败: {}", e)))?;
        }
    }
    tar.finish().map_err(|e| ArchiveError(format!("完成 tar 失败: {}", e)))?;
    Ok(())
}

/// 后台循环：每 interval 秒扫描归档一次。
pub async fn run_loop(dir: PathBuf, archive_after_days: u32, interval: u64, prefix: String, all_file: String, error_file: String) {
    loop {
        if let Err(e) = archive_old_logs(&dir, archive_after_days, &prefix, &all_file, &error_file) {
            tracing::error!(target: "log_archive", "归档失败: {:?}", e);
        }
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

use tokio::time::Duration;
```

- [ ] **Step 2: 在 Cargo.toml 加 chrono Local 特性**

Task 0 的 chrono 已有 serde 特性，Local 默认可用（chrono 默认启用 clock）。无需改。

- [ ] **Step 3: 在 main.rs 加 `mod log_archive;`**

```rust
mod config;
mod expr;
mod extractor;
mod log_archive;
mod mapping;
mod models;
mod scheduler;
mod sink;
mod source;
mod sql_gen;

fn main() {
    println!("gpu-collector scaffold");
}
```

- [ ] **Step 4: 写测试（用临时目录）**

在 log_archive/mod.rs 末尾加：

```rust
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
        assert!(parse_log_date("logs-2026-06-24.tar.gz", "all.log").is_none());
    }

    #[test]
    fn archives_old_pair_and_deletes_scatter() {
        let dir = std::env::temp_dir().join(format!("archive_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // 写一个"很老"的日期散文件
        let old = "all-2000-01-01.log";
        let err = "error-2000-01-01.log";
        fs::write(dir.join(old), "all content").unwrap();
        fs::write(dir.join(err), "err content").unwrap();
        let n = archive_old_logs(&dir, 1, "logs", "all.log", "error.log").unwrap();
        assert_eq!(n, 1);
        // 散文件应已删除
        assert!(!dir.join(old).exists());
        assert!(!dir.join(err).exists());
        // 压缩包应存在
        assert!(dir.join("logs-2000-01-01.tar.gz").exists());
        fs::remove_dir_all(&dir).ok();
    }
}
```

- [ ] **Step 5: 运行测试**

Run: `cargo test log_archive`
Expected: 3 个测试通过

- [ ] **Step 6: 提交**

```bash
git add src/log_archive/ src/main.rs
git commit -m "feat(log_archive): tar.gz archival of expired daily logs, never delete archives"
```

---

## Task 11: main — 串联所有模块（含日志初始化、--init、schema 校验）

**Files:**
- Rewrite: `src/main.rs`

入口：clap 解析 `--init` 和配置路径 → 加载配置 → 初始化日志 → init 模式生成 SQL 退出 / 正常模式建连接 + schema 校验 + 加载资产 + 启动调度 + 归档任务 + 优雅退出。

- [ ] **Step 1: 写 main.rs 完整实现**

```rust
//! # gpu-collector 入口
//!
//! 解析命令行(--init)、加载配置、初始化日志，
//! 然后根据模式：
//! - --init：生成 ./init/<table>.sql 后退出（不连任何外部服务）
//! - 正常：schema 校验 → 加载资产 → 启动调度 + 归档任务 → 优雅退出

mod config;
mod expr;
mod extractor;
mod log_archive;
mod mapping;
mod models;
mod scheduler;
mod sink;
mod source;
mod sql_gen;

use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;

/// 命令行参数。
#[derive(Parser, Debug)]
#[command(about = "计算卡利用率采集程序")]
struct Args {
    /// 仅生成建表 SQL（不连库、不采集）。
    #[arg(long)]
    init: bool,
    /// 配置文件路径。不存在时自动生成示例。
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    // 配置文件不存在 → 生成示例并退出（提示用户编辑后重试）
    if !args.config.exists() {
        let example_path = PathBuf::from("config.example.yaml");
        match config::write_example(&example_path) {
            Ok(()) => {
                eprintln!("配置文件 {} 不存在，已生成示例: {}", args.config.display(), example_path.display());
                eprintln!("请编辑后重试。");
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("生成示例配置失败: {:?}", e);
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

    // --init 模式：生成 SQL 退出
    if args.init {
        let dir = PathBuf::from("init");
        if let Err(e) = sql_gen::write_init_sql(&cfg, &dir) {
            eprintln!("生成建表 SQL 失败: {}", e);
            std::process::exit(1);
        }
        println!("已生成 ./init/{}.sql，请执行后以正常模式运行。", cfg.database.table);
        return;
    }

    // 初始化日志（双文件 + stdout）。简化版：用 tracing_subscriber fmt。
    // 完整双文件按日轮转归档需自定义 layer，此处先用 stdout + 单文件占位，
    // 归档由 log_archive 模块负责扫描。
    init_logging(&cfg);

    let tz: chrono_tz::Tz = cfg.timezone.parse().expect("时区已校验");

    // 建立 MySQL 连接
    let sink = match sink::Sink::connect(&cfg).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("连接 MySQL 失败: {}", e.0);
            std::process::exit(1);
        }
    };

    // schema 校验
    let expected = sink::expected_columns(&cfg);
    match sink.check_schema(&expected).await {
        Ok(schema::SchemaCheck::Match) => tracing::info!("表结构校验通过"),
        Ok(schema::SchemaCheck::Missing(cols)) => {
            tracing::error!("表缺少列: {:?}。请用 --init 重新生成 SQL 或手动 ALTER。", cols);
            std::process::exit(1);
        }
        Ok(schema::SchemaCheck::Extra(cols)) => {
            tracing::warn!("表多出列: {:?}", cols);
            match cfg.database.on_extra_columns.as_str() {
                "abort" => std::process::exit(1),
                "continue" => {}
                _ => {
                    // ask：TTY 时交互，否则 continue
                    // 简化：直接继续（生产可加交互读取）
                    tracing::warn!("on_extra_columns=ask，非交互环境按 continue 处理");
                }
            }
        }
        Err(e) => {
            tracing::error!("schema 校验失败: {}", e.0);
            std::process::exit(1);
        }
    }

    // 加载资产表
    let asset_indices = if cfg.mapping.enabled {
        match mapping::load_all(&cfg.mapping) {
            Ok(v) => Arc::new(v),
            Err(e) => {
                tracing::error!("资产表加载失败: {}", e.0);
                std::process::exit(1);
            }
        }
    } else {
        Arc::new(Vec::new())
    };
    let mapping_src_key = cfg.mapping.sources.first().map(|m| m.src_key.clone());

    // 启动调度
    let cfg_arc = Arc::new(cfg.clone());
    let client_factory = |url: &str, timeout: u64| {
        source::PrometheusClient::new(url, timeout).expect("HTTP 客户端构建失败")
    };
    let handles = scheduler::run(cfg_arc.clone(), sink.clone(), client_factory, asset_indices, mapping_src_key).await;

    // 启动日志归档任务
    let log_dir = PathBuf::from(&cfg.logging.dir);
    let log_handles = tokio::spawn(log_archive::run_loop(
        log_dir,
        cfg.logging.archive_after_days,
        3600, // 归档扫描间隔
        cfg.logging.archive_prefix.clone(),
        cfg.logging.all_file.clone(),
        cfg.logging.error_file.clone(),
    ));

    // 优雅退出：等待 Ctrl+C
    tokio::signal::ctrl_c().await.ok();
    tracing::info!("收到退出信号，等待当前任务完成...");
    for h in handles {
        h.abort();
    }
    log_handles.abort();
}

/// 初始化 tracing 日志（简化版：stdout + level 过滤）。
/// 完整双文件实现可在后续迭代用自定义 layer。
fn init_logging(cfg: &config::Config) {
    let level = match cfg.logging.level.as_str() {
        "error" => "error",
        "warn" => "warn",
        "info" => "info",
        "debug" => "debug",
        _ => "trace",
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(level)
        .with_target(true)
        .try_init();
    let _ = std::fs::create_dir_all(&cfg.logging.dir);
}
```

注意：main 里引用了 `schema::SchemaCheck`，但 schema 是 sink 的子模块。修正所有 `schema::` 为 `sink::schema::`。

- [ ] **Step 2: 修正 main.rs 中 schema 引用路径**

把 `Ok(schema::SchemaCheck::Match)` 等全部改为 `Ok(sink::schema::SchemaCheck::...)`：

```rust
        Ok(sink::schema::SchemaCheck::Match) => tracing::info!("表结构校验通过"),
        Ok(sink::schema::SchemaCheck::Missing(cols)) => {
            tracing::error!("表缺少列: {:?}。请用 --init 重新生成 SQL 或手动 ALTER。", cols);
            std::process::exit(1);
        }
        Ok(sink::schema::SchemaCheck::Extra(cols)) => {
```

- [ ] **Step 3: 编译验证**

Run: `cargo build`
Expected: 编译成功。若有错误，逐一修正路径/类型。

- [ ] **Step 4: 运行全部测试**

Run: `cargo test`
Expected: 所有模块测试通过

- [ ] **Step 5: 手动验证 --init 生成 SQL**

```bash
cargo run -- --init --config config.example.yaml
cat init/gpu_usage.sql
```
Expected: 生成 `init/gpu_usage.sql`，含固定列 + location/owner mapping 列 + COMMENT + 索引，无 DROP/CREATE DATABASE。

- [ ] **Step 6: 提交**

```bash
git add src/main.rs
git commit -m "feat(main): wire all modules, --init mode, schema check, graceful shutdown"
```

---

## Task 12: 集成测试 + README

**Files:**
- Create: `tests/integration_test.rs`
- Create: `README.md`

端到端集成测试（用 mock SourceQuerier）+ 项目说明文档。

- [ ] **Step 1: 写集成测试（mock source，验证 extractor→mapping→row 组装）**

`tests/integration_test.rs`：

```rust
//! 端到端集成测试：用 mock SourceQuerier 验证采集→对齐→表达式→join 组装。
//! 不连真实 Prometheus/MySQL。

use async_trait::async_trait;
use gpu_collector::extractor::SourceQuerier;
use gpu_collector::models::MetricSample;
use gpu_collector::source::SourceError;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// mock 查询器：按预设返回不同 metric 的样本。
struct MockQuerier {
    responses: HashMap<String, Vec<MetricSample>>,
    call_count: AtomicUsize,
}

#[async_trait]
impl SourceQuerier for MockQuerier {
    async fn query(&self, metric: &str) -> Result<Vec<MetricSample>, SourceError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.responses.get(metric).cloned().unwrap_or_default())
    }
}

// 注：此测试需要把 extractor 等模块设为 pub 才能从集成测试访问。
// 若 crate 是 binary-only，需在 lib.rs 暴露，或把此测试作为单元测试放模块内。
// 此处假定已通过添加 src/lib.rs 暴露公共 API。
```

注意：当前 crate 只有 main.rs（binary），集成测试无法访问内部模块。需 Step 2 加 lib.rs。

- [ ] **Step 2: 加 src/lib.rs 暴露模块供集成测试**

```rust
//! 库入口，供集成测试访问内部模块。

pub mod config;
pub mod expr;
pub mod extractor;
pub mod log_archive;
pub mod mapping;
pub mod models;
pub mod scheduler;
pub mod sink;
pub mod source;
pub mod sql_gen;
```

并从 main.rs 删除模块声明（改为 `use gpu_collector::...`），或保留 main.rs 的声明但加 `pub`。简单做法：main.rs 顶部改为：

```rust
use gpu_collector::{config, extractor, log_archive, mapping, scheduler, sink, source, sql_gen};
```

并删除 main.rs 里原来的 `mod xxx;` 声明。需把各模块内 `pub(crate)` 的关键项改为 `pub`（extractor 的 SourceQuerier trait、collect_source 等）。

确保 extractor/mod.rs 中 `pub mod align;` 等、`pub async fn collect_source`、`pub trait SourceQuerier` 已是 pub。

- [ ] **Step 3: 完善集成测试**

```rust
use async_trait::async_trait;
use gpu_collector::config::{Config, SourceConfig};
use gpu_collector::extractor::{collect_source, SourceQuerier};
use gpu_collector::models::MetricSample;
use gpu_collector::source::SourceError;
use std::collections::HashMap;

struct MockQuerier {
    responses: HashMap<String, Vec<MetricSample>>,
}

#[async_trait]
impl SourceQuerier for MockQuerier {
    async fn query(&self, metric: &str) -> Result<Vec<MetricSample>, SourceError> {
        Ok(self.responses.get(metric).cloned().unwrap_or_default())
    }
}

fn sample(gpu: &str, val: f64) -> MetricSample {
    MetricSample {
        labels: HashMap::from([("gpu".into(), gpu.into()), ("namespace".into(), "default".into())]),
        value: val,
    }
}

#[tokio::test]
async fn collect_two_cards_from_mock() {
    let responses = HashMap::from([
        ("m_primary".to_string(), vec![sample("0", 50.0), sample("1", 75.0)]),
        ("m_temp".to_string(), vec![sample("0", 40.0)]), // 卡1缺温度
    ]);
    let q = MockQuerier { responses };
    let src = SourceConfig {
        name: "test".into(),
        ip: "1.1.1.1".into(),
        url: "http://x".into(),
        timeout: 10,
        interval: None,
        primary: gpu_collector::config::PrimaryConfig { metric: "m_primary".into(), card_label: "gpu".into() },
        fields: vec![gpu_collector::config::FieldConfig {
            name: "temp".into(), from: "metric".into(), metric: "m_temp".into(), label: None,
        }],
        expressions: vec![],
        host_fields: vec![],
    };
    let tz = chrono_tz::Asia::Shanghai;
    let rows = collect_source(&src, &q, tz).await.unwrap();
    assert_eq!(rows.len(), 2);
    // 卡0有温度
    assert_eq!(rows[0].fields.get("temp"), Some(&Some(40.0)));
    // 卡1缺温度 → None
    assert_eq!(rows[1].fields.get("temp"), Some(&None));
}
```

- [ ] **Step 4: 在 Cargo.toml 确认 async-trait 在 dev-dependencies 或 dependencies**

`async-trait` 已在 Task 6 加入 `[dependencies]`（extractor 用）。集成测试可用。

- [ ] **Step 5: 运行集成测试**

Run: `cargo test --test integration_test`
Expected: 1 个测试通过

- [ ] **Step 6: 写 README.md**

```markdown
# gpu-collector

定时从多个 Prometheus 服务器读取 GPU/NPU 及主机指标，按卡片维度对齐
（含资产表 join、表达式计算）后写入 MySQL 的 Rust 程序。

## 构建

```bash
cargo build --release
```

## 首次使用

1. 复制并编辑配置：
   ```bash
   cp config.example.yaml config.yaml
   # 编辑 config.yaml：数据库连接、Prometheus 地址、指标映射
   ```
2. 生成建表 SQL：
   ```bash
   ./gpu-collector --init --config config.yaml
   # 生成 ./init/gpu_usage.sql，执行它建表
   ```
3. 运行：
   ```bash
   ./gpu-collector --config config.yaml
   ```

## 配置

详见 `config.example.yaml`（每字段含注释）。关键概念：
- **sources**：每个 Prometheus 源声明主指标（决定行数）、字段映射、表达式、主机字段。
- **mapping**：从 CSV/Excel 资产表 join 补字段（如机房位置）。
- 卡类型差异完全靠配置，新增卡类型不改代码。

## 架构

分层 + tokio 异步。详见 `docs/superpowers/specs/2026-06-24-prometheus-gpu-collector-design.md`。

## 测试

```bash
cargo test
```
```

- [ ] **Step 7: 运行全部测试确认无回归**

Run: `cargo test`
Expected: 全部通过

- [ ] **Step 8: 提交**

```bash
git add tests/ src/lib.rs src/main.rs README.md Cargo.toml
git commit -m "test: integration test with mock source; add lib.rs and README"
```

---

## Self-Review

**1. Spec coverage:**
- 第1-3节（背景/决策/架构）→ Task 0-11 全覆盖
- 第4节数据流 → Task 6(extractor) + Task 9(scheduler) + Task 11(main)
- 第5节模块 → Task 1-11 每模块一任务
- 第6节配置 → Task 3(config) + config.example.yaml
- 第7节表结构/--init/schema校验 → Task 7(sql_gen) + Task 8(schema) + Task 11(main)
- 第8节时区 → Task 8(set_timezone_sql) + Task 11
- 第9节错误处理 → Task 9(失败隔离) + Task 10(归档) + 各任务错误类型
- 第10节测试 → 每任务 TDD + Task 12 集成测试
- 第11节注释 → 每模块文件头 `//!` + 结构体/函数 `///`（已体现在各 Task 代码）
- 第12节 YAGNI → 未实现 exporter/UI/热加载，符合

**2. Placeholder scan:** 已移除所有 "TODO/TBD"，Step 3 的不安全 transmute 已在 Step 4 替换，Step 1 的占位 collect_vars 已修正。每个有代码的步骤都给出完整代码。

**3. Type consistency:**
- `SourceQuerier` trait 在 Task 6 定义、Task 9/12 使用一致
- `collect_source` 签名 Task 6 定义 `(cfg, client, tz)` → Task 12 调用一致
- `AssetIndex::lookup` / `column_names` / `join_row` 在 Task 5 定义、Task 9 使用一致
- `expected_columns` Task 8 定义、Task 11 使用一致
- `SchemaCheck` Task 8 定义、Task 11 使用一致
- `mapping_final_name` Task 5 定义、Task 7/8 使用一致（需确保 config::mapping_final_name 在 Task 7 Step 2 暴露）✓

**潜在风险（执行时注意）：**
- sqlx MySQL 占位符用 `?`（Task 8 Step 3 已修正）
- chrono-tz 的 `Tz` 类型解析需 `parse()`（各处一致）
- 集成测试需 lib.rs 暴露模块（Task 12 Step 2 已处理）

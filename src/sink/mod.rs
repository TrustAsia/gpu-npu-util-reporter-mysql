//! # sink 模块
//!
//! 落库层（纯 I/O 边界）。只负责"写 MySQL"，不知道指标含义。
//!
//! 职责：
//! - 建立连接池并对每个连接 `SET time_zone`（程序/连接/清理三方同一时区）。
//! - schema 校验（读实际表列，与期望列对比）。
//! - 批量写入采集行（固定列 + mapping 列动态拼入）。
//! - 保留期清理（删除早于 retention_days 的旧行）。
//!
//! schema 对比等纯逻辑放在 [`schema`] 子模块，便于单元测试。

pub mod schema;

use crate::config::{mapping_final_name, Config};
use crate::models::Row;
use schema::{compare, SchemaCheck};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::MySqlPool;
use std::collections::HashSet;

/// MySQL 连接池封装。
pub struct Sink {
    pool: MySqlPool,
    table: String,
}

/// sink 错误（携带可读描述）。
#[derive(Debug)]
pub struct SinkError(pub String);

impl Sink {
    /// 建立连接池，并对**每个**连接执行 `SET time_zone`。
    ///
    /// 时区必须与程序采集时间、保留期清理基准一致，故用连接池的 `after_connect`
    /// 回调在每个连接建立时统一设置（仅 set 一次连接不够：池中其它连接不会被设置，
    /// 会影响 `run_retention` 里 `NOW()` 的时区基准）。
    pub async fn connect(cfg: &Config) -> Result<Self, SinkError> {
        // 用 MySqlConnectOptions 的 builder API 构造连接参数，而非拼接
        // `mysql://user:password@host` URL。原因：password 中若含 `@`/`:`/`/`/`#`/空格
        // 等字符（生成式密码极常见，如 `P@ss:w0rd/123`），原样 format! 进 URL 会被
        // sqlx 的 URL 解析器错切（`@` 被当主机分隔符、`#` 截断 fragment 等），导致
        // "连接失败"或连到错误的主机——这与 R10(表名插值)同性质的"配置值原样拼接"
        // 隐患。builder API 各字段以 &str 直传，不经 URL 编码/解析，从根上消除该问题。
        let options = MySqlConnectOptions::new()
            .host(&cfg.database.host)
            .port(cfg.database.port)
            .username(&cfg.database.user)
            .password(&cfg.database.password)
            .database(&cfg.database.database);
        let tz: chrono_tz::Tz = cfg.timezone.parse().expect("时区已校验");
        let tz_sql = schema::set_timezone_sql(tz);
        let pool = MySqlPoolOptions::new()
            .max_connections(cfg.database.max_connections)
            .after_connect(move |conn, _meta| {
                // 每个新连接建立时执行 SET time_zone（捕获 tz_sql 的克隆）。
                let sql = tz_sql.clone();
                Box::pin(async move {
                    sqlx::query(&sql).execute(conn).await.map(|_| ())
                })
            })
            .connect_with(options)
            .await
            .map_err(|e| SinkError(format!("连接 MySQL 失败: {}", e)))?;
        Ok(Self {
            pool,
            table: cfg.database.table.clone(),
        })
    }

    /// 校验表结构。`expected` 为期望列集合（由 [`expected_columns`] 计算）。
    pub async fn check_schema(&self, expected: &HashSet<String>) -> Result<SchemaCheck, SinkError> {
        let rows: Vec<(String,)> = sqlx::query_as(&schema::list_columns_sql(&self.table))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SinkError(format!("读取表结构失败: {}", e)))?;
        let actual: HashSet<String> = rows.into_iter().map(|(c,)| c).collect();
        Ok(compare(expected, &actual))
    }

    /// 写入采集行（固定列 + mapping 列动态拼入）。
    ///
    /// 采用**逐行 INSERT**而非单条多 VALUES 批量，且**单行失败不中断**：某行 INSERT
    /// 失败时记 ERROR 并跳过该行继续写其余行（失败隔离优先于全批回滚）。
    ///
    /// 这是有意的语义选择：MySQL 抖动/单行违规（如超长值在严格模式下被拒）只应丢该行，
    /// 不应让同批其余几十行一并丢失。返回值为**成功写入**的行数（可能 < 入参行数）。
    /// 若调用方需"全批原子"，应自行在外层用事务包裹——但本采集场景下"部分成功"
    /// 优于"全丢"（一轮通常仅几十行，单行失败极少）。
    ///
    /// `mapping_cols` 为该批次要写入的 mapping 列名（最终名）列表，按此顺序绑定。
    /// 每个值从 `row.strings` 取；缺失则写 NULL。
    ///
    /// **列顺序单一真相源**：写入的固定列名来自 [`crate::config::FIXED_COLUMNS`]
    /// （排除自增的 `id`），由 [`fixed_write_values`] 按相同顺序产出各列的绑定值，
    /// 故"SQL 列名顺序"与"bind 值顺序"由同一段逻辑驱动，杜绝二者漂移导致写串列。
    pub async fn insert_rows(&self, rows: &[Row], mapping_cols: &[String]) -> Result<u64, SinkError> {
        if rows.is_empty() {
            return Ok(0);
        }
        // 固定列名（不含自增 id），来自 FIXED_COLUMNS 单一真相源。
        let fixed_names: Vec<&str> = crate::config::FIXED_COLUMNS
            .iter()
            .map(|(n, _, _, _)| *n)
            .filter(|n| *n != "id")
            .collect();
        let all_cols: Vec<&str> = fixed_names
            .iter()
            .copied()
            .chain(mapping_cols.iter().map(|s| s.as_str()))
            .collect();
        // MySQL sqlx 用 ? 占位符。
        let placeholders: Vec<&str> = all_cols.iter().map(|_| "?").collect();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table,
            all_cols.join(", "),
            placeholders.join(", ")
        );

        let mut written: u64 = 0;
        for row in rows {
            // 固定列值：与 fixed_names 完全相同的顺序产出（见 fixed_write_values）。
            // 列名顺序与取值顺序都由 FIXED_COLUMNS 单一真相源驱动。
            let mut q = sqlx::query(&sql);
            for v in fixed_write_values(row) {
                q = match v {
                    ColVal::Time(t) => q.bind(t),
                    ColVal::Str(s) => q.bind(s),
                    ColVal::F64(f) => q.bind(f),
                };
            }
            // mapping 列：均为字符串，从 row.strings 取，缺失写 NULL。
            for mc in mapping_cols {
                q = q.bind(row.strings.get(mc).cloned().flatten());
            }
            match q.execute(&self.pool).await {
                Ok(_) => written += 1,
                // 单行失败：记 ERROR 跳过该行继续（失败隔离），而非 `?` 中断整批。
                // 中断会让"一行违规/抖动 → 同批其余行全丢"，与逐行 INSERT 的初衷相悖。
                Err(e) => tracing::error!(
                    target: "sink",
                    ip = %row.ip,
                    card_id = %row.card_id,
                    source = %row.source,
                    error = %e,
                    "单行 INSERT 失败，跳过该行继续写其余行（失败隔离）"
                ),
            }
        }
        Ok(written)
    }

    /// 执行保留期清理，返回删除行数。
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
            set.insert(mapping_final_name(col));
        }
    }
    set
}

/// 固定列绑定值的承载类型。
///
/// 固定列混合了三种 SQL 类型：时间戳(`ts`)、字符串(`ip`/`card_id`/`namespace`/
/// `pod`/`source`)、浮点(各数值列)。用一个枚举统一表达，使 [`fixed_write_values`]
/// 能按 [`crate::config::FIXED_COLUMNS`] 顺序产出一个同构列表，再在 [`Sink::insert_rows`]
/// 中按变体逐个 `bind`，避免"SQL 列名顺序"与"绑定值顺序"两套独立维护的真相源。
enum ColVal {
    /// DATETIME 列（ts）。
    Time(chrono::NaiveDateTime),
    /// VARCHAR 列（ip/card_id/namespace/pod/source)；None 写 NULL。
    Str(Option<String>),
    /// DOUBLE 列（各数值字段）；None 写 NULL。
    F64(Option<f64>),
}

#[cfg(test)]
impl std::fmt::Debug for ColVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColVal::Time(t) => f.debug_tuple("Time").field(t).finish(),
            ColVal::Str(s) => f.debug_tuple("Str").field(s).finish(),
            ColVal::F64(v) => f.debug_tuple("F64").field(v).finish(),
        }
    }
}

/// 按 [`crate::config::FIXED_COLUMNS`]（排除自增 `id`）的**同一顺序**产出各行固定列的值。
///
/// 顺序与 [`Sink::insert_rows`] 中拼 SQL 用的列名顺序严格一致——二者都派生自
/// FIXED_COLUMNS，故增删/调整固定列时此处自动跟随，不会出现"列名变了、绑定没变"
/// 的写串列风险。
///
/// 返回顺序为：`ts, ip, card_id, namespace, pod, gpu_util, mem_util, temp, power,
/// host_cpu, host_mem, host_fds, source`。
fn fixed_write_values(row: &Row) -> Vec<ColVal> {
    vec![
        ColVal::Time(row.ts.naive_local()),
        ColVal::Str(Some(row.ip.clone())),
        ColVal::Str(Some(row.card_id.clone())),
        ColVal::Str(row.strings.get("namespace").cloned().flatten()),
        ColVal::Str(row.strings.get("pod").cloned().flatten()),
        ColVal::F64(row.fields.get("gpu_util").copied().flatten()),
        ColVal::F64(row.fields.get("mem_util").copied().flatten()),
        ColVal::F64(row.fields.get("temp").copied().flatten()),
        ColVal::F64(row.fields.get("power").copied().flatten()),
        ColVal::F64(row.fields.get("host_cpu").copied().flatten()),
        ColVal::F64(row.fields.get("host_mem").copied().flatten()),
        ColVal::F64(row.fields.get("host_fds").copied().flatten()),
        ColVal::Str(Some(row.source.clone())),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FIXED_COLUMNS;
    use chrono_tz::Asia::Shanghai;
    use std::collections::HashMap;

    /// 守护测试：[`fixed_write_values`] 产出的值序列，其顺序必须与
    /// [`crate::config::FIXED_COLUMNS`] 去掉自增 `id` 后的列顺序**逐项一致**。
    ///
    /// 这是防止"写串列"的关键回归测试。关键点：给**每个**数值列赋一个唯一可辨识的
    /// 值（编码进整数位：gpu_util=101.0、mem_util=102.0…），每个字符串列赋唯一串，
    /// 再逐位置断言"第 i 个值 == 第 i 个列名所应得的值"。这样**同类型的相邻列互换
    /// 也能被捕获**（仅校验类型变体无法发现两个 DOUBLE 列互换）。
    #[test]
    fn fixed_values_order_matches_fixed_columns() {
        // 每个数值列给一个唯一标识值，编码方式：列序号 +100。
        // strings 列也各给唯一串。
        let row = Row {
            ts: chrono::Utc::now().with_timezone(&Shanghai),
            ip: "IP-VALUE".into(),
            card_id: "CARD-VALUE".into(),
            fields: HashMap::from([
                ("gpu_util".into(), Some(101.0)),
                ("mem_util".into(), Some(102.0)),
                ("temp".into(), Some(103.0)),
                ("power".into(), Some(104.0)),
                ("host_cpu".into(), Some(105.0)),
                ("host_mem".into(), Some(106.0)),
                ("host_fds".into(), Some(107.0)),
            ]),
            strings: HashMap::from([
                ("namespace".into(), Some("NS-VALUE".into())),
                ("pod".into(), Some("POD-VALUE".into())),
            ]),
            source: "SRC-VALUE".into(),
        };
        let vals = fixed_write_values(&row);

        // 期望列序：FIXED_COLUMNS 去掉 id（与 insert_rows 拼 SQL 用的是同一来源）。
        let expected_names: Vec<&str> = FIXED_COLUMNS
            .iter()
            .map(|(n, _, _, _)| *n)
            .filter(|n| *n != "id")
            .collect();
        assert_eq!(
            vals.len(),
            expected_names.len(),
            "fixed_write_values 产出数量({}) 与 FIXED_COLUMNS(去id)({}) 不符",
            vals.len(),
            expected_names.len()
        );

        // 逐位置断言：每个列名位置上的 ColVal 必须携带"该列"应有的值。
        // 这是顺序锁定的核心——值与列名一一对应，互换即失败。
        for (name, val) in expected_names.iter().zip(vals.iter()) {
            match *name {
                "ts" => assert!(matches!(val, ColVal::Time(_)), "ts 位置应为 Time"),
                "ip" => assert_colval_str(val, "IP-VALUE", "ip"),
                "card_id" => assert_colval_str(val, "CARD-VALUE", "card_id"),
                "namespace" => assert_colval_str(val, "NS-VALUE", "namespace"),
                "pod" => assert_colval_str(val, "POD-VALUE", "pod"),
                "source" => assert_colval_str(val, "SRC-VALUE", "source"),
                "gpu_util" => assert_colval_f64(val, 101.0, "gpu_util"),
                "mem_util" => assert_colval_f64(val, 102.0, "mem_util"),
                "temp" => assert_colval_f64(val, 103.0, "temp"),
                "power" => assert_colval_f64(val, 104.0, "power"),
                "host_cpu" => assert_colval_f64(val, 105.0, "host_cpu"),
                "host_mem" => assert_colval_f64(val, 106.0, "host_mem"),
                "host_fds" => assert_colval_f64(val, 107.0, "host_fds"),
                other => panic!("未知的固定列 {}: 测试需同步更新", other),
            }
        }
    }

    /// 断言某位置的 ColVal 是携带期望字符串的 Str。
    fn assert_colval_str(val: &ColVal, expect: &str, col: &str) {
        match val {
            ColVal::Str(Some(s)) => assert_eq!(
                s, expect,
                "{} 位置的字符串值不符(可能列顺序错乱)", col
            ),
            other => panic!("{} 位置期望 Str(Some({:?}))，实际 {:?}", col, expect, other),
        }
    }

    /// 断言某位置的 ColVal 是携带期望数值的 F64。
    fn assert_colval_f64(val: &ColVal, expect: f64, col: &str) {
        match val {
            ColVal::F64(Some(f)) => assert_eq!(
                *f, expect,
                "{} 位置的数值不符(可能列顺序错乱)", col
            ),
            other => panic!("{} 位置期望 F64(Some({}))，实际 {:?}", col, expect, other),
        }
    }

    /// 守护 R12：含特殊字符的密码（`@`/`:`/`/`/`#`/空格，生成式密码极常见）
    /// 若用 `format!` 拼进 `mysql://user:pass@host` URL，`@` 会被当成主机分隔符、
    /// `#` 截断 fragment，导致连到错误主机或连接失败。改用 `MySqlConnectOptions`
    /// 的 builder API（字段以 &str 直传，不经 URL 解析）后，密码被正确 percent-编码，
    /// host/port/username 不受影响。本测试用 `to_url_lossy()` 重建 URL 验证字段完整
    /// （无需联网）。
    #[test]
    fn connect_options_preserves_special_char_password() {
        use sqlx::ConnectOptions;
        let opts = MySqlConnectOptions::new()
            .host("10.0.0.1")
            .port(3306)
            .username("u")
            .password("P@ss:w0rd/123#x")
            .database("db");
        let url = opts.to_url_lossy();
        // host/port/username 必须完好（旧 format! 方式会让 @ 截断 host）。
        assert_eq!(url.host_str(), Some("10.0.0.1"), "host 不应被密码里的 @ 破坏");
        assert_eq!(url.port(), Some(3306), "port 应保留");
        assert_eq!(url.username(), "u", "username 应保留");
        // 密码经 percent-编码后应含编码后的 @ (%40)，而非裸 @（裸 @ 会再次被解析为分隔符）。
        let pw = url.password().expect("密码应存在");
        assert!(pw.contains("%40"), "密码里的 @ 应被编码为 %40，实际: {}", pw);
        assert!(!pw.contains('@'), "密码里不应残留裸 @，实际: {}", pw);
    }
}

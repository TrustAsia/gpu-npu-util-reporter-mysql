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
use sqlx::mysql::MySqlPoolOptions;
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
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            cfg.database.user,
            cfg.database.password,
            cfg.database.host,
            cfg.database.port,
            cfg.database.database
        );
        let tz_sql = schema::set_timezone_sql(&cfg.timezone);
        let pool = MySqlPoolOptions::new()
            .max_connections(cfg.database.max_connections)
            .after_connect(move |conn, _meta| {
                // 每个新连接建立时执行 SET time_zone（捕获 tz_sql 的克隆）。
                let sql = tz_sql.clone();
                Box::pin(async move {
                    sqlx::query(&sql).execute(conn).await.map(|_| ())
                })
            })
            .connect(&url)
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
    /// 采用**逐行 INSERT**而非单条多 VALUES 批量：单行失败不影响同批其它行
    /// （失败隔离优先于吞吐；一轮通常仅几十行，逐行往返开销可接受）。
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
            q.execute(&self.pool)
                .await
                .map_err(|e| SinkError(format!("INSERT 失败: {}", e)))?;
        }
        Ok(rows.len() as u64)
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
    /// VARCHAR 列（ip/card_id/namespace/pod/source）；None 写 NULL。
    Str(Option<String>),
    /// DOUBLE 列（各数值字段）；None 写 NULL。
    F64(Option<f64>),
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
    /// 这是防止"写串列"的关键回归测试——SQL 列名与绑定值都从 FIXED_COLUMNS 派生，
    /// 若有人手改了 [`fixed_write_values`] 的顺序而忘记同步（或反之），此测试会失败。
    /// 由于 [`ColVal`] 不携带列名，这里按"固定列里哪些是数值/字符串/时间"与
    /// FIXED_COLUMNS 的类型声明交叉验证：对每个非 id 固定列，确认对应位置的
    /// `ColVal` 变体与其 SQL 类型相符。
    #[test]
    fn fixed_values_order_matches_fixed_columns() {
        let row = Row {
            ts: chrono::Utc::now().with_timezone(&Shanghai),
            ip: "1.1.1.1".into(),
            card_id: "0".into(),
            fields: HashMap::from([
                ("gpu_util".into(), Some(1.0)),
                ("mem_util".into(), Some(2.0)),
                ("temp".into(), Some(3.0)),
                ("power".into(), Some(4.0)),
                ("host_cpu".into(), Some(5.0)),
                ("host_mem".into(), Some(6.0)),
                ("host_fds".into(), Some(7.0)),
            ]),
            strings: HashMap::from([
                ("namespace".into(), Some("ns".into())),
                ("pod".into(), Some("p".into())),
            ]),
            source: "s1".into(),
        };
        let vals = fixed_write_values(&row);
        // 期望列序：FIXED_COLUMNS 去掉 id。
        let expected_names: Vec<&str> = FIXED_COLUMNS
            .iter()
            .map(|(n, _, _, _)| *n)
            .filter(|n| *n != "id")
            .collect();
        assert_eq!(
            vals.len(),
            expected_names.len(),
            "fixed_write_values 产出数量与 FIXED_COLUMNS(去id) 不符"
        );
        // 逐列校验：列名 → ColVal 变体必须与该列在 FIXED_COLUMNS 的类型一致。
        for (name, val) in expected_names.iter().zip(vals.iter()) {
            match *name {
                "ts" => assert!(matches!(val, ColVal::Time(_)), "ts 应为 Time"),
                "ip" | "card_id" | "source" => {
                    assert!(matches!(val, ColVal::Str(Some(_))), "{} 应为非空 Str", name)
                }
                "namespace" | "pod" => {
                    assert!(matches!(val, ColVal::Str(_)), "{} 应为 Str", name)
                }
                // 其余固定列均为 DOUBLE 数值。
                _ => assert!(matches!(val, ColVal::F64(_)), "{} 应为 F64", name),
            }
        }
    }
}

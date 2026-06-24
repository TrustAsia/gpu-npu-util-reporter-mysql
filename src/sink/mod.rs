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
    /// 建立连接池并对连接执行 `SET time_zone`。
    ///
    /// 时区必须与程序采集时间、保留期清理基准一致，故在此统一设置。
    pub async fn connect(cfg: &Config) -> Result<Self, SinkError> {
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            cfg.database.user,
            cfg.database.password,
            cfg.database.host,
            cfg.database.port,
            cfg.database.database
        );
        let pool = MySqlPoolOptions::new()
            .max_connections(cfg.database.max_connections)
            .connect(&url)
            .await
            .map_err(|e| SinkError(format!("连接 MySQL 失败: {}", e)))?;
        // 连接级时区（影响 NOW() 与写入的 DATETIME 解释）。
        sqlx::query(&schema::set_timezone_sql(&cfg.timezone))
            .execute(&pool)
            .await
            .map_err(|e| SinkError(format!("SET time_zone 失败: {}", e)))?;
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

    /// 批量写入行（固定列 + mapping 列动态拼入）。
    ///
    /// `mapping_cols` 为该批次要写入的 mapping 列名（最终名）列表，按此顺序绑定。
    /// 每个值从 `row.strings` 取；缺失则写 NULL。
    pub async fn insert_rows(&self, rows: &[Row], mapping_cols: &[String]) -> Result<u64, SinkError> {
        if rows.is_empty() {
            return Ok(0);
        }
        // 固定列顺序（须与绑定时一致）。
        let fixed = [
            "ts", "ip", "card_id", "namespace", "pod", "gpu_util", "mem_util", "temp", "power",
            "host_cpu", "host_mem", "host_fds", "source",
        ];
        let all_cols: Vec<&str> = fixed
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
            let ts = row.ts.naive_local();
            let mut q = sqlx::query(&sql)
                .bind(ts)
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

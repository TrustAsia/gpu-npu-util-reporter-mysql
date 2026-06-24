//! # gpu-npu-util-reporter 库入口
//!
//! 把各业务模块暴露为公共 API，供集成测试（`tests/`）与未来其它调用方访问。
//! 二进制入口在 [`main`](../main.rs)，二者共享同一组模块实现。
//!
//! ## 模块分层（依赖自底向上）
//! - [`models`]：共享数据结构（依赖图叶子）。
//! - [`expr`]：表达式解析求值（纯函数）。
//! - [`config`]：YAML 配置反序列化 + 校验 + 生成示例。
//! - [`source`]：Prometheus 客户端（I/O 边界）。
//! - [`mapping`]：资产表加载与 join（纯内存）。
//! - [`extractor`]：主指标提取与字段对齐（核心业务）。
//! - [`sql_gen`]：`--init` 建表 SQL 生成。
//! - [`sink`]：MySQL 写入 + schema 校验（I/O 边界）。
//! - [`scheduler`]：每源并发采集调度 + 失败隔离。
//! - [`log_archive`]：日志按日 tar.gz 归档。

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

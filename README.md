# gpu-npu-util-reporter-mysql

定时从多个 Prometheus 服务器读取计算卡（GPU / NPU）及主机指标，按**每张卡一行**对齐（含资产表 join、表达式计算）后写入 MySQL 的 Rust 程序。

NVIDIA DCGM、昇腾 NPU 及未来其它厂商的原始指标名 / 标签 / 显存字段各不相同，但最终写入同一张表、同一组列。**卡类型差异完全由 YAML 配置表达，新增卡类型零代码改动。**

---

## 特性

- **多源异构统一**：一个程序同时采集多个 Prometheus，卡类型差异靠配置驱动。
- **配置驱动**：指标映射、字段来源、派生表达式、采集周期、时区、资产表关联全在 YAML。
- **健壮无人值守**：
  - 单源失败隔离（该源本轮跳过，不影响其他源，下一轮重试）；
  - 字段缺失填 `NULL`（不把"无数据"与"值为 0"混淆）；
  - 自动保留期清理（按 `retention_days` 定期删旧行）；
  - 按日双文件日志（完整 INFO+ / 错误 ERROR），超期散日志打包成 `tar.gz` 归档，压缩包永不删除。
- **`--init` 模式**：根据配置一键生成完整建表 SQL（固定列 + mapping 列 + COMMENT + 索引），不连任何外部服务。
- **启动 schema 校验**：正常启动对比实际表列与期望列，缺列硬失败、多列按策略（`ask` / `continue` / `abort`）处理。
- **时区一致**：采集时间、MySQL 连接 `time_zone`、保留期清理 `NOW()` 三方同一配置时区。

---

## 构建

```bash
cargo build --release
# 产物：target/release/gpu-npu-util-reporter-mysql
```

依赖见 `Cargo.toml`。HTTP 用 rustls、MySQL 用 tokio-rustls，不依赖系统 OpenSSL，便于静态编译。

---

## 首次使用

### 1. 生成并编辑配置

```bash
./gpu-npu-util-reporter-mysql --config config.yaml
# 首次运行：config.yaml 不存在 → 自动生成 config.example.yaml 并退出
cp config.example.yaml config.yaml
# 编辑 config.yaml：数据库连接、各 Prometheus 地址、指标映射、（可选）资产表
```

`config.example.yaml` 即文档：每个字段都带注释，含 NVIDIA GPU 与昇腾 NPU 两个真实示例。

### 2. 生成建表 SQL 并执行

```bash
./gpu-npu-util-reporter-mysql --init --config config.yaml
# 生成 ./init/<table>.sql（如 ./init/gpu_usage.sql），随后退出
mysql -u collector -p gpu_metrics < init/gpu_usage.sql
```

生成的 SQL 形如：

```sql
CREATE TABLE IF NOT EXISTS gpu_usage (
    id        BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    ts        DATETIME(3) NOT NULL COMMENT '采集时间(毫秒精度,配置时区)',
    ip        VARCHAR(64) NOT NULL COMMENT '主机IP',
    card_id   VARCHAR(32) NOT NULL COMMENT 'GPU/NPU卡号',
    namespace VARCHAR(128) NULL COMMENT 'K8s命名空间,裸金属场景为NULL',
    location  varchar(255) NULL COMMENT '设备所在机房位置',   -- mapping 列
    pod       VARCHAR(256) NULL COMMENT 'Pod名,裸金属场景为NULL',
    gpu_util  DOUBLE NULL COMMENT 'GPU核心利用率(%)',
    ...
    source    VARCHAR(64) NOT NULL COMMENT '数据源名',
    PRIMARY KEY (id),
    INDEX idx_ts_ip_card (ts, ip, card_id),
    INDEX idx_ip_card (ip, card_id),
    INDEX idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

不含 `CREATE DATABASE`、不含 `DROP TABLE`。

### 3. 正常运行

```bash
./gpu-npu-util-reporter-mysql --config config.yaml
# 连 MySQL → 校验 schema → 加载资产表 → 启动各源采集 + 保留期清理 + 日志归档
# Ctrl+C 优雅退出
```

---

## 配置速览

详见 `config.example.yaml`（每字段含注释）。核心概念：

| 段 | 作用 |
|----|------|
| `interval` / `retention_days` / `timezone` | 全局采集周期、数据保留期、统一时区 |
| `database` | MySQL 连接、表名、连接池、`on_extra_columns` 策略 |
| `logging` | 双文件日志 + 按日归档（前缀、保留天数） |
| `mapping` | 从 CSV/Excel 资产表 join 补字段（如机房位置、负责人） |
| `sources[]` | 每个 Prometheus 源：主指标（决定行数）、字段映射、表达式、主机字段 |

**source 配置示例（NVIDIA GPU）：**

```yaml
sources:
  - name: "gpu-cluster-a"
    ip: "10.0.0.1"
    url: "http://10.0.0.1:9400"
    interval: 30                 # 覆盖全局 interval
    primary:
      metric: "DCGM_FI_DEV_GPU_UTIL"
      card_label: "gpu"          # 用作卡号的标签
    fields:
      - { name: "gpu_util", from: "metric", metric: "DCGM_FI_DEV_GPU_UTIL" }
      - { name: "temp",     from: "metric", metric: "DCGM_FI_DEV_GPU_TEMP" }
      - { name: "namespace", from: "label", metric: "DCGM_FI_DEV_GPU_UTIL", label: "namespace" }
    expressions:
      - name: "mem_util"
        expr: "DCGM_FI_DEV_FB_USED / (DCGM_FI_DEV_FB_USED + DCGM_FI_DEV_FB_FREE)"
    host_fields:                 # 主机级，整主机单值复制到每张卡
      - name: "host_cpu"
        expr: '100 - (avg by(instance)(irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)'
```

表达式仅支持 `+ - * / ()` 与变量名（变量名 = metric 名），如显存占用率 `USED / (USED + FREE)`。除零或变量缺失时该字段写 `NULL`，不污染整行。

---

## 架构

分层 + tokio 异步。业务逻辑集中在 `extractor`，I/O 为纯边界，调度与归档为独立后台任务。

```
┌─────────────────────────────────────────────────────────────┐
│ main: CLI(--init) → 配置加载 → 日志初始化 → schema 校验 → 退出│
└─────────────────────────────────────────────────────────────┘
        │ scheduler（每源一个 tokio 任务，失败隔离）
        ▼
┌──────────┐   query    ┌───────────┐  对齐/表达式  ┌──────────┐
│ source   │◀──────────│ extractor │──────────────▶│  Row[]   │
│ (Prom)   │           │ (业务核心) │               └────┬─────┘
└──────────┘           └───────────┘                    │
                          ▲                             ▼
                       mapping join            ┌──────────┐
                       (资产表补字段)           │  sink    │── INSERT ──▶ MySQL
                                               │ (MySQL)  │── 保留期清理
                                               └──────────┘
        │ log_archive（后台任务）：超期散日志 → tar.gz，压缩包永不删
```

- **source**：Prometheus 客户端，查瞬时向量（纯 I/O 边界）。
- **extractor**：主指标 → 行骨架 → 按 `(ip, card_id)` 对齐字段 → 表达式求值（唯一持有业务规则的层）。
- **mapping**：启动时加载 CSV/Excel 资产表建索引，采集时 join 补字段。
- **sink**：批量 INSERT + schema 校验 + 保留期清理（纯 I/O 边界）。
- **scheduler**：每源一个任务循环采集 + 一个保留期清理任务，单源/单轮失败隔离。
- **log_archive**：自定义归档（不用 tracing-appender 的删除，因其只删不归档）。

完整设计见 `docs/superpowers/specs/2026-06-24-prometheus-gpu-collector-design.md`。

---

## 测试

```bash
cargo test             # 单元测试 + 集成测试（不连真实 DB/Prometheus）
```

- 纯逻辑模块（`expr` / `config` / `mapping` / `sql_gen` / `sink::schema` / `log_archive`）有完整单元测试。
- `tests/integration_test.rs` 用 mock `SourceQuerier` 端到端验证采集→对齐→表达式→join 链路。
- I/O 层（真实 MySQL/Prometheus）不在 CI 连真实服务，靠 mock 保证逻辑正确。

---

## 项目结构

```
src/
├── lib.rs              # 库入口，暴露各模块供集成测试访问
├── main.rs             # 二进制入口：CLI、日志初始化、schema 校验、优雅退出
├── models.rs           # 共享数据结构
├── config/mod.rs       # YAML 配置反序列化 + 校验 + 生成示例
├── expr/mod.rs         # 表达式解析与求值（纯函数）
├── source/mod.rs       # Prometheus 客户端 + 响应解析
├── extractor/          # 主指标提取 + 字段对齐（核心业务）
├── mapping/mod.rs      # 资产表加载 + join
├── sql_gen/mod.rs      # --init 建表 SQL 生成
├── sink/               # MySQL 写入 + schema 校验 + 保留期清理
│   ├── mod.rs
│   └── schema.rs
├── scheduler/mod.rs    # 每源并发采集调度 + 失败隔离
└── log_archive/mod.rs  # 日志按日 tar.gz 归档
```

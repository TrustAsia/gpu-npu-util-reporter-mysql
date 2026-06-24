# 计算卡利用率采集程序 — 产品需求文档 / 设计规格

- **状态**：已确认，待实现
- **日期**：2026-06-24
- **技术栈**：Rust + tokio + reqwest + sqlx + serde_yaml + tracing
- **目标数据库**：MySQL
- **参考数据源**：`dcgm-exporter.csv`（NVIDIA DCGM）、`npu-exporter.xlsx`（昇腾 NPU）

---

## 1. 背景与目标

构建一个 Rust 应用程序，定时从多个 Prometheus 服务器读取计算卡（GPU/NPU）及主机指标，按卡片维度对齐后写入 MySQL，用于后续的利用率监控与历史趋势分析。

核心价值：

- **多源异构统一**：NVIDIA DCGM 与昇腾 NPU（以及未来其它厂商）的原始指标名、标签、显存字段各不相同，但最终写入同一张表、同一组列。差异完全由配置表达，**新增卡类型零代码改动**。
- **配置驱动**：所有指标映射、字段来源、表达式计算、采集周期、时区均保存在 YAML 配置文件中。配置文件不存在时自动生成示例。
- **高内聚低耦合**：业务逻辑集中在单一层，I/O（HTTP 查询 / MySQL 写入）作为可替换的纯边界。
- **健壮无人值守**：单源失败隔离、字段缺失填 NULL、自动保留期清理、按日日志轮转。

---

## 2. 已确认的关键决策

| # | 决策点 | 结论 |
|---|--------|------|
| 1 | 目标数据库 | MySQL（追加写入 + 定期清理） |
| 2 | 行粒度 | 每张计算卡一行 |
| 3 | 派生指标 | 多指标表达式计算后，单独命名为新指标/新列 |
| 4 | 异构卡统一方式 | **配置驱动的字段映射**（方案 A），新增卡类型只改配置 |
| 5 | Namespace/Pod | 允许 NULL（裸金属场景无 Pod 概念） |
| 6 | 采集调度 | 全局默认 interval + 每源可覆盖（方案 C） |
| 7 | 单源失败 | 隔离，跳过该源本轮，不影响其他源 |
| 8 | IP 字段来源 | 配置里为每个源手动指定 `ip`（不依赖 instance 标签） |
| 9 | 写入策略 | 追加 INSERT + 配置化保留期清理 |
| 10 | 主机级字段 | 按 ip 对齐后复制到该主机每张卡的每一行 |
| 11 | 主机指标来源 | node_exporter（CPU/内存/句柄数） |
| 12 | 时区 | 配置化时区，默认 `Asia/Shanghai`；程序/连接/清理三方同一时区 |
| 13 | 日志 | 双文件（完整日志 + 单独错误日志），按日轮转；超期散日志打包压缩为单个 tar.gz 归档（all+error 同包），原始散文件删除，**归档压缩包永不删除** |
| 14 | 表结构 | 固定列 + mapping 列；**不在运行时动态改表**。`--init` 生成完整建表 SQL 供用户执行 |
| 15 | 数据库集成测试 | 暂时跳过真实数据库，用 mock |
| 16 | 资产表关联 | `mapping` 配置从外部 CSV/Excel join 补字段；`src_key`=行内键、`dest_key`=**资产表列**；启动时加载一次；多匹配取首条+WARN；无匹配/解析失败填 NULL |
| 17 | mapping 列 | `enabled:false` 时仍建列、采集不填值（NULL）；mapping 列配置含 `comment`，写入 SQL 列 COMMENT |
| 18 | `--init` 模式 | 命令行参数，仅生成 `./init/<table>.sql`（建表 DDL：固定列+mapping列+每列COMMENT+索引，不建库、不加 DROP），**不连 DB、不连 Prom、不采集** |
| 19 | 启动 schema 校验 | 正常启动连 DB 读表结构对比期望列：**缺列→报错退出**；**多列→告警询问是否继续** |

---

## 3. 整体架构（方案 A：分层 + tokio 异步并发）

```
┌─────────────────────────────────────────────────────────┐
│  main.rs         入口：加载配置 → 启动调度器 + 清理任务   │
│  scheduler/      tokio::spawn 每源一个任务，按 interval  │
│  config/         YAML 读取/校验/生成示例 (serde_yaml)     │
│  source/         PromQL 客户端，查瞬时向量 (reqwest)      │
│  extractor/      主指标枚举卡片 → 按 ip+card_id 对齐字段  │
│  expr/           表达式求值 A/B,(A+B)/C (纯函数)          │
│  sink/           MySQL 批量写入 (sqlx)                    │
│  (retention)     定期清理旧数据，并入 sink 模块            │
└─────────────────────────────────────────────────────────┘
```

**crate 选型**：

- `tokio` — 异步运行时，每源一个并发任务
- `reqwest` — Prometheus HTTP 客户端，连接池 + 超时
- `sqlx` — MySQL 异步驱动 + 连接池
- `serde` / `serde_yaml` — 配置反序列化
- `tracing` + `tracing-subscriber` — 结构化日志。**不使用 `tracing-appender`**（其内置 `max_log_files` 只能删除超期文件，无法实现"重命名归档"，故改用自定义日志写入与归档逻辑）
- `flate2` + `tar` — 日志超期归档：把当天的 `all` + `error` 两份日志打包压缩成单个 `tar.gz`，原始散文件删除，压缩包永不删除
- `csv` — 资产表 CSV 解析（mapping 关联）
- `calamine` — 资产表 Excel(.xlsx) 解析（mapping 关联，按 source_sheet 读取）
- `clap` — 命令行参数解析（`--init` 标志、配置文件路径）
- `chrono` / `chrono-tz` — 时区处理
- 表达式求值：自写轻量递归下降解析器（仅支持 `+ - * / ()`），避免引入过重依赖

---

## 4. 数据流

### 4.1 启动流程

1. **解析命令行**：检查 `--init` 标志与配置文件路径。
2. **加载 YAML 配置**；不存在则生成示例 `config.yaml`。
3. **若 `--init`**：调用 `sql_gen` 生成 `./init/<table>.sql` 后退出（不连任何外部服务）。
4. **若正常启动**：
   - 初始化日志（双文件 + stdout + 归档任务）。
   - 连接 MySQL，执行 **schema 校验**（缺列退出 / 多列告警询问）；缺列则终止。
   - 启动时加载一次资产文件（`mapping.enabled=true` 时）构建 join 索引。
   - 启动调度器（每源一个采集任务）+ 保留期清理任务 + 日志归档任务。
   - 等待 SIGINT/SIGTERM 优雅退出。

### 4.2 每轮采集数据流

每轮采集（每个 Prometheus 源一个 tokio 任务，并发执行）：

1. **读取本源配置**（ip, primary, fields, expressions, host_fields...）
2. **查询主指标**（枚举本源所有卡片序列）→ 返回 `[{ ip, card_id, labels... }, ...]`，这决定本轮产生几行
3. **对每个业务字段，按 (ip, card_id) 对齐**：
   - `from: metric` → 查对应指标，按卡号匹配 value
   - `from: label` → 从该指标的某 label 取字符串
   - 主机级字段 → 查主机级指标，**按 ip 对齐**，复制到该主机每张卡的每一行
4. **表达式求值**：把表达式中变量替换为该卡对齐后的数值，求得派生指标（如显存占用率）
5. **组装成统一行结构** `{ip, card_id, namespace, pod, gpu_util, mem_util, temp, power, host_cpu, host_mem, host_fds, ts}`
6. **资产 join（若 mapping.enabled）**：用行内 `src_key`（如 namespace）的值，去启动时加载的资产表 `dest_key` 列查匹配行，把 `columns` 字段补进行；无匹配填 NULL，多匹配取首条+WARN，`type` 解析失败填 NULL+WARN
7. **批量写入 MySQL**（INSERT，单源一批，失败回退不影响其他源）

独立任务：

- **retention 清理任务**：按配置保留期，定期 `DELETE` 早于阈值的行
- **日志归档任务**：超期散日志打包成 tar.gz
- **优雅退出**：捕获 `SIGINT/SIGTERM`，等当前轮采集/写入完成再退出

**核心机制：**

- **主指标决定行数**：主指标返回多少条序列，本源本轮就产生多少行。
- **对齐键**：卡维度用 `(ip, card_id)`；主机维度只用 `ip`。其中 `card_id` 来自配置声明的标签（DCGM 用 `gpu`，NPU 用 `id`）。
- **资产 join 在行组装后、写入前**：以行内已有列值作 join 键，纯内存查找（资产表启动时已加载为索引），不增加 I/O。

---

## 5. 模块划分与职责

```
src/
├── main.rs              程序入口：解析命令行(--init) → 加载配置
│                        → init 模式生成SQL退出 / 正常模式启动调度器+校验+任务
│
├── config/              配置层
│   └── mod.rs           - Config 结构体（serde 反序列化 YAML）
│                         - 校验逻辑（必填项、表达式变量合法性、时区名合法、
│                           mapping position 锚点列存在、rename 不与固定列冲突）
│                         - 若配置文件不存在 → 生成示例 config.yaml
│
├── source/              数据源层（只负责"查 Prometheus"，不知道业务）
│   └── mod.rs           - PrometheusClient：reqwest 查询瞬时向量
│                         - parse_vector()：把 Prometheus 响应解析成
│                           { labels: HashMap, value: f64 } 的列表
│
├── extractor/           提取对齐层（核心业务逻辑）
│   ├── mod.rs           - 提取主指标序列 → 生成行骨架（按 ip+card_id）
│   ├── align.rs         - 按 (ip, card_id) 或 ip 对齐各字段值/标签
│   └── host.rs          - 主机级字段单独查、按 ip 对齐后复制到每张卡
│
├── expr/                表达式求值层（纯函数，无副作用）
│   └── mod.rs           - 解析 "A/B","(A+B)/C" → AST
│                         - evaluate(ast, vars: HashMap<String,f64>) -> Option<f64>
│                         - 除零保护：返回 None
│
├── mapping/             资产表关联层（纯内存查找，无 I/O）
│   └── mod.rs           - 启动时加载 CSV/Excel 资产表 → 建 dest_key 索引
│                         - join(row, src_key值) → 补 mapping 列；无匹配NULL/
│                           多匹配首条+WARN/解析失败NULL+WARN
│                         - enabled=false 时返回空(不补值)
│
├── sql_gen/             建表 SQL 生成层（仅 --init 用）
│   └── mod.rs           - 固定列基线 + mapping 列按 position 插入排序
│                         - 输出 ./init/<table>.sql(含COMMENT/索引,无DROP/无建库)
│
├── sink/                落库层（只负责"写 MySQL"，不知道指标含义）
│   ├── mod.rs           - 批量 INSERT，列名来自配置的字段映射 + mapping 列
│   ├── schema.rs        - schema 校验(读INFORMATION_SCHEMA)/保留期清理DELETE/
│                         连接级时区SET
│
├── scheduler/           调度层（编排上面各层）
│   └── mod.rs           - tokio::spawn 每源一个任务，按 interval 循环
│                         - 单源失败隔离：catch 包裹，记日志，不影响其他源
│
├── log_archive/         日志归档任务（独立后台任务）
│   └── mod.rs           - 扫描日志目录，对超期散日志用 flate2+tar
│                         - 把当天 all+error 打包成单个 tar.gz，删散文件
│                         - 压缩包永不删除
│
└── models.rs            共享数据结构：Row（统一行）、MetricSample、ColumnDef 等
```

**依赖方向（单向，无环）：**

```
main → {scheduler, sql_gen, config, sink(schema校验)}
scheduler → {extractor, sink, mapping, models}
extractor → {source, expr, models, config}
mapping → {config, models}        # 启动时读资产文件
sql_gen → {config, models}        # 仅 --init
sink → {config, models}
所有层 → config, models
```

**设计原则：**

- `source` 和 `sink` 是**纯 I/O 边界**，互不依赖，可单独替换（换库、换协议）。
- `extractor` 是**唯一持有业务逻辑**的层，但它不直接发 HTTP、不直接写 SQL——它调用 `source` 拿数据、调用 `expr` 算值、把结果交给 `scheduler` 让 `sink` 写。业务规则集中、I/O 解耦。
- `expr` 是**纯函数模块**，无 I/O 无状态，极易单元测试。
- 新增一种卡：**零代码改动**，只在 `config.yaml` 加一段该源的配置。

---

## 6. 配置文件 Schema（YAML）

```yaml
# =====================================================================
# 计算卡利用率采集配置
# 本程序从多个 Prometheus 服务器读取计算卡与主机指标，
# 按卡片维度对齐后写入 MySQL。详见各字段注释。
# =====================================================================

# 全局默认采集间隔（秒）。每个 source 未单独配置 interval 时使用此值。
interval: 60

# 数据保留期（天）。retention 任务据此定期删除早于该天数的旧行。
retention_days: 30

# 清理任务执行间隔（秒）。
retention_interval: 3600

# 时区设置。程序采集时间、MySQL 连接 session time_zone、
# 保留期清理函数三方必须使用同一时区，否则会出现时间偏差或误删。
# 取 IANA 时区名，如 "Asia/Shanghai"、"UTC"、"America/New_York"。
timezone: "Asia/Shanghai"

database:
  host: "127.0.0.1"
  port: 3306
  user: "collector"
  password: "secret"
  database: "gpu_metrics"
  table: "gpu_usage"          # 写入的目标表名
  max_connections: 10         # 连接池大小
  # schema 校验策略：正常启动时对比实际表列与期望列。
  # 缺列恒为报错退出；多列时：
  #   ask      交互式询问是否继续(默认,TTY 场景)
  #   continue 非交互/默认继续运行,仅告警
  #   abort    遇多列直接退出
  on_extra_columns: "ask"

# ---------------------------------------------------------------------
# 日志配置
# 同时输出两份日志：完整日志(INFO及以上) 与 错误日志(ERROR)。
# 按日轮转。超期日志不删除，而是把当天的 all+error 两份打包压缩成单个
# tar.gz 归档（原始散文件删除，压缩包永不删除）。同时输出 stdout。
# ---------------------------------------------------------------------
logging:
  level: "info"            # 全局日志级别: error/warn/info/debug/trace
  dir: "./logs"            # 日志目录，自动创建（归档压缩包也存于此目录）
  all_file: "all.log"      # 完整日志文件名前缀(实际文件: all-2026-06-24.log)
  error_file: "error.log"  # 错误日志文件名前缀(实际文件: error-2026-06-24.log)
  rotation: "daily"        # 轮转周期: daily/hourly/never
  archive_after_days: 7    # 散日志保留天数；超期后打包归档。如 7 表示
                           # all-2026-06-24.log 在 6-24 之后第 7 天被归档
  archive_prefix: "logs"   # 归档包前缀(归档包名: logs-2026-06-24.tar.gz)
  stdout: true             # 是否同时输出到标准输出(容器场景建议 true)

# ---------------------------------------------------------------------
# 资产表关联（可选）
# 从外部 CSV/Excel 资产表 join 补充字段（如机房位置、团队归属）。
# 语义：拿【行内】src_key 列的值，去【资产表】dest_key 列找匹配行，
#       把该匹配行的 columns 字段补进采集行。
# enabled: false 时【仍然建列】(列结构由 --init 的 SQL 决定)，
#          只是采集时不填值(留 NULL)。
# 程序启动时加载一次资产文件，运行期不重载(改资产表需重启)。
# ---------------------------------------------------------------------
mapping:
  enabled: true                # 是否启用资产关联采集。false 时仍建列、不填值
  sources:
    - source_path: "./assets.csv"   # 资产文件路径(CSV) 或 .xlsx(Excel)
      src_key: "namespace"          # 【行内】用于关联的键(采集行中的列名)
      dest_key: "Namespace"         # 【资产表】中对应的列名
      source_sheet: "Sheet1"        # 可选，仅 Excel 有效，指定工作表名
      columns:
        - source_field: "机房位置"   # 资产表中要关联的列名
          rename: "location"        # 可选，最终落库的列名(缺省=source_field)
          type: "varchar(255)"      # 列类型(写入建表 SQL 的列定义)
          comment: "设备所在机房位置" # 列备注(写入建表 SQL 的 COMMENT)
          position:                 # 关联列在表中的位置(仅影响 --init 生成的 SQL 列顺序)
            direction: after        # after=在锚点列后 / before=在锚点列前
            anchor: "namespace"     # 锚点列名
        # 可声明多个列，同一 position 按声明顺序排列
        # - source_field: "负责人"
        #   rename: "owner"
        #   type: "varchar(64)"
        #   comment: "设备负责人"
        #   position: { direction: after, anchor: "namespace" }
    # 可配置多个资产文件，每个独立 join
    # - source_path: "./teams.xlsx"
    #   src_key: "ip"
    #   dest_key: "HostIp"
    #   source_sheet: "Sheet1"
    #   columns:
    #     - source_field: "Team"
    #       rename: "team"
    #       type: "varchar(64)"
    #       comment: "所属团队"
    #       position: { direction: after, anchor: "ip" }

# ---------------------------------------------------------------------
# 数据源列表：每个 source 对应一台/一组同构的 Prometheus。
# 卡类型差异完全靠配置表达，不改代码。
# ---------------------------------------------------------------------
sources:
  # ============ 示例 1：NVIDIA GPU（dcgm-exporter）============
  - name: "gpu-cluster-a"
    ip: "10.0.0.1"                     # 本源代表的主机 IP（写入行的 ip 字段）
    url: "http://10.0.0.1:9400"        # Prometheus 地址
    timeout: 10                        # 查询超时（秒），可选，默认 10
    interval: 30                       # 覆盖全局 interval（可选）

    # 主指标：枚举本源所有卡片，决定本轮产生几行。
    # card_label 声明用哪个标签作为"卡号"，与 ip 共同构成对齐键。
    primary:
      metric: "DCGM_FI_DEV_GPU_UTIL"
      card_label: "gpu"                # dcgm 用 gpu 标签作卡号

    # 业务字段映射：每个字段最终成为 MySQL 一列。
    # 取值方式三选一（见下方各字段示例）。
    fields:
      # 方式一：取指标值。from=metric 表示取该指标的 value。
      gpu_util:
        from: metric
        metric: "DCGM_FI_DEV_GPU_UTIL"
      temp:
        from: metric
        metric: "DCGM_FI_DEV_GPU_TEMP"
      power:
        from: metric
        metric: "DCGM_FI_DEV_POWER_USAGE"

      # 方式二：取标签值。from=label 表示取该指标某标签的字符串。
      # 用于 namespace / pod 这类"非数值"维度字段。
      namespace:
        from: label
        metric: "DCGM_FI_DEV_GPU_UTIL"
        label: "namespace"             # 裸金属场景无此标签 → 留空 NULL
      pod:
        from: label
        metric: "DCGM_FI_DEV_GPU_UTIL"
        label: "pod"

    # 派生指标：多指标表达式计算，结果单独命名成一列。
    # 表达式中的变量名 = 该指标的 metric 名。程序先查询表达式中出现的每个
    # metric、按卡对齐其值，再代入表达式求值。表达式里用的 metric
    # 不必出现在上面 fields 里（如 FB_USED/FB_FREE 仅用于计算占用率）。
    # 除零或变量缺失时结果为 NULL，不污染整行。
    expressions:
      mem_util:                                 # 显存占用率（新列名）
        expr: "DCGM_FI_DEV_FB_USED / (DCGM_FI_DEV_FB_USED + DCGM_FI_DEV_FB_FREE)"
        unit: "%"                               # 可选，仅文档用

    # 主机级字段：一台主机一个值，按 ip 对齐后复制到该主机每张卡。
    # 这些指标通常来自 node_exporter（可能与本卡 exporter 同一 Prometheus）。
    # host 级字段直接用一段完整 PromQL 查询单值（让 Prometheus 算好返回单值）。
    host_fields:
      host_cpu:
        expr: "100 - (avg by(instance)(irate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)"
      host_mem:
        expr: "(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100"
      host_fds:
        expr: "node_filefd_allocated"

  # ============ 示例 2：昇腾 NPU（npu-exporter）============
  - name: "npu-cluster-b"
    ip: "10.0.0.2"
    url: "http://10.0.0.2:9401"
    primary:
      metric: "npu_chip_info_utilization"
      card_label: "id"                  # npu 用 id 标签作卡号
    fields:
      gpu_util:                         # 复用同一列名，不同源不同指标
        from: metric
        metric: "npu_chip_info_utilization"
      temp:
        from: metric
        metric: "npu_chip_info_temperature"
      power:
        from: metric
        metric: "npu_chip_info_power"
      namespace:
        from: label
        metric: "npu_chip_info_utilization"
        label: "namespace"
      pod:
        from: label
        metric: "npu_chip_info_utilization"
        label: "pod_name"               # 注意 npu 标签是 pod_name
    expressions:
      mem_util:
        # NPU 用片上内存算占用率，与 DCGM 完全不同的原始指标，
        # 但最终都落到同一列 mem_util —— 这正是配置驱动的价值。
        expr: "npu_chip_info_hbm_used_memory / npu_chip_info_hbm_total_memory"
```

**关键设计点：**

1. **`primary` 决定行数**：每源的行数 = 主指标返回的序列数（每张卡一条）。
2. **三种字段取值方式**：`from: metric`（取值）、`from: label`（取标签）、`expressions`（表达式求值）。覆盖"可能是指标值，也可能是指标标签"以及"A/B 类型计算"。表达式的变量名 = metric 名，表达式里用到的 metric 不必出现在 `fields` 里（可仅为计算服务），程序自动查询并对齐这些 metric。
3. **同列名跨源统一**：`gpu_util`/`temp`/`power`/`mem_util` 等列名在所有源里保持一致，不同源指向不同原始指标——这是"多源异构卡写进同一张表"的关键。
4. **`host_fields` 单独成组**：因为它们按 `ip` 对齐（而非卡号），且复制到该主机每张卡。用完整 PromQL `expr`（让 Prometheus 算好返回单值），避免程序解析复杂 PromQL。
5. **`from: label` 的卡级维度**（namespace/pod）裸金属场景无该标签则留空 NULL。

---

## 7. MySQL 表结构

表名由配置 `database.table` 决定（默认 `gpu_usage`）。列由两部分组成：

- **固定列**：IP/卡号/Namespace/Pod/核心利用率/显存占用率/温度/功率/主机CPU/内存/句柄数/source/ts/id，按需求字段集合预先定义。
- **mapping 列**：来自配置 `mapping.sources[].columns`，列名取 `rename`（缺省=`source_field`），类型取 `type`，备注取 `comment`，列位置由 `position` 决定。

**重要：程序运行时不动态改表结构。** 表的建立与列的增减完全通过 `--init` 生成的 SQL 文件由用户手动执行。

### 7.1 固定列基线（不含 mapping）

```sql
CREATE TABLE IF NOT EXISTS gpu_usage (
    id              BIGINT       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    ts              DATETIME(3)  NOT NULL                COMMENT '采集时间(毫秒精度,配置时区)',
    ip              VARCHAR(64)  NOT NULL                COMMENT '主机IP',
    card_id         VARCHAR(32)  NOT NULL                COMMENT 'GPU/NPU卡号(来自配置的card_label)',
    namespace       VARCHAR(128)     NULL                COMMENT 'K8s命名空间,裸金属场景为NULL',
    pod             VARCHAR(256)     NULL                COMMENT 'Pod名,裸金属场景为NULL',
    gpu_util        DOUBLE       NULL                    COMMENT 'GPU核心利用率(%)',
    mem_util        DOUBLE       NULL                    COMMENT '显存/片上内存占用率',
    temp            DOUBLE       NULL                    COMMENT '显卡温度(℃)',
    power           DOUBLE       NULL                    COMMENT '显卡功率(W)',
    host_cpu        DOUBLE       NULL                    COMMENT '主机CPU使用率(%)',
    host_mem        DOUBLE       NULL                    COMMENT '主机内存使用率(%)',
    host_fds        DOUBLE       NULL                    COMMENT '主机系统句柄数',
    source          VARCHAR(64)  NOT NULL                COMMENT '数据源名(配置中的source.name)',
    PRIMARY KEY (id),
    INDEX idx_ts_ip_card (ts, ip, card_id),
    INDEX idx_ip_card (ip, card_id),
    INDEX idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='计算卡利用率采集记录';
```

### 7.2 mapping 列的插入（由 `--init` 拼接）

`--init` 生成 SQL 时，根据 `position` 把每个 mapping 列插入到固定列基线中：

- `direction: after, anchor: "namespace"` → 该列插在 `namespace` 列之后。
- `direction: before, anchor: "ip"` → 插在 `ip` 列之前。
- 同一 `position` 的多个 mapping 列按配置声明顺序排列。
- `mapping.enabled: false` 时**仍插入这些列**（建列留空，只是采集时不填值）。

示例（承接第 6 节 mapping 配置，`location` 列插在 `namespace` 后）：

```sql
    namespace       VARCHAR(128)     NULL                COMMENT 'K8s命名空间,裸金属场景为NULL',
    location        VARCHAR(255)     NULL                COMMENT '设备所在机房位置',
    pod             VARCHAR(256)     NULL                COMMENT 'Pod名,裸金属场景为NULL',
    ...
```

### 7.3 `--init` 模式（仅生成 SQL，不读写）

命令行加 `--init` 触发初始化模式：

- **读取当前 YAML 配置**（固定列定义 + mapping 配置）。
- **生成完整建表 SQL 文件**到 `./init/<table>.sql`（如 `./init/gpu_usage.sql`）。
- SQL 内容：`CREATE TABLE` 语句，含所有固定列 + mapping 列（含 type、COMMENT、position 排序）+ 主键 + 三个索引。
- **不含** `CREATE DATABASE`、**不含** `DROP TABLE`、**不含**示例数据。
- **此模式不连接 MySQL、不连接 Prometheus、不采集、不写日志文件**。完成后退出。
- 用户需手动执行该 SQL 建表后再以正常模式运行程序。

### 7.4 正常启动的 schema 校验

正常启动（无 `--init`）时，连上 MySQL 后**先校验表结构**再开始采集：

1. 读取实际表的列集合（`INFORMATION_SCHEMA.COLUMNS`）。
2. 与期望列集合（固定列 + 当前 mapping 配置的列）对比：
   - **缺列**（期望有、实际没有）→ **报错退出**，提示用户用 `--init` 重新生成 SQL 或手动 ALTER。缺列会导致 INSERT 失败，故硬失败。
   - **多列**（实际有、期望没有）→ **告警**，提示多出的列并**按 `database.on_extra_columns` 处理**：`ask`（TTY 时交互式询问，非 TTY 自动回退 continue）、`continue`（仅告警继续）、`abort`（退出）。
   - **完全匹配** → 继续启动。

**设计说明：**

- **数值列允许 NULL**：不同源采集到的字段可能不全（某卡没查到温度，或该源没配 power），写 NULL 比写 0 更诚实，避免把"无数据"和"值为0"混淆。
- **时间列 `ts`**：`DATETIME(3)` 保留毫秒，值为程序采集时刻（配置时区），不依赖 Prometheus 采集时间戳。
- **索引**：`idx_ts`（清理 + 时间范围查询）、`idx_ip_card`（按主机+卡号查历史）、`idx_ts_ip_card`（某段时间内某张卡）。
- **`source` 列**：记录数据来自哪个 source.name，便于排查异常、区分集群。

**保留期清理：**

```sql
DELETE FROM gpu_usage WHERE ts < DATE_SUB(NOW(), INTERVAL ? DAY);
```

每 `retention_interval` 秒执行一次，参数取配置的 `retention_days`。

---

## 8. 时区处理

`DATETIME(3)` 不带时区信息，存进去的值取决于写入方；清理 `NOW()` 用 MySQL 服务器时区。三方（程序、连接、清理函数）必须同一时区，否则会出现"刚写入的数据被清理误删"或"查询差 8 小时"。

**策略：配置化时区（默认 `Asia/Shanghai`）**

- **`sink` 层**：建立连接池后对每个连接执行 `SET time_zone = '<配置时区>'`（连接级，不影响 MySQL 全局配置）。
- **采集时间 `ts`**：用配置时区生成 `DateTime`，写入 `DATETIME(3)`。
- **清理任务**：用 `NOW()`（此时它已等于配置时区的当前时间），与写入值同源，杜绝误删。
- **校验**：配置加载时校验 `timezone` 是合法 IANA 名（`chrono-tz` 解析），非法则报错并提示。
- **不约束 MySQL 服务器全局时区**：用连接级 `SET time_zone` 与服务器解耦，迁移到任何 MySQL 实例都安全。

---

## 9. 错误处理与可观测性

### 错误分类与处理策略

| 错误类型 | 处理 | 示例 |
|---------|------|------|
| **配置错误**（文件不存在且无法生成 / 字段非法 / 表达式语法错 / 时区名非法 / mapping position 锚点列不存在 / rename 与固定列冲突） | **启动即失败退出**，明确报错提示修复。确定性错误，重试无意义 | `expr: "A /"` → 启动失败提示"表达式语法错误" |
| **schema 校验-缺列**（正常启动） | **报错退出**，提示用 `--init` 重新生成 SQL 或手动 ALTER | 表里缺 `gpu_util` 列 |
| **schema 校验-多列**（正常启动） | **告警并询问是否继续**（非交互环境按 `on_extra_columns` 配置） | 表里多了旧版遗留列 |
| **资产文件加载失败**（文件不存在 / 格式错 / Excel 工作表不存在） | `mapping.enabled=true` 时 **启动失败退出**；资产文件问题不应用 NULL 掩盖 | assets.csv 路径错误 |
| **单源查询失败**（Prometheus 超时 / 502 / 网络断） | **隔离**：仅该源本轮跳过，记 WARN 日志，不影响其他源，等下一轮重试 | 某台 Prometheus 重启中 |
| **单字段对齐失败**（某卡的温度指标缺失） | 该字段写 **NULL**，该卡其余字段正常入库。记 DEBUG 日志 | 某张卡这次没上报温度 |
| **表达式求值失败**（除零 / 变量缺失） | 该派生字段写 **NULL**，不污染整行。除零视为可接受情况 | 显存总量为0时算占用率 |
| **mapping join 无匹配 / 多匹配 / 类型解析失败** | 无匹配/解析失败填 NULL；多匹配取首条并记 WARN（提示资产表键重复） | 行内 namespace 在资产表找不到 |
| **MySQL 写入失败**（连接断 / 死锁） | 本批数据重试 N 次（退避），仍失败则记 ERROR 并**保留该批在内存**，下轮合并重试；持续失败告警 | MySQL 短暂抖动 |
| **清理任务失败** | 记 ERROR，下个 `retention_interval` 周期重试。不影响采集 | 清理 SQL 超时 |

### 单源失败隔离边界

```
scheduler 每轮：
  for source in sources:           # 并发 tokio::spawn 每源一个任务
    └─ 整个 source 的采集用 catch 包裹
       任何 panic/Err → 仅记日志，永不向上传播
       （绝不让一个坏源拖垮整个调度器）
```

每个 source 任务是独立隔离单元：失败 ≠ 程序崩溃，只是该源这一轮没有数据。

### 日志（结构化，`tracing` crate）

- **双文件**：完整日志（INFO+）与单独错误日志（ERROR）。错误日志是完整日志的子集。
- **按日轮转 + 超期归档压缩**：
  - **轮转**：按日切分，文件名带日期（`all-2026-06-24.log` / `error-2026-06-24.log`）。
  - **归档**：散日志保留 `archive_after_days` 天；超期后，把当天的 `all-YYYY-MM-DD.log` 与 `error-YYYY-MM-DD.log` **打包压缩到同一个 `tar.gz`**（命名 `<archive_prefix>-YYYY-MM-DD.tar.gz`，如 `logs-2026-06-24.tar.gz`），原始两个散文件删除，压缩包**永不删除**。
  - **实现**：自定义后台归档任务（与数据保留期清理任务同模式）扫描日志目录，对超期散日志用 `flate2`+`tar` 打包。**不使用 `tracing-appender` 的 `max_log_files`**（它只能删除，无法归档重命名）。
- **同时输出 stdout**：容器场景可配置开关。
- **级别**：`ERROR`（需介入）、`WARN`（单源跳过）、`INFO`（启动配置摘要、每轮采集成功行数）、`DEBUG`（单字段缺失、表达式代入细节）。
- **关键字段**：每条日志带 `source=<name>`、`ip`、`card_id`（如有），便于按卡定位。
- **采样摘要**：每轮结束记一条 INFO：`source=gpu-cluster-a rows=8 failed_fields=2 elapsed=320ms`，避免日志爆炸。

### 健壮性细节

- **HTTP 客户端**：`reqwest` 设连接池 + 超时（取配置 `timeout`），避免单源卡死整个任务。
- **MySQL 连接池**：`sqlx` 连接池，空闲健康检查 + 自动重连；`max_connections` 可配。
- **优雅退出**：捕获 `SIGINT/SIGTERM`，等当前轮采集/写入完成再退出，避免写一半中断。
- **不引入 Prometheus exporter / metrics 端点**（YAGNI，需求未提，避免过度设计）。

---

## 10. 测试策略

监控程序测试重点是**配置解析、对齐逻辑、表达式求值**三个纯逻辑核心，I/O 边界用 mock。

### 测试分层

| 层 | 测试重点 | 方式 |
|----|---------|------|
| **`expr`（最高优先级）** | 表达式解析与求值正确性、除零保护、变量缺失 | 纯单元测试，无依赖 |
| **`config`** | YAML 解析、校验（非法时区/缺字段/表达式语法错/mapping position 锚点不存在/rename 冲突应报错）、文件不存在时生成示例 | 单元测试，读测试用 YAML 文件 |
| **`extractor`/`align`** | 按卡对齐、主机级按 ip 复制、字段缺失填 NULL、主指标决定行数 | 单元测试，喂构造的 MetricSample 数据（不碰真实 HTTP） |
| **`source`** | 解析 Prometheus JSON 响应 → MetricSample 列表 | 单元测试，用真实 Prom 响应样例字符串（不联网） |
| **`mapping`** | CSV/Excel 解析建索引、join 正确性（命中/无匹配NULL/多匹配首条+WARN/类型解析失败NULL）、enabled=false 不补值 | 单元测试，用 `tests/fixtures/*.csv`、`*.xlsx` 样例 |
| **`sql_gen`** | 固定列+mapping列按 position 排序正确、含 COMMENT/索引、无 DROP/无建库、enabled=false 仍含列 | 单元测试，断言生成的 SQL 字符串 |
| **`sink`/schema校验** | SQL 生成正确、批量插入、时区 SET 生效、schema 对比逻辑（缺列/多列/匹配判定） | **暂时跳过真实数据库**，用 mock 验证 SQL 字符串与对比逻辑 |
| **`scheduler`** | 单源失败隔离、并发不串扰 | 单元测试，注入会失败的 mock source |

### 关键单元测试用例

**表达式层（必须覆盖）：**

```rust
// 基本算术
assert_eq!(eval("A / B", {A:6.0, B:3.0}), Some(2.0));
assert_eq!(eval("(A + B) / C", {A:1.0,B:2.0,C:3.0}), Some(1.0));
// 除零保护
assert_eq!(eval("A / B", {A:1.0, B:0.0}), None);
// 变量缺失
assert_eq!(eval("A / B", {A:1.0}), None);       // B 未提供
// 语法错误（配置加载阶段就应拦截）
assert!(parse("A /").is_err());
```

**对齐层（核心业务）：**

```
// 主指标返回 2 张卡 → 产出 2 行
// 卡 A 有温度，卡 B 温度指标缺失 → B.temp = NULL，其余正常
// 主机级 host_cpu 按 ip 复制到该主机 2 张卡的每一行（值相同）
// 裸金属场景 namespace 标签缺失 → 该列 NULL
```

**mapping join 层（资产关联）：**

```
// 资产表 Namespace 列命中 → 补 location="机房A"
// 行内 namespace 在资产表无匹配 → location=NULL
// 资产表同 namespace 出现 2 行 → 取首条 + 记 WARN
// type=double 但资产值非数字 → NULL + WARN
// enabled=false → mapping 列全部不补值（仅 --init 仍建列）
```

**sql_gen 层（建表 SQL 生成）：**

```
// position {after, namespace} → location 列出现在 namespace 之后、pod 之前
// 同 position 多列 → 按配置声明顺序排列
// 生成的 SQL 含每列 COMMENT、含 3 个索引、无 DROP、无 CREATE DATABASE
// enabled=false 的 mapping → SQL 仍包含该列（采集时不填值）
```

### I/O 边界处理

- `source`/`sink` 抽象出 trait 接口，测试时用 mock 实现，验证 extractor 逻辑不依赖真实 Prometheus/MySQL。
- 真实 Prom JSON 样例存 `tests/fixtures/*.json`，断言解析结果。
- MySQL 集成测试：暂时跳过真实数据库，用 mock 验证 SQL 生成与时区 SET 语句。

---

## 11. 注释与文档要求

用户明确要求"详尽的注释说明，包括配置文件"。下面给出具体可执行的规范。

### 11.1 代码注释（Rust）

| 对象 | 要求 | 形式 |
|------|------|------|
| **每个 crate / 模块文件** | 文件头注释：说明本模块职责、在整体架构中的位置、依赖关系、关键设计决策。读者看完应能在不读实现的情况下理解模块边界 | `//!` 模块级文档注释，置于文件顶部 |
| **每个 `pub` 项**（结构体/枚举/trait/函数/方法） | 文档注释：一行概述 + 详解 + 参数 + 返回 + 错误 + Panics + 示例（函数可加） | `///` 文档注释，支持 Markdown |
| **`pub` 结构体字段** | 逐字段注释含义、单位、是否可空、取值来源 | `///` 紧贴字段 |
| **非显而易见的内部逻辑** | 行内注释解释"为什么这么做"，而非"做了什么"。如对齐键选 `(ip, card_id)` 的原因、除零返回 `None` 而非 `0` 的原因、单源失败隔离的边界 | `//` 行内注释 |
| **TODO / FIXME / HACK** | 必须标注作者意图与跟进条件，不允许无说明的占位 | `// TODO(原因): ...` |
| **复杂算法**（如表达式递归下降解析、schema 列对比、mapping join） | 在函数注释里给出算法概述（步骤）、时间复杂度、边界条件 | `///` + 行内 |

**文档注释必须覆盖的信息（按对象）：**

```rust
/// 一句话概述函数做什么。
///
/// # 详解
/// 为什么需要它、在数据流的哪一步、调用时机、与其它模块如何协作。
///
/// # 参数
/// - `param`: 含义、单位、是否可空、合法范围。
///
/// # 返回
/// 正常返回什么；什么情况返回 None/Err；返回值如何被调用方使用。
///
/// # 错误 / Panics
/// 何时返回 Err、Err 的种类；何时 panic（理想情况下不 panic）。
///
/// # 示例（可选，函数级）
/// ```
/// let r = foo(42);
/// ```
```

```rust
//! # extractor 模块
//!
//! ## 职责
//! 提取主指标序列，按 (ip, card_id) 对齐各字段，组装成统一行。
//!
//! ## 架构位置
//! 介于 source（取数）与 sink（落库）之间，是唯一持有业务规则的层。
//!
//! ## 依赖
//! - source: 获取原始指标
//! - expr: 计算派生指标
//! - mapping: 资产 join
//!
//! ## 关键设计
//! - 主指标决定行数；其余字段按卡标识对齐，缺失填 NULL。
//! - 主机级字段按 ip 对齐并复制到每张卡。
```

**注释密度原则：** 注释解释意图（why），代码表达行为（what）。显而易见的 `let x = 1` 不注释；但 `let card_id = labels.get(card_label)` 需注释"从配置声明的标签取卡号作对齐键"。

### 11.2 配置文件注释（YAML）

生成或手写的 `config.yaml` 必须做到**不读文档也能正确配置**：

| 要求 | 说明 |
|------|------|
| **文件头总述** | 开头用注释块说明：本文件用途、程序如何加载它、修改后是否需重启、生成示例的命令 |
| **每个顶层段** | 每段（database/logging/mapping/sources）前用 `#-----` 分隔块 + 注释说明该段作用 |
| **每个字段** | 行尾或上方注释：含义 + 单位 + 默认值 + 取值范围/枚举 + 是否必填 + 示例 |
| **枚举/有限取值** | 列出所有合法值，如 `rotation: daily/hourly/never`、`on_extra_columns: ask/continue/abort` |
| **关联关系说明** | 说明字段间依赖，如"archive_after_days 须 < retention 才有意义"、"position.anchor 必须是已存在的列" |
| **示例值真实可用** | 示例配置填入可运行的真实值（如 DCGM/NPU 真实指标名），而非 placeholder |
| **复杂段配完整示例** | sources 段给出 DCGM + NPU 两个完整真实示例，mapping 段给出 CSV + 多列示例 |

**示例字段注释规范：**

```yaml
  interval: 60   # 采集间隔(秒)。必填。默认 60。每个 source 可用自身 interval 覆盖。
                 # 取值范围: 正整数。建议 >=15，过小会压垮 Prometheus。
```

### 11.3 表结构注释（SQL）

- **每列 `COMMENT`**：`--init` 生成的 SQL 中每列必须有 COMMENT，说明含义 + 单位 + 是否可空语义（如"裸金属场景为NULL"）。
- **表 `COMMENT`**：`CREATE TABLE ... COMMENT='计算卡利用率采集记录'`。
- **SQL 文件头**：`./init/<table>.sql` 顶部用 `--` 注释说明生成时间、对应配置文件、执行方式、是否含 mapping 列、注意事项（如"勿重复执行会报已存在，本文件不含 DROP"）。

### 11.4 错误信息注释（用户可读）

所有面向用户的错误（panic 之外）信息必须：

- **可定位**：含出错的配置项路径（如 `sources[1].fields.temp.metric`）、文件名+行号、或 source.name。
- **可操作**：提示修复方法（如"请用 --init 重新生成 SQL 或手动 ALTER TABLE 添加该列"）。
- **不暴露敏感信息**：错误信息不得打印数据库密码；日志中密码字段脱敏。

### 11.5 文档

- 本 spec 文档作为产品需求文档归档于 `docs/superpowers/specs/`。
- 代码顶层 `README.md`：项目简介、构建运行方式、`--init` 用法、配置文件说明、表结构说明、常见问题。
- 公共 API（如有意作为库）用 `cargo doc` 生成文档，确保所有 `pub` 项文档注释齐全。

---

## 12. 范围说明（YAGNI）

明确**不做**的事项，避免范围蔓延：

- 不做 Web UI / 查询 API / 可视化看板（仅采集落库，查询由外部系统完成）。
- 不做 Prometheus exporter / 自身 metrics 端点。
- 不做配置热加载（需重启生效，避免运行时一致性问题）。
- 不做动态建列（表结构固定）。
- 不做多租户 / 鉴权（内网采集场景）。

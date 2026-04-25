# 多数据源模拟数据生成器测试报告

测试日期：2026-04-22  
测试人：Codex  
代码目录：`D:\datagenerator\data-generatorv2`

## 1. 测试目标

本轮测试目标是对当前系统进行一次完整回归，确认以下内容：

- 后端核心能力是否可用，包括连接管理、写入任务、关系任务、实时/Kafka 写入相关逻辑。
- 前端核心页面与关键交互是否可用，包括连接管理、任务管理、执行记录、关系任务页面。
- 前端产物是否可以成功构建。
- 后端在隔离环境下是否可以正常启动并通过健康检查。
- 系统是否可以对真实 MySQL 目标端完成一次端到端模拟数据写入。
- 当前版本是否存在阻塞上线或阻塞全新环境部署的缺陷。

## 2. 测试范围

本轮已执行测试范围如下：

- 后端自动化测试
- 前端自动化测试
- 前端生产构建验证
- 后端运行态启动与健康检查
- 真实 MySQL 目标端写入冒烟
- 全新 MySQL 元库启动验证

本轮未执行的真实外部环境联调如下：

- PostgreSQL 目标端真实写入
- SQL Server 目标端真实写入
- Oracle 目标端真实写入
- 外部 Kafka 集群真实联调

说明：Kafka 写入能力在后端自动化测试中已有集成测试覆盖，但本轮没有额外连接外部 Kafka 集群做人工联调。

## 3. 测试环境

- 操作系统：Windows，PowerShell
- 工作目录：`D:\datagenerator\data-generatorv2`
- 时区：`Asia/Shanghai`
- 后端：Spring Boot 3.3.5，Maven，Java 21
- 前端：Vue 3，Vite 6，Vitest 4
- 默认 MySQL 连接配置：
  - Host：`127.0.0.1`
  - Port：`3306`
  - Database：`multisource_data_generator`
  - Username：`root`

## 4. 测试方案

### 4.1 后端回归

执行后端全量测试，确认控制器、应用服务、预览、调度、连接元数据、Kafka 写入、关系任务组执行等能力均可通过。

执行命令：

```powershell
mvn test
```

### 4.2 前端回归

执行前端全量测试，确认关键视图交互与 API 适配逻辑可通过。

执行命令：

```powershell
npm run test:run
```

### 4.3 前端构建

验证前端是否可以成功生成生产构建产物。

执行命令：

```powershell
npm run build
```

### 4.4 后端运行态冒烟

复核隔离 H2 环境下的后端启动日志，确认应用可成功启动，健康检查可达。

证据文件：

- `backend/run-backend-smoke-h2-env.log`
- `backend/run-backend-smoke-h2-env.err.log`

### 4.5 真实 MySQL 目标端端到端写入

使用隔离 H2 元库启动后端，再通过真实 MySQL 目标连接执行一次完整写入冒烟，校验：

- 连接创建
- 连接测试
- 写入任务创建
- 写入执行
- 目标表字段导入
- 写入前后行数变化
- 空值与空字符串校验

执行命令：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/smoke-write-target.ps1 `
  -DbType MYSQL `
  -TargetHost 127.0.0.1 `
  -Port 3306 `
  -DatabaseName multisource_data_generator `
  -Username root `
  -Password 123456 `
  -TableName mdg_smoke_mysql_20260422 `
  -RowCount 5 `
  -ApiBaseUrl http://127.0.0.1:8899/api
```

### 4.6 全新 MySQL 元库启动验证

使用当前源码直接以 MySQL 作为平台元库启动后端，验证 Flyway 迁移是否可在干净数据库上完成。

执行命令：

```powershell
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8890
```

## 5. 执行结果

### 5.1 后端自动化测试

- 结果：通过
- 汇总：`92/92` 通过，`0` 失败，`0` 错误，`0` 跳过
- 总耗时：约 `1分48秒`

关键覆盖点：

- 连接管理接口与 schema-model 元数据接口
- 写入任务创建、预览、执行、调度
- 关系任务组预览与执行汇总
- Kafka 写入集成测试
- 系统概览与演示数据接口

### 5.2 前端自动化测试

- 结果：通过
- 汇总：`41/41` 通过
- 总耗时：约 `51.55秒`

关键覆盖点：

- 连接管理页面
- 写入任务页面
- 执行记录页面
- 关系任务页面
- API 客户端与视图交互

### 5.3 前端生产构建

- 结果：通过
- 构建耗时：约 `13.53秒`
- 构建产物：
  - `dist/index.html`
  - `dist/assets/index-W83HamjD.css`
  - `dist/assets/index-CvSm__ZB.js`

### 5.4 后端运行态冒烟

- 结果：通过
- 结论：
  - H2 隔离环境下后端启动成功
  - 应用端口 `8899` 健康检查返回 `UP`
  - 启动日志中可以看到 Hikari 连接池启动、Tomcat 启动以及 `dispatcherServlet` 初始化记录

### 5.5 真实 MySQL 目标端写入冒烟

- 结果：通过
- 冒烟输出摘要：
  - `connectionStatus = READY`
  - `executionStatus = SUCCESS`
  - `importedColumnCount = 4`
  - `writtenRowCount = 5`
  - `beforeWriteRowCount = 0`
  - `afterWriteRowCount = 5`
  - `rowDelta = 5`
  - `nullValueCount = 0`
  - `blankStringCount = 0`

结论：

- 当前系统对真实 MySQL 目标端的连接、建表/写入、字段识别、写入前后行数统计、非空校验链路是可用的。

## 6. 发现的问题

### 6.1 阻塞缺陷：全新 MySQL 元库启动失败

严重级别：高

现象：

- 使用当前源码直接以 MySQL 作为平台元库启动后端时，Flyway 在执行 `V8__add_relational_write_task_groups.sql` 期间失败，应用无法完成启动。

错误摘要：

```text
Migration V8__add_relational_write_task_groups.sql failed
SQL State  : 42000
Error Code : 1067
Message    : Invalid default value for 'started_at'
```

定位：

- 文件：`backend/src/main/resources/db/migration/V8__add_relational_write_task_groups.sql`
- 触发语句：`write_task_group_execution` 表中的 `started_at TIMESTAMP NOT NULL`

影响：

- 新环境使用 MySQL 初始化平台元库时会直接失败。
- 该问题会阻塞“从零部署”和“新库初始化”。

当前判断：

- 这是一个真实代码缺陷，不是测试环境偶发现象。
- H2 环境不会暴露该问题，因此仅依靠 H2 或单元测试无法发现。

### 6.2 环境观察：已有 8888 在线实例存在运行依赖不一致

严重级别：中

现象：

- 对当前已在线的 `http://127.0.0.1:8888` 实例调用 `/api/connections` 时，返回 `NoClassDefFoundError: ch/qos/logback/core/rolling/helper/Compressor$CompressionRunnable`。

判断：

- 该问题没有在基于当前源码重新拉起的 H2 隔离实例中复现。
- 更接近“当前在线实例依赖或运行目录不一致”，不作为本轮源码回归失败项，但需要在实际运行环境中清理旧进程并重新启动验证。

## 7. 修复与稳定性改进说明

本轮测试之前，已经补齐并验证了以下测试能力：

- 新增关系任务组后端测试
- 新增关系任务页面前端测试
- 补充 schema-model 元数据接口测试
- 修正前端 Vitest 执行模式，避免 worker 超时导致的不稳定

本次测试执行过程中未再对业务代码做额外修改，重点是完成正式回归和问题确认。

## 8. 剩余风险

- 平台元库若继续使用全新 MySQL 库初始化，当前版本仍存在启动阻塞缺陷。
- PostgreSQL、SQL Server、Oracle 目标端本轮未做真实外部写入验证，真实驱动、权限、字符集和方言细节仍需专门联调。
- 外部 Kafka 集群本轮未做独立人工联调，虽然已有自动化集成测试覆盖，但真实网络、认证配置和消息保留策略仍需现场验证。

## 9. 结论

本轮测试结论如下：

- 自动化测试整体通过，共 `133` 条用例通过。
- 前端生产构建通过。
- 后端隔离运行态冒烟通过。
- 对真实 MySQL 目标端的端到端写入验证通过。
- 但当前版本存在一个高优先级阻塞缺陷：全新 MySQL 平台元库启动失败。

最终判断：

- 如果继续使用当前已有可运行元库，系统主要功能链路基本可用。
- 如果目标是支持“从零启动一个全新的 MySQL 平台环境”，当前版本不能视为完全通过，必须先修复 Flyway `V8` 迁移问题。

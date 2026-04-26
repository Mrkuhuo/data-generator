# 多数据源模拟数据生成器

一个面向中文场景的模拟数据写入平台。

当前版本的核心目标很明确：

1. 连接目标数据源
2. 选择已有表，或新建目标表
3. 配置字段与生成规则
4. 执行单次、持续或定时写入
5. 查看写入结果、前后条数和校验信息

项目已经从早期演示代码重构为可实际联调的多目标端数据生成平台，重点覆盖数据库写入、关系任务、Kafka 复杂 JSON 消息和执行结果回溯。

## 本次更新总结

- 完成后端重构，新增目标连接、写入任务、关系任务、执行记录四条主线能力
- 前端重构为中文工作台，统一为连接管理、写入任务、关系任务、执行记录的使用路径
- 支持 5 类目标端：
  - MySQL
  - PostgreSQL
  - SQL Server
  - Oracle
  - Kafka
- 支持两类核心任务：
  - 单表模拟数据写入
  - 父子表 / 父子消息关系任务
- 支持三类调度方式：
  - 手动执行
  - 持续写入（间隔调度）
  - 定时任务（Cron / 触发时间）
- 支持 Kafka JSON Schema / 示例 JSON 导入、字段路径映射、消息头配置、父子消息映射
- 执行结果支持返回：
  - 写入前条数
  - 写入后条数
  - 净增条数
  - 实际写入条数
  - 空值校验
  - 空字符串校验
  - 关系任务外键缺失统计

## 当前能力范围

### 1. 目标连接管理

- 新建、编辑、删除、测试目标连接
- 读取目标端表列表
- 读取表字段结构
- 读取多表关系模型

当前已支持的目标连接类型：

- `MYSQL`
- `POSTGRESQL`
- `SQLSERVER`
- `ORACLE`
- `KAFKA`

### 2. 单表写入任务

- 选择目标连接
- 选择已有表并自动映射字段
- 或手动输入表名并自定义字段
- 配置字段数据类型、主键、非空、生成器
- 配置批量大小、行数、写入模式、调度方式
- 立即执行、启动持续写入、暂停、恢复、停止

当前字段生成器支持：

- `SEQUENCE`
- `RANDOM_INT`
- `RANDOM_DECIMAL`
- `STRING`
- `ENUM`
- `BOOLEAN`
- `DATETIME`
- `UUID`

### 3. 关系任务

- 支持父子表一对多关系写入
- 支持根据父任务驱动子任务行数
- 支持关系列自动映射
- 支持执行级汇总和分表明细
- 支持数据库关系任务和 Kafka 父子消息任务

数据库关系任务的核心校验项：

- 父表写入条数
- 子表写入条数
- 外键缺失数
- 非空校验数
- 主键重复数

### 4. Kafka 复杂消息

- 支持普通 JSON 消息写入
- 支持复杂嵌套 JSON Schema 编辑
- 支持示例 JSON 解析为 Schema
- 支持消息 Key / Header 配置
- 支持父子 Topic 的路径级字段映射

### 5. 执行记录与结果回溯

- 查看任务执行列表
- 查看单次执行明细
- 查看关系任务执行汇总
- 查看每张表 / 每个 Topic 的执行结果
- 查看写入统计与异常摘要

## 技术栈

### 后端

- Java 21
- Spring Boot 3.3.5
- Spring Validation
- Spring Data JPA
- Flyway
- Quartz
- Kafka Clients

### 前端

- Vue 3
- TypeScript
- Vite
- Vitest

### 默认平台元数据库

- MySQL 8

## 目录结构

```text
backend/                     后端服务
frontend/                    前端工作台
infra/mysql/                 MySQL 初始化脚本
infra/postgres/              PostgreSQL 初始化脚本
infra/sqlserver/             SQL Server 初始化脚本
infra/oracle/                Oracle 初始化脚本
scripts/                     启动、联调、冒烟脚本
docs/                        操作文档与联调文档
docker-compose.yml           本地依赖环境
DESIGN.md                    设计约束与界面原则
```

## 本地启动

### 环境要求

- JDK 21
- Node.js 20 及以上
- npm
- Docker / Docker Compose
- Windows 环境下如果没有 Docker Desktop，可使用 WSL + Docker

### 1. 启动基础依赖

```bash
docker compose up -d mysql postgres redpanda redpanda-init http-echo
```

如需启动 SQL Server：

```bash
docker compose --profile sqlserver up -d sqlserver
docker exec -i mdg-sqlserver /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "MdgSqlServer123!" -i /work/init/01_demo_sink.sql
```

如需启动 Oracle：

```bash
docker login container-registry.oracle.com
docker compose --profile oracle up -d oracle
```

### 2. 启动后端

```bash
cd backend
mvn spring-boot:run
```

默认端口：

- 后端 API: `http://127.0.0.1:8888`
- 健康检查: `http://127.0.0.1:8888/actuator/health`
- Swagger: `http://127.0.0.1:8888/swagger-ui.html`

默认元数据库配置：

- Host: `127.0.0.1`
- Port: `3306`
- Database: `multisource_data_generator`
- Username: `root`
- Password: `123456`

如需覆盖元数据库连接，可使用环境变量：

- `APP_DATASOURCE_URL`
- `APP_DATASOURCE_USERNAME`
- `APP_DATASOURCE_PASSWORD`

### 3. 启动前端

```bash
cd frontend
npm install
npm run dev
```

默认开发地址：

- 前端: `http://127.0.0.1:5173`

### 4. Docker Compose 一体化启动

如果直接使用仓库内的容器编排，也可以启动前后端：

```bash
docker compose up -d backend frontend
```

对应地址：

- 前端: `http://127.0.0.1:8080`
- 后端: `http://127.0.0.1:8888`

## 默认本地目标端

`docker-compose.yml` 当前提供以下本地目标端：

- `mysql`
  - 端口 `3306`
  - 含平台元数据库
  - 含目标库初始化脚本
- `postgres`
  - 端口 `5432`
  - 默认库 `demo_sink`
- `sqlserver`
  - 端口 `1433`
  - 默认库 `mdg_demo`
  - 默认用户 `sa`
- `oracle`
  - 端口 `1521`
  - 默认服务名 `FREEPDB1`
  - 默认 schema `MDG_DEMO`
- `redpanda`
  - Kafka 兼容 broker
  - 外部端口 `9092`
- `http-echo`
  - HTTP 回显服务
  - 端口 `9000`

## 使用流程

### 单表写入

1. 进入“数据源连接”
2. 新建并测试目标连接
3. 进入“写入任务”
4. 选择目标连接
5. 选择已有表，或输入新表名并配置字段
6. 设置写入条数、批量大小、调度方式
7. 点击“立即执行”或启动持续写入
8. 在“执行记录”中查看写入结果

### 关系任务

1. 进入“数据源连接”
2. 连接目标数据库或 Kafka
3. 进入“关系任务”
4. 配置父任务与子任务
5. 配置关系列或 Kafka 路径映射
6. 预览样例数据
7. 执行关系任务
8. 查看执行汇总和子任务明细

## 真实测试结果

最近一次完整测试结论如下：

### 自动化测试

- 后端测试通过：`113` 个测试
- 前端测试通过：`56` 个测试
- 前端构建通过：`npm run build`
- 后端打包通过：`mvn -DskipTests package`

### 真实目标端联调

以下 5 类目标端均已完成真实连接测试：

- MySQL
- PostgreSQL
- SQL Server
- Oracle
- Kafka

以下场景已完成真实验证：

- MySQL 单表写入
- MySQL 关系任务
- MySQL 持续写入
- PostgreSQL 单表写入
- PostgreSQL 关系任务
- SQL Server 单表写入
- SQL Server 关系任务
- Oracle 单表写入
- Oracle 关系任务
- Kafka 父子 JSON 关系任务
- Kafka 持续写入

已验证的结果项包括：

- 平台执行状态与真实目标端结果一致
- 写入前后条数统计正确
- 净增条数统计正确
- 非空校验正确
- 关系任务外键缺失校验正确

## 常用脚本

### 真实目标端冒烟脚本

```bash
powershell -ExecutionPolicy Bypass -File scripts/smoke-write-target.ps1
```

该脚本支持以下目标端：

- `MYSQL`
- `POSTGRESQL`
- `SQLSERVER`
- `ORACLE`
- `KAFKA`

典型用途：

- 自动创建临时连接
- 自动创建临时写入任务
- 自动执行一次真实写入
- 自动返回写入结果和校验结果

## 相关文档

- 平台操作手册：`docs/平台操作手册.md`
- 目标数据库联调指南：`docs/目标数据库联调指南.md`
- Kafka 复杂消息 API 验收说明：`docs/Kafka复杂消息API验收说明.md`
- 历史测试报告：`docs/test-report-2026-04-22.md`
- 设计说明：`DESIGN.md`

## 当前定位

这个项目当前不是通用 BI 平台，也不是数据同步平台。

它的定位是：

- 面向测试、联调、演示、压测准备的模拟数据写入工具
- 面向多目标端的结构化数据与消息生成平台
- 面向中文团队的可视化工作台

如果后续继续演进，最值得优先扩展的方向是：

1. 更多目标端类型
2. 更强的字段生成器模板库
3. 更完整的关系任务模板
4. 更丰富的执行监控与统计分析

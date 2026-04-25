# Kafka 复杂消息 API 验收说明

这份说明用于复现当前项目里 Kafka 复杂 JSON 消息的整链路验收。

## 覆盖范围

当前验收测试会完整覆盖以下流程：

1. 创建 Kafka 目标连接
2. 测试 Kafka 连接
3. 预览复杂消息 Schema 生成结果
4. 创建 Kafka 写入任务
5. 手动执行写入任务
6. 查询执行详情与执行日志
7. 消费 Kafka 消息并校验：
   - `keyPath`
   - headers
   - 嵌套对象
   - 数组结构
   - `deliveryDetailsJson`

## 测试入口

后端集成测试类：

- `backend/src/test/java/com/datagenerator/task/application/WriteTaskKafkaApiFlowIntegrationTest.java`

执行命令：

```bash
cd backend
mvn -q -Dtest=WriteTaskKafkaApiFlowIntegrationTest test
```

如果要连同后端全部测试一起回归：

```bash
cd backend
mvn -q test
```

## 运行方式

这条验收链路不依赖 Docker。

测试内部会自动启动：

- `EmbeddedKafkaKraftBroker` 作为 Kafka Broker
- H2 内存数据库作为测试库

因此本地只需要具备 Java 21 与 Maven 环境即可复现。

## 当前验证点

测试里已经固定校验以下关键结果：

- 连接测试返回 `READY`
- 复杂 Schema 预览能生成嵌套 JSON
- `order.id` 能作为 Kafka `keyPath`
- 消息会写入指定 partition
- headers 能正确透传
- 执行日志会记录开始执行、生成模拟数据、写入完成
- `deliveryDetailsJson` 会返回：
  - `targetType`
  - `deliveryType`
  - `topic`
  - `keyMode`
  - `keyPath`
  - `partition`
  - `writtenRowCount`
  - `generatedCount`
  - `plannedRowCount`

## 相关实现文件

- `backend/src/main/java/com/datagenerator/task/application/WriteTaskKafkaWriter.java`
- `backend/src/main/java/com/datagenerator/task/application/WriteTaskPreviewService.java`
- `backend/src/main/java/com/datagenerator/task/application/WriteTaskService.java`
- `backend/src/main/java/com/datagenerator/task/application/KafkaPayloadSchemaService.java`

## 本轮顺手修复

在补这条 API 验收时，同时修复了一个真实问题：

- `WriteTaskExecutionLog` 的 `@PrePersist` 覆盖了基类时间戳初始化，导致某些建表方式下 `created_at` / `updated_at` 可能为 `null`

对应修复文件：

- `backend/src/main/java/com/datagenerator/task/domain/WriteTaskExecutionLog.java`

## 其他验收补充

如果需要验证 JDBC 目标端真实写入结果，可继续使用已有脚本：

```bash
powershell -ExecutionPolicy Bypass -File scripts/smoke-write-target.ps1
```

该脚本当前已经用于 MySQL 真实写入 smoke 验收。

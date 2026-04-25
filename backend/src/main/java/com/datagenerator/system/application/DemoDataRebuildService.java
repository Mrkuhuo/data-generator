package com.datagenerator.system.application;

import com.datagenerator.connection.api.TargetConnectionUpsertRequest;
import com.datagenerator.connection.application.ConnectionJdbcSupport;
import com.datagenerator.connection.application.TargetConnectionService;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.connection.domain.TargetConnectionStatus;
import com.datagenerator.connection.repository.TargetConnectionRepository;
import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupPreviewRequest;
import com.datagenerator.task.api.WriteTaskGroupRelationUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupResponse;
import com.datagenerator.task.api.WriteTaskGroupRowPlanRequest;
import com.datagenerator.task.api.WriteTaskGroupTaskUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupUpsertRequest;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.application.WriteTaskGroupService;
import com.datagenerator.task.application.WriteTaskService;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.ReferenceSourceMode;
import com.datagenerator.task.domain.RelationReusePolicy;
import com.datagenerator.task.domain.RelationSelectionStrategy;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.datagenerator.task.domain.WriteTaskExecution;
import com.datagenerator.task.domain.WriteTaskExecutionLog;
import com.datagenerator.task.domain.WriteTaskGroupRowPlanMode;
import com.datagenerator.task.domain.WriteTaskRelationMode;
import com.datagenerator.task.domain.WriteTaskRelationType;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.datagenerator.task.repository.WriteTaskColumnRepository;
import com.datagenerator.task.repository.WriteTaskExecutionLogRepository;
import com.datagenerator.task.repository.WriteTaskExecutionRepository;
import com.datagenerator.task.repository.WriteTaskRepository;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DemoDataRebuildService {

    private static final String DEMO_TABLE_NAME = "synthetic_demo_orders_cn";
    private static final long COMPLEX_KAFKA_GROUP_SEED = 2026042501L;
    private static final long PAYLOAD_KAFKA_GROUP_SEED = 2026042502L;
    private static final DateTimeFormatter DEMO_TOPIC_SUFFIX_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("Asia/Shanghai"));
    private static final String DEMO_CONNECTION_DESCRIPTION = "系统自动重建的中文演示目标连接，用于快速验证写入链路。";
    private static final String DEMO_TASK_DESCRIPTION = "自动生成中文订单模拟数据并写入目标端，用于快速验收当前流程。";

    private final TargetConnectionRepository connectionRepository;
    private final WriteTaskRepository taskRepository;
    private final WriteTaskColumnRepository taskColumnRepository;
    private final WriteTaskExecutionRepository executionRepository;
    private final WriteTaskExecutionLogRepository executionLogRepository;
    private final TargetConnectionService targetConnectionService;
    private final WriteTaskService writeTaskService;
    private final WriteTaskGroupService writeTaskGroupService;
    private final ConnectionJdbcSupport connectionJdbcSupport;

    public DemoDataRebuildService(
            TargetConnectionRepository connectionRepository,
            WriteTaskRepository taskRepository,
            WriteTaskColumnRepository taskColumnRepository,
            WriteTaskExecutionRepository executionRepository,
            WriteTaskExecutionLogRepository executionLogRepository,
            TargetConnectionService targetConnectionService,
            WriteTaskService writeTaskService,
            WriteTaskGroupService writeTaskGroupService,
            ConnectionJdbcSupport connectionJdbcSupport
    ) {
        this.connectionRepository = connectionRepository;
        this.taskRepository = taskRepository;
        this.taskColumnRepository = taskColumnRepository;
        this.executionRepository = executionRepository;
        this.executionLogRepository = executionLogRepository;
        this.targetConnectionService = targetConnectionService;
        this.writeTaskService = writeTaskService;
        this.writeTaskGroupService = writeTaskGroupService;
        this.connectionJdbcSupport = connectionJdbcSupport;
    }

    @Transactional
    public DemoDataRebuildResponse rebuild(Long sourceConnectionId) {
        TargetConnection sourceConnection = resolveSourceConnection(sourceConnectionId);

        TargetConnection recreatedConnection = targetConnectionService.create(new TargetConnectionUpsertRequest(
                buildConnectionName(sourceConnection.getDbType()),
                sourceConnection.getDbType(),
                sourceConnection.getHost(),
                sourceConnection.getPort(),
                sourceConnection.getDatabaseName(),
                sourceConnection.getSchemaName(),
                sourceConnection.getUsername(),
                sourceConnection.getPasswordValue(),
                connectionJdbcSupport.normalizeParamsForStorage(sourceConnection.getDbType(), sourceConnection.getJdbcParams()),
                sourceConnection.getConfigJson(),
                TargetConnectionStatus.READY,
                DEMO_CONNECTION_DESCRIPTION
        ));

        WriteTask recreatedTask = writeTaskService.create(new WriteTaskUpsertRequest(
                "中文订单演示任务",
                recreatedConnection.getId(),
                DEMO_TABLE_NAME,
                sourceConnection.getDbType() == DatabaseType.KAFKA ? TableMode.USE_EXISTING : TableMode.CREATE_IF_MISSING,
                sourceConnection.getDbType() == DatabaseType.KAFKA ? WriteMode.APPEND : WriteMode.OVERWRITE,
                20,
                200,
                20260413L,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                DEMO_TASK_DESCRIPTION,
                sourceConnection.getDbType() == DatabaseType.KAFKA ? "{\"payloadFormat\":\"JSON\",\"keyMode\":\"NONE\"}" : null,
                null,
                buildColumns(sourceConnection.getDbType())
        ));

        deleteSourceData(sourceConnection.getId());

        return new DemoDataRebuildResponse(
                sourceConnection.getId(),
                recreatedConnection.getId(),
                recreatedConnection.getName(),
                recreatedConnection.getDbType(),
                recreatedConnection.getHost(),
                recreatedConnection.getPort(),
                recreatedConnection.getDatabaseName(),
                recreatedConnection.getJdbcParams(),
                recreatedTask.getId(),
                recreatedTask.getName(),
                recreatedTask.getTableName()
        );
    }

    @Transactional
    public DemoComplexKafkaJsonGroupResponse createComplexKafkaJsonGroup(Long connectionId) {
        TargetConnection connection = resolveKafkaConnection(connectionId);
        String suffix = DEMO_TOPIC_SUFFIX_FORMATTER.format(Instant.now());
        String parentTopic = "mdg.demo.parent.order-profile." + suffix;
        String childTopic = "mdg.demo.child.order-fulfillment." + suffix;
        WriteTaskGroupUpsertRequest request = buildComplexKafkaGroupRequest(connection.getId(), suffix, parentTopic, childTopic);

        return previewAndCreateKafkaGroup(connection, request, parentTopic, childTopic);
    }

    @Transactional
    public DemoComplexKafkaJsonGroupResponse createPayloadKafkaJsonGroup(Long connectionId) {
        TargetConnection connection = resolveKafkaConnection(connectionId);
        String suffix = DEMO_TOPIC_SUFFIX_FORMATTER.format(Instant.now());
        String parentTopic = "mdg.demo.payload.parent.order-envelope." + suffix;
        String childTopic = "mdg.demo.payload.child.shipment-envelope." + suffix;
        WriteTaskGroupUpsertRequest request = buildPayloadKafkaGroupRequest(connection.getId(), suffix, parentTopic, childTopic);

        return previewAndCreateKafkaGroup(connection, request, parentTopic, childTopic);
    }

    private DemoComplexKafkaJsonGroupResponse previewAndCreateKafkaGroup(
            TargetConnection connection,
            WriteTaskGroupUpsertRequest request,
            String parentTopic,
            String childTopic
    ) {
        writeTaskGroupService.preview(new WriteTaskGroupPreviewRequest(request, 3, request.seed()));
        WriteTaskGroupResponse createdGroup = writeTaskGroupService.create(request);
        return new DemoComplexKafkaJsonGroupResponse(
                connection.getId(),
                connection.getName(),
                createdGroup.id(),
                createdGroup.name(),
                request.seed(),
                parentTopic,
                childTopic,
                createdGroup.tasks() == null ? 0 : createdGroup.tasks().size(),
                createdGroup.relations() == null ? 0 : createdGroup.relations().size()
        );
    }

    private TargetConnection resolveKafkaConnection(Long connectionId) {
        TargetConnection connection;
        if (connectionId != null) {
            connection = connectionRepository.findById(connectionId)
                    .orElseThrow(() -> new IllegalArgumentException("未找到指定的 Kafka 连接: " + connectionId));
        } else {
            connection = connectionRepository.findAll(Sort.by(Sort.Direction.DESC, "updatedAt")).stream()
                    .filter(item -> item.getDbType() == DatabaseType.KAFKA)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("当前没有可用的 Kafka 连接，请先创建 Kafka 目标连接"));
        }
        if (connection.getDbType() != DatabaseType.KAFKA) {
            throw new IllegalArgumentException("复杂父子 JSON 演示任务仅支持 Kafka 连接");
        }
        return connection;
    }

    private WriteTaskGroupUpsertRequest buildComplexKafkaGroupRequest(
            Long connectionId,
            String suffix,
            String parentTopic,
            String childTopic
    ) {
        return new WriteTaskGroupUpsertRequest(
                "\u590d\u6742\u7236\u5b50 JSON \u6f14\u793a\u4efb\u52a1 " + suffix,
                connectionId,
                "system-demo: complex parent child kafka json group",
                COMPLEX_KAFKA_GROUP_SEED,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                List.of(
                        buildComplexKafkaParentTask(parentTopic),
                        buildComplexKafkaChildTask(childTopic)
                ),
                List.of(buildComplexKafkaRelation())
        );
    }

    private WriteTaskGroupUpsertRequest buildPayloadKafkaGroupRequest(
            Long connectionId,
            String suffix,
            String parentTopic,
            String childTopic
    ) {
        return new WriteTaskGroupUpsertRequest(
                "\u8d1f\u8f7d\u7236\u5b50 JSON \u6f14\u793a\u4efb\u52a1 " + suffix,
                connectionId,
                "system-demo: payload envelope kafka json group",
                PAYLOAD_KAFKA_GROUP_SEED,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                List.of(
                        buildPayloadKafkaParentTask(parentTopic),
                        buildPayloadKafkaChildTask(childTopic)
                ),
                List.of(buildPayloadKafkaRelation())
        );
    }

    private WriteTaskGroupTaskUpsertRequest buildComplexKafkaParentTask(String topic) {
        return new WriteTaskGroupTaskUpsertRequest(
                null,
                "order_profile",
                "\u7236\u8ba2\u5355 JSON \u4e8b\u4ef6",
                topic,
                TableMode.CREATE_IF_MISSING,
                WriteMode.APPEND,
                50,
                COMPLEX_KAFKA_GROUP_SEED,
                "parent-json-demo",
                WriteTaskStatus.READY,
                new WriteTaskGroupRowPlanRequest(
                        WriteTaskGroupRowPlanMode.FIXED,
                        24,
                        null,
                        null,
                        null
                ),
                """
                {
                  "payloadFormat": "JSON",
                  "keyMode": "FIELD",
                  "keyPath": "order.orderId",
                  "headerDefinitions": [
                    { "name": "eventType", "mode": "FIXED", "value": "order_profile" },
                    { "name": "tenantId", "mode": "FIELD", "path": "meta.tenantId" },
                    { "name": "traceId", "mode": "FIELD", "path": "meta.traceId" }
                  ]
                }
                """,
                """
                {
                  "type": "OBJECT",
                  "children": [
                    {
                      "name": "meta",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "tenantId",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["mall-cn", "mall-global"] },
                          "nullable": false
                        },
                        {
                          "name": "traceId",
                          "type": "SCALAR",
                          "valueType": "UUID",
                          "generatorType": "UUID",
                          "generatorConfig": {},
                          "nullable": false
                        },
                        {
                          "name": "eventTime",
                          "type": "SCALAR",
                          "valueType": "DATETIME",
                          "generatorType": "DATETIME",
                          "generatorConfig": { "from": "2026-04-01T00:00:00Z", "to": "2026-04-30T23:59:59Z" },
                          "nullable": false
                        },
                        {
                          "name": "source",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["app", "mini_program", "pos"] },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "order",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "orderId",
                          "type": "SCALAR",
                          "valueType": "LONG",
                          "generatorType": "SEQUENCE",
                          "generatorConfig": { "start": 8800000001, "step": 1 },
                          "nullable": false
                        },
                        {
                          "name": "orderNo",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "STRING",
                          "generatorConfig": { "prefix": "SO", "length": 10, "charset": "0123456789" },
                          "nullable": false
                        },
                        {
                          "name": "channel",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["APP", "STORE", "WEB"] },
                          "nullable": false
                        },
                        {
                          "name": "status",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["CREATED", "PAID", "FULFILLING", "DONE"] },
                          "nullable": false
                        },
                        {
                          "name": "amount",
                          "type": "SCALAR",
                          "valueType": "DECIMAL",
                          "generatorType": "RANDOM_DECIMAL",
                          "generatorConfig": { "min": 199.0, "max": 9999.0, "scale": 2 },
                          "nullable": false
                        },
                        {
                          "name": "currency",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["CNY", "USD"] },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "buyer",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "userId",
                          "type": "SCALAR",
                          "valueType": "LONG",
                          "generatorType": "SEQUENCE",
                          "generatorConfig": { "start": 990001, "step": 1 },
                          "nullable": false
                        },
                        {
                          "name": "userTier",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["SILVER", "GOLD", "PLATINUM"] },
                          "nullable": false
                        },
                        {
                          "name": "region",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["EAST", "NORTH", "SOUTH", "WEST"] },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "risk",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "score",
                          "type": "SCALAR",
                          "valueType": "INT",
                          "generatorType": "RANDOM_INT",
                          "generatorConfig": { "min": 1, "max": 100 },
                          "nullable": false
                        },
                        {
                          "name": "manualReview",
                          "type": "SCALAR",
                          "valueType": "BOOLEAN",
                          "generatorType": "BOOLEAN",
                          "generatorConfig": { "trueRate": 0.15 },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "shipping",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "province",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["Zhejiang", "Jiangsu", "Guangdong", "Sichuan"] },
                          "nullable": false
                        },
                        {
                          "name": "city",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["Hangzhou", "Nanjing", "Shenzhen", "Chengdu"] },
                          "nullable": false
                        },
                        {
                          "name": "district",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "STRING",
                          "generatorConfig": { "prefix": "district-", "length": 5, "charset": "abcdef0123456789" },
                          "nullable": false
                        }
                      ]
                    }
                  ]
                }
                """,
                List.of()
        );
    }

    private WriteTaskGroupTaskUpsertRequest buildComplexKafkaChildTask(String topic) {
        return new WriteTaskGroupTaskUpsertRequest(
                null,
                "order_fulfillment",
                "\u5b50\u5c65\u7ea6 JSON \u4e8b\u4ef6",
                topic,
                TableMode.CREATE_IF_MISSING,
                WriteMode.APPEND,
                100,
                COMPLEX_KAFKA_GROUP_SEED,
                "child-json-demo",
                WriteTaskStatus.READY,
                new WriteTaskGroupRowPlanRequest(
                        WriteTaskGroupRowPlanMode.CHILD_PER_PARENT,
                        null,
                        "order_profile",
                        2,
                        4
                ),
                """
                {
                  "payloadFormat": "JSON",
                  "keyMode": "FIELD",
                  "keyPath": "orderRef.orderId",
                  "headerDefinitions": [
                    { "name": "eventType", "mode": "FIXED", "value": "order_fulfillment" },
                    { "name": "tenantId", "mode": "FIELD", "path": "meta.tenantId" },
                    { "name": "parentTraceId", "mode": "FIELD", "path": "meta.parentTraceId" }
                  ]
                }
                """,
                """
                {
                  "type": "OBJECT",
                  "children": [
                    {
                      "name": "meta",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "tenantId",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["mall-cn", "mall-global"] },
                          "nullable": false
                        },
                        {
                          "name": "parentTraceId",
                          "type": "SCALAR",
                          "valueType": "UUID",
                          "generatorType": "UUID",
                          "generatorConfig": {},
                          "nullable": false
                        },
                        {
                          "name": "eventTime",
                          "type": "SCALAR",
                          "valueType": "DATETIME",
                          "generatorType": "DATETIME",
                          "generatorConfig": { "from": "2026-04-01T00:00:00Z", "to": "2026-04-30T23:59:59Z" },
                          "nullable": false
                        },
                        {
                          "name": "eventType",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["FULFILLMENT_CREATED", "FULFILLMENT_PICKED", "FULFILLMENT_SHIPPED"] },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "orderRef",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "orderId",
                          "type": "SCALAR",
                          "valueType": "LONG",
                          "generatorType": "SEQUENCE",
                          "generatorConfig": { "start": 9900000001, "step": 1 },
                          "nullable": false
                        },
                        {
                          "name": "orderNo",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "STRING",
                          "generatorConfig": { "prefix": "TMP", "length": 8, "charset": "0123456789" },
                          "nullable": false
                        },
                        {
                          "name": "channel",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["APP", "STORE", "WEB"] },
                          "nullable": false
                        },
                        {
                          "name": "status",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["CREATED", "PAID", "FULFILLING", "DONE"] },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "buyer",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "userId",
                          "type": "SCALAR",
                          "valueType": "LONG",
                          "generatorType": "SEQUENCE",
                          "generatorConfig": { "start": 770001, "step": 1 },
                          "nullable": false
                        },
                        {
                          "name": "userTier",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["SILVER", "GOLD", "PLATINUM"] },
                          "nullable": false
                        },
                        {
                          "name": "region",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["EAST", "NORTH", "SOUTH", "WEST"] },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "item",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "skuId",
                          "type": "SCALAR",
                          "valueType": "LONG",
                          "generatorType": "RANDOM_INT",
                          "generatorConfig": { "min": 100000, "max": 999999 },
                          "nullable": false
                        },
                        {
                          "name": "skuName",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "STRING",
                          "generatorConfig": { "prefix": "SKU-", "length": 6, "charset": "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" },
                          "nullable": false
                        },
                        {
                          "name": "category",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["DIGITAL", "HOME", "BEAUTY", "FRESH"] },
                          "nullable": false
                        },
                        {
                          "name": "quantity",
                          "type": "SCALAR",
                          "valueType": "INT",
                          "generatorType": "RANDOM_INT",
                          "generatorConfig": { "min": 1, "max": 5 },
                          "nullable": false
                        },
                        {
                          "name": "unitPrice",
                          "type": "SCALAR",
                          "valueType": "DECIMAL",
                          "generatorType": "RANDOM_DECIMAL",
                          "generatorConfig": { "min": 39.0, "max": 4999.0, "scale": 2 },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "fulfillment",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "warehouseCode",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["HZ-A1", "NJ-B2", "SZ-C3", "CD-D4"] },
                          "nullable": false
                        },
                        {
                          "name": "province",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["Zhejiang", "Jiangsu", "Guangdong", "Sichuan"] },
                          "nullable": false
                        },
                        {
                          "name": "city",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["Hangzhou", "Nanjing", "Shenzhen", "Chengdu"] },
                          "nullable": false
                        },
                        {
                          "name": "promiseHours",
                          "type": "SCALAR",
                          "valueType": "INT",
                          "generatorType": "RANDOM_INT",
                          "generatorConfig": { "min": 4, "max": 72 },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "audit",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "riskScore",
                          "type": "SCALAR",
                          "valueType": "INT",
                          "generatorType": "RANDOM_INT",
                          "generatorConfig": { "min": 1, "max": 100 },
                          "nullable": false
                        },
                        {
                          "name": "manualReview",
                          "type": "SCALAR",
                          "valueType": "BOOLEAN",
                          "generatorType": "BOOLEAN",
                          "generatorConfig": { "trueRate": 0.15 },
                          "nullable": false
                        }
                      ]
                    }
                  ]
                }
                """,
                List.of()
        );
    }

    private WriteTaskGroupTaskUpsertRequest buildPayloadKafkaParentTask(String topic) {
        return new WriteTaskGroupTaskUpsertRequest(
                null,
                "order_payload",
                "\u7236\u8d1f\u8f7d JSON \u4e8b\u4ef6",
                topic,
                TableMode.CREATE_IF_MISSING,
                WriteMode.APPEND,
                40,
                PAYLOAD_KAFKA_GROUP_SEED,
                "payload-parent-json-demo",
                WriteTaskStatus.READY,
                new WriteTaskGroupRowPlanRequest(
                        WriteTaskGroupRowPlanMode.FIXED,
                        18,
                        null,
                        null,
                        null
                ),
                """
                {
                  "payloadFormat": "JSON",
                  "keyMode": "FIELD",
                  "keyPath": "payload.order.orderId",
                  "headerDefinitions": [
                    { "name": "eventType", "mode": "FIELD", "path": "event.eventType" },
                    { "name": "tenantId", "mode": "FIELD", "path": "event.tenantId" },
                    { "name": "orderNo", "mode": "FIELD", "path": "payload.order.orderNo" }
                  ]
                }
                """,
                """
                {
                  "type": "OBJECT",
                  "children": [
                    {
                      "name": "event",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "tenantId",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["retail-cn", "retail-global"] },
                          "nullable": false
                        },
                        {
                          "name": "traceId",
                          "type": "SCALAR",
                          "valueType": "UUID",
                          "generatorType": "UUID",
                          "generatorConfig": {},
                          "nullable": false
                        },
                        {
                          "name": "eventId",
                          "type": "SCALAR",
                          "valueType": "UUID",
                          "generatorType": "UUID",
                          "generatorConfig": {},
                          "nullable": false
                        },
                        {
                          "name": "eventType",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["ORDER_PAYLOAD_CREATED", "ORDER_PAYLOAD_CONFIRMED"] },
                          "nullable": false
                        },
                        {
                          "name": "producedAt",
                          "type": "SCALAR",
                          "valueType": "DATETIME",
                          "generatorType": "DATETIME",
                          "generatorConfig": { "from": "2026-04-01T00:00:00Z", "to": "2026-04-30T23:59:59Z" },
                          "nullable": false
                        },
                        {
                          "name": "producer",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["oms", "checkout", "crm"] },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "payload",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "order",
                          "type": "OBJECT",
                          "children": [
                            {
                              "name": "orderId",
                              "type": "SCALAR",
                              "valueType": "LONG",
                              "generatorType": "SEQUENCE",
                              "generatorConfig": { "start": 7300000001, "step": 1 },
                              "nullable": false
                            },
                            {
                              "name": "orderNo",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "STRING",
                              "generatorConfig": { "prefix": "PO", "length": 12, "charset": "0123456789" },
                              "nullable": false
                            },
                            {
                              "name": "amount",
                              "type": "SCALAR",
                              "valueType": "DECIMAL",
                              "generatorType": "RANDOM_DECIMAL",
                              "generatorConfig": { "min": 299.0, "max": 15999.0, "scale": 2 },
                              "nullable": false
                            },
                            {
                              "name": "currency",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["CNY", "USD", "EUR"] },
                              "nullable": false
                            },
                            {
                              "name": "payStatus",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["CREATED", "PAID", "SETTLED"] },
                              "nullable": false
                            }
                          ]
                        },
                        {
                          "name": "customer",
                          "type": "OBJECT",
                          "children": [
                            {
                              "name": "customerId",
                              "type": "SCALAR",
                              "valueType": "LONG",
                              "generatorType": "SEQUENCE",
                              "generatorConfig": { "start": 660001, "step": 1 },
                              "nullable": false
                            },
                            {
                              "name": "customerLevel",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["NORMAL", "VIP", "SVIP"] },
                              "nullable": false
                            },
                            {
                              "name": "email",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "STRING",
                              "generatorConfig": { "prefix": "buyer", "length": 8, "suffix": "@demo.local" },
                              "nullable": false
                            }
                          ]
                        },
                        {
                          "name": "delivery",
                          "type": "OBJECT",
                          "children": [
                            {
                              "name": "province",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["Zhejiang", "Jiangsu", "Guangdong", "Sichuan"] },
                              "nullable": false
                            },
                            {
                              "name": "city",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["Hangzhou", "Suzhou", "Guangzhou", "Chengdu"] },
                              "nullable": false
                            },
                            {
                              "name": "addressTag",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["HOME", "OFFICE", "STORE"] },
                              "nullable": false
                            }
                          ]
                        },
                        {
                          "name": "flags",
                          "type": "OBJECT",
                          "children": [
                            {
                              "name": "priority",
                              "type": "SCALAR",
                              "valueType": "BOOLEAN",
                              "generatorType": "BOOLEAN",
                              "generatorConfig": { "trueRate": 0.35 },
                              "nullable": false
                            },
                            {
                              "name": "gift",
                              "type": "SCALAR",
                              "valueType": "BOOLEAN",
                              "generatorType": "BOOLEAN",
                              "generatorConfig": { "trueRate": 0.18 },
                              "nullable": false
                            }
                          ]
                        },
                        {
                          "name": "lines",
                          "type": "ARRAY",
                          "minItems": 1,
                          "maxItems": 3,
                          "itemSchema": {
                            "type": "OBJECT",
                            "children": [
                              {
                                "name": "skuId",
                                "type": "SCALAR",
                                "valueType": "LONG",
                                "generatorType": "RANDOM_INT",
                                "generatorConfig": { "min": 100000, "max": 999999 },
                                "nullable": false
                              },
                              {
                                "name": "skuName",
                                "type": "SCALAR",
                                "valueType": "STRING",
                                "generatorType": "STRING",
                                "generatorConfig": { "prefix": "SKU-", "length": 6, "charset": "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" },
                                "nullable": false
                              },
                              {
                                "name": "quantity",
                                "type": "SCALAR",
                                "valueType": "INT",
                                "generatorType": "RANDOM_INT",
                                "generatorConfig": { "min": 1, "max": 4 },
                                "nullable": false
                              },
                              {
                                "name": "unitPrice",
                                "type": "SCALAR",
                                "valueType": "DECIMAL",
                                "generatorType": "RANDOM_DECIMAL",
                                "generatorConfig": { "min": 19.0, "max": 3999.0, "scale": 2 },
                                "nullable": false
                              }
                            ]
                          }
                        }
                      ]
                    }
                  ]
                }
                """,
                List.of()
        );
    }

    private WriteTaskGroupTaskUpsertRequest buildPayloadKafkaChildTask(String topic) {
        return new WriteTaskGroupTaskUpsertRequest(
                null,
                "shipment_payload",
                "\u5b50\u8d1f\u8f7d JSON \u4e8b\u4ef6",
                topic,
                TableMode.CREATE_IF_MISSING,
                WriteMode.APPEND,
                60,
                PAYLOAD_KAFKA_GROUP_SEED,
                "payload-child-json-demo",
                WriteTaskStatus.READY,
                new WriteTaskGroupRowPlanRequest(
                        WriteTaskGroupRowPlanMode.CHILD_PER_PARENT,
                        null,
                        "order_payload",
                        1,
                        3
                ),
                """
                {
                  "payloadFormat": "JSON",
                  "keyMode": "FIELD",
                  "keyPath": "payload.orderRef.orderId",
                  "headerDefinitions": [
                    { "name": "eventType", "mode": "FIELD", "path": "event.eventType" },
                    { "name": "tenantId", "mode": "FIELD", "path": "event.tenantId" },
                    { "name": "parentEventId", "mode": "FIELD", "path": "event.parentEventId" }
                  ]
                }
                """,
                """
                {
                  "type": "OBJECT",
                  "children": [
                    {
                      "name": "event",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "tenantId",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["retail-cn", "retail-global"] },
                          "nullable": false
                        },
                        {
                          "name": "traceId",
                          "type": "SCALAR",
                          "valueType": "UUID",
                          "generatorType": "UUID",
                          "generatorConfig": {},
                          "nullable": false
                        },
                        {
                          "name": "parentEventId",
                          "type": "SCALAR",
                          "valueType": "UUID",
                          "generatorType": "UUID",
                          "generatorConfig": {},
                          "nullable": false
                        },
                        {
                          "name": "eventId",
                          "type": "SCALAR",
                          "valueType": "UUID",
                          "generatorType": "UUID",
                          "generatorConfig": {},
                          "nullable": false
                        },
                        {
                          "name": "eventType",
                          "type": "SCALAR",
                          "valueType": "STRING",
                          "generatorType": "ENUM",
                          "generatorConfig": { "values": ["SHIPMENT_PAYLOAD_CREATED", "SHIPMENT_PAYLOAD_UPDATED", "SHIPMENT_PAYLOAD_DELIVERED"] },
                          "nullable": false
                        },
                        {
                          "name": "producedAt",
                          "type": "SCALAR",
                          "valueType": "DATETIME",
                          "generatorType": "DATETIME",
                          "generatorConfig": { "from": "2026-04-01T00:00:00Z", "to": "2026-04-30T23:59:59Z" },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "payload",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "orderRef",
                          "type": "OBJECT",
                          "children": [
                            {
                              "name": "orderId",
                              "type": "SCALAR",
                              "valueType": "LONG",
                              "generatorType": "SEQUENCE",
                              "generatorConfig": { "start": 9100000001, "step": 1 },
                              "nullable": false
                            },
                            {
                              "name": "orderNo",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "STRING",
                              "generatorConfig": { "prefix": "TMP", "length": 10, "charset": "0123456789" },
                              "nullable": false
                            },
                            {
                              "name": "payStatus",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["CREATED", "PAID", "SETTLED"] },
                              "nullable": false
                            }
                          ]
                        },
                        {
                          "name": "customer",
                          "type": "OBJECT",
                          "children": [
                            {
                              "name": "customerId",
                              "type": "SCALAR",
                              "valueType": "LONG",
                              "generatorType": "SEQUENCE",
                              "generatorConfig": { "start": 880001, "step": 1 },
                              "nullable": false
                            },
                            {
                              "name": "customerLevel",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["NORMAL", "VIP", "SVIP"] },
                              "nullable": false
                            },
                            {
                              "name": "email",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "STRING",
                              "generatorConfig": { "prefix": "ship", "length": 8, "suffix": "@demo.local" },
                              "nullable": false
                            }
                          ]
                        },
                        {
                          "name": "shipment",
                          "type": "OBJECT",
                          "children": [
                            {
                              "name": "shipmentId",
                              "type": "SCALAR",
                              "valueType": "LONG",
                              "generatorType": "SEQUENCE",
                              "generatorConfig": { "start": 55000001, "step": 1 },
                              "nullable": false
                            },
                            {
                              "name": "carrier",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["SF", "JD", "YTO", "DHL"] },
                              "nullable": false
                            },
                            {
                              "name": "status",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["CREATED", "PICKED", "IN_TRANSIT", "DELIVERED"] },
                              "nullable": false
                            },
                            {
                              "name": "province",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["Zhejiang", "Jiangsu", "Guangdong", "Sichuan"] },
                              "nullable": false
                            },
                            {
                              "name": "city",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "ENUM",
                              "generatorConfig": { "values": ["Hangzhou", "Suzhou", "Guangzhou", "Chengdu"] },
                              "nullable": false
                            },
                            {
                              "name": "priorityFlag",
                              "type": "SCALAR",
                              "valueType": "BOOLEAN",
                              "generatorType": "BOOLEAN",
                              "generatorConfig": { "trueRate": 0.35 },
                              "nullable": false
                            }
                          ]
                        },
                        {
                          "name": "package",
                          "type": "OBJECT",
                          "children": [
                            {
                              "name": "packageNo",
                              "type": "SCALAR",
                              "valueType": "STRING",
                              "generatorType": "STRING",
                              "generatorConfig": { "prefix": "PKG", "length": 10, "charset": "0123456789" },
                              "nullable": false
                            },
                            {
                              "name": "giftWrap",
                              "type": "SCALAR",
                              "valueType": "BOOLEAN",
                              "generatorType": "BOOLEAN",
                              "generatorConfig": { "trueRate": 0.18 },
                              "nullable": false
                            },
                            {
                              "name": "weightKg",
                              "type": "SCALAR",
                              "valueType": "DECIMAL",
                              "generatorType": "RANDOM_DECIMAL",
                              "generatorConfig": { "min": 0.3, "max": 15.0, "scale": 2 },
                              "nullable": false
                            }
                          ]
                        },
                        {
                          "name": "checkpoints",
                          "type": "ARRAY",
                          "minItems": 2,
                          "maxItems": 4,
                          "itemSchema": {
                            "type": "OBJECT",
                            "children": [
                              {
                                "name": "node",
                                "type": "SCALAR",
                                "valueType": "STRING",
                                "generatorType": "ENUM",
                                "generatorConfig": { "values": ["WAREHOUSE", "SORTING", "TRANSIT", "DELIVERY"] },
                                "nullable": false
                              },
                              {
                                "name": "success",
                                "type": "SCALAR",
                                "valueType": "BOOLEAN",
                                "generatorType": "BOOLEAN",
                                "generatorConfig": { "trueRate": 0.92 },
                                "nullable": false
                              }
                            ]
                          }
                        }
                      ]
                    }
                  ]
                }
                """,
                List.of()
        );
    }

    private WriteTaskGroupRelationUpsertRequest buildComplexKafkaRelation() {
        return new WriteTaskGroupRelationUpsertRequest(
                null,
                "order_profile_to_fulfillment",
                "order_profile",
                "order_fulfillment",
                WriteTaskRelationMode.KAFKA_EVENT,
                WriteTaskRelationType.ONE_TO_MANY,
                ReferenceSourceMode.CURRENT_BATCH,
                RelationSelectionStrategy.PARENT_DRIVEN,
                RelationReusePolicy.ALLOW_REPEAT,
                List.of(),
                List.of(),
                0D,
                null,
                2,
                4,
                """
                {
                  "fieldMappings": [
                    { "from": "meta.tenantId", "to": "meta.tenantId", "required": true },
                    { "from": "meta.traceId", "to": "meta.parentTraceId", "required": true },
                    { "from": "order.orderId", "to": "orderRef.orderId", "required": true },
                    { "from": "order.orderNo", "to": "orderRef.orderNo", "required": true },
                    { "from": "order.channel", "to": "orderRef.channel", "required": true },
                    { "from": "order.status", "to": "orderRef.status", "required": true },
                    { "from": "buyer.userId", "to": "buyer.userId", "required": true },
                    { "from": "buyer.userTier", "to": "buyer.userTier", "required": true },
                    { "from": "buyer.region", "to": "buyer.region", "required": true },
                    { "from": "shipping.province", "to": "fulfillment.province", "required": true },
                    { "from": "shipping.city", "to": "fulfillment.city", "required": true },
                    { "from": "risk.score", "to": "audit.riskScore", "required": true },
                    { "from": "risk.manualReview", "to": "audit.manualReview", "required": true }
                  ]
                }
                """,
                0
        );
    }

    private WriteTaskGroupRelationUpsertRequest buildPayloadKafkaRelation() {
        return new WriteTaskGroupRelationUpsertRequest(
                null,
                "order_payload_to_shipment_payload",
                "order_payload",
                "shipment_payload",
                WriteTaskRelationMode.KAFKA_EVENT,
                WriteTaskRelationType.ONE_TO_MANY,
                ReferenceSourceMode.CURRENT_BATCH,
                RelationSelectionStrategy.PARENT_DRIVEN,
                RelationReusePolicy.ALLOW_REPEAT,
                List.of(),
                List.of(),
                0D,
                null,
                1,
                3,
                """
                {
                  "fieldMappings": [
                    { "from": "event.tenantId", "to": "event.tenantId", "required": true },
                    { "from": "event.traceId", "to": "event.traceId", "required": true },
                    { "from": "event.eventId", "to": "event.parentEventId", "required": true },
                    { "from": "payload.order.orderId", "to": "payload.orderRef.orderId", "required": true },
                    { "from": "payload.order.orderNo", "to": "payload.orderRef.orderNo", "required": true },
                    { "from": "payload.order.payStatus", "to": "payload.orderRef.payStatus", "required": true },
                    { "from": "payload.customer.customerId", "to": "payload.customer.customerId", "required": true },
                    { "from": "payload.customer.customerLevel", "to": "payload.customer.customerLevel", "required": true },
                    { "from": "payload.customer.email", "to": "payload.customer.email", "required": true },
                    { "from": "payload.delivery.province", "to": "payload.shipment.province", "required": true },
                    { "from": "payload.delivery.city", "to": "payload.shipment.city", "required": true },
                    { "from": "payload.flags.priority", "to": "payload.shipment.priorityFlag", "required": true },
                    { "from": "payload.flags.gift", "to": "payload.package.giftWrap", "required": true }
                  ]
                }
                """,
                0
        );
    }

    private TargetConnection resolveSourceConnection(Long sourceConnectionId) {
        if (sourceConnectionId != null) {
            return connectionRepository.findById(sourceConnectionId)
                    .orElseThrow(() -> new IllegalArgumentException("未找到需要重建的目标连接: " + sourceConnectionId));
        }

        return connectionRepository.findAll(Sort.by(Sort.Direction.DESC, "updatedAt")).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("当前没有可重建的目标连接，请先创建一个连接"));
    }

    private void deleteSourceData(Long connectionId) {
        List<WriteTask> sourceTasks = taskRepository.findByConnectionId(connectionId);
        List<Long> taskIds = sourceTasks.stream()
                .map(WriteTask::getId)
                .toList();

        if (!taskIds.isEmpty()) {
            List<WriteTaskExecution> executions = executionRepository.findByWriteTaskIdIn(taskIds);
            List<Long> executionIds = executions.stream()
                    .map(WriteTaskExecution::getId)
                    .toList();

            if (!executionIds.isEmpty()) {
                List<WriteTaskExecutionLog> executionLogs = executionLogRepository.findByWriteTaskExecutionIdIn(executionIds);
                if (!executionLogs.isEmpty()) {
                    executionLogRepository.deleteAllInBatch(executionLogs);
                }
                executionRepository.deleteAllInBatch(executions);
            }

            List<WriteTaskColumn> columns = taskColumnRepository.findByTaskIdIn(taskIds);
            if (!columns.isEmpty()) {
                taskColumnRepository.deleteAllInBatch(columns);
            }

            taskRepository.deleteAllInBatch(sourceTasks);
        }

        connectionRepository.deleteById(connectionId);
    }

    private String buildConnectionName(DatabaseType dbType) {
        return switch (dbType) {
            case MYSQL -> "演示 MySQL 目标";
            case POSTGRESQL -> "演示 PostgreSQL 目标";
            case SQLSERVER -> "演示 SQL Server 目标";
            case ORACLE -> "演示 Oracle 目标";
            case KAFKA -> "演示 Kafka 目标";
        };
    }

    private List<WriteTaskColumnUpsertRequest> buildColumns(DatabaseType dbType) {
        String datetimeType = switch (dbType) {
            case MYSQL -> "DATETIME";
            case POSTGRESQL, ORACLE, KAFKA -> "TIMESTAMP";
            case SQLSERVER -> "DATETIME2";
        };
        return List.of(
                new WriteTaskColumnUpsertRequest(
                        "order_id",
                        "BIGINT",
                        null,
                        null,
                        null,
                        false,
                        true,
                        ColumnGeneratorType.SEQUENCE,
                        Map.of("start", 10001, "step", 1),
                        0
                ),
                new WriteTaskColumnUpsertRequest(
                        "customer_name",
                        "VARCHAR",
                        32,
                        null,
                        null,
                        false,
                        false,
                        ColumnGeneratorType.STRING,
                        Map.of("prefix", "用户", "length", 6, "charset", "0123456789"),
                        1
                ),
                new WriteTaskColumnUpsertRequest(
                        "city",
                        "VARCHAR",
                        16,
                        null,
                        null,
                        false,
                        false,
                        ColumnGeneratorType.ENUM,
                        Map.of("values", List.of("上海", "北京", "深圳", "杭州")),
                        2
                ),
                new WriteTaskColumnUpsertRequest(
                        "order_status",
                        "VARCHAR",
                        16,
                        null,
                        null,
                        false,
                        false,
                        ColumnGeneratorType.ENUM,
                        Map.of("values", List.of("待支付", "已支付", "已发货", "已完成")),
                        3
                ),
                new WriteTaskColumnUpsertRequest(
                        "amount",
                        "DECIMAL",
                        null,
                        10,
                        2,
                        false,
                        false,
                        ColumnGeneratorType.RANDOM_DECIMAL,
                        Map.of("min", 99.0, "max", 9999.0, "scale", 2),
                        4
                ),
                new WriteTaskColumnUpsertRequest(
                        "created_at",
                        datetimeType,
                        null,
                        null,
                        null,
                        false,
                        false,
                        ColumnGeneratorType.DATETIME,
                        Map.of("from", "2026-01-01T00:00:00Z", "to", "2026-12-31T23:59:59Z"),
                        5
                )
        );
    }
}

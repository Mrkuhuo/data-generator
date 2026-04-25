package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datagenerator.connection.application.TargetConnectionService;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskExecutionResponse;
import com.datagenerator.task.api.WriteTaskPreviewResponse;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.datagenerator.task.domain.WriteTaskExecution;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.datagenerator.task.repository.WriteTaskExecutionLogRepository;
import com.datagenerator.task.repository.WriteTaskExecutionRepository;
import com.datagenerator.task.repository.WriteTaskRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WriteTaskServiceTest {

    @Mock
    private WriteTaskRepository repository;

    @Mock
    private WriteTaskExecutionRepository executionRepository;

    @Mock
    private WriteTaskExecutionLogRepository executionLogRepository;

    @Mock
    private TargetConnectionService connectionService;

    @Mock
    private WriteTaskPreviewService previewService;

    @Mock
    private WriteTaskExecutionPreparationService executionPreparationService;

    @Mock
    private WriteTaskDeliveryWriterRegistry writerRegistry;

    @Mock
    private WriteTaskDeliveryWriter writer;

    private WriteTaskService service;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        service = new WriteTaskService(
                repository,
                executionRepository,
                executionLogRepository,
                connectionService,
                previewService,
                executionPreparationService,
                writerRegistry,
                new KafkaPayloadSchemaService(objectMapper),
                objectMapper
        );

        lenient().when(repository.save(any(WriteTask.class))).thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(executionRepository.save(any(WriteTaskExecution.class))).thenAnswer(invocation -> {
            WriteTaskExecution execution = invocation.getArgument(0);
            if (execution.getId() == null) {
                execution.setId(1L);
            }
            return execution;
        });
        lenient().when(executionPreparationService.prepareForExecution(any(), any(), any()))
                .thenAnswer(invocation -> invocation.getArgument(1));
        lenient().when(writerRegistry.get(any())).thenReturn(writer);
        lenient().when(writer.write(any(), any(), any(), any())).thenReturn(new WriteTaskDeliveryResult(
                2,
                0,
                "目标表写入完成",
                Map.of(
                        "beforeWriteRowCount", 10L,
                        "afterWriteRowCount", 12L,
                        "rowDelta", 2L,
                        "writtenRowCount", 2L
                )
        ));
    }

    @Test
    void run_shouldReturnBeforeAfterCountsAndValidationSummary() throws Exception {
        WriteTask task = sampleTask();
        TargetConnection connection = sampleConnection();
        when(repository.findById(1L)).thenReturn(Optional.of(task));
        when(previewService.preview(any(), any(), any())).thenReturn(new WriteTaskPreviewResponse(
                2,
                7L,
                List.of(
                        Map.of("order_id", 1L, "customer_name", "张三"),
                        Map.of("order_id", 2L, "customer_name", "李四")
                )
        ));
        when(connectionService.findById(9L)).thenReturn(connection);

        WriteTaskExecutionResponse response = service.run(1L);

        assertThat(response.status()).isEqualTo(WriteExecutionStatus.SUCCESS);
        Map<?, ?> deliveryDetails = objectMapper.readValue(response.deliveryDetailsJson(), Map.class);
        assertThat(deliveryDetails.get("beforeWriteRowCount")).isEqualTo(10);
        assertThat(deliveryDetails.get("afterWriteRowCount")).isEqualTo(12);
        assertThat(deliveryDetails.get("rowDelta")).isEqualTo(2);
        assertThat(deliveryDetails.get("writtenRowCount")).isEqualTo(2);

        Map<?, ?> validation = (Map<?, ?>) deliveryDetails.get("nonNullValidation");
        assertThat(validation.get("passed")).isEqualTo(true);
        assertThat(validation.get("nullValueCount")).isEqualTo(0);
        assertThat(validation.get("blankStringCount")).isEqualTo(0);
    }

    @Test
    void run_shouldUseKafkaWriterForKafkaConnection() throws Exception {
        WriteTask task = sampleTask();
        task.setTableName("demo.topic");
        TargetConnection connection = sampleConnection();
        connection.setDbType(DatabaseType.KAFKA);
        when(repository.findById(1L)).thenReturn(Optional.of(task));
        when(previewService.preview(any(), any(), any())).thenReturn(new WriteTaskPreviewResponse(
                2,
                7L,
                List.of(
                        Map.of("order_id", 1L, "customer_name", "张三"),
                        Map.of("order_id", 2L, "customer_name", "李四")
                )
        ));
        when(connectionService.findById(9L)).thenReturn(connection);
        when(writer.write(any(), any(), any(), any())).thenReturn(new WriteTaskDeliveryResult(
                2,
                0,
                "Kafka Topic 写入完成",
                Map.of(
                        "deliveryType", "KAFKA",
                        "topic", "demo.topic",
                        "writtenRowCount", 2L
                )
        ));

        WriteTaskExecutionResponse response = service.run(1L);

        Map<?, ?> deliveryDetails = objectMapper.readValue(response.deliveryDetailsJson(), Map.class);
        assertThat(deliveryDetails.get("targetType")).isEqualTo("KAFKA");
        assertThat(deliveryDetails.get("topic")).isEqualTo("demo.topic");
        verify(writerRegistry).get(DatabaseType.KAFKA);
    }

    @Test
    void run_shouldFailBeforeWriteWhenRequiredFieldContainsNullOrBlank() throws Exception {
        WriteTask task = sampleTask();
        when(repository.findById(1L)).thenReturn(Optional.of(task));
        when(connectionService.findById(9L)).thenReturn(sampleConnection());
        when(previewService.preview(any(), any(), any())).thenReturn(new WriteTaskPreviewResponse(
                2,
                7L,
                List.of(
                        Map.of("order_id", 1L, "customer_name", ""),
                        rowWithNullCustomerName()
                )
        ));

        WriteTaskExecutionResponse response = service.run(1L);

        assertThat(response.status()).isEqualTo(WriteExecutionStatus.FAILED);
        assertThat(response.errorSummary()).contains("非空字段校验未通过");
        Map<?, ?> deliveryDetails = objectMapper.readValue(response.deliveryDetailsJson(), Map.class);
        Map<?, ?> validation = (Map<?, ?>) deliveryDetails.get("nonNullValidation");
        assertThat(validation.get("passed")).isEqualTo(false);
        assertThat(validation.get("nullValueCount")).isEqualTo(1);
        assertThat(validation.get("blankStringCount")).isEqualTo(1);
        assertThat(validation.get("issueCount")).isEqualTo(2);
        verify(writer, never()).write(any(), any(), any(), any());
    }

    @Test
    void delete_shouldRemoveExecutionLogsBeforeDeletingTask() {
        WriteTask task = sampleTask();
        task.setId(1L);

        WriteTaskExecution execution = new WriteTaskExecution();
        execution.setId(10L);
        execution.setWriteTaskId(1L);

        com.datagenerator.task.domain.WriteTaskExecutionLog log = new com.datagenerator.task.domain.WriteTaskExecutionLog();
        log.setId(20L);
        log.setWriteTaskExecutionId(10L);

        when(repository.findById(1L)).thenReturn(Optional.of(task));
        when(executionRepository.findByWriteTaskIdIn(List.of(1L))).thenReturn(List.of(execution));
        when(executionLogRepository.findByWriteTaskExecutionIdIn(List.of(10L))).thenReturn(List.of(log));

        service.delete(1L);

        verify(executionLogRepository).deleteAllInBatch(List.of(log));
        verify(executionRepository).deleteAllInBatch(List.of(execution));
        verify(repository).delete(task);
    }

    @Test
    void create_shouldRejectIntervalTaskWithoutIntervalSeconds() {
        when(connectionService.findById(9L)).thenReturn(sampleConnection());

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> service.create(intervalRequestWithoutIntervalSeconds())
        );

        assertThat(exception.getMessage()).contains("intervalSeconds");
    }

    @Test
    void create_shouldRejectKafkaOverwriteMode() {
        TargetConnection kafkaConnection = sampleConnection();
        kafkaConnection.setDbType(DatabaseType.KAFKA);
        when(connectionService.findById(9L)).thenReturn(kafkaConnection);

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> service.create(kafkaOverwriteRequest())
        );

        assertThat(exception.getMessage()).contains("Kafka");
        assertThat(exception.getMessage()).contains("APPEND");
    }

    @Test
    void create_shouldFillMysqlManualColumnDefaults() {
        when(connectionService.findById(9L)).thenReturn(sampleConnection());

        WriteTask task = service.create(singleColumnRequest(new WriteTaskColumnUpsertRequest(
                "customer_name",
                "VARCHAR",
                null,
                null,
                null,
                true,
                false,
                ColumnGeneratorType.STRING,
                Map.of(),
                0
        )));

        WriteTaskColumn column = task.getColumns().get(0);
        assertThat(column.getDbType()).isEqualTo("VARCHAR");
        assertThat(column.getLengthValue()).isEqualTo(255);
        assertThat(column.getPrecisionValue()).isNull();
        assertThat(column.getScaleValue()).isNull();
    }

    @Test
    void create_shouldResolveSqlServerStringTypeWhenDbTypeMissing() {
        TargetConnection sqlServerConnection = sampleConnection();
        sqlServerConnection.setDbType(DatabaseType.SQLSERVER);
        when(connectionService.findById(9L)).thenReturn(sqlServerConnection);

        WriteTask task = service.create(singleColumnRequest(new WriteTaskColumnUpsertRequest(
                "customer_name",
                null,
                null,
                null,
                null,
                true,
                false,
                ColumnGeneratorType.STRING,
                Map.of(),
                0
        )));

        WriteTaskColumn column = task.getColumns().get(0);
        assertThat(column.getDbType()).isEqualTo("NVARCHAR");
        assertThat(column.getLengthValue()).isEqualTo(255);
    }

    @Test
    void create_shouldFillOracleNumberDefaultsForDecimalColumns() {
        TargetConnection oracleConnection = sampleConnection();
        oracleConnection.setDbType(DatabaseType.ORACLE);
        when(connectionService.findById(9L)).thenReturn(oracleConnection);

        WriteTask task = service.create(singleColumnRequest(new WriteTaskColumnUpsertRequest(
                "amount",
                "NUMBER",
                null,
                null,
                null,
                false,
                false,
                ColumnGeneratorType.RANDOM_DECIMAL,
                Map.of("min", 1, "max", 100),
                0
        )));

        WriteTaskColumn column = task.getColumns().get(0);
        assertThat(column.getDbType()).isEqualTo("NUMBER");
        assertThat(column.getPrecisionValue()).isEqualTo(18);
        assertThat(column.getScaleValue()).isEqualTo(2);
    }

    @Test
    void create_shouldPreservePostgresqlJsonbColumnType() {
        TargetConnection postgresqlConnection = sampleConnection();
        postgresqlConnection.setDbType(DatabaseType.POSTGRESQL);
        when(connectionService.findById(9L)).thenReturn(postgresqlConnection);

        WriteTask task = service.create(singleColumnRequest(new WriteTaskColumnUpsertRequest(
                "profile",
                "JSONB",
                null,
                null,
                null,
                true,
                false,
                ColumnGeneratorType.STRING,
                Map.of("mode", "random"),
                0
        )));

        WriteTaskColumn column = task.getColumns().get(0);
        assertThat(column.getDbType()).isEqualTo("JSONB");
        assertThat(column.getLengthValue()).isNull();
        assertThat(column.getPrecisionValue()).isNull();
        assertThat(column.getScaleValue()).isNull();
    }

    @Test
    void create_shouldPersistKafkaComplexPayloadSchemaAndNormalizeKeyPath() throws Exception {
        TargetConnection kafkaConnection = sampleConnection();
        kafkaConnection.setDbType(DatabaseType.KAFKA);
        when(connectionService.findById(9L)).thenReturn(kafkaConnection);

        WriteTask task = service.create(kafkaComplexRequest("order.id"));

        assertThat(task.getPayloadSchemaJson()).isNotBlank();
        assertThat(task.getColumns()).isEmpty();

        Map<?, ?> targetConfig = objectMapper.readValue(task.getTargetConfigJson(), Map.class);
        assertThat(targetConfig.get("payloadFormat")).isEqualTo("JSON");
        assertThat(targetConfig.get("keyMode")).isEqualTo("FIELD");
        assertThat(targetConfig.get("keyPath")).isEqualTo("order.id");
        assertThat(targetConfig.containsKey("keyField")).isFalse();
    }

    @Test
    void create_shouldPersistKafkaHeaderDefinitionsForFieldHeaders() throws Exception {
        TargetConnection kafkaConnection = sampleConnection();
        kafkaConnection.setDbType(DatabaseType.KAFKA);
        when(connectionService.findById(9L)).thenReturn(kafkaConnection);

        WriteTask task = service.create(kafkaComplexRequestWithHeaders("order.id", "order.amount"));

        Map<?, ?> targetConfig = objectMapper.readValue(task.getTargetConfigJson(), Map.class);
        assertThat(targetConfig.get("headers")).isEqualTo(Map.of("source", "mdg"));
        assertThat(targetConfig.get("headerDefinitions")).isInstanceOf(List.class);
        List<?> headerDefinitions = (List<?>) targetConfig.get("headerDefinitions");
        assertThat(headerDefinitions).hasSize(2);
        assertThat(headerDefinitions).anySatisfy(item ->
                assertThat(item).isEqualTo(Map.of("name", "source", "mode", "FIXED", "value", "mdg"))
        );
        assertThat(headerDefinitions).anySatisfy(item ->
                assertThat(item).isEqualTo(Map.of("name", "amount", "mode", "FIELD", "path", "order.amount"))
        );
    }

    @Test
    void create_shouldRejectKafkaHeaderPathOutsideSchema() {
        TargetConnection kafkaConnection = sampleConnection();
        kafkaConnection.setDbType(DatabaseType.KAFKA);
        when(connectionService.findById(9L)).thenReturn(kafkaConnection);

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> service.create(kafkaComplexRequestWithHeaders("order.id", "order.missing"))
        );

        assertThat(exception.getMessage()).contains("Header path");
    }

    @Test
    void create_shouldRejectKafkaComplexKeyPathOutsideSchema() {
        TargetConnection kafkaConnection = sampleConnection();
        kafkaConnection.setDbType(DatabaseType.KAFKA);
        when(connectionService.findById(9L)).thenReturn(kafkaConnection);

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> service.create(kafkaComplexRequest("order.missing"))
        );

        assertThat(exception.getMessage()).contains("keyPath");
    }

    @Test
    void run_shouldFailWhenKafkaComplexNestedRequiredFieldContainsNull() throws Exception {
        WriteTask task = sampleKafkaComplexTask();
        TargetConnection connection = sampleConnection();
        connection.setDbType(DatabaseType.KAFKA);

        when(repository.findById(1L)).thenReturn(Optional.of(task));
        when(connectionService.findById(9L)).thenReturn(connection);
        when(previewService.preview(any(), any(), any())).thenReturn(new WriteTaskPreviewResponse(
                1,
                7L,
                List.of(rowWithNullNestedAmount())
        ));

        WriteTaskExecutionResponse response = service.run(1L);

        assertThat(response.status()).isEqualTo(WriteExecutionStatus.FAILED);
        assertThat(response.errorSummary()).contains("非空字段校验未通过");
        Map<?, ?> deliveryDetails = objectMapper.readValue(response.deliveryDetailsJson(), Map.class);
        Map<?, ?> validation = (Map<?, ?>) deliveryDetails.get("nonNullValidation");
        assertThat(validation.get("passed")).isEqualTo(false);
        assertThat(validation.get("nullValueCount")).isEqualTo(1);
        verify(writer, never()).write(any(), any(), any(), any());
    }

    @Test
    void runScheduled_shouldRejectUnsupportedTriggerType() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> service.runScheduled(1L, WriteExecutionTriggerType.MANUAL)
        );

        assertThat(exception.getMessage()).contains("触发类型");
    }

    private WriteTask sampleTask() {
        WriteTask task = new WriteTask();
        task.setId(1L);
        task.setName("订单任务");
        task.setConnectionId(9L);
        task.setTableName("synthetic_demo_orders_cn");
        task.setTableMode(TableMode.CREATE_IF_MISSING);
        task.setWriteMode(WriteMode.APPEND);
        task.setRowCount(2);
        task.setBatchSize(100);
        task.setSeed(7L);
        task.setStatus(WriteTaskStatus.READY);

        WriteTaskColumn idColumn = new WriteTaskColumn();
        idColumn.setColumnName("order_id");
        idColumn.setDbType("BIGINT");
        idColumn.setNullableFlag(false);
        idColumn.setPrimaryKeyFlag(true);
        idColumn.setGeneratorType(ColumnGeneratorType.SEQUENCE);
        idColumn.setGeneratorConfigJson("{\"start\":1}");
        idColumn.setSortOrder(0);

        WriteTaskColumn nameColumn = new WriteTaskColumn();
        nameColumn.setColumnName("customer_name");
        nameColumn.setDbType("VARCHAR");
        nameColumn.setNullableFlag(false);
        nameColumn.setPrimaryKeyFlag(false);
        nameColumn.setGeneratorType(ColumnGeneratorType.STRING);
        nameColumn.setGeneratorConfigJson("{\"prefix\":\"用户\"}");
        nameColumn.setSortOrder(1);

        task.replaceColumns(List.of(idColumn, nameColumn));
        return task;
    }

    private WriteTask sampleKafkaComplexTask() {
        WriteTask task = new WriteTask();
        task.setId(1L);
        task.setName("Kafka 复杂任务");
        task.setConnectionId(9L);
        task.setTableName("demo.topic");
        task.setTableMode(TableMode.CREATE_IF_MISSING);
        task.setWriteMode(WriteMode.APPEND);
        task.setRowCount(1);
        task.setBatchSize(100);
        task.setSeed(7L);
        task.setStatus(WriteTaskStatus.READY);
        task.setTargetConfigJson("{\"payloadFormat\":\"JSON\",\"keyMode\":\"FIELD\",\"keyPath\":\"order.id\"}");
        task.setPayloadSchemaJson(complexPayloadSchemaJson());
        task.replaceColumns(List.of());
        return task;
    }

    private WriteTaskUpsertRequest intervalRequestWithoutIntervalSeconds() {
        return new WriteTaskUpsertRequest(
                "interval-task",
                9L,
                "synthetic_demo_orders_cn",
                TableMode.CREATE_IF_MISSING,
                WriteMode.APPEND,
                10,
                10,
                null,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.INTERVAL,
                null,
                null,
                null,
                null,
                null,
                "desc",
                null,
                null,
                List.of(new WriteTaskColumnUpsertRequest(
                        "order_id",
                        "BIGINT",
                        null,
                        null,
                        null,
                        false,
                        true,
                        ColumnGeneratorType.SEQUENCE,
                        Map.of("start", 1, "step", 1),
                        0
                ))
        );
    }

    private WriteTaskUpsertRequest kafkaOverwriteRequest() {
        return new WriteTaskUpsertRequest(
                "kafka-task",
                9L,
                "demo.topic",
                TableMode.USE_EXISTING,
                WriteMode.OVERWRITE,
                10,
                10,
                null,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                "desc",
                "{\"keyMode\":\"NONE\"}",
                null,
                List.of(new WriteTaskColumnUpsertRequest(
                        "order_id",
                        "BIGINT",
                        null,
                        null,
                        null,
                        false,
                        true,
                        ColumnGeneratorType.SEQUENCE,
                        Map.of("start", 1, "step", 1),
                0
                ))
        );
    }

    private WriteTaskUpsertRequest kafkaComplexRequest(String keyPath) {
        return kafkaComplexRequest(keyPath, "{\"keyMode\":\"FIELD\",\"keyPath\":\"%s\"}".formatted(keyPath));
    }

    private WriteTaskUpsertRequest kafkaComplexRequestWithHeaders(String keyPath, String headerPath) {
        return kafkaComplexRequest(
                keyPath,
                """
                        {
                          "keyMode": "FIELD",
                          "keyPath": "%s",
                          "headerDefinitions": [
                            {
                              "name": "source",
                              "mode": "FIXED",
                              "value": "mdg"
                            },
                            {
                              "name": "amount",
                              "mode": "FIELD",
                              "path": "%s"
                            }
                          ]
                        }
                        """.formatted(keyPath, headerPath).replace("\r", "").replace("\n", "")
        );
    }

    private WriteTaskUpsertRequest kafkaComplexRequest(String keyPath, String targetConfigJson) {
        return new WriteTaskUpsertRequest(
                "kafka-complex-task",
                9L,
                "demo.topic",
                TableMode.CREATE_IF_MISSING,
                WriteMode.APPEND,
                10,
                10,
                null,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                "desc",
                targetConfigJson,
                complexPayloadSchemaJson(),
                List.of()
        );
    }

    private WriteTaskUpsertRequest singleColumnRequest(WriteTaskColumnUpsertRequest column) {
        return new WriteTaskUpsertRequest(
                "manual-task",
                9L,
                "synthetic_demo_orders_cn",
                TableMode.CREATE_IF_MISSING,
                WriteMode.APPEND,
                10,
                10,
                null,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                "desc",
                null,
                null,
                List.of(column)
        );
    }

    private String complexPayloadSchemaJson() {
        return """
                {
                  "type": "OBJECT",
                  "children": [
                    {
                      "name": "order",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "id",
                          "type": "SCALAR",
                          "valueType": "LONG",
                          "generatorType": "SEQUENCE",
                          "generatorConfig": { "start": 1, "step": 1 },
                          "nullable": false
                        },
                        {
                          "name": "amount",
                          "type": "SCALAR",
                          "valueType": "DECIMAL",
                          "generatorType": "RANDOM_DECIMAL",
                          "generatorConfig": { "min": 10, "max": 999, "scale": 2 },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "tags",
                      "type": "ARRAY",
                      "minItems": 1,
                      "maxItems": 2,
                      "itemSchema": {
                        "type": "SCALAR",
                        "valueType": "STRING",
                        "generatorType": "ENUM",
                        "generatorConfig": { "values": ["new", "vip"] },
                        "nullable": false
                      }
                    }
                  ]
                }
                """;
    }

    private TargetConnection sampleConnection() {
        TargetConnection connection = new TargetConnection();
        connection.setId(9L);
        connection.setDbType(DatabaseType.MYSQL);
        connection.setHost("127.0.0.1");
        connection.setPort(3306);
        connection.setDatabaseName("synthetic_demo_target");
        connection.setUsername("root");
        connection.setPasswordValue("123456");
        return connection;
    }

    private Map<String, Object> rowWithNullCustomerName() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", 2L);
        row.put("customer_name", null);
        return row;
    }

    private Map<String, Object> rowWithNullNestedAmount() {
        Map<String, Object> order = new LinkedHashMap<>();
        order.put("id", 1L);
        order.put("amount", null);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order", order);
        row.put("tags", List.of("vip"));
        return row;
    }
}

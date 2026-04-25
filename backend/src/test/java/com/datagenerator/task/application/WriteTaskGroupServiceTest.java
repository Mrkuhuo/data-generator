package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datagenerator.connection.application.TargetConnectionService;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupRowPlanRequest;
import com.datagenerator.task.api.WriteTaskGroupTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupTaskUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupUpsertRequest;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.datagenerator.task.domain.WriteTaskGroup;
import com.datagenerator.task.domain.WriteTaskGroupExecution;
import com.datagenerator.task.domain.WriteTaskGroupRowPlanMode;
import com.datagenerator.task.domain.WriteTaskGroupTableExecution;
import com.datagenerator.task.domain.WriteTaskRelation;
import com.datagenerator.task.domain.WriteTaskRelationMode;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.datagenerator.task.repository.WriteTaskGroupExecutionRepository;
import com.datagenerator.task.repository.WriteTaskGroupRepository;
import com.datagenerator.task.repository.WriteTaskGroupTableExecutionRepository;
import com.datagenerator.task.repository.WriteTaskRelationRepository;
import com.datagenerator.task.repository.WriteTaskRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class WriteTaskGroupServiceTest {

    private WriteTaskGroupRepository groupRepository;
    private WriteTaskRepository taskRepository;
    private WriteTaskRelationRepository relationRepository;
    private WriteTaskGroupExecutionRepository executionRepository;
    private WriteTaskGroupTableExecutionRepository tableExecutionRepository;
    private TargetConnectionService connectionService;
    private WriteTaskGroupPreviewService previewService;
    private WriteTaskExecutionPreparationService executionPreparationService;
    private WriteTaskDeliveryWriterRegistry writerRegistry;
    private WriteTaskJdbcWriter jdbcWriter;
    private WriteTaskDefinitionNormalizationService definitionNormalizationService;
    private WriteTaskGroupService service;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        groupRepository = mock(WriteTaskGroupRepository.class);
        taskRepository = mock(WriteTaskRepository.class);
        relationRepository = mock(WriteTaskRelationRepository.class);
        executionRepository = mock(WriteTaskGroupExecutionRepository.class);
        tableExecutionRepository = mock(WriteTaskGroupTableExecutionRepository.class);
        connectionService = mock(TargetConnectionService.class);
        previewService = mock(WriteTaskGroupPreviewService.class);
        executionPreparationService = mock(WriteTaskExecutionPreparationService.class);
        writerRegistry = mock(WriteTaskDeliveryWriterRegistry.class);
        jdbcWriter = mock(WriteTaskJdbcWriter.class);
        objectMapper = new ObjectMapper();
        definitionNormalizationService = new WriteTaskDefinitionNormalizationService(
                new KafkaPayloadSchemaService(objectMapper),
                objectMapper
        );
        service = new WriteTaskGroupService(
                groupRepository,
                taskRepository,
                relationRepository,
                executionRepository,
                tableExecutionRepository,
                connectionService,
                previewService,
                executionPreparationService,
                writerRegistry,
                definitionNormalizationService,
                objectMapper
        );

        lenient().when(groupRepository.save(any(WriteTaskGroup.class))).thenAnswer(invocation -> {
            WriteTaskGroup group = invocation.getArgument(0);
            if (group.getId() == null) {
                group.setId(501L);
            }
            return group;
        });
        lenient().when(executionPreparationService.prepareForExecution(any(), any(), any()))
                .thenAnswer(invocation -> invocation.getArgument(1));
        lenient().when(writerRegistry.get(any())).thenReturn(jdbcWriter);
        lenient().when(executionRepository.existsByWriteTaskGroupIdAndStatus(any(Long.class), any()))
                .thenReturn(false);
        lenient().when(executionRepository.save(any(WriteTaskGroupExecution.class))).thenAnswer(invocation -> {
            WriteTaskGroupExecution execution = invocation.getArgument(0);
            if (execution.getId() == null) {
                execution.setId(101L);
            }
            return execution;
        });
        lenient().when(tableExecutionRepository.save(any(WriteTaskGroupTableExecution.class))).thenAnswer(invocation -> {
            WriteTaskGroupTableExecution execution = invocation.getArgument(0);
            if (execution.getId() == null) {
                execution.setId((long) (execution.getTableName().hashCode() & 0x7fffffff));
            }
            return execution;
        });
    }

    @Test
    void run_shouldWriteTablesInGeneratedOrderAndAggregateSummary() throws Exception {
        WriteTaskGroup group = new WriteTaskGroup();
        group.setId(10L);
        group.setName("order-flow");
        group.setConnectionId(9L);
        group.setStatus(WriteTaskStatus.READY);
        group.setScheduleType(WriteTaskScheduleType.MANUAL);
        group.setSeed(20260422L);

        WriteTask customerTask = task(21L, "customer", "customer");
        WriteTask orderTask = task(22L, "orders", "orders");
        WriteTaskRelation relation = new WriteTaskRelation();
        relation.setId(31L);
        relation.setGroupId(10L);
        relation.setParentTaskId(21L);
        relation.setChildTaskId(22L);
        relation.setRelationName("fk_orders_customer");
        relation.setParentColumnsJson("[\"id\"]");
        relation.setChildColumnsJson("[\"customer_id\"]");

        TargetConnection connection = new TargetConnection();
        connection.setDbType(DatabaseType.MYSQL);

        when(groupRepository.findById(10L)).thenReturn(Optional.of(group));
        when(taskRepository.findByGroupIdOrderByIdAsc(10L)).thenReturn(List.of(customerTask, orderTask));
        when(relationRepository.findByGroupIdOrderBySortOrderAscIdAsc(10L)).thenReturn(List.of(relation));
        when(connectionService.findById(9L)).thenReturn(connection);
        when(previewService.generate(any(), any(), any(), any())).thenReturn(
                new WriteTaskGroupGenerationResult(
                        20260422L,
                        List.of(
                                new WriteTaskTableGenerationResult(
                                        customerTask,
                                        List.of(Map.of("id", 1L), Map.of("id", 2L)),
                                        0,
                                        0,
                                        0,
                                        0L
                                ),
                                new WriteTaskTableGenerationResult(
                                        orderTask,
                                        List.of(
                                                Map.of("id", 100L, "customer_id", 1L),
                                                Map.of("id", 101L, "customer_id", 2L),
                                                Map.of("id", 102L, "customer_id", 2L)
                                        ),
                                        0,
                                        0,
                                        0,
                                        0L
                                )
                        )
                )
        );
        when(jdbcWriter.write(customerTask, connection, List.of(Map.of("id", 1L), Map.of("id", 2L)), 101L))
                .thenReturn(new WriteTaskDeliveryResult(
                        2L,
                        0L,
                        "customer ok",
                        Map.of("beforeWriteRowCount", 10L, "afterWriteRowCount", 12L, "writtenRowCount", 2L)
                ));
        when(jdbcWriter.write(
                orderTask,
                connection,
                List.of(
                        Map.of("id", 100L, "customer_id", 1L),
                        Map.of("id", 101L, "customer_id", 2L),
                        Map.of("id", 102L, "customer_id", 2L)
                ),
                101L))
                .thenReturn(new WriteTaskDeliveryResult(
                        3L,
                        0L,
                        "orders ok",
                        Map.of("beforeWriteRowCount", 20L, "afterWriteRowCount", 23L, "writtenRowCount", 3L)
                ));
        when(tableExecutionRepository.findByWriteTaskGroupExecutionIdOrderByIdAsc(101L)).thenReturn(List.of(
                savedTableExecution(101L, customerTask, "customer", 10L, 12L, 2L),
                savedTableExecution(101L, orderTask, "orders", 20L, 23L, 3L)
        ));

        var response = service.run(10L);

        InOrder inOrder = inOrder(jdbcWriter);
        inOrder.verify(jdbcWriter).write(customerTask, connection, List.of(Map.of("id", 1L), Map.of("id", 2L)), 101L);
        inOrder.verify(jdbcWriter).write(
                orderTask,
                connection,
                List.of(
                        Map.of("id", 100L, "customer_id", 1L),
                        Map.of("id", 101L, "customer_id", 2L),
                        Map.of("id", 102L, "customer_id", 2L)
                ),
                101L
        );

        assertThat(response.triggerType()).isEqualTo("MANUAL");
        assertThat(response.status()).isEqualTo("SUCCESS");
        assertThat(response.plannedTableCount()).isEqualTo(2);
        assertThat(response.successTableCount()).isEqualTo(2);
        assertThat(response.insertedRowCount()).isEqualTo(5L);
        assertThat(((Number) response.summary().get("insertedRowCount")).longValue()).isEqualTo(5L);
        assertThat(response.tables()).hasSize(2);
    }

    @Test
    void runScheduled_shouldUseAdjustedSequenceStartsWhenPreparingExistingTables() {
        WriteTaskGroup group = new WriteTaskGroup();
        group.setId(11L);
        group.setName("existing-sequence");
        group.setConnectionId(19L);
        group.setStatus(WriteTaskStatus.READY);
        group.setScheduleType(WriteTaskScheduleType.CRON);
        group.setCronExpression("0 0/5 * * * ?");
        group.setSeed(20260422L);

        WriteTask existingTask = task(31L, "users", "mysql_test.users");

        TargetConnection connection = new TargetConnection();
        connection.setDbType(DatabaseType.MYSQL);

        when(groupRepository.findById(11L)).thenReturn(Optional.of(group));
        when(taskRepository.findByGroupIdOrderByIdAsc(11L)).thenReturn(List.of(existingTask));
        when(relationRepository.findByGroupIdOrderBySortOrderAscIdAsc(11L)).thenReturn(List.of());
        when(connectionService.findById(19L)).thenReturn(connection);
        when(executionPreparationService.prepareForExecution(any(), any(), any())).thenAnswer(invocation -> {
            WriteTaskUpsertRequest request = invocation.getArgument(1);
            List<WriteTaskColumnUpsertRequest> adjustedColumns = request.columns().stream()
                    .map(column -> new WriteTaskColumnUpsertRequest(
                            column.columnName(),
                            column.dbType(),
                            column.lengthValue(),
                            column.precisionValue(),
                            column.scaleValue(),
                            column.nullableFlag(),
                            column.primaryKeyFlag(),
                            column.generatorType(),
                            Map.of("start", 7001L, "step", 1L),
                            column.sortOrder()
                    ))
                    .toList();
            return new WriteTaskUpsertRequest(
                    request.name(),
                    request.connectionId(),
                    request.tableName(),
                    request.tableMode(),
                    request.writeMode(),
                    request.rowCount(),
                    request.batchSize(),
                    request.seed(),
                    request.status(),
                    request.scheduleType(),
                    request.cronExpression(),
                    request.triggerAt(),
                    request.intervalSeconds(),
                    request.maxRuns(),
                    request.maxRowsTotal(),
                    request.description(),
                    request.targetConfigJson(),
                    request.payloadSchemaJson(),
                    adjustedColumns
            );
        });
        when(previewService.generate(any(), any(), any(), any())).thenReturn(
                new WriteTaskGroupGenerationResult(20260422L, List.of())
        );
        when(tableExecutionRepository.findByWriteTaskGroupExecutionIdOrderByIdAsc(101L)).thenReturn(List.of());

        var response = service.runScheduled(11L, WriteExecutionTriggerType.SCHEDULED);

        ArgumentCaptor<WriteTaskGroupUpsertRequest> requestCaptor = ArgumentCaptor.forClass(WriteTaskGroupUpsertRequest.class);
        ArgumentCaptor<List<WriteTask>> tasksCaptor = ArgumentCaptor.forClass(List.class);
        verify(previewService).generate(requestCaptor.capture(), tasksCaptor.capture(), any(), any());

        WriteTaskGroupUpsertRequest runtimeRequest = requestCaptor.getValue();
        Assertions.assertNotNull(runtimeRequest.tasks());
        assertThat(runtimeRequest.tasks()).hasSize(1);
        assertThat(runtimeRequest.tasks().getFirst().columns().getFirst().generatorConfig()).containsEntry("start", 7001L);
        assertThat(response.triggerType()).isEqualTo("SCHEDULED");

        @SuppressWarnings("unchecked")
        List<WriteTask> runtimeTasks = tasksCaptor.getValue();
        assertThat(runtimeTasks).hasSize(1);
        assertThat(runtimeTasks.getFirst().getColumns().getFirst().getGeneratorConfigJson()).contains("7001");
    }

    @Test
    void create_shouldPersistScheduleFields() {
        TargetConnection connection = new TargetConnection();
        connection.setId(29L);
        connection.setDbType(DatabaseType.MYSQL);

        when(connectionService.findById(29L)).thenReturn(connection);
        when(taskRepository.findByGroupIdOrderByIdAsc(501L)).thenReturn(List.of());
        when(taskRepository.save(any(WriteTask.class))).thenAnswer(invocation -> {
            WriteTask task = invocation.getArgument(0);
            if (task.getId() == null) {
                task.setId(601L);
            }
            return task;
        });
        when(relationRepository.findByGroupIdOrderBySortOrderAscIdAsc(501L)).thenReturn(List.of());

        WriteTaskGroupUpsertRequest request = new WriteTaskGroupUpsertRequest(
                "save-debug",
                29L,
                null,
                99L,
                WriteTaskStatus.PAUSED,
                WriteTaskScheduleType.CRON,
                "0 0/10 * * * ?",
                null,
                null,
                null,
                null,
                List.of(taskRequest("mysql_test_products", "mysql_test.products")),
                List.of()
        );

        var response = service.create(request);

        assertThat(response.name()).isEqualTo("save-debug");
        assertThat(response.scheduleType()).isEqualTo("CRON");
        assertThat(response.cronExpression()).isEqualTo("0 0/10 * * * ?");

        ArgumentCaptor<WriteTaskGroup> groupCaptor = ArgumentCaptor.forClass(WriteTaskGroup.class);
        verify(groupRepository, org.mockito.Mockito.atLeastOnce()).save(groupCaptor.capture());
        WriteTaskGroup savedGroup = groupCaptor.getAllValues().getLast();
        assertThat(savedGroup.getScheduleType()).isEqualTo(WriteTaskScheduleType.CRON);
        assertThat(savedGroup.getCronExpression()).isEqualTo("0 0/10 * * * ?");
        assertThat(savedGroup.getStatus()).isEqualTo(WriteTaskStatus.PAUSED);
    }

    @Test
    void create_shouldPersistIntervalFields() {
        TargetConnection connection = new TargetConnection();
        connection.setId(39L);
        connection.setDbType(DatabaseType.MYSQL);

        when(connectionService.findById(39L)).thenReturn(connection);
        when(taskRepository.findByGroupIdOrderByIdAsc(501L)).thenReturn(List.of());
        when(taskRepository.save(any(WriteTask.class))).thenAnswer(invocation -> {
            WriteTask task = invocation.getArgument(0);
            if (task.getId() == null) {
                task.setId(701L);
            }
            return task;
        });
        when(relationRepository.findByGroupIdOrderBySortOrderAscIdAsc(501L)).thenReturn(List.of());

        service.create(new WriteTaskGroupUpsertRequest(
                "continuous-demo",
                39L,
                "demo",
                null,
                WriteTaskStatus.RUNNING,
                WriteTaskScheduleType.INTERVAL,
                null,
                null,
                15,
                5,
                500L,
                List.of(taskRequest("continuous_orders", "mysql_test.orders")),
                List.of()
        ));

        ArgumentCaptor<WriteTaskGroup> groupCaptor = ArgumentCaptor.forClass(WriteTaskGroup.class);
        verify(groupRepository, org.mockito.Mockito.atLeastOnce()).save(groupCaptor.capture());
        WriteTaskGroup savedGroup = groupCaptor.getAllValues().getLast();
        assertThat(savedGroup.getScheduleType()).isEqualTo(WriteTaskScheduleType.INTERVAL);
        assertThat(savedGroup.getIntervalSeconds()).isEqualTo(15);
        assertThat(savedGroup.getMaxRuns()).isEqualTo(5);
        assertThat(savedGroup.getMaxRowsTotal()).isEqualTo(500L);
    }

    private WriteTaskGroupTaskUpsertRequest taskRequest(String taskKey, String tableName) {
        return new WriteTaskGroupTaskUpsertRequest(
                null,
                taskKey,
                tableName,
                tableName,
                TableMode.USE_EXISTING,
                WriteMode.APPEND,
                500,
                null,
                null,
                WriteTaskStatus.READY,
                new WriteTaskGroupRowPlanRequest(
                        WriteTaskGroupRowPlanMode.FIXED,
                        100,
                        null,
                        null,
                        null
                ),
                null,
                null,
                List.of(new WriteTaskGroupTaskColumnUpsertRequest(
                        "id",
                        "INT",
                        null,
                        10,
                        null,
                        false,
                        true,
                        false,
                        ColumnGeneratorType.SEQUENCE,
                        Map.of("start", 1, "step", 1),
                        0
                ))
        );
    }

    private WriteTask task(Long id, String taskKey, String tableName) {
        WriteTask task = new WriteTask();
        task.setId(id);
        task.setTaskKey(taskKey);
        task.setConnectionId(9L);
        task.setName(taskKey);
        task.setTableName(tableName);
        task.setTableMode(TableMode.USE_EXISTING);
        task.setWriteMode(WriteMode.APPEND);
        task.setBatchSize(100);
        task.setRowCount(10);
        task.setStatus(WriteTaskStatus.READY);
        task.setCreatedAt(Instant.parse("2026-04-22T00:00:00Z"));
        task.setUpdatedAt(Instant.parse("2026-04-22T00:00:00Z"));

        WriteTaskColumn idColumn = new WriteTaskColumn();
        idColumn.setId(id * 10);
        idColumn.setColumnName("id");
        idColumn.setDbType("BIGINT");
        idColumn.setNullableFlag(false);
        idColumn.setPrimaryKeyFlag(true);
        idColumn.setGeneratorType(ColumnGeneratorType.SEQUENCE);
        idColumn.setGeneratorConfigJson("{\"start\":1,\"step\":1}");
        idColumn.setSortOrder(0);
        task.replaceColumns(List.of(idColumn));
        return task;
    }

    private WriteTaskGroupTableExecution savedTableExecution(
            Long groupExecutionId,
            WriteTask task,
            String tableName,
            Long beforeCount,
            Long afterCount,
            Long insertedCount
    ) {
        WriteTaskGroupTableExecution execution = new WriteTaskGroupTableExecution();
        execution.setId((long) tableName.hashCode() & 0x7fffffff);
        execution.setWriteTaskGroupExecutionId(groupExecutionId);
        execution.setWriteTaskId(task.getId());
        execution.setTableName(tableName);
        execution.setStatus(WriteExecutionStatus.SUCCESS);
        execution.setBeforeWriteRowCount(beforeCount);
        execution.setAfterWriteRowCount(afterCount);
        execution.setInsertedCount(insertedCount);
        execution.setNullViolationCount(0L);
        execution.setBlankStringCount(0L);
        execution.setFkMissCount(0L);
        execution.setPkDuplicateCount(0L);
        execution.setSummaryJson("{\"writtenRowCount\":" + insertedCount + "}");
        return execution;
    }
}

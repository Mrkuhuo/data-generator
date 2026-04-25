package com.datagenerator.system.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

import com.datagenerator.connection.api.TargetConnectionUpsertRequest;
import com.datagenerator.connection.application.ConnectionJdbcSupport;
import com.datagenerator.connection.application.TargetConnectionService;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.api.WriteTaskGroupPreviewRequest;
import com.datagenerator.task.api.WriteTaskGroupPreviewResponse;
import com.datagenerator.task.api.WriteTaskGroupRelationResponse;
import com.datagenerator.task.api.WriteTaskGroupResponse;
import com.datagenerator.task.api.WriteTaskGroupRowPlanResponse;
import com.datagenerator.task.api.WriteTaskGroupTaskResponse;
import com.datagenerator.task.api.WriteTaskGroupUpsertRequest;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.application.WriteTaskGroupService;
import com.datagenerator.task.application.WriteTaskService;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.datagenerator.task.domain.WriteTaskExecution;
import com.datagenerator.task.domain.WriteTaskExecutionLog;
import com.datagenerator.task.domain.WriteTaskGroupRowPlanMode;
import com.datagenerator.task.domain.WriteTaskRelationMode;
import com.datagenerator.task.repository.WriteTaskColumnRepository;
import com.datagenerator.task.repository.WriteTaskExecutionLogRepository;
import com.datagenerator.task.repository.WriteTaskExecutionRepository;
import com.datagenerator.task.repository.WriteTaskRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Sort;

@ExtendWith(MockitoExtension.class)
class DemoDataRebuildServiceTest {

    @Mock
    private com.datagenerator.connection.repository.TargetConnectionRepository connectionRepository;

    @Mock
    private WriteTaskRepository taskRepository;

    @Mock
    private WriteTaskColumnRepository taskColumnRepository;

    @Mock
    private WriteTaskExecutionRepository executionRepository;

    @Mock
    private WriteTaskExecutionLogRepository executionLogRepository;

    @Mock
    private TargetConnectionService targetConnectionService;

    @Mock
    private WriteTaskService writeTaskService;

    @Mock
    private WriteTaskGroupService writeTaskGroupService;

    @Mock
    private ConnectionJdbcSupport connectionJdbcSupport;

    @Test
    void rebuild_shouldRecreateCleanChineseDemoDataFromExistingConnectionSettings() {
        DemoDataRebuildService service = new DemoDataRebuildService(
                connectionRepository,
                taskRepository,
                taskColumnRepository,
                executionRepository,
                executionLogRepository,
                targetConnectionService,
                writeTaskService,
                writeTaskGroupService,
                connectionJdbcSupport
        );

        TargetConnection sourceConnection = new TargetConnection();
        sourceConnection.setId(1L);
        sourceConnection.setDbType(DatabaseType.MYSQL);
        sourceConnection.setHost("127.0.0.1");
        sourceConnection.setPort(3306);
        sourceConnection.setDatabaseName("synthetic_demo_target");
        sourceConnection.setSchemaName(null);
        sourceConnection.setUsername("root");
        sourceConnection.setPasswordValue("123456");
        sourceConnection.setJdbcParams("useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai");

        WriteTask oldTask = new WriteTask();
        oldTask.setId(3L);
        oldTask.setConnectionId(1L);

        WriteTaskExecution oldExecution = new WriteTaskExecution();
        oldExecution.setId(5L);
        oldExecution.setWriteTaskId(3L);

        WriteTaskExecutionLog oldLog = new WriteTaskExecutionLog();
        oldLog.setId(7L);
        oldLog.setWriteTaskExecutionId(5L);

        WriteTaskColumn oldColumn = new WriteTaskColumn();
        oldColumn.setId(11L);

        given(connectionRepository.findById(1L)).willReturn(Optional.of(sourceConnection));
        given(taskRepository.findByConnectionId(1L)).willReturn(List.of(oldTask));
        given(executionRepository.findByWriteTaskIdIn(List.of(3L))).willReturn(List.of(oldExecution));
        given(executionLogRepository.findByWriteTaskExecutionIdIn(List.of(5L))).willReturn(List.of(oldLog));
        given(taskColumnRepository.findByTaskIdIn(List.of(3L))).willReturn(List.of(oldColumn));
        given(targetConnectionService.create(any(TargetConnectionUpsertRequest.class))).willAnswer(invocation -> {
            TargetConnectionUpsertRequest request = invocation.getArgument(0);
            TargetConnection connection = new TargetConnection();
            connection.setId(9L);
            connection.setName(request.name());
            connection.setDbType(request.dbType());
            connection.setHost(request.host());
            connection.setPort(request.port());
            connection.setDatabaseName(request.databaseName());
            connection.setJdbcParams(request.jdbcParams());
            return connection;
        });
        given(writeTaskService.create(any(WriteTaskUpsertRequest.class))).willAnswer(invocation -> {
            WriteTaskUpsertRequest request = invocation.getArgument(0);
            WriteTask task = new WriteTask();
            task.setId(12L);
            task.setName(request.name());
            task.setTableName(request.tableName());
            return task;
        });
        given(connectionJdbcSupport.normalizeParamsForStorage(
                DatabaseType.MYSQL,
                "useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai"
        )).willReturn("useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai");

        DemoDataRebuildResponse response = service.rebuild(1L);

        ArgumentCaptor<TargetConnectionUpsertRequest> connectionCaptor =
                ArgumentCaptor.forClass(TargetConnectionUpsertRequest.class);
        verify(targetConnectionService).create(connectionCaptor.capture());
        TargetConnectionUpsertRequest recreatedConnection = connectionCaptor.getValue();
        assertThat(recreatedConnection.name()).contains("MySQL");
        assertThat(recreatedConnection.description()).isNotBlank();
        assertThat(recreatedConnection.jdbcParams()).contains("characterEncoding=UTF-8");

        ArgumentCaptor<WriteTaskUpsertRequest> taskCaptor = ArgumentCaptor.forClass(WriteTaskUpsertRequest.class);
        verify(writeTaskService).create(taskCaptor.capture());
        WriteTaskUpsertRequest recreatedTask = taskCaptor.getValue();
        assertThat(recreatedTask.name()).isNotBlank();
        assertThat(recreatedTask.tableName()).isEqualTo("synthetic_demo_orders_cn");
        assertThat(recreatedTask.columns()).hasSize(6);
        assertThat(recreatedTask.columns().get(0).columnName()).isEqualTo("order_id");
        assertThat(recreatedTask.columns().get(1).generatorConfig()).containsKey("prefix");
        assertThat(recreatedTask.columns().get(2).generatorConfig()).containsKey("values");

        InOrder inOrder = inOrder(
                targetConnectionService,
                writeTaskService,
                executionLogRepository,
                executionRepository,
                taskColumnRepository,
                taskRepository,
                connectionRepository
        );
        inOrder.verify(targetConnectionService).create(any(TargetConnectionUpsertRequest.class));
        inOrder.verify(writeTaskService).create(any(WriteTaskUpsertRequest.class));
        inOrder.verify(executionLogRepository).deleteAllInBatch(List.of(oldLog));
        inOrder.verify(executionRepository).deleteAllInBatch(List.of(oldExecution));
        inOrder.verify(taskColumnRepository).deleteAllInBatch(List.of(oldColumn));
        inOrder.verify(taskRepository).deleteAllInBatch(List.of(oldTask));
        inOrder.verify(connectionRepository).deleteById(1L);

        assertThat(response.replacedConnectionId()).isEqualTo(1L);
        assertThat(response.connectionId()).isEqualTo(9L);
        assertThat(response.taskId()).isEqualTo(12L);
    }

    @Test
    void createComplexKafkaJsonGroup_shouldPreviewAndCreateValidatedParentChildJsonGroup() {
        DemoDataRebuildService service = new DemoDataRebuildService(
                connectionRepository,
                taskRepository,
                taskColumnRepository,
                executionRepository,
                executionLogRepository,
                targetConnectionService,
                writeTaskService,
                writeTaskGroupService,
                connectionJdbcSupport
        );

        TargetConnection kafkaConnection = new TargetConnection();
        kafkaConnection.setId(21L);
        kafkaConnection.setName("Kafka complex demo");
        kafkaConnection.setDbType(DatabaseType.KAFKA);

        given(connectionRepository.findAll(any(Sort.class))).willReturn(List.of(kafkaConnection));
        given(writeTaskGroupService.preview(any(WriteTaskGroupPreviewRequest.class)))
                .willReturn(new WriteTaskGroupPreviewResponse(2026042501L, List.of()));
        given(writeTaskGroupService.create(any(WriteTaskGroupUpsertRequest.class))).willAnswer(invocation -> {
            WriteTaskGroupUpsertRequest request = invocation.getArgument(0);
            return new WriteTaskGroupResponse(
                    88L,
                    Instant.parse("2026-04-25T03:30:00Z"),
                    Instant.parse("2026-04-25T03:30:00Z"),
                    request.name(),
                    request.connectionId(),
                    request.description(),
                    request.seed(),
                    request.status().name(),
                    request.scheduleType().name(),
                    request.cronExpression(),
                    request.triggerAt(),
                    request.intervalSeconds(),
                    request.maxRuns(),
                    request.maxRowsTotal(),
                    null,
                    null,
                    null,
                    null,
                    List.of(
                            new WriteTaskGroupTaskResponse(
                                    101L,
                                    "order_profile",
                                    "parent",
                                    request.tasks().get(0).tableName(),
                                    request.tasks().get(0).tableMode().name(),
                                    request.tasks().get(0).writeMode().name(),
                                    request.tasks().get(0).batchSize(),
                                    request.tasks().get(0).seed(),
                                    request.tasks().get(0).description(),
                                    request.tasks().get(0).status().name(),
                                    new WriteTaskGroupRowPlanResponse(
                                            request.tasks().get(0).rowPlan().mode(),
                                            request.tasks().get(0).rowPlan().rowCount(),
                                            request.tasks().get(0).rowPlan().driverTaskKey(),
                                            request.tasks().get(0).rowPlan().minChildrenPerParent(),
                                            request.tasks().get(0).rowPlan().maxChildrenPerParent()
                                    ),
                                    request.tasks().get(0).targetConfigJson(),
                                    request.tasks().get(0).payloadSchemaJson(),
                                    List.of()
                            ),
                            new WriteTaskGroupTaskResponse(
                                    102L,
                                    "order_fulfillment",
                                    "child",
                                    request.tasks().get(1).tableName(),
                                    request.tasks().get(1).tableMode().name(),
                                    request.tasks().get(1).writeMode().name(),
                                    request.tasks().get(1).batchSize(),
                                    request.tasks().get(1).seed(),
                                    request.tasks().get(1).description(),
                                    request.tasks().get(1).status().name(),
                                    new WriteTaskGroupRowPlanResponse(
                                            request.tasks().get(1).rowPlan().mode(),
                                            request.tasks().get(1).rowPlan().rowCount(),
                                            request.tasks().get(1).rowPlan().driverTaskKey(),
                                            request.tasks().get(1).rowPlan().minChildrenPerParent(),
                                            request.tasks().get(1).rowPlan().maxChildrenPerParent()
                                    ),
                                    request.tasks().get(1).targetConfigJson(),
                                    request.tasks().get(1).payloadSchemaJson(),
                                    List.of()
                            )
                    ),
                    List.of(
                            new WriteTaskGroupRelationResponse(
                                    201L,
                                    request.relations().get(0).relationName(),
                                    101L,
                                    102L,
                                    request.relations().get(0).parentTaskKey(),
                                    request.relations().get(0).childTaskKey(),
                                    request.relations().get(0).relationMode().name(),
                                    request.relations().get(0).relationType().name(),
                                    request.relations().get(0).sourceMode().name(),
                                    request.relations().get(0).selectionStrategy().name(),
                                    request.relations().get(0).reusePolicy().name(),
                                    request.relations().get(0).parentColumns(),
                                    request.relations().get(0).childColumns(),
                                    request.relations().get(0).nullRate(),
                                    request.relations().get(0).mixedExistingRatio(),
                                    request.relations().get(0).minChildrenPerParent(),
                                    request.relations().get(0).maxChildrenPerParent(),
                                    request.relations().get(0).mappingConfigJson(),
                                    request.relations().get(0).sortOrder()
                            )
                    )
            );
        });

        DemoComplexKafkaJsonGroupResponse response = service.createComplexKafkaJsonGroup(null);

        ArgumentCaptor<WriteTaskGroupPreviewRequest> previewCaptor =
                ArgumentCaptor.forClass(WriteTaskGroupPreviewRequest.class);
        ArgumentCaptor<WriteTaskGroupUpsertRequest> createCaptor =
                ArgumentCaptor.forClass(WriteTaskGroupUpsertRequest.class);
        verify(writeTaskGroupService).preview(previewCaptor.capture());
        verify(writeTaskGroupService).create(createCaptor.capture());

        InOrder inOrder = inOrder(writeTaskGroupService);
        inOrder.verify(writeTaskGroupService).preview(any(WriteTaskGroupPreviewRequest.class));
        inOrder.verify(writeTaskGroupService).create(any(WriteTaskGroupUpsertRequest.class));

        WriteTaskGroupPreviewRequest previewRequest = previewCaptor.getValue();
        WriteTaskGroupUpsertRequest createRequest = createCaptor.getValue();
        assertThat(previewRequest.previewCount()).isEqualTo(3);
        assertThat(previewRequest.seed()).isEqualTo(2026042501L);
        assertThat(createRequest).isEqualTo(previewRequest.group());
        assertThat(createRequest.connectionId()).isEqualTo(21L);
        assertThat(createRequest.name()).contains("JSON");
        assertThat(createRequest.tasks()).hasSize(2);
        assertThat(createRequest.relations()).hasSize(1);

        assertThat(createRequest.tasks().get(0).taskKey()).isEqualTo("order_profile");
        assertThat(createRequest.tasks().get(0).tableName()).startsWith("mdg.demo.parent.order-profile.");
        assertThat(createRequest.tasks().get(0).payloadSchemaJson()).contains("\"name\": \"meta\"");
        assertThat(createRequest.tasks().get(0).targetConfigJson()).contains("\"payloadFormat\": \"JSON\"");

        assertThat(createRequest.tasks().get(1).taskKey()).isEqualTo("order_fulfillment");
        assertThat(createRequest.tasks().get(1).tableName()).startsWith("mdg.demo.child.order-fulfillment.");
        assertThat(createRequest.tasks().get(1).rowPlan().mode()).isEqualTo(WriteTaskGroupRowPlanMode.CHILD_PER_PARENT);
        assertThat(createRequest.tasks().get(1).rowPlan().driverTaskKey()).isEqualTo("order_profile");
        assertThat(createRequest.tasks().get(1).rowPlan().minChildrenPerParent()).isEqualTo(2);
        assertThat(createRequest.tasks().get(1).rowPlan().maxChildrenPerParent()).isEqualTo(4);
        assertThat(createRequest.tasks().get(1).payloadSchemaJson()).contains("\"name\": \"orderRef\"");

        assertThat(createRequest.relations().get(0).relationMode()).isEqualTo(WriteTaskRelationMode.KAFKA_EVENT);
        assertThat(createRequest.relations().get(0).mappingConfigJson()).contains("\"from\": \"order.orderId\"");
        assertThat(createRequest.relations().get(0).mappingConfigJson()).contains("\"to\": \"orderRef.orderId\"");

        assertThat(response.connectionId()).isEqualTo(21L);
        assertThat(response.groupId()).isEqualTo(88L);
        assertThat(response.parentTopic()).startsWith("mdg.demo.parent.order-profile.");
        assertThat(response.childTopic()).startsWith("mdg.demo.child.order-fulfillment.");
        assertThat(response.taskCount()).isEqualTo(2);
        assertThat(response.relationCount()).isEqualTo(1);
    }

    @Test
    void createPayloadKafkaJsonGroup_shouldPreviewAndCreatePayloadEnvelopeParentChildJsonGroup() {
        DemoDataRebuildService service = new DemoDataRebuildService(
                connectionRepository,
                taskRepository,
                taskColumnRepository,
                executionRepository,
                executionLogRepository,
                targetConnectionService,
                writeTaskService,
                writeTaskGroupService,
                connectionJdbcSupport
        );

        TargetConnection kafkaConnection = new TargetConnection();
        kafkaConnection.setId(21L);
        kafkaConnection.setName("Kafka payload demo");
        kafkaConnection.setDbType(DatabaseType.KAFKA);

        given(connectionRepository.findAll(any(Sort.class))).willReturn(List.of(kafkaConnection));
        given(writeTaskGroupService.preview(any(WriteTaskGroupPreviewRequest.class)))
                .willReturn(new WriteTaskGroupPreviewResponse(2026042502L, List.of()));
        given(writeTaskGroupService.create(any(WriteTaskGroupUpsertRequest.class)))
                .willAnswer(invocation -> buildCreatedGroupResponse(invocation.getArgument(0), 89L));

        DemoComplexKafkaJsonGroupResponse response = service.createPayloadKafkaJsonGroup(null);

        ArgumentCaptor<WriteTaskGroupPreviewRequest> previewCaptor =
                ArgumentCaptor.forClass(WriteTaskGroupPreviewRequest.class);
        ArgumentCaptor<WriteTaskGroupUpsertRequest> createCaptor =
                ArgumentCaptor.forClass(WriteTaskGroupUpsertRequest.class);
        verify(writeTaskGroupService).preview(previewCaptor.capture());
        verify(writeTaskGroupService).create(createCaptor.capture());

        WriteTaskGroupPreviewRequest previewRequest = previewCaptor.getValue();
        WriteTaskGroupUpsertRequest createRequest = createCaptor.getValue();
        assertThat(previewRequest.previewCount()).isEqualTo(3);
        assertThat(previewRequest.seed()).isEqualTo(2026042502L);
        assertThat(createRequest).isEqualTo(previewRequest.group());
        assertThat(createRequest.connectionId()).isEqualTo(21L);
        assertThat(createRequest.name()).contains("\u8d1f\u8f7d");
        assertThat(createRequest.tasks()).hasSize(2);
        assertThat(createRequest.relations()).hasSize(1);

        assertThat(createRequest.tasks().get(0).taskKey()).isEqualTo("order_payload");
        assertThat(createRequest.tasks().get(0).tableName()).startsWith("mdg.demo.payload.parent.order-envelope.");
        assertThat(createRequest.tasks().get(0).payloadSchemaJson()).contains("\"name\": \"event\"");
        assertThat(createRequest.tasks().get(0).payloadSchemaJson()).contains("\"name\": \"payload\"");
        assertThat(createRequest.tasks().get(0).payloadSchemaJson()).contains("\"name\": \"lines\"");
        assertThat(createRequest.tasks().get(0).targetConfigJson()).contains("\"keyPath\": \"payload.order.orderId\"");

        assertThat(createRequest.tasks().get(1).taskKey()).isEqualTo("shipment_payload");
        assertThat(createRequest.tasks().get(1).tableName()).startsWith("mdg.demo.payload.child.shipment-envelope.");
        assertThat(createRequest.tasks().get(1).rowPlan().mode()).isEqualTo(WriteTaskGroupRowPlanMode.CHILD_PER_PARENT);
        assertThat(createRequest.tasks().get(1).rowPlan().driverTaskKey()).isEqualTo("order_payload");
        assertThat(createRequest.tasks().get(1).rowPlan().minChildrenPerParent()).isEqualTo(1);
        assertThat(createRequest.tasks().get(1).rowPlan().maxChildrenPerParent()).isEqualTo(3);
        assertThat(createRequest.tasks().get(1).payloadSchemaJson()).contains("\"name\": \"orderRef\"");
        assertThat(createRequest.tasks().get(1).payloadSchemaJson()).contains("\"name\": \"checkpoints\"");

        assertThat(createRequest.relations().get(0).relationMode()).isEqualTo(WriteTaskRelationMode.KAFKA_EVENT);
        assertThat(createRequest.relations().get(0).mappingConfigJson()).contains("\"from\": \"payload.order.orderId\"");
        assertThat(createRequest.relations().get(0).mappingConfigJson()).contains("\"to\": \"payload.orderRef.orderId\"");
        assertThat(createRequest.relations().get(0).mappingConfigJson()).contains("\"to\": \"payload.package.giftWrap\"");

        assertThat(response.connectionId()).isEqualTo(21L);
        assertThat(response.groupId()).isEqualTo(89L);
        assertThat(response.parentTopic()).startsWith("mdg.demo.payload.parent.order-envelope.");
        assertThat(response.childTopic()).startsWith("mdg.demo.payload.child.shipment-envelope.");
        assertThat(response.taskCount()).isEqualTo(2);
        assertThat(response.relationCount()).isEqualTo(1);
    }

    private WriteTaskGroupResponse buildCreatedGroupResponse(WriteTaskGroupUpsertRequest request, long groupId) {
        return new WriteTaskGroupResponse(
                groupId,
                Instant.parse("2026-04-25T03:30:00Z"),
                Instant.parse("2026-04-25T03:30:00Z"),
                request.name(),
                request.connectionId(),
                request.description(),
                request.seed(),
                request.status().name(),
                request.scheduleType().name(),
                request.cronExpression(),
                request.triggerAt(),
                request.intervalSeconds(),
                request.maxRuns(),
                request.maxRowsTotal(),
                null,
                null,
                null,
                null,
                List.of(
                        toTaskResponse(101L, request.tasks().get(0)),
                        toTaskResponse(102L, request.tasks().get(1))
                ),
                List.of(toRelationResponse(201L, request.relations().get(0)))
        );
    }

    private WriteTaskGroupTaskResponse toTaskResponse(
            long id,
            com.datagenerator.task.api.WriteTaskGroupTaskUpsertRequest request
    ) {
        return new WriteTaskGroupTaskResponse(
                id,
                request.taskKey(),
                request.name(),
                request.tableName(),
                request.tableMode().name(),
                request.writeMode().name(),
                request.batchSize(),
                request.seed(),
                request.description(),
                request.status().name(),
                new WriteTaskGroupRowPlanResponse(
                        request.rowPlan().mode(),
                        request.rowPlan().rowCount(),
                        request.rowPlan().driverTaskKey(),
                        request.rowPlan().minChildrenPerParent(),
                        request.rowPlan().maxChildrenPerParent()
                ),
                request.targetConfigJson(),
                request.payloadSchemaJson(),
                List.of()
        );
    }

    private WriteTaskGroupRelationResponse toRelationResponse(
            long id,
            com.datagenerator.task.api.WriteTaskGroupRelationUpsertRequest request
    ) {
        return new WriteTaskGroupRelationResponse(
                id,
                request.relationName(),
                101L,
                102L,
                request.parentTaskKey(),
                request.childTaskKey(),
                request.relationMode().name(),
                request.relationType().name(),
                request.sourceMode().name(),
                request.selectionStrategy().name(),
                request.reusePolicy().name(),
                request.parentColumns(),
                request.childColumns(),
                request.nullRate(),
                request.mixedExistingRatio(),
                request.minChildrenPerParent(),
                request.maxChildrenPerParent(),
                request.mappingConfigJson(),
                request.sortOrder()
        );
    }
}

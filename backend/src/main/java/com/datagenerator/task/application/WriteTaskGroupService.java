package com.datagenerator.task.application;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.connection.application.TargetConnectionService;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupExecutionResponse;
import com.datagenerator.task.api.WriteTaskGroupPreviewResponse;
import com.datagenerator.task.api.WriteTaskGroupPreviewRequest;
import com.datagenerator.task.api.WriteTaskGroupRelationResponse;
import com.datagenerator.task.api.WriteTaskGroupRelationUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupResponse;
import com.datagenerator.task.api.WriteTaskGroupRowPlanRequest;
import com.datagenerator.task.api.WriteTaskGroupTaskColumnResponse;
import com.datagenerator.task.api.WriteTaskGroupTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupTaskResponse;
import com.datagenerator.task.api.WriteTaskGroupTaskUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupTableExecutionResponse;
import com.datagenerator.task.api.WriteTaskGroupUpsertRequest;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class WriteTaskGroupService {

    private final WriteTaskGroupRepository groupRepository;
    private final WriteTaskRepository taskRepository;
    private final WriteTaskRelationRepository relationRepository;
    private final WriteTaskGroupExecutionRepository executionRepository;
    private final WriteTaskGroupTableExecutionRepository tableExecutionRepository;
    private final TargetConnectionService connectionService;
    private final WriteTaskGroupPreviewService previewService;
    private final WriteTaskExecutionPreparationService executionPreparationService;
    private final WriteTaskDeliveryWriterRegistry writerRegistry;
    private final WriteTaskDefinitionNormalizationService definitionNormalizationService;
    private final ObjectMapper objectMapper;

    public WriteTaskGroupService(
            WriteTaskGroupRepository groupRepository,
            WriteTaskRepository taskRepository,
            WriteTaskRelationRepository relationRepository,
            WriteTaskGroupExecutionRepository executionRepository,
            WriteTaskGroupTableExecutionRepository tableExecutionRepository,
            TargetConnectionService connectionService,
            WriteTaskGroupPreviewService previewService,
            WriteTaskExecutionPreparationService executionPreparationService,
            WriteTaskDeliveryWriterRegistry writerRegistry,
            WriteTaskDefinitionNormalizationService definitionNormalizationService,
            ObjectMapper objectMapper
    ) {
        this.groupRepository = groupRepository;
        this.taskRepository = taskRepository;
        this.relationRepository = relationRepository;
        this.executionRepository = executionRepository;
        this.tableExecutionRepository = tableExecutionRepository;
        this.connectionService = connectionService;
        this.previewService = previewService;
        this.executionPreparationService = executionPreparationService;
        this.writerRegistry = writerRegistry;
        this.definitionNormalizationService = definitionNormalizationService;
        this.objectMapper = objectMapper;
    }

    public List<WriteTaskGroupResponse> findAll() {
        return groupRepository.findAll(Sort.by(Sort.Direction.DESC, "updatedAt")).stream()
                .map(this::toResponse)
                .toList();
    }

    public WriteTaskGroupResponse findResponseById(Long id) {
        return toResponse(findById(id));
    }

    public WriteTaskGroup findById(Long id) {
        return groupRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("\u672a\u627e\u5230\u5173\u7cfb\u4efb\u52a1\u7ec4: " + id));
    }

    @Transactional
    public WriteTaskGroupResponse create(WriteTaskGroupUpsertRequest request) {
        TargetConnection connection = loadConnection(request.connectionId());
        WriteTaskGroup group = new WriteTaskGroup();
        apply(group, request, connection);
        return toResponse(groupRepository.save(group));
    }

    @Transactional
    public WriteTaskGroupResponse update(Long id, WriteTaskGroupUpsertRequest request) {
        TargetConnection connection = loadConnection(request.connectionId());
        WriteTaskGroup group = findById(id);
        apply(group, request, connection);
        return toResponse(groupRepository.save(group));
    }

    @Transactional
    public void delete(Long id) {
        WriteTaskGroup group = findById(id);
        relationRepository.deleteAll(relationRepository.findByGroupIdOrderBySortOrderAscIdAsc(id));
        for (WriteTask task : taskRepository.findByGroupIdOrderByIdAsc(id)) {
            taskRepository.delete(task);
        }
        groupRepository.delete(group);
    }

    public WriteTaskGroupPreviewResponse preview(WriteTaskGroupPreviewRequest request) {
        TargetConnection connection = loadConnection(request.group().connectionId());
        return previewService.preview(request.group(), null, connection, request.previewCount(), request.seed());
    }

    public WriteTaskGroupPreviewResponse previewExisting(Long id, Integer previewCount, Long seed) {
        WriteTaskGroup group = findById(id);
        List<WriteTask> tasks = taskRepository.findByGroupIdOrderByIdAsc(id);
        TargetConnection connection = loadConnection(group.getConnectionId());
        return previewService.preview(toRequest(group, tasks, relationRepository.findByGroupIdOrderBySortOrderAscIdAsc(id)), tasks, connection, previewCount, seed);
    }

    @Transactional
    public WriteTaskGroupExecutionResponse run(Long id) {
        return runInternal(findById(id), WriteExecutionTriggerType.MANUAL);
    }

    @Transactional
    public WriteTaskGroupExecutionResponse runScheduled(Long id, WriteExecutionTriggerType triggerType) {
        if (triggerType != WriteExecutionTriggerType.SCHEDULED
                && triggerType != WriteExecutionTriggerType.CONTINUOUS) {
            throw new IllegalArgumentException("\u4e0d\u652f\u6301\u7684\u8c03\u5ea6\u89e6\u53d1\u7c7b\u578b: " + triggerType);
        }
        return runInternal(findById(id), triggerType);
    }

    @Transactional
    public WriteTaskGroupExecutionResponse runInternal(WriteTaskGroup group, WriteExecutionTriggerType triggerType) {
        if (executionRepository.existsByWriteTaskGroupIdAndStatus(group.getId(), WriteExecutionStatus.RUNNING)) {
            throw new IllegalArgumentException("\u5f53\u524d\u5173\u7cfb\u4efb\u52a1\u7ec4\u4ecd\u5728\u6267\u884c\u4e2d\uff0c\u8bf7\u7b49\u5f85\u4e0a\u4e00\u8f6e\u5b8c\u6210\u540e\u518d\u8bd5");
        }

        Instant startedAt = Instant.now();
        group.setLastTriggeredAt(startedAt);
        groupRepository.save(group);

        TargetConnection connection = loadConnection(group.getConnectionId());
        List<WriteTask> tasks = taskRepository.findByGroupIdOrderByIdAsc(group.getId());
        List<WriteTaskRelation> relations = relationRepository.findByGroupIdOrderBySortOrderAscIdAsc(group.getId());
        WriteTaskGroupUpsertRequest request = prepareForExecution(toRequest(group, tasks, relations), tasks, connection);
        List<WriteTask> runtimeTasks = request.tasks() == null
                ? List.of()
                : request.tasks().stream().map(task -> toRuntimeTask(task, request.connectionId())).toList();

        WriteTaskGroupGenerationResult generated = previewService.generate(request, runtimeTasks, connection, group.getSeed());
        WriteTaskDeliveryWriter writer = writerRegistry.get(connection.getDbType());
        WriteTaskGroupExecution execution = new WriteTaskGroupExecution();
        execution.setWriteTaskGroupId(group.getId());
        execution.setTriggerType(triggerType);
        execution.setStatus(WriteExecutionStatus.RUNNING);
        execution.setStartedAt(startedAt);
        execution.setPlannedTableCount(generated.tables().size());
        execution = executionRepository.save(execution);

        List<WriteTaskGroupTableExecution> tableExecutions = new ArrayList<>();
        long totalInserted = 0L;
        try {
            for (WriteTaskTableGenerationResult table : generated.tables()) {
                WriteTaskGroupTableExecution tableExecution = new WriteTaskGroupTableExecution();
                tableExecution.setWriteTaskGroupExecutionId(execution.getId());
                tableExecution.setWriteTaskId(table.task().getId());
                tableExecution.setTableName(table.task().getTableName());
                tableExecution.setStatus(WriteExecutionStatus.RUNNING);
                tableExecution = tableExecutionRepository.save(tableExecution);

                try {
                    validateGeneratedTable(table);
                    WriteTaskDeliveryResult result = writer.write(table.task(), connection, table.rows(), execution.getId());
                    long insertedCount = result.successCount();
                    WriteExecutionStatus tableStatus = result.errorCount() > 0
                            ? (result.successCount() > 0 ? WriteExecutionStatus.PARTIAL_SUCCESS : WriteExecutionStatus.FAILED)
                            : WriteExecutionStatus.SUCCESS;
                    tableExecution.setStatus(tableStatus);
                    tableExecution.setInsertedCount(insertedCount);
                    tableExecution.setNullViolationCount((long) table.nullViolationCount());
                    tableExecution.setBlankStringCount((long) table.blankStringCount());
                    tableExecution.setFkMissCount((long) table.foreignKeyMissCount());
                    tableExecution.setPkDuplicateCount(table.primaryKeyDuplicateCount());
                    tableExecution.setBeforeWriteRowCount(readLong(result.details().get("beforeWriteRowCount")));
                    tableExecution.setAfterWriteRowCount(readLong(result.details().get("afterWriteRowCount")));
                    tableExecution.setSummaryJson(writeJson(result.details()));
                    totalInserted += insertedCount;
                } catch (Exception exception) {
                    tableExecution.setStatus(WriteExecutionStatus.FAILED);
                    tableExecution.setInsertedCount(0L);
                    tableExecution.setNullViolationCount((long) table.nullViolationCount());
                    tableExecution.setBlankStringCount((long) table.blankStringCount());
                    tableExecution.setFkMissCount((long) table.foreignKeyMissCount());
                    tableExecution.setPkDuplicateCount(table.primaryKeyDuplicateCount());
                    tableExecution.setErrorSummary(exception.getMessage());
                    tableExecution.setSummaryJson(writeJson(Map.of(
                            "tableName", table.task().getTableName(),
                            "error", exception.getMessage()
                    )));
                    tableExecutionRepository.save(tableExecution);
                    tableExecutions.add(tableExecution);
                    throw exception;
                }
                tableExecutionRepository.save(tableExecution);
                tableExecutions.add(tableExecution);
            }

            int successTables = (int) tableExecutions.stream()
                    .filter(item -> item.getStatus() == WriteExecutionStatus.SUCCESS)
                    .count();
            int failureTables = (int) tableExecutions.stream()
                    .filter(item -> item.getStatus() != WriteExecutionStatus.SUCCESS)
                    .count();
            execution.setStatus(failureTables > 0 ? WriteExecutionStatus.PARTIAL_SUCCESS : WriteExecutionStatus.SUCCESS);
            execution.setCompletedTableCount(generated.tables().size());
            execution.setSuccessTableCount(successTables);
            execution.setFailureTableCount(failureTables);
            execution.setInsertedRowCount(totalInserted);
            execution.setFinishedAt(Instant.now());
            execution.setSummaryJson(writeJson(Map.of(
                    "groupName", group.getName(),
                    "triggerType", triggerType.name(),
                    "deliveryType", connection.getDbType().name(),
                    "seed", generated.seed(),
                    "insertedRowCount", totalInserted
            )));
        } catch (Exception exception) {
            execution.setStatus(WriteExecutionStatus.FAILED);
            execution.setCompletedTableCount(tableExecutions.size());
            execution.setSuccessTableCount((int) tableExecutions.stream().filter(item -> item.getStatus() == WriteExecutionStatus.SUCCESS).count());
            execution.setFailureTableCount((int) tableExecutions.stream().filter(item -> item.getStatus() == WriteExecutionStatus.FAILED).count());
            execution.setInsertedRowCount(totalInserted);
            execution.setFinishedAt(Instant.now());
            execution.setErrorSummary(exception.getMessage());
            execution.setSummaryJson(writeJson(Map.of(
                    "groupName", group.getName(),
                    "triggerType", triggerType.name(),
                    "deliveryType", connection.getDbType().name(),
                    "seed", generated.seed(),
                    "insertedRowCount", totalInserted,
                    "error", exception.getMessage()
            )));
        }

        execution = executionRepository.save(execution);
        List<WriteTaskGroupTableExecutionResponse> tableResponses = tableExecutionRepository
                .findByWriteTaskGroupExecutionIdOrderByIdAsc(execution.getId()).stream()
                .map(item -> WriteTaskGroupTableExecutionResponse.from(item, objectMapper))
                .toList();
        return WriteTaskGroupExecutionResponse.from(execution, tableResponses);
    }

    public List<WriteTaskGroupExecutionResponse> findExecutions(Long groupId) {
        return executionRepository.findByWriteTaskGroupIdOrderByStartedAtDesc(groupId).stream()
                .map(this::toExecutionResponse)
                .toList();
    }

    public WriteTaskGroupExecutionResponse findExecutionById(Long id) {
        WriteTaskGroupExecution execution = executionRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("\u672a\u627e\u5230\u5173\u7cfb\u4efb\u52a1\u7ec4\u6267\u884c\u8bb0\u5f55: " + id));
        return toExecutionResponse(execution);
    }

    private void apply(WriteTaskGroup group, WriteTaskGroupUpsertRequest request, TargetConnection connection) {
        if (request.tasks() == null || request.tasks().isEmpty()) {
            throw new IllegalArgumentException("Relationship task group must contain at least one task");
        }
        validateSchedule(request);

        group.setName(request.name().trim());
        group.setConnectionId(request.connectionId());
        group.setDescription(normalizeText(request.description()));
        group.setSeed(request.seed());
        group.setStatus(request.status());
        group.setScheduleType(request.scheduleType());
        group.setCronExpression(request.scheduleType() == WriteTaskScheduleType.CRON ? normalizeText(request.cronExpression()) : null);
        group.setTriggerAt(request.scheduleType() == WriteTaskScheduleType.ONCE ? request.triggerAt() : null);
        group.setIntervalSeconds(request.scheduleType() == WriteTaskScheduleType.INTERVAL ? request.intervalSeconds() : null);
        group.setMaxRuns(request.scheduleType() == WriteTaskScheduleType.INTERVAL ? request.maxRuns() : null);
        group.setMaxRowsTotal(request.scheduleType() == WriteTaskScheduleType.INTERVAL ? request.maxRowsTotal() : null);
        WriteTaskGroup savedGroup = groupRepository.save(group);

        Map<String, WriteTask> existingTasksByKey = taskRepository.findByGroupIdOrderByIdAsc(savedGroup.getId()).stream()
                .filter(task -> task.getTaskKey() != null)
                .collect(Collectors.toMap(WriteTask::getTaskKey, Function.identity(), (left, right) -> left, LinkedHashMap::new));

        Map<String, WriteTask> savedTasksByKey = new LinkedHashMap<>();
        for (WriteTaskGroupTaskUpsertRequest taskRequest : request.tasks()) {
            WriteTask task = existingTasksByKey.remove(taskRequest.taskKey());
            if (task == null) {
                task = new WriteTask();
            }
            applyTask(task, savedGroup, taskRequest, connection.getDbType());
            savedTasksByKey.put(taskRequest.taskKey(), taskRepository.save(task));
        }

        relationRepository.deleteAll(relationRepository.findByGroupIdOrderBySortOrderAscIdAsc(savedGroup.getId()));
        if (request.relations() != null) {
            for (WriteTaskGroupRelationUpsertRequest relationRequest : request.relations()) {
                WriteTask parentTask = Optional.ofNullable(savedTasksByKey.get(relationRequest.parentTaskKey()))
                        .orElseThrow(() -> new IllegalArgumentException("\u7236\u4efb\u52a1\u4e0d\u5b58\u5728: " + relationRequest.parentTaskKey()));
                WriteTask childTask = Optional.ofNullable(savedTasksByKey.get(relationRequest.childTaskKey()))
                        .orElseThrow(() -> new IllegalArgumentException("\u5b50\u4efb\u52a1\u4e0d\u5b58\u5728: " + relationRequest.childTaskKey()));
                WriteTaskRelation relation = new WriteTaskRelation();
                relation.setGroupId(savedGroup.getId());
                relation.setRelationName(relationRequest.relationName().trim());
                relation.setParentTaskId(parentTask.getId());
                relation.setChildTaskId(childTask.getId());
                relation.setRelationMode(relationRequest.relationMode());
                relation.setRelationType(relationRequest.relationType());
                relation.setSourceMode(relationRequest.sourceMode());
                relation.setSelectionStrategy(relationRequest.selectionStrategy());
                relation.setReusePolicy(relationRequest.reusePolicy());
                relation.setParentColumnsJson(writeJsonList(relationRequest.parentColumns()));
                relation.setChildColumnsJson(writeJsonList(relationRequest.childColumns()));
                relation.setNullRate(toBigDecimal(relationRequest.nullRate() == null ? 0D : relationRequest.nullRate()));
                relation.setMixedExistingRatio(toBigDecimal(relationRequest.mixedExistingRatio()));
                relation.setMinChildrenPerParent(relationRequest.minChildrenPerParent());
                relation.setMaxChildrenPerParent(relationRequest.maxChildrenPerParent());
                relation.setMappingConfigJson(normalizeText(relationRequest.mappingConfigJson()));
                relation.setSortOrder(relationRequest.sortOrder());
                validateRelationRequest(connection.getDbType(), relationRequest);
                relationRepository.save(relation);
            }
        }

        for (WriteTask obsoleteTask : existingTasksByKey.values()) {
            taskRepository.delete(obsoleteTask);
        }
    }

    private void validateSchedule(WriteTaskGroupUpsertRequest request) {
        switch (request.scheduleType()) {
            case MANUAL -> {
                if (request.status() == WriteTaskStatus.PAUSED || request.status() == WriteTaskStatus.RUNNING) {
                    throw new IllegalArgumentException("\u624b\u52a8\u5173\u7cfb\u4efb\u52a1\u7ec4\u72b6\u6001\u53ea\u80fd\u662f\u8349\u7a3f\u3001\u5c31\u7eea\u6216\u5df2\u7981\u7528");
                }
            }
            case ONCE -> {
                if (request.triggerAt() == null) {
                    throw new IllegalArgumentException("\u5355\u6b21\u5173\u7cfb\u4efb\u52a1\u7ec4\u5fc5\u987b\u8bbe\u7f6e\u6267\u884c\u65f6\u95f4");
                }
                if (request.status() == WriteTaskStatus.RUNNING) {
                    throw new IllegalArgumentException("\u5355\u6b21\u5173\u7cfb\u4efb\u52a1\u7ec4\u4fdd\u5b58\u65f6\u4e0d\u80fd\u5904\u4e8e\u8fd0\u884c\u4e2d");
                }
            }
            case CRON -> {
                if (request.cronExpression() == null || request.cronExpression().isBlank()) {
                    throw new IllegalArgumentException("\u5468\u671f\u5173\u7cfb\u4efb\u52a1\u7ec4\u5fc5\u987b\u586b\u5199 cronExpression");
                }
                if (request.status() == WriteTaskStatus.RUNNING) {
                    throw new IllegalArgumentException("\u5468\u671f\u5173\u7cfb\u4efb\u52a1\u7ec4\u4fdd\u5b58\u65f6\u4e0d\u80fd\u5904\u4e8e\u8fd0\u884c\u4e2d");
                }
            }
            case INTERVAL -> {
                if (request.intervalSeconds() == null || request.intervalSeconds() < 1) {
                    throw new IllegalArgumentException("\u6301\u7eed\u5199\u5165\u5173\u7cfb\u4efb\u52a1\u7ec4\u5fc5\u987b\u8bbe\u7f6e intervalSeconds");
                }
            }
        }
    }

    private void applyTask(WriteTask task, WriteTaskGroup group, WriteTaskGroupTaskUpsertRequest request, DatabaseType databaseType) {
        WriteTaskUpsertRequest normalizedRequest = toWriteTaskRequest(request, group.getConnectionId());
        WriteTaskDefinitionNormalizationService.NormalizedWriteTaskDefinition normalizedDefinition =
                definitionNormalizationService.normalize(databaseType, normalizedRequest);
        task.setGroupId(group.getId());
        task.setTaskKey(request.taskKey().trim());
        task.setName(request.name().trim());
        task.setConnectionId(group.getConnectionId());
        task.setTableName(request.tableName().trim());
        task.setTableMode(request.tableMode());
        task.setWriteMode(request.writeMode());
        task.setRowCount(request.rowPlan().rowCount() == null ? 1 : request.rowPlan().rowCount());
        task.setBatchSize(request.batchSize());
        task.setSeed(request.seed());
        task.setStatus(request.status());
        task.setScheduleType(WriteTaskScheduleType.MANUAL);
        task.setCronExpression(null);
        task.setTriggerAt(null);
        task.setIntervalSeconds(null);
        task.setMaxRuns(null);
        task.setMaxRowsTotal(null);
        task.setDescription(normalizeText(request.description()));
        task.setTargetConfigJson(normalizedDefinition.targetConfigJson());
        task.setPayloadSchemaJson(normalizedDefinition.payloadSchemaJson());
        Map<String, Object> rowPlan = new LinkedHashMap<>();
        rowPlan.put("mode", request.rowPlan().mode().name());
        rowPlan.put("rowCount", request.rowPlan().rowCount());
        rowPlan.put("driverTaskKey", request.rowPlan().driverTaskKey());
        rowPlan.put("minChildrenPerParent", request.rowPlan().minChildrenPerParent());
        rowPlan.put("maxChildrenPerParent", request.rowPlan().maxChildrenPerParent());
        task.setRowPlanJson(writeJson(rowPlan));
        Map<String, Boolean> foreignKeyFlags = request.columns() == null
                ? Map.of()
                : request.columns().stream()
                        .collect(Collectors.toMap(
                                WriteTaskGroupTaskColumnUpsertRequest::columnName,
                                WriteTaskGroupTaskColumnUpsertRequest::foreignKeyFlag,
                                (left, right) -> left,
                                LinkedHashMap::new
                        ));
        task.replaceColumns(normalizedDefinition.columns().stream()
                .map(column -> toEntity(column, foreignKeyFlags.getOrDefault(column.columnName(), false)))
                .toList());
    }

    private BigDecimal toBigDecimal(Double value) {
        if (value == null) {
            return null;
        }
        return BigDecimal.valueOf(value);
    }

    private Double toDouble(BigDecimal value) {
        if (value == null) {
            return null;
        }
        return value.doubleValue();
    }

    private double toPrimitiveDouble(BigDecimal value) {
        if (value == null) {
            return 0D;
        }
        return value.doubleValue();
    }

    private WriteTaskColumn toEntity(WriteTaskGroupTaskColumnUpsertRequest request) {
        return toEntity(toWriteTaskColumnRequest(request), request.foreignKeyFlag());
    }

    private WriteTaskColumn toEntity(WriteTaskColumnUpsertRequest request, boolean foreignKeyFlag) {
        WriteTaskColumn column = new WriteTaskColumn();
        column.setColumnName(request.columnName().trim());
        column.setDbType(request.dbType() == null ? null : request.dbType().trim().toUpperCase(Locale.ROOT));
        column.setLengthValue(request.lengthValue());
        column.setPrecisionValue(request.precisionValue());
        column.setScaleValue(request.scaleValue());
        column.setNullableFlag(request.nullableFlag());
        column.setPrimaryKeyFlag(request.primaryKeyFlag());
        column.setForeignKeyFlag(foreignKeyFlag);
        column.setGeneratorType(request.generatorType());
        column.setGeneratorConfigJson(writeJson(request.generatorConfig() == null ? Map.of() : request.generatorConfig()));
        column.setSortOrder(request.sortOrder());
        return column;
    }

    private WriteTaskGroupUpsertRequest toRequest(
            WriteTaskGroup group,
            List<WriteTask> tasks,
            List<WriteTaskRelation> relations
    ) {
        Map<Long, String> taskKeysById = tasks.stream()
                .collect(Collectors.toMap(WriteTask::getId, WriteTask::getTaskKey));
        return new WriteTaskGroupUpsertRequest(
                group.getName(),
                group.getConnectionId(),
                group.getDescription(),
                group.getSeed(),
                group.getStatus(),
                group.getScheduleType(),
                group.getCronExpression(),
                group.getTriggerAt(),
                group.getIntervalSeconds(),
                group.getMaxRuns(),
                group.getMaxRowsTotal(),
                tasks.stream().map(this::toTaskRequest).toList(),
                relations.stream().map(relation -> toRelationRequest(relation, taskKeysById)).toList()
        );
    }

    private WriteTaskGroupUpsertRequest prepareForExecution(
            WriteTaskGroupUpsertRequest request,
            List<WriteTask> tasks,
            TargetConnection connection
    ) {
        if (request.tasks() == null || request.tasks().isEmpty()) {
            return request;
        }

        Map<String, WriteTask> taskByKey = tasks.stream()
                .filter(task -> task.getTaskKey() != null)
                .collect(Collectors.toMap(WriteTask::getTaskKey, Function.identity(), (left, right) -> left, LinkedHashMap::new));

        List<WriteTaskGroupTaskUpsertRequest> adjustedTasks = request.tasks().stream()
                .map(taskRequest -> {
                    WriteTask persistedTask = taskByKey.get(taskRequest.taskKey());
                    if (persistedTask == null) {
                        return taskRequest;
                    }
                    WriteTaskUpsertRequest runtimeTaskRequest = executionPreparationService.prepareForExecution(
                            persistedTask,
                            toWriteTaskRequest(taskRequest, request.connectionId()),
                            connection
                    );
                    return mergeTaskRequest(taskRequest, runtimeTaskRequest);
                })
                .toList();

        return new WriteTaskGroupUpsertRequest(
                request.name(),
                request.connectionId(),
                request.description(),
                request.seed(),
                request.status(),
                request.scheduleType(),
                request.cronExpression(),
                request.triggerAt(),
                request.intervalSeconds(),
                request.maxRuns(),
                request.maxRowsTotal(),
                adjustedTasks,
                request.relations()
        );
    }

    private WriteTaskGroupTaskUpsertRequest toTaskRequest(WriteTask task) {
        Map<String, Object> rowPlan = JsonConfigSupport.readConfig(task.getRowPlanJson(), "rowPlanJson");
        return new WriteTaskGroupTaskUpsertRequest(
                task.getId(),
                task.getTaskKey(),
                task.getName(),
                task.getTableName(),
                task.getTableMode(),
                task.getWriteMode(),
                task.getBatchSize(),
                task.getSeed(),
                task.getDescription(),
                task.getStatus(),
                new WriteTaskGroupRowPlanRequest(
                        rowPlan.containsKey("mode") ? WriteTaskGroupRowPlanMode.valueOf(String.valueOf(rowPlan.get("mode"))) : WriteTaskGroupRowPlanMode.FIXED,
                        JsonConfigSupport.optionalInteger(rowPlan, "rowCount"),
                        JsonConfigSupport.optionalString(rowPlan, "driverTaskKey"),
                        JsonConfigSupport.optionalInteger(rowPlan, "minChildrenPerParent"),
                        JsonConfigSupport.optionalInteger(rowPlan, "maxChildrenPerParent")
                ),
                task.getTargetConfigJson(),
                task.getPayloadSchemaJson(),
                task.getColumns().stream().map(this::toColumnRequest).toList()
        );
    }

    private WriteTaskUpsertRequest toWriteTaskRequest(WriteTaskGroupTaskUpsertRequest task, Long connectionId) {
        return new WriteTaskUpsertRequest(
                task.name(),
                connectionId,
                task.tableName(),
                task.tableMode(),
                task.writeMode(),
                task.rowPlan().rowCount() == null ? 1 : task.rowPlan().rowCount(),
                task.batchSize(),
                task.seed(),
                task.status(),
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                task.description(),
                task.targetConfigJson(),
                task.payloadSchemaJson(),
                task.columns() == null ? List.of() : task.columns().stream().map(this::toWriteTaskColumnRequest).toList()
        );
    }

    private WriteTaskColumnUpsertRequest toWriteTaskColumnRequest(WriteTaskGroupTaskColumnUpsertRequest column) {
        return new WriteTaskColumnUpsertRequest(
                column.columnName(),
                column.dbType(),
                column.lengthValue(),
                column.precisionValue(),
                column.scaleValue(),
                column.nullableFlag(),
                column.primaryKeyFlag(),
                column.generatorType(),
                column.generatorConfig(),
                column.sortOrder()
        );
    }

    private WriteTaskGroupTaskColumnUpsertRequest toColumnRequest(WriteTaskColumn column) {
        return new WriteTaskGroupTaskColumnUpsertRequest(
                column.getColumnName(),
                column.getDbType(),
                column.getLengthValue(),
                column.getPrecisionValue(),
                column.getScaleValue(),
                column.isNullableFlag(),
                column.isPrimaryKeyFlag(),
                column.isForeignKeyFlag(),
                column.getGeneratorType(),
                JsonConfigSupport.readConfig(column.getGeneratorConfigJson(), "generatorConfigJson"),
                column.getSortOrder()
        );
    }

    private WriteTaskGroupTaskUpsertRequest mergeTaskRequest(
            WriteTaskGroupTaskUpsertRequest original,
            WriteTaskUpsertRequest runtime
    ) {
        Map<String, WriteTaskColumnUpsertRequest> runtimeColumnsByName = runtime.columns() == null
                ? Map.of()
                : runtime.columns().stream()
                        .collect(Collectors.toMap(
                                WriteTaskColumnUpsertRequest::columnName,
                                Function.identity(),
                                (left, right) -> left,
                                LinkedHashMap::new
                        ));
        List<WriteTaskGroupTaskColumnUpsertRequest> adjustedColumns = original.columns() == null
                ? List.of()
                : original.columns().stream()
                        .map(column -> mergeColumnRequest(column, runtimeColumnsByName.get(column.columnName())))
                        .toList();

        return new WriteTaskGroupTaskUpsertRequest(
                original.id(),
                original.taskKey(),
                original.name(),
                original.tableName(),
                original.tableMode(),
                original.writeMode(),
                original.batchSize(),
                original.seed(),
                original.description(),
                original.status(),
                original.rowPlan(),
                runtime.targetConfigJson(),
                runtime.payloadSchemaJson(),
                adjustedColumns
        );
    }

    private WriteTaskGroupTaskColumnUpsertRequest mergeColumnRequest(
            WriteTaskGroupTaskColumnUpsertRequest original,
            WriteTaskColumnUpsertRequest runtime
    ) {
        if (runtime == null) {
            return original;
        }
        return new WriteTaskGroupTaskColumnUpsertRequest(
                original.columnName(),
                original.dbType(),
                original.lengthValue(),
                original.precisionValue(),
                original.scaleValue(),
                original.nullableFlag(),
                original.primaryKeyFlag(),
                original.foreignKeyFlag(),
                runtime.generatorType(),
                runtime.generatorConfig(),
                original.sortOrder()
        );
    }

    private WriteTask toRuntimeTask(WriteTaskGroupTaskUpsertRequest request, Long connectionId) {
        WriteTask task = new WriteTask();
        task.setId(request.id());
        task.setTaskKey(request.taskKey());
        task.setName(request.name());
        task.setConnectionId(connectionId);
        task.setTableName(request.tableName());
        task.setTableMode(request.tableMode());
        task.setWriteMode(request.writeMode());
        task.setRowCount(request.rowPlan().rowCount() == null ? 1 : request.rowPlan().rowCount());
        task.setBatchSize(request.batchSize());
        task.setSeed(request.seed());
        task.setStatus(request.status());
        task.setScheduleType(WriteTaskScheduleType.MANUAL);
        task.setDescription(request.description());
        task.setTargetConfigJson(request.targetConfigJson());
        task.setPayloadSchemaJson(request.payloadSchemaJson());
        task.replaceColumns(request.columns() == null ? List.of() : request.columns().stream().map(this::toEntity).toList());
        return task;
    }

    private WriteTaskGroupRelationUpsertRequest toRelationRequest(
            WriteTaskRelation relation,
            Map<Long, String> taskKeysById
    ) {
        return new WriteTaskGroupRelationUpsertRequest(
                relation.getId(),
                relation.getRelationName(),
                taskKeysById.get(relation.getParentTaskId()),
                taskKeysById.get(relation.getChildTaskId()),
                relation.getRelationMode(),
                relation.getRelationType(),
                relation.getSourceMode(),
                relation.getSelectionStrategy(),
                relation.getReusePolicy(),
                readJsonList(relation.getParentColumnsJson()),
                readJsonList(relation.getChildColumnsJson()),
                toPrimitiveDouble(relation.getNullRate()),
                toDouble(relation.getMixedExistingRatio()),
                relation.getMinChildrenPerParent(),
                relation.getMaxChildrenPerParent(),
                relation.getMappingConfigJson(),
                relation.getSortOrder()
        );
    }

    private void validateGeneratedTable(WriteTaskTableGenerationResult table) {
        if (table.foreignKeyMissCount() > 0) {
            throw new IllegalArgumentException("\u8868 " + table.task().getTableName() + " \u5b58\u5728\u672a\u547d\u4e2d\u7684\u5916\u952e\u5f15\u7528");
        }
        if (table.nullViolationCount() > 0 || table.blankStringCount() > 0) {
            throw new IllegalArgumentException("\u8868 " + table.task().getTableName() + " \u5b58\u5728\u975e\u7a7a\u5b57\u6bb5\u6821\u9a8c\u5931\u8d25");
        }
        if (table.primaryKeyDuplicateCount() > 0) {
            throw new IllegalArgumentException("\u8868 " + table.task().getTableName() + " \u5b58\u5728\u4e3b\u952e\u91cd\u590d");
        }
    }

    private TargetConnection loadConnection(Long connectionId) {
        return connectionService.findById(connectionId);
    }

    private WriteTaskGroupResponse toResponse(WriteTaskGroup group) {
        List<WriteTask> tasks = taskRepository.findByGroupIdOrderByIdAsc(group.getId());
        Map<Long, String> taskKeysById = tasks.stream()
                .collect(Collectors.toMap(WriteTask::getId, WriteTask::getTaskKey));
        List<WriteTaskGroupTaskResponse> taskResponses = tasks.stream()
                .map(task -> WriteTaskGroupTaskResponse.from(task, objectMapper))
                .toList();
        List<WriteTaskGroupRelationResponse> relationResponses = relationRepository.findByGroupIdOrderBySortOrderAscIdAsc(group.getId()).stream()
                .map(relation -> WriteTaskGroupRelationResponse.from(relation, taskKeysById, objectMapper))
                .toList();
        return WriteTaskGroupResponse.from(group, taskResponses, relationResponses);
    }

    private WriteTaskGroupExecutionResponse toExecutionResponse(WriteTaskGroupExecution execution) {
        List<WriteTaskGroupTableExecutionResponse> tableResponses = tableExecutionRepository
                .findByWriteTaskGroupExecutionIdOrderByIdAsc(execution.getId()).stream()
                .map(item -> WriteTaskGroupTableExecutionResponse.from(item, objectMapper))
                .toList();
        return WriteTaskGroupExecutionResponse.from(execution, tableResponses);
    }

    private List<String> readJsonList(String json) {
        try {
            if (json == null || json.isBlank()) {
                return List.of();
            }
            return objectMapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException("\u5173\u7cfb\u5b57\u6bb5\u914d\u7f6e\u89e3\u6790\u5931\u8d25: " + exception.getMessage(), exception);
        }
    }

    private String writeJson(Map<String, ?> value) {
        return JsonConfigSupport.writeJson(value, "writeTaskGroup");
    }

    private String writeJsonList(List<String> value) {
        try {
            return objectMapper.writeValueAsString(value == null ? List.of() : value);
        } catch (Exception exception) {
            throw new IllegalArgumentException("\u5173\u7cfb\u5b57\u6bb5\u914d\u7f6e\u5e8f\u5217\u5316\u5931\u8d25: " + exception.getMessage(), exception);
        }
    }

    private Long readLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    private String normalizeText(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private void validateRelationRequest(DatabaseType databaseType, WriteTaskGroupRelationUpsertRequest relationRequest) {
        if (databaseType == DatabaseType.KAFKA) {
            if (relationRequest.relationMode() != WriteTaskRelationMode.KAFKA_EVENT) {
                throw new IllegalArgumentException("Kafka relationship tasks only support KAFKA_EVENT relation mode");
            }
            if (relationRequest.mappingConfigJson() == null || relationRequest.mappingConfigJson().isBlank()) {
                throw new IllegalArgumentException("Kafka relationship requires mappingConfigJson");
            }
            if (relationRequest.sourceMode() != com.datagenerator.task.domain.ReferenceSourceMode.CURRENT_BATCH) {
                throw new IllegalArgumentException("Kafka relationship currently only supports CURRENT_BATCH");
            }
            return;
        }

        if (relationRequest.relationMode() != WriteTaskRelationMode.DATABASE_COLUMNS) {
            throw new IllegalArgumentException("Database relationship tasks only support DATABASE_COLUMNS relation mode");
        }
        if (relationRequest.parentColumns() == null || relationRequest.parentColumns().isEmpty()) {
            throw new IllegalArgumentException("Database relationship must contain parent columns");
        }
        if (relationRequest.childColumns() == null || relationRequest.childColumns().isEmpty()) {
            throw new IllegalArgumentException("Database relationship must contain child columns");
        }
        if (relationRequest.parentColumns().size() != relationRequest.childColumns().size()) {
            throw new IllegalArgumentException("Database relationship parent and child columns must have the same size");
        }
    }
}

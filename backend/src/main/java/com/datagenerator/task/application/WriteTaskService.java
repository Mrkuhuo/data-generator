package com.datagenerator.task.application;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.connection.application.TargetConnectionService;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskExecutionResponse;
import com.datagenerator.task.api.WriteTaskPreviewRequest;
import com.datagenerator.task.api.WriteTaskPreviewResponse;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.KafkaPayloadSchemaNode;
import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import com.datagenerator.task.domain.WriteLogLevel;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.datagenerator.task.domain.WriteTaskExecution;
import com.datagenerator.task.domain.WriteTaskExecutionLog;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.datagenerator.task.repository.WriteTaskExecutionLogRepository;
import com.datagenerator.task.repository.WriteTaskExecutionRepository;
import com.datagenerator.task.repository.WriteTaskRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class WriteTaskService {

    private final WriteTaskRepository repository;
    private final WriteTaskExecutionRepository executionRepository;
    private final WriteTaskExecutionLogRepository executionLogRepository;
    private final TargetConnectionService connectionService;
    private final WriteTaskPreviewService previewService;
    private final WriteTaskExecutionPreparationService executionPreparationService;
    private final WriteTaskDeliveryWriterRegistry writerRegistry;
    private final KafkaPayloadSchemaService payloadSchemaService;
    private final ObjectMapper objectMapper;

    public WriteTaskService(
            WriteTaskRepository repository,
            WriteTaskExecutionRepository executionRepository,
            WriteTaskExecutionLogRepository executionLogRepository,
            TargetConnectionService connectionService,
            WriteTaskPreviewService previewService,
            WriteTaskExecutionPreparationService executionPreparationService,
            WriteTaskDeliveryWriterRegistry writerRegistry,
            KafkaPayloadSchemaService payloadSchemaService,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.executionRepository = executionRepository;
        this.executionLogRepository = executionLogRepository;
        this.connectionService = connectionService;
        this.previewService = previewService;
        this.executionPreparationService = executionPreparationService;
        this.writerRegistry = writerRegistry;
        this.payloadSchemaService = payloadSchemaService;
        this.objectMapper = objectMapper;
    }

    public List<WriteTask> findAll() {
        return repository.findAll(Sort.by(Sort.Direction.DESC, "updatedAt"));
    }

    public WriteTask findById(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("未找到写入任务: " + id));
    }

    @Transactional
    public WriteTask create(WriteTaskUpsertRequest request) {
        WriteTask task = new WriteTask();
        apply(task, request);
        return repository.save(task);
    }

    @Transactional
    public WriteTask update(Long id, WriteTaskUpsertRequest request) {
        WriteTask task = findById(id);
        apply(task, request);
        return repository.save(task);
    }

    @Transactional
    public void delete(Long id) {
        WriteTask task = findById(id);
        List<WriteTaskExecution> executions = executionRepository.findByWriteTaskIdIn(List.of(id));
        if (!executions.isEmpty()) {
            List<Long> executionIds = executions.stream()
                    .map(WriteTaskExecution::getId)
                    .toList();
            List<WriteTaskExecutionLog> logs = executionLogRepository.findByWriteTaskExecutionIdIn(executionIds);
            if (!logs.isEmpty()) {
                executionLogRepository.deleteAllInBatch(logs);
            }
            executionRepository.deleteAllInBatch(executions);
        }
        repository.delete(task);
    }

    public WriteTaskPreviewResponse preview(WriteTaskPreviewRequest request) {
        if (request.task() == null) {
            throw new IllegalArgumentException("预览请求缺少任务配置");
        }
        return previewService.preview(request.task(), request.count(), request.seed());
    }

    public WriteTaskPreviewResponse previewExisting(Long id, Integer count, Long seed) {
        WriteTask task = findById(id);
        return previewService.preview(toUpsertRequest(task), count, seed);
    }

    public List<WriteTaskExecution> findExecutions() {
        return executionRepository.findAll(Sort.by(Sort.Direction.DESC, "startedAt"));
    }

    public List<WriteTaskExecution> findExecutionsByTaskId(Long taskId) {
        findById(taskId);
        return executionRepository.findByWriteTaskIdOrderByStartedAtDesc(taskId);
    }

    public WriteTaskExecution findExecutionById(Long id) {
        return executionRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("未找到写入执行记录: " + id));
    }

    public List<WriteTaskExecutionLog> findExecutionLogs(Long executionId) {
        return executionLogRepository.findOrderedByExecutionId(executionId);
    }

    @Transactional
    public WriteTaskExecutionResponse run(Long id) {
        return runInternal(findById(id), WriteExecutionTriggerType.MANUAL);
    }

    @Transactional
    public WriteTaskExecutionResponse runScheduled(Long id, WriteExecutionTriggerType triggerType) {
        if (triggerType != WriteExecutionTriggerType.SCHEDULED && triggerType != WriteExecutionTriggerType.CONTINUOUS) {
            throw new IllegalArgumentException("不支持的调度触发类型: " + triggerType);
        }
        return runInternal(findById(id), triggerType);
    }

    @Transactional
    public WriteTaskExecutionResponse runInternal(WriteTask task, WriteExecutionTriggerType triggerType) {
        if (executionRepository.existsByWriteTaskIdAndStatus(task.getId(), WriteExecutionStatus.RUNNING)) {
            throw new IllegalArgumentException("当前任务仍在执行中，请等待上一批写入完成");
        }

        task.setLastTriggeredAt(Instant.now());
        repository.save(task);

        WriteTaskExecution execution = new WriteTaskExecution();
        execution.setWriteTaskId(task.getId());
        execution.setTriggerType(triggerType);
        execution.setStatus(WriteExecutionStatus.RUNNING);
        execution.setStartedAt(Instant.now());
        WriteTaskExecution savedExecution = executionRepository.save(execution);

        Map<String, Object> deliveryDetails = new LinkedHashMap<>();
        deliveryDetails.put("tableName", task.getTableName());
        deliveryDetails.put("writeMode", task.getWriteMode());
        deliveryDetails.put("tableMode", task.getTableMode());
        deliveryDetails.put("plannedRowCount", task.getRowCount());

        log(savedExecution.getId(), WriteLogLevel.INFO, "开始执行写入任务", Map.of(
                "taskId", task.getId(),
                "tableName", task.getTableName(),
                "connectionId", task.getConnectionId(),
                "triggerType", triggerType.name()
        ));

        try {
            var connection = connectionService.findById(task.getConnectionId());
            deliveryDetails.put("targetType", connection.getDbType().name());

            WriteTaskUpsertRequest runtimeRequest = executionPreparationService.prepareForExecution(
                    task,
                    toUpsertRequest(task),
                    connection
            );
            logSequenceAdjustments(savedExecution.getId(), toUpsertRequest(task), runtimeRequest);

            WriteTaskPreviewResponse preview = previewService.preview(runtimeRequest, task.getRowCount(), task.getSeed());
            savedExecution.setGeneratedCount((long) preview.count());
            deliveryDetails.put("generatedCount", preview.count());
            deliveryDetails.put("seed", preview.seed());
            log(savedExecution.getId(), WriteLogLevel.INFO, "已生成模拟数据", Map.of(
                    "count", preview.count(),
                    "seed", preview.seed()
            ));

            RowValidationSummary validationSummary = validateRows(task, preview.rows());
            deliveryDetails.put("nonNullValidation", validationSummary.toMap());
            if (validationSummary.passed()) {
                log(savedExecution.getId(), WriteLogLevel.INFO, "非空字段校验通过", Map.of(
                        "requiredColumnCount", validationSummary.requiredColumnCount(),
                        "checkedRowCount", validationSummary.checkedRowCount()
                ));
            } else {
                savedExecution.setDeliveryDetailsJson(writeJson(deliveryDetails));
                log(savedExecution.getId(), WriteLogLevel.WARN, "非空字段校验未通过", Map.of(
                        "nullValueCount", validationSummary.nullValueCount(),
                        "blankStringCount", validationSummary.blankStringCount(),
                        "issueCount", validationSummary.issues().size()
                ));
                throw new IllegalArgumentException("非空字段校验未通过，请检查空值或空字符串字段");
            }

            WriteTaskDeliveryWriter writer = writerRegistry.get(connection.getDbType());
            WriteTaskDeliveryResult result = writer.write(task, connection, preview.rows(), savedExecution.getId());

            savedExecution.setStatus(result.errorCount() > 0 ? WriteExecutionStatus.PARTIAL_SUCCESS : WriteExecutionStatus.SUCCESS);
            savedExecution.setSuccessCount(result.successCount());
            savedExecution.setErrorCount(result.errorCount());
            savedExecution.setFinishedAt(Instant.now());
            deliveryDetails.putAll(result.details());
            savedExecution.setDeliveryDetailsJson(writeJson(deliveryDetails));

            LinkedHashMap<String, Object> logDetails = new LinkedHashMap<>(result.details());
            logDetails.putIfAbsent("tableName", task.getTableName());
            logDetails.putIfAbsent("successCount", result.successCount());
            log(savedExecution.getId(), WriteLogLevel.INFO, result.summary(), logDetails);
        } catch (Exception exception) {
            savedExecution.setStatus(WriteExecutionStatus.FAILED);
            savedExecution.setFinishedAt(Instant.now());
            savedExecution.setErrorCount(Math.max(savedExecution.getErrorCount(), 1));
            savedExecution.setErrorSummary(exception.getMessage());
            deliveryDetails.put("error", exception.getMessage());
            savedExecution.setDeliveryDetailsJson(writeJson(deliveryDetails));
            log(savedExecution.getId(), WriteLogLevel.ERROR, "写入任务执行失败", Map.of("error", exception.getMessage()));
        }

        return WriteTaskExecutionResponse.from(executionRepository.save(savedExecution));
    }

    public long count() {
        return repository.count();
    }

    private void apply(WriteTask task, WriteTaskUpsertRequest request) {
        var connection = connectionService.findById(request.connectionId());
        boolean kafkaComplexMode = isKafkaComplexMode(connection.getDbType(), request.payloadSchemaJson());
        validateSchedule(request);
        validateTaskShape(connection.getDbType(), request, kafkaComplexMode);
        String normalizedPayloadSchemaJson = normalizePayloadSchema(connection.getDbType(), request);
        String normalizedTargetConfigJson = normalizeTargetConfig(
                connection.getDbType(),
                request,
                normalizedPayloadSchemaJson
        );
        List<WriteTaskColumnUpsertRequest> columnsToStore = kafkaComplexMode
                ? List.of()
                : normalizeColumns(connection.getDbType(), safeColumns(request));

        task.setName(request.name().trim());
        task.setConnectionId(request.connectionId());
        task.setTableName(request.tableName().trim());
        task.setTableMode(request.tableMode());
        task.setWriteMode(request.writeMode());
        task.setRowCount(request.rowCount());
        task.setBatchSize(request.batchSize());
        task.setSeed(request.seed());
        task.setStatus(request.status());
        task.setScheduleType(request.scheduleType());
        task.setCronExpression(request.scheduleType() == WriteTaskScheduleType.CRON ? normalizeText(request.cronExpression()) : null);
        task.setTriggerAt(request.scheduleType() == WriteTaskScheduleType.ONCE ? request.triggerAt() : null);
        task.setIntervalSeconds(request.scheduleType() == WriteTaskScheduleType.INTERVAL ? request.intervalSeconds() : null);
        task.setMaxRuns(request.scheduleType() == WriteTaskScheduleType.INTERVAL ? request.maxRuns() : null);
        task.setMaxRowsTotal(request.scheduleType() == WriteTaskScheduleType.INTERVAL ? request.maxRowsTotal() : null);
        task.setDescription(normalizeText(request.description()));
        task.setTargetConfigJson(normalizedTargetConfigJson);
        task.setPayloadSchemaJson(normalizedPayloadSchemaJson);
        task.replaceColumns(columnsToStore.stream().map(this::toEntity).toList());
    }

    private void validateTaskShape(
            DatabaseType databaseType,
            WriteTaskUpsertRequest request,
            boolean kafkaComplexMode
    ) {
        if (databaseType != DatabaseType.KAFKA) {
            if (request.payloadSchemaJson() != null && !request.payloadSchemaJson().isBlank()) {
                throw new IllegalArgumentException("仅 Kafka 目标支持 payloadSchemaJson");
            }
            if (safeColumns(request).isEmpty()) {
                throw new IllegalArgumentException("写入任务至少需要一个字段");
            }
            return;
        }

        if (!kafkaComplexMode && safeColumns(request).isEmpty()) {
            throw new IllegalArgumentException("Kafka 简单消息模式至少需要一个字段");
        }
    }

    private boolean isKafkaComplexMode(DatabaseType databaseType, String payloadSchemaJson) {
        return databaseType == DatabaseType.KAFKA
                && payloadSchemaJson != null
                && !payloadSchemaJson.isBlank();
    }

    private String normalizePayloadSchema(DatabaseType databaseType, WriteTaskUpsertRequest request) {
        if (!isKafkaComplexMode(databaseType, request.payloadSchemaJson())) {
            return null;
        }
        return payloadSchemaService.normalizeJson(request.payloadSchemaJson());
    }

    private List<WriteTaskColumnUpsertRequest> normalizeColumns(
            DatabaseType databaseType,
            List<WriteTaskColumnUpsertRequest> columns
    ) {
        return columns.stream()
                .map(column -> WriteTaskColumnDefaults.normalize(databaseType, column))
                .toList();
    }

    private WriteTaskColumn toEntity(WriteTaskColumnUpsertRequest request) {
        WriteTaskColumn column = new WriteTaskColumn();
        column.setColumnName(request.columnName().trim());
        column.setDbType(request.dbType().trim().toUpperCase(Locale.ROOT));
        column.setLengthValue(request.lengthValue());
        column.setPrecisionValue(request.precisionValue());
        column.setScaleValue(request.scaleValue());
        column.setNullableFlag(request.nullableFlag());
        column.setPrimaryKeyFlag(request.primaryKeyFlag());
        column.setGeneratorType(request.generatorType());
        column.setGeneratorConfigJson(writeJson(request.generatorConfig() == null ? Map.of() : request.generatorConfig()));
        column.setSortOrder(request.sortOrder());
        return column;
    }

    private WriteTaskUpsertRequest toUpsertRequest(WriteTask task) {
        return new WriteTaskUpsertRequest(
                task.getName(),
                task.getConnectionId(),
                task.getTableName(),
                task.getTableMode(),
                task.getWriteMode(),
                task.getRowCount(),
                task.getBatchSize(),
                task.getSeed(),
                task.getStatus(),
                task.getScheduleType(),
                task.getCronExpression(),
                task.getTriggerAt(),
                task.getIntervalSeconds(),
                task.getMaxRuns(),
                task.getMaxRowsTotal(),
                task.getDescription(),
                task.getTargetConfigJson(),
                task.getPayloadSchemaJson(),
                task.getColumns().stream().map(this::toRequest).toList()
        );
    }

    private WriteTaskColumnUpsertRequest toRequest(WriteTaskColumn column) {
        return new WriteTaskColumnUpsertRequest(
                column.getColumnName(),
                column.getDbType(),
                column.getLengthValue(),
                column.getPrecisionValue(),
                column.getScaleValue(),
                column.isNullableFlag(),
                column.isPrimaryKeyFlag(),
                column.getGeneratorType(),
                readJson(column.getGeneratorConfigJson()),
                column.getSortOrder()
        );
    }

    private Map<String, Object> readJson(String json) {
        try {
            if (json == null || json.isBlank()) {
                return Map.of();
            }
            return objectMapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException("字段生成配置 JSON 非法: " + exception.getMessage(), exception);
        }
    }

    private void log(Long executionId, WriteLogLevel level, String message, Map<String, Object> details) {
        WriteTaskExecutionLog log = new WriteTaskExecutionLog();
        log.setWriteTaskExecutionId(executionId);
        log.setLogLevel(level);
        log.setMessage(message);
        log.setDetailJson(writeJson(new LinkedHashMap<>(details)));
        executionLogRepository.save(log);
    }

    private String normalizeText(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private String writeJson(Map<String, ?> value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            throw new IllegalArgumentException("JSON 序列化失败: " + exception.getMessage(), exception);
        }
    }

    private void validateSchedule(WriteTaskUpsertRequest request) {
        switch (request.scheduleType()) {
            case MANUAL -> {
                if (request.status() == WriteTaskStatus.PAUSED || request.status() == WriteTaskStatus.RUNNING) {
                    throw new IllegalArgumentException("手动任务状态只能是草稿、就绪或已禁用");
                }
            }
            case ONCE -> {
                if (request.triggerAt() == null) {
                    throw new IllegalArgumentException("单次任务必须设置执行时间");
                }
                if (request.status() == WriteTaskStatus.RUNNING) {
                    throw new IllegalArgumentException("单次任务保存时不能处于运行中");
                }
            }
            case CRON -> {
                if (request.cronExpression() == null || request.cronExpression().isBlank()) {
                    throw new IllegalArgumentException("周期任务必须填写 cronExpression");
                }
                if (request.status() == WriteTaskStatus.RUNNING) {
                    throw new IllegalArgumentException("周期任务保存时不能处于运行中");
                }
            }
            case INTERVAL -> {
                if (request.intervalSeconds() == null || request.intervalSeconds() < 1) {
                    throw new IllegalArgumentException("持续写入任务必须设置 intervalSeconds");
                }
            }
        }
    }

    private String normalizeTargetConfig(
            DatabaseType databaseType,
            WriteTaskUpsertRequest request,
            String normalizedPayloadSchemaJson
    ) {
        Map<String, Object> targetConfig = JsonConfigSupport.copyMap(
                JsonConfigSupport.readConfig(request.targetConfigJson(), "targetConfigJson")
        );
        if (databaseType != DatabaseType.KAFKA) {
            return JsonConfigSupport.normalizeJson(request.targetConfigJson(), "targetConfigJson");
        }

        if (request.writeMode() != WriteMode.APPEND) {
            throw new IllegalArgumentException("Kafka 目标仅支持 APPEND 写入模式");
        }

        String payloadFormat = JsonConfigSupport.optionalString(targetConfig, "payloadFormat");
        if (payloadFormat != null && !"JSON".equalsIgnoreCase(payloadFormat)) {
            throw new IllegalArgumentException("Kafka 目标当前仅支持 JSON 负载格式");
        }
        targetConfig.put("payloadFormat", "JSON");

        String keyMode = JsonConfigSupport.optionalString(targetConfig, "keyMode");
        String normalizedKeyMode = keyMode == null || keyMode.isBlank()
                ? "NONE"
                : keyMode.trim().toUpperCase(Locale.ROOT);
        targetConfig.put("keyMode", normalizedKeyMode);
        boolean kafkaComplexMode = normalizedPayloadSchemaJson != null && !normalizedPayloadSchemaJson.isBlank();

        switch (normalizedKeyMode) {
            case "NONE" -> {
            }
            case "FIELD" -> normalizeKafkaFieldKey(targetConfig, request, normalizedPayloadSchemaJson, kafkaComplexMode);
            case "FIXED" -> JsonConfigSupport.requireString(targetConfig, "fixedKey");
            default -> throw new IllegalArgumentException("不支持的 Kafka keyMode: " + normalizedKeyMode);
        }

        Integer partition = JsonConfigSupport.optionalInteger(targetConfig, "partition");
        if (partition != null && partition < 0) {
            throw new IllegalArgumentException("Kafka partition 不能小于 0");
        }

        normalizeHeaders(targetConfig, request, normalizedPayloadSchemaJson, kafkaComplexMode);
        return writeJson(targetConfig);
    }

    private void normalizeKafkaFieldKey(
            Map<String, Object> targetConfig,
            WriteTaskUpsertRequest request,
            String normalizedPayloadSchemaJson,
            boolean kafkaComplexMode
    ) {
        String keyPath = JsonConfigSupport.requireString(targetConfig, "keyPath", "keyField");
        List<String> availablePaths = resolveKafkaAvailablePaths(request, normalizedPayloadSchemaJson, kafkaComplexMode);
        if (!kafkaComplexMode) {
            if (!availablePaths.contains(keyPath)) {
                throw new IllegalArgumentException("Kafka keyField 必须来自当前字段列表");
            }
            targetConfig.put("keyField", keyPath);
            targetConfig.put("keyPath", keyPath);
            return;
        }

        if (!availablePaths.contains(keyPath)) {
            throw new IllegalArgumentException("Kafka keyPath 必须来自当前消息 Schema 的标量路径");
        }
        targetConfig.put("keyPath", keyPath);
        if (!keyPath.contains(".") && !keyPath.contains("[")) {
            targetConfig.put("keyField", keyPath);
        } else {
            targetConfig.remove("keyField");
        }
    }

    private List<String> resolveKafkaAvailablePaths(
            WriteTaskUpsertRequest request,
            String normalizedPayloadSchemaJson,
            boolean kafkaComplexMode
    ) {
        if (!kafkaComplexMode) {
            return safeColumns(request).stream()
                    .map(WriteTaskColumnUpsertRequest::columnName)
                    .filter(columnName -> columnName != null && !columnName.isBlank())
                    .map(String::trim)
                    .toList();
        }
        KafkaPayloadSchemaNode payloadSchema = payloadSchemaService.parseAndValidate(normalizedPayloadSchemaJson);
        return payloadSchemaService.collectScalarPaths(payloadSchema);
    }

    private void normalizeHeaders(
            Map<String, Object> targetConfig,
            WriteTaskUpsertRequest request,
            String normalizedPayloadSchemaJson,
            boolean kafkaComplexMode
    ) {
        Object headerDefinitionsValue = JsonConfigSupport.findValue(targetConfig, "headerDefinitions");
        if (headerDefinitionsValue != null) {
            if (!(headerDefinitionsValue instanceof List<?> rawDefinitions)) {
                throw new IllegalArgumentException("Kafka headerDefinitions 必须是数组格式");
            }
            List<String> availablePaths = resolveKafkaAvailablePaths(request, normalizedPayloadSchemaJson, kafkaComplexMode);
            LinkedHashMap<String, Object> normalizedHeaders = new LinkedHashMap<>();
            ArrayList<Map<String, Object>> normalizedDefinitions = new ArrayList<>();
            Set<String> headerNames = new HashSet<>();
            boolean hasFieldHeader = false;

            for (Object rawDefinition : rawDefinitions) {
                if (!(rawDefinition instanceof Map<?, ?> rawDefinitionMap)) {
                    throw new IllegalArgumentException("Kafka headerDefinitions 必须由对象组成");
                }
                Map<String, Object> definition = objectMapper.convertValue(rawDefinitionMap, new TypeReference<>() {
                });
                String name = JsonConfigSupport.requireString(definition, "name");
                String normalizedName = name.toLowerCase(Locale.ROOT);
                if (!headerNames.add(normalizedName)) {
                    throw new IllegalArgumentException("Kafka Header 名称不能重复: " + name);
                }

                String mode = JsonConfigSupport.optionalString(definition, "mode");
                String normalizedMode = mode == null || mode.isBlank()
                        ? "FIXED"
                        : mode.trim().toUpperCase(Locale.ROOT);
                LinkedHashMap<String, Object> normalizedDefinition = new LinkedHashMap<>();
                normalizedDefinition.put("name", name);

                switch (normalizedMode) {
                    case "FIXED" -> {
                        String value = JsonConfigSupport.requireString(definition, "value");
                        normalizedDefinition.put("mode", "FIXED");
                        normalizedDefinition.put("value", value);
                        normalizedHeaders.put(name, value);
                    }
                    case "FIELD" -> {
                        String path = JsonConfigSupport.requireString(definition, "path");
                        if (!availablePaths.contains(path)) {
                            throw new IllegalArgumentException(
                                    kafkaComplexMode
                                            ? "Kafka Header path 必须来自当前消息 Schema 的标量路径"
                                            : "Kafka Header path 必须来自当前字段列表"
                            );
                        }
                        normalizedDefinition.put("mode", "FIELD");
                        normalizedDefinition.put("path", path);
                        hasFieldHeader = true;
                    }
                    default -> throw new IllegalArgumentException("不支持的 Kafka Header mode: " + normalizedMode);
                }
                normalizedDefinitions.add(normalizedDefinition);
            }

            if (normalizedHeaders.isEmpty()) {
                targetConfig.remove("headers");
            } else {
                targetConfig.put("headers", normalizedHeaders);
            }
            if (hasFieldHeader) {
                targetConfig.put("headerDefinitions", normalizedDefinitions);
            } else {
                targetConfig.remove("headerDefinitions");
            }
            return;
        }

        Object headers = JsonConfigSupport.findValue(targetConfig, "headers");
        if (headers == null) {
            targetConfig.remove("headerDefinitions");
            return;
        }
        if (!(headers instanceof Map<?, ?> rawHeaders)) {
            throw new IllegalArgumentException("Kafka headers 必须是对象格式");
        }
        LinkedHashMap<String, Object> normalizedHeaders = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : rawHeaders.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                normalizedHeaders.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        targetConfig.put("headers", normalizedHeaders);
        targetConfig.remove("headerDefinitions");
    }

    private RowValidationSummary validateRows(WriteTask task, List<Map<String, Object>> rows) {
        KafkaPayloadSchemaNode payloadSchema = parsePayloadSchema(task.getPayloadSchemaJson());
        if (payloadSchema != null) {
            return validatePayloadRows(payloadSchema, rows);
        }

        List<RowValidationIssue> issues = new ArrayList<>();
        long nullValueCount = 0;
        long blankStringCount = 0;
        List<WriteTaskColumn> requiredColumns = task.getColumns().stream()
                .filter(column -> !column.isNullableFlag())
                .toList();

        for (WriteTaskColumn column : requiredColumns) {
            long columnNullCount = 0;
            long columnBlankCount = 0;
            for (Map<String, Object> row : rows) {
                Object value = row.get(column.getColumnName());
                if (value == null) {
                    columnNullCount++;
                    continue;
                }
                if (value instanceof String stringValue && stringValue.isBlank()) {
                    columnBlankCount++;
                }
            }

            if (columnNullCount > 0) {
                nullValueCount += columnNullCount;
                issues.add(new RowValidationIssue(
                        column.getColumnName(),
                        "NULL_VALUE",
                        columnNullCount,
                        "非空字段生成了 null"
                ));
            }
            if (columnBlankCount > 0) {
                blankStringCount += columnBlankCount;
                issues.add(new RowValidationIssue(
                        column.getColumnName(),
                        "BLANK_STRING",
                        columnBlankCount,
                        "非空字符串字段生成了空字符串"
                ));
            }
        }

        return new RowValidationSummary(
                issues.isEmpty(),
                rows.size(),
                requiredColumns.size(),
                nullValueCount,
                blankStringCount,
                issues
        );
    }

    private KafkaPayloadSchemaNode parsePayloadSchema(String payloadSchemaJson) {
        if (payloadSchemaJson == null || payloadSchemaJson.isBlank()) {
            return null;
        }
        return payloadSchemaService.parseAndValidate(payloadSchemaJson);
    }

    private RowValidationSummary validatePayloadRows(KafkaPayloadSchemaNode payloadSchema, List<Map<String, Object>> rows) {
        LinkedHashMap<String, ValidationCounter> counters = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            validatePayloadNode(payloadSchema, row, "", counters);
        }

        List<RowValidationIssue> issues = new ArrayList<>();
        long nullValueCount = 0;
        long blankStringCount = 0;
        for (Map.Entry<String, ValidationCounter> entry : counters.entrySet()) {
            if (entry.getValue().nullValueCount() > 0) {
                nullValueCount += entry.getValue().nullValueCount();
                issues.add(new RowValidationIssue(
                        entry.getKey(),
                        "NULL_VALUE",
                        entry.getValue().nullValueCount(),
                        "非空字段生成了 null"
                ));
            }
            if (entry.getValue().blankStringCount() > 0) {
                blankStringCount += entry.getValue().blankStringCount();
                issues.add(new RowValidationIssue(
                        entry.getKey(),
                        "BLANK_STRING",
                        entry.getValue().blankStringCount(),
                        "非空字符串字段生成了空字符串"
                ));
            }
        }

        return new RowValidationSummary(
                issues.isEmpty(),
                rows.size(),
                countRequiredScalarFields(payloadSchema),
                nullValueCount,
                blankStringCount,
                issues
        );
    }

    private void validatePayloadNode(
            KafkaPayloadSchemaNode node,
            Object value,
            String path,
            Map<String, ValidationCounter> counters
    ) {
        String displayPath = path == null || path.isBlank() ? "$" : path;
        if (value == null) {
            if (!node.nullableOrDefault() && !"$".equals(displayPath)) {
                counters.computeIfAbsent(displayPath, key -> new ValidationCounter()).incrementNull();
            }
            return;
        }

        switch (node.type()) {
            case OBJECT -> {
                if (!(value instanceof Map<?, ?> mapValue)) {
                    counters.computeIfAbsent(displayPath, key -> new ValidationCounter()).incrementNull();
                    return;
                }
                for (KafkaPayloadSchemaNode child : node.childrenOrEmpty()) {
                    validatePayloadNode(child, mapValue.get(child.name()), appendPath(path, child.name()), counters);
                }
            }
            case ARRAY -> {
                if (!(value instanceof List<?> listValue)) {
                    counters.computeIfAbsent(displayPath, key -> new ValidationCounter()).incrementNull();
                    return;
                }
                for (Object item : listValue) {
                    validatePayloadNode(node.itemSchema(), item, displayPath + "[]", counters);
                }
            }
            case SCALAR -> {
                if (!node.nullableOrDefault() && value instanceof String stringValue && stringValue.isBlank()) {
                    counters.computeIfAbsent(displayPath, key -> new ValidationCounter()).incrementBlank();
                }
            }
        }
    }

    private int countRequiredScalarFields(KafkaPayloadSchemaNode node) {
        return switch (node.type()) {
            case OBJECT -> node.childrenOrEmpty().stream()
                    .mapToInt(this::countRequiredScalarFields)
                    .sum();
            case ARRAY -> countRequiredScalarFields(node.itemSchema());
            case SCALAR -> node.nullableOrDefault() ? 0 : 1;
        };
    }

    private void logSequenceAdjustments(
            Long executionId,
            WriteTaskUpsertRequest originalRequest,
            WriteTaskUpsertRequest runtimeRequest
    ) {
        List<WriteTaskColumnUpsertRequest> originalColumns = safeColumns(originalRequest);
        List<WriteTaskColumnUpsertRequest> runtimeColumns = safeColumns(runtimeRequest);
        if (originalColumns.isEmpty() || runtimeColumns.isEmpty()) {
            return;
        }

        List<Map<String, Object>> adjustments = runtimeColumns.stream()
                .<Map<String, Object>>map(runtimeColumn -> {
                    WriteTaskColumnUpsertRequest originalColumn = originalColumns.stream()
                            .filter(column -> column.columnName().equals(runtimeColumn.columnName()))
                            .findFirst()
                            .orElse(null);
                    if (originalColumn == null || runtimeColumn.generatorType() != originalColumn.generatorType()) {
                        return null;
                    }
                    if (runtimeColumn.generatorType() != ColumnGeneratorType.SEQUENCE) {
                        return null;
                    }
                    long originalStart = asLong(
                            originalColumn.generatorConfig() == null ? null : originalColumn.generatorConfig().get("start"),
                            1L
                    );
                    long runtimeStart = asLong(
                            runtimeColumn.generatorConfig() == null ? null : runtimeColumn.generatorConfig().get("start"),
                            1L
                    );
                    if (runtimeStart == originalStart) {
                        return null;
                    }
                    Map<String, Object> adjustment = new LinkedHashMap<>();
                    adjustment.put("columnName", runtimeColumn.columnName());
                    adjustment.put("originalStart", originalStart);
                    adjustment.put("runtimeStart", runtimeStart);
                    return adjustment;
                })
                .filter(adjustment -> adjustment != null)
                .toList();

        if (!adjustments.isEmpty()) {
            log(executionId, WriteLogLevel.INFO, "已根据目标表现有数据调整序列起点", Map.of("columns", adjustments));
        }
    }

    private List<WriteTaskColumnUpsertRequest> safeColumns(WriteTaskUpsertRequest request) {
        return request.columns() == null ? List.of() : request.columns();
    }

    private String appendPath(String path, String segment) {
        if (path == null || path.isBlank()) {
            return segment;
        }
        return path + "." + segment;
    }

    private long asLong(Object value, long fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        String text = value.toString().trim();
        if (text.isEmpty()) {
            return fallback;
        }
        return Long.parseLong(text);
    }

    private record RowValidationSummary(
            boolean passed,
            int checkedRowCount,
            int requiredColumnCount,
            long nullValueCount,
            long blankStringCount,
            List<RowValidationIssue> issues
    ) {
        private Map<String, Object> toMap() {
            return Map.of(
                    "passed", passed,
                    "checkedRowCount", checkedRowCount,
                    "requiredColumnCount", requiredColumnCount,
                    "nullValueCount", nullValueCount,
                    "blankStringCount", blankStringCount,
                    "issueCount", issues.size(),
                    "issues", issues.stream().map(RowValidationIssue::toMap).toList()
            );
        }
    }

    private record RowValidationIssue(
            String columnName,
            String issueType,
            long affectedRowCount,
            String message
    ) {
        private Map<String, Object> toMap() {
            return Map.of(
                    "columnName", columnName,
                    "fieldPath", columnName,
                    "issueType", issueType,
                    "affectedRowCount", affectedRowCount,
                    "message", message
            );
        }
    }

    private static final class ValidationCounter {

        private long nullValueCount;
        private long blankStringCount;

        private void incrementNull() {
            nullValueCount++;
        }

        private void incrementBlank() {
            blankStringCount++;
        }

        private long nullValueCount() {
            return nullValueCount;
        }

        private long blankStringCount() {
            return blankStringCount;
        }
    }
}

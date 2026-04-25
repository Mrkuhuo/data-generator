package com.datagenerator.task.application;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.connection.application.ConnectionJdbcSupport;
import com.datagenerator.connection.application.dialect.DatabaseDialect;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.api.WriteTaskGroupPreviewResponse;
import com.datagenerator.task.api.WriteTaskGroupPreviewTableResponse;
import com.datagenerator.task.api.WriteTaskGroupRelationUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupTaskUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupUpsertRequest;
import com.datagenerator.task.domain.KafkaPayloadSchemaNode;
import com.datagenerator.task.domain.KafkaPayloadValueType;
import com.datagenerator.task.domain.ReferenceSourceMode;
import com.datagenerator.task.domain.RelationReusePolicy;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.datagenerator.task.domain.WriteTaskGroupRowPlanMode;
import com.datagenerator.task.domain.WriteTaskRelationMode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

@Service
public class WriteTaskGroupPreviewService {

    private static final Pattern INDEXED_SEGMENT = Pattern.compile("^(.*)\\[(\\d+)]$");

    private final WriteTaskValueGenerator valueGenerator;
    private final ConnectionJdbcSupport jdbcSupport;
    private final KafkaPayloadSchemaService payloadSchemaService;
    private final ObjectMapper objectMapper;

    public WriteTaskGroupPreviewService(
            WriteTaskValueGenerator valueGenerator,
            ConnectionJdbcSupport jdbcSupport,
            KafkaPayloadSchemaService payloadSchemaService,
            ObjectMapper objectMapper
    ) {
        this.valueGenerator = valueGenerator;
        this.jdbcSupport = jdbcSupport;
        this.payloadSchemaService = payloadSchemaService;
        this.objectMapper = objectMapper;
    }

    public WriteTaskGroupPreviewResponse preview(
            WriteTaskGroupUpsertRequest request,
            List<WriteTask> persistedTasks,
            TargetConnection connection,
            Integer previewCount,
            Long requestedSeed
    ) {
        WriteTaskGroupGenerationResult result = generate(request, persistedTasks, connection, requestedSeed);
        int effectivePreviewCount = sanitizePreviewCount(previewCount);
        return new WriteTaskGroupPreviewResponse(
                result.seed(),
                result.tables().stream()
                        .map(table -> new WriteTaskGroupPreviewTableResponse(
                                table.task().getId(),
                                table.task().getTaskKey(),
                                table.task().getName(),
                                table.task().getTableName(),
                                table.rows().size(),
                                Math.min(table.rows().size(), effectivePreviewCount),
                                table.foreignKeyMissCount(),
                                table.nullViolationCount() + table.blankStringCount(),
                                table.rows().stream().limit(effectivePreviewCount).toList()
                        ))
                        .toList()
        );
    }

    public WriteTaskGroupGenerationResult generate(
            WriteTaskGroupUpsertRequest request,
            List<WriteTask> persistedTasks,
            TargetConnection connection,
            Long requestedSeed
    ) {
        return connection.getDbType() == DatabaseType.KAFKA
                ? generateKafka(request, persistedTasks, requestedSeed)
                : generateDatabase(request, persistedTasks, connection, requestedSeed);
    }

    private WriteTaskGroupGenerationResult generateDatabase(
            WriteTaskGroupUpsertRequest request,
            List<WriteTask> persistedTasks,
            TargetConnection connection,
            Long requestedSeed
    ) {
        DatabaseResolvedDefinition definition = resolveDatabaseDefinition(request, persistedTasks);
        long seed = requestedSeed != null
                ? requestedSeed
                : (request.seed() != null ? request.seed() : System.currentTimeMillis());
        Random random = new Random(seed);
        Map<String, Long> sequenceState = new LinkedHashMap<>();
        Map<String, List<Map<String, Object>>> generatedRowsByTaskKey = new LinkedHashMap<>();
        Map<String, List<Map<String, Object>>> existingRowsCache = new HashMap<>();
        Map<String, Deque<Map<String, Object>>> uniqueCandidateCache = new HashMap<>();
        List<WriteTaskTableGenerationResult> tables = new ArrayList<>();

        Connection jdbcConnection = null;
        try {
            if (definition.requiresExistingRows()) {
                jdbcConnection = jdbcSupport.open(connection);
            }
            for (String taskKey : definition.topologicalOrder()) {
                WriteTask task = definition.tasksByKey().get(taskKey);
                List<DatabaseResolvedRelation> incomingRelations = definition.incomingRelations().getOrDefault(taskKey, List.of());
                GeneratedRowsResult generatedRows = generateDatabaseTaskRows(
                        task,
                        definition,
                        incomingRelations,
                        generatedRowsByTaskKey,
                        existingRowsCache,
                        uniqueCandidateCache,
                        jdbcConnection,
                        connection,
                        random,
                        sequenceState
                );
                generatedRowsByTaskKey.put(taskKey, generatedRows.rows());
                tables.add(new WriteTaskTableGenerationResult(
                        task,
                        generatedRows.rows(),
                        generatedRows.foreignKeyMissCount(),
                        generatedRows.nullViolationCount(),
                        generatedRows.blankStringCount(),
                        countPrimaryKeyDuplicates(task, generatedRows.rows())
                ));
            }
            return new WriteTaskGroupGenerationResult(seed, tables);
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalArgumentException("Failed to preview relationship task group: " + exception.getMessage(), exception);
        } finally {
            if (jdbcConnection != null) {
                try {
                    jdbcConnection.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private GeneratedRowsResult generateDatabaseTaskRows(
            WriteTask task,
            DatabaseResolvedDefinition definition,
            List<DatabaseResolvedRelation> incomingRelations,
            Map<String, List<Map<String, Object>>> generatedRowsByTaskKey,
            Map<String, List<Map<String, Object>>> existingRowsCache,
            Map<String, Deque<Map<String, Object>>> uniqueCandidateCache,
            Connection jdbcConnection,
            TargetConnection connection,
            Random random,
            Map<String, Long> sequenceState
    ) throws Exception {
        RowPlan rowPlan = definition.rowPlans().get(task.getTaskKey());
        List<Map<String, Object>> rows = initializeDatabaseRows(
                task,
                rowPlan,
                incomingRelations,
                generatedRowsByTaskKey,
                existingRowsCache,
                uniqueCandidateCache,
                jdbcConnection,
                connection,
                random
        );

        int foreignKeyMissCount = 0;
        int nullViolationCount = 0;
        int blankStringCount = 0;
        List<WriteTaskColumn> sortedColumns = task.getColumns().stream()
                .sorted(Comparator.comparing(WriteTaskColumn::getSortOrder).thenComparing(WriteTaskColumn::getColumnName))
                .toList();

        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            Map<String, Object> row = rows.get(rowIndex);
            for (DatabaseResolvedRelation relation : incomingRelations) {
                if (relation.rowDriver() && rowPlan.mode() == WriteTaskGroupRowPlanMode.CHILD_PER_PARENT) {
                    continue;
                }
                if (isDatabaseRelationAlreadyAssigned(row, relation.request())) {
                    continue;
                }
                Map<String, Object> parentValues = chooseDatabaseCandidate(
                        relation,
                        generatedRowsByTaskKey,
                        existingRowsCache,
                        uniqueCandidateCache,
                        jdbcConnection,
                        connection,
                        random
                );
                if (parentValues == null) {
                    if (!shouldAssignNull(relation.request(), random)) {
                        foreignKeyMissCount++;
                    }
                    continue;
                }
                assignDatabaseRelationValues(row, relation.request(), parentValues);
            }

            for (WriteTaskColumn column : sortedColumns) {
                if (row.containsKey(column.getColumnName())) {
                    continue;
                }
                row.put(
                        column.getColumnName(),
                        valueGenerator.generateValue(
                                task.getTaskKey() + "." + column.getColumnName(),
                                column.getGeneratorType(),
                                JsonSupport.readMap(column.getGeneratorConfigJson()),
                                rowIndex,
                                random,
                                sequenceState
                        )
                );
            }

            for (WriteTaskColumn column : sortedColumns) {
                Object value = row.get(column.getColumnName());
                if (column.isNullableFlag()) {
                    continue;
                }
                if (value == null) {
                    nullViolationCount++;
                    continue;
                }
                if (value instanceof String stringValue && stringValue.isBlank()) {
                    blankStringCount++;
                }
            }
        }

        return new GeneratedRowsResult(rows, foreignKeyMissCount, nullViolationCount, blankStringCount);
    }

    private List<Map<String, Object>> initializeDatabaseRows(
            WriteTask task,
            RowPlan rowPlan,
            List<DatabaseResolvedRelation> incomingRelations,
            Map<String, List<Map<String, Object>>> generatedRowsByTaskKey,
            Map<String, List<Map<String, Object>>> existingRowsCache,
            Map<String, Deque<Map<String, Object>>> uniqueCandidateCache,
            Connection jdbcConnection,
            TargetConnection connection,
            Random random
    ) throws Exception {
        if (rowPlan.mode() == WriteTaskGroupRowPlanMode.FIXED) {
            int rowCount = rowPlan.rowCount() == null ? 1 : rowPlan.rowCount();
            List<Map<String, Object>> rows = new ArrayList<>(rowCount);
            for (int index = 0; index < rowCount; index++) {
                rows.add(new LinkedHashMap<>());
            }
            return rows;
        }

        DatabaseResolvedRelation driverRelation = incomingRelations.stream()
                .filter(DatabaseResolvedRelation::rowDriver)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Child task " + task.getTaskKey() + " is missing driver relationship"));
        List<Map<String, Object>> driverCandidates = allDatabaseCandidates(
                driverRelation,
                generatedRowsByTaskKey,
                existingRowsCache,
                jdbcConnection,
                connection
        );
        if (driverCandidates.isEmpty()) {
            throw new IllegalArgumentException("Driver relationship " + driverRelation.request().relationName() + " has no available parent rows");
        }

        int minChildren = firstNonNull(rowPlan.minChildrenPerParent(), driverRelation.request().minChildrenPerParent(), 1);
        int maxChildren = firstNonNull(rowPlan.maxChildrenPerParent(), driverRelation.request().maxChildrenPerParent(), minChildren);
        if (maxChildren < minChildren) {
            throw new IllegalArgumentException("Invalid child row range for task " + task.getTaskKey());
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> driverCandidate : driverCandidates) {
            int childCount = minChildren == maxChildren
                    ? minChildren
                    : minChildren + random.nextInt(maxChildren - minChildren + 1);
            for (int index = 0; index < childCount; index++) {
                LinkedHashMap<String, Object> row = new LinkedHashMap<>();
                assignDatabaseRelationValues(row, driverRelation.request(), driverCandidate);
                rows.add(row);
            }
        }
        return rows;
    }

    private boolean isDatabaseRelationAlreadyAssigned(Map<String, Object> row, WriteTaskGroupRelationUpsertRequest relation) {
        for (String childColumn : safeList(relation.childColumns())) {
            if (!row.containsKey(childColumn)) {
                return false;
            }
        }
        return true;
    }

    private void assignDatabaseRelationValues(
            Map<String, Object> row,
            WriteTaskGroupRelationUpsertRequest relation,
            Map<String, Object> parentValues
    ) {
        List<String> parentColumns = safeList(relation.parentColumns());
        List<String> childColumns = safeList(relation.childColumns());
        for (int index = 0; index < childColumns.size(); index++) {
            row.put(childColumns.get(index), parentValues.get(parentColumns.get(index)));
        }
    }

    private Map<String, Object> chooseDatabaseCandidate(
            DatabaseResolvedRelation relation,
            Map<String, List<Map<String, Object>>> generatedRowsByTaskKey,
            Map<String, List<Map<String, Object>>> existingRowsCache,
            Map<String, Deque<Map<String, Object>>> uniqueCandidateCache,
            Connection jdbcConnection,
            TargetConnection connection,
            Random random
    ) throws Exception {
        String relationKey = relation.request().relationName();
        if (relation.request().reusePolicy() == RelationReusePolicy.UNIQUE_ONCE) {
            Deque<Map<String, Object>> uniqueCandidates = uniqueCandidateCache.get(relationKey);
            if (uniqueCandidates == null) {
                List<Map<String, Object>> all = allDatabaseCandidates(
                        relation,
                        generatedRowsByTaskKey,
                        existingRowsCache,
                        jdbcConnection,
                        connection
                );
                uniqueCandidates = new ArrayDeque<>(all);
                uniqueCandidateCache.put(relationKey, uniqueCandidates);
            }
            return uniqueCandidates.pollFirst();
        }

        return switch (relation.request().sourceMode()) {
            case CURRENT_BATCH -> pickRandom(currentDatabaseCandidates(relation, generatedRowsByTaskKey), random);
            case TARGET_TABLE -> pickRandom(existingDatabaseCandidates(relation, existingRowsCache, jdbcConnection, connection), random);
            case MIXED -> chooseMixedDatabaseCandidate(relation, generatedRowsByTaskKey, existingRowsCache, jdbcConnection, connection, random);
        };
    }

    private Map<String, Object> chooseMixedDatabaseCandidate(
            DatabaseResolvedRelation relation,
            Map<String, List<Map<String, Object>>> generatedRowsByTaskKey,
            Map<String, List<Map<String, Object>>> existingRowsCache,
            Connection jdbcConnection,
            TargetConnection connection,
            Random random
    ) throws Exception {
        List<Map<String, Object>> current = currentDatabaseCandidates(relation, generatedRowsByTaskKey);
        List<Map<String, Object>> existing = existingDatabaseCandidates(relation, existingRowsCache, jdbcConnection, connection);
        if (current.isEmpty()) {
            return pickRandom(existing, random);
        }
        if (existing.isEmpty()) {
            return pickRandom(current, random);
        }
        double existingRatio = relation.request().mixedExistingRatio() == null ? 0.5D : relation.request().mixedExistingRatio();
        return random.nextDouble() <= existingRatio
                ? pickRandom(existing, random)
                : pickRandom(current, random);
    }

    private List<Map<String, Object>> allDatabaseCandidates(
            DatabaseResolvedRelation relation,
            Map<String, List<Map<String, Object>>> generatedRowsByTaskKey,
            Map<String, List<Map<String, Object>>> existingRowsCache,
            Connection jdbcConnection,
            TargetConnection connection
    ) throws Exception {
        return switch (relation.request().sourceMode()) {
            case CURRENT_BATCH -> currentDatabaseCandidates(relation, generatedRowsByTaskKey);
            case TARGET_TABLE -> existingDatabaseCandidates(relation, existingRowsCache, jdbcConnection, connection);
            case MIXED -> {
                List<Map<String, Object>> mixed = new ArrayList<>(currentDatabaseCandidates(relation, generatedRowsByTaskKey));
                mixed.addAll(existingDatabaseCandidates(relation, existingRowsCache, jdbcConnection, connection));
                yield mixed;
            }
        };
    }

    private List<Map<String, Object>> currentDatabaseCandidates(
            DatabaseResolvedRelation relation,
            Map<String, List<Map<String, Object>>> generatedRowsByTaskKey
    ) {
        List<Map<String, Object>> rows = generatedRowsByTaskKey.getOrDefault(relation.parentTask().getTaskKey(), List.of());
        if (rows.isEmpty()) {
            return List.of();
        }
        return rows.stream()
                .filter(row -> containsAllColumns(row, safeList(relation.request().parentColumns())))
                .toList();
    }

    private List<Map<String, Object>> existingDatabaseCandidates(
            DatabaseResolvedRelation relation,
            Map<String, List<Map<String, Object>>> existingRowsCache,
            Connection jdbcConnection,
            TargetConnection connection
    ) throws Exception {
        if (jdbcConnection == null) {
            return List.of();
        }
        String cacheKey = relation.parentTask().getTaskKey() + "|" + String.join(",", safeList(relation.request().parentColumns()));
        return existingRowsCache.computeIfAbsent(cacheKey, ignored -> loadExistingRows(jdbcConnection, connection, relation));
    }

    private List<Map<String, Object>> loadExistingRows(
            Connection jdbcConnection,
            TargetConnection connection,
            DatabaseResolvedRelation relation
    ) {
        try {
            DatabaseDialect dialect = jdbcSupport.dialect(connection.getDbType());
            List<String> parentColumns = safeList(relation.request().parentColumns());
            String selectColumns = parentColumns.stream()
                    .map(dialect::quoteIdentifier)
                    .reduce((left, right) -> left + ", " + right)
                    .orElseThrow();
            String whereClause = parentColumns.stream()
                    .map(column -> dialect.quoteIdentifier(column) + " IS NOT NULL")
                    .reduce((left, right) -> left + " AND " + right)
                    .orElse("1=1");
            String sql = "SELECT " + selectColumns + " FROM "
                    + dialect.quoteQualifiedIdentifier(connection, relation.parentTask().getTableName())
                    + " WHERE " + whereClause;
            try (Statement statement = jdbcConnection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (resultSet.next()) {
                    LinkedHashMap<String, Object> row = new LinkedHashMap<>();
                    for (String column : parentColumns) {
                        row.put(column, resultSet.getObject(column));
                    }
                    if (containsAllColumns(row, parentColumns)) {
                        rows.add(row);
                    }
                }
                return rows;
            }
        } catch (Exception exception) {
            throw new IllegalArgumentException("Failed to load existing parent rows: " + exception.getMessage(), exception);
        }
    }

    private WriteTaskGroupGenerationResult generateKafka(
            WriteTaskGroupUpsertRequest request,
            List<WriteTask> persistedTasks,
            Long requestedSeed
    ) {
        KafkaResolvedDefinition definition = resolveKafkaDefinition(request, persistedTasks);
        long seed = requestedSeed != null
                ? requestedSeed
                : (request.seed() != null ? request.seed() : System.currentTimeMillis());
        Random random = new Random(seed);
        Map<String, Long> sequenceState = new LinkedHashMap<>();
        Map<String, List<Map<String, Object>>> generatedRowsByTaskKey = new LinkedHashMap<>();
        List<WriteTaskTableGenerationResult> tables = new ArrayList<>();

        for (String taskKey : definition.topologicalOrder()) {
            WriteTask task = definition.tasksByKey().get(taskKey);
            List<KafkaResolvedRelation> incomingRelations = definition.incomingRelations().getOrDefault(taskKey, List.of());
            GeneratedRowsResult generatedRows = generateKafkaTaskRows(
                    task,
                    definition,
                    incomingRelations,
                    generatedRowsByTaskKey,
                    random,
                    sequenceState
            );
            generatedRowsByTaskKey.put(taskKey, generatedRows.rows());
            tables.add(new WriteTaskTableGenerationResult(
                    task,
                    generatedRows.rows(),
                    generatedRows.foreignKeyMissCount(),
                    generatedRows.nullViolationCount(),
                    generatedRows.blankStringCount(),
                    countPrimaryKeyDuplicates(task, generatedRows.rows())
            ));
        }
        return new WriteTaskGroupGenerationResult(seed, tables);
    }

    private GeneratedRowsResult generateKafkaTaskRows(
            WriteTask task,
            KafkaResolvedDefinition definition,
            List<KafkaResolvedRelation> incomingRelations,
            Map<String, List<Map<String, Object>>> generatedRowsByTaskKey,
            Random random,
            Map<String, Long> sequenceState
    ) {
        RowPlan rowPlan = definition.rowPlans().get(task.getTaskKey());
        int foreignKeyMissCount = 0;
        List<Map<String, Object>> rows = new ArrayList<>();

        if (rowPlan.mode() == WriteTaskGroupRowPlanMode.CHILD_PER_PARENT) {
            KafkaResolvedRelation driverRelation = incomingRelations.stream()
                    .filter(KafkaResolvedRelation::rowDriver)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Kafka child task " + task.getTaskKey() + " is missing driver relationship"));
            List<Map<String, Object>> driverCandidates = generatedRowsByTaskKey.getOrDefault(driverRelation.parentTask().getTaskKey(), List.of());
            if (driverCandidates.isEmpty()) {
                throw new IllegalArgumentException("Kafka driver relationship " + driverRelation.request().relationName() + " has no available parent messages");
            }

            int minChildren = firstNonNull(rowPlan.minChildrenPerParent(), driverRelation.request().minChildrenPerParent(), 1);
            int maxChildren = firstNonNull(rowPlan.maxChildrenPerParent(), driverRelation.request().maxChildrenPerParent(), minChildren);
            if (maxChildren < minChildren) {
                throw new IllegalArgumentException("Invalid Kafka child message range for task " + task.getTaskKey());
            }

            for (Map<String, Object> parentRow : driverCandidates) {
                int childCount = minChildren == maxChildren
                        ? minChildren
                        : minChildren + random.nextInt(maxChildren - minChildren + 1);
                for (int index = 0; index < childCount; index++) {
                    Map<String, Object> row = generateKafkaRow(task, rows.size(), random, sequenceState);
                    foreignKeyMissCount += applyKafkaRelationMapping(row, parentRow, driverRelation.mappingConfig());
                    for (KafkaResolvedRelation relation : incomingRelations) {
                        if (relation == driverRelation) {
                            continue;
                        }
                        Map<String, Object> parentCandidate = pickRandom(
                                generatedRowsByTaskKey.getOrDefault(relation.parentTask().getTaskKey(), List.of()),
                                random
                        );
                        if (parentCandidate == null) {
                            foreignKeyMissCount++;
                            continue;
                        }
                        foreignKeyMissCount += applyKafkaRelationMapping(row, parentCandidate, relation.mappingConfig());
                    }
                    rows.add(row);
                }
            }
        } else {
            int rowCount = rowPlan.rowCount() == null ? 1 : rowPlan.rowCount();
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Map<String, Object> row = generateKafkaRow(task, rowIndex, random, sequenceState);
                for (KafkaResolvedRelation relation : incomingRelations) {
                    Map<String, Object> parentCandidate = pickRandom(
                            generatedRowsByTaskKey.getOrDefault(relation.parentTask().getTaskKey(), List.of()),
                            random
                    );
                    if (parentCandidate == null) {
                        foreignKeyMissCount++;
                        continue;
                    }
                    foreignKeyMissCount += applyKafkaRelationMapping(row, parentCandidate, relation.mappingConfig());
                }
                rows.add(row);
            }
        }

        ValidationSummary validationSummary = validateKafkaRows(task, rows);
        return new GeneratedRowsResult(
                rows,
                foreignKeyMissCount,
                Math.toIntExact(validationSummary.nullViolationCount()),
                Math.toIntExact(validationSummary.blankStringCount())
        );
    }

    private Map<String, Object> generateKafkaRow(
            WriteTask task,
            int rowIndex,
            Random random,
            Map<String, Long> sequenceState
    ) {
        if (task.getPayloadSchemaJson() != null && !task.getPayloadSchemaJson().isBlank()) {
            KafkaPayloadSchemaNode payloadSchema = payloadSchemaService.parseAndValidate(task.getPayloadSchemaJson());
            Object generated = generatePayloadNode(payloadSchema, "", rowIndex, random, sequenceState);
            if (!(generated instanceof Map<?, ?> rawRow)) {
                throw new IllegalArgumentException("Kafka payload schema root must generate an object");
            }
            return castRow(rawRow);
        }

        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        List<WriteTaskColumn> columns = task.getColumns().stream()
                .sorted(Comparator.comparing(WriteTaskColumn::getSortOrder).thenComparing(WriteTaskColumn::getColumnName))
                .toList();
        for (WriteTaskColumn column : columns) {
            row.put(
                    column.getColumnName(),
                    valueGenerator.generateValue(
                            task.getTaskKey() + "." + column.getColumnName(),
                            column.getGeneratorType(),
                            JsonSupport.readMap(column.getGeneratorConfigJson()),
                            rowIndex,
                            random,
                            sequenceState
                    )
            );
        }
        return row;
    }

    private int applyKafkaRelationMapping(
            Map<String, Object> childRow,
            Map<String, Object> parentRow,
            KafkaRelationMappingConfig mappingConfig
    ) {
        int missingRequiredCount = 0;
        for (KafkaRelationFieldMapping mapping : mappingConfig.fieldMappings()) {
            Object value = resolvePathValue(parentRow, mapping.from());
            if (value == null) {
                if (mapping.required()) {
                    missingRequiredCount++;
                }
                continue;
            }
            boolean assigned = assignPathValue(childRow, mapping.to(), deepCopyValue(value));
            if (!assigned && mapping.required()) {
                missingRequiredCount++;
            }
        }
        return missingRequiredCount;
    }

    private ValidationSummary validateKafkaRows(WriteTask task, List<Map<String, Object>> rows) {
        if (task.getPayloadSchemaJson() != null && !task.getPayloadSchemaJson().isBlank()) {
            KafkaPayloadSchemaNode payloadSchema = payloadSchemaService.parseAndValidate(task.getPayloadSchemaJson());
            LinkedHashMap<String, ValidationCounter> counters = new LinkedHashMap<>();
            for (Map<String, Object> row : rows) {
                validatePayloadNode(payloadSchema, row, "", counters);
            }
            long nullValueCount = 0L;
            long blankStringCount = 0L;
            for (ValidationCounter counter : counters.values()) {
                nullValueCount += counter.nullValueCount();
                blankStringCount += counter.blankStringCount();
            }
            return new ValidationSummary(nullValueCount, blankStringCount);
        }

        long nullValueCount = 0L;
        long blankStringCount = 0L;
        for (WriteTaskColumn column : task.getColumns()) {
            if (column.isNullableFlag()) {
                continue;
            }
            for (Map<String, Object> row : rows) {
                Object value = row.get(column.getColumnName());
                if (value == null) {
                    nullValueCount++;
                    continue;
                }
                if (value instanceof String stringValue && stringValue.isBlank()) {
                    blankStringCount++;
                }
            }
        }
        return new ValidationSummary(nullValueCount, blankStringCount);
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

    private Object generatePayloadNode(
            KafkaPayloadSchemaNode node,
            String path,
            int rowIndex,
            Random random,
            Map<String, Long> sequenceState
    ) {
        return switch (node.type()) {
            case OBJECT -> generateObjectNode(node, path, rowIndex, random, sequenceState);
            case ARRAY -> generateArrayNode(node, path, rowIndex, random, sequenceState);
            case SCALAR -> generateScalarNode(node, path, rowIndex, random, sequenceState);
        };
    }

    private Map<String, Object> generateObjectNode(
            KafkaPayloadSchemaNode node,
            String path,
            int rowIndex,
            Random random,
            Map<String, Long> sequenceState
    ) {
        LinkedHashMap<String, Object> value = new LinkedHashMap<>();
        for (KafkaPayloadSchemaNode child : node.childrenOrEmpty()) {
            value.put(
                    child.name(),
                    generatePayloadNode(child, appendPath(path, child.name()), rowIndex, random, sequenceState)
            );
        }
        return value;
    }

    private List<Object> generateArrayNode(
            KafkaPayloadSchemaNode node,
            String path,
            int rowIndex,
            Random random,
            Map<String, Long> sequenceState
    ) {
        int size = resolveArraySize(node, random);
        ArrayList<Object> items = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            items.add(generatePayloadNode(node.itemSchema(), path + "[]", rowIndex, random, sequenceState));
        }
        return items;
    }

    private Object generateScalarNode(
            KafkaPayloadSchemaNode node,
            String path,
            int rowIndex,
            Random random,
            Map<String, Long> sequenceState
    ) {
        Object rawValue = valueGenerator.generateValue(
                path,
                node.generatorType(),
                node.generatorConfigOrEmpty(),
                rowIndex,
                random,
                sequenceState
        );
        return castScalarValue(node.valueType(), rawValue);
    }

    private Object castScalarValue(KafkaPayloadValueType valueType, Object value) {
        if (value == null || valueType == null) {
            return value;
        }
        return switch (valueType) {
            case STRING, DATETIME, UUID -> String.valueOf(value);
            case INT -> value instanceof Number number ? number.intValue() : Integer.parseInt(String.valueOf(value));
            case LONG -> value instanceof Number number ? number.longValue() : Long.parseLong(String.valueOf(value));
            case DECIMAL -> value instanceof BigDecimal decimal ? decimal : new BigDecimal(String.valueOf(value));
            case BOOLEAN -> value instanceof Boolean booleanValue
                    ? booleanValue
                    : Boolean.parseBoolean(String.valueOf(value));
        };
    }

    private int resolveArraySize(KafkaPayloadSchemaNode node, Random random) {
        int minItems = node.minItems() == null ? 1 : Math.max(0, node.minItems());
        int maxItems = node.maxItems() == null ? minItems : node.maxItems();
        if (maxItems <= minItems) {
            return minItems;
        }
        return minItems + random.nextInt(maxItems - minItems + 1);
    }

    private Map<String, WriteTask> resolveTasksByKey(
            WriteTaskGroupUpsertRequest request,
            List<WriteTask> persistedTasks
    ) {
        if (request.tasks() == null || request.tasks().isEmpty()) {
            throw new IllegalArgumentException("Relationship task group must contain at least one task");
        }

        Map<String, WriteTaskGroupTaskUpsertRequest> requestsByKey = new LinkedHashMap<>();
        for (WriteTaskGroupTaskUpsertRequest task : request.tasks()) {
            String taskKey = normalizeKey(task.taskKey(), "taskKey");
            if (requestsByKey.putIfAbsent(taskKey, task) != null) {
                throw new IllegalArgumentException("Duplicate taskKey: " + taskKey);
            }
        }

        Map<String, WriteTask> persistedByKey = persistedTasks == null
                ? Map.of()
                : persistedTasks.stream()
                        .filter(task -> task.getTaskKey() != null)
                        .collect(Collectors.toMap(WriteTask::getTaskKey, task -> task, (left, right) -> left, LinkedHashMap::new));

        Map<String, WriteTask> tasksByKey = new LinkedHashMap<>();
        for (WriteTaskGroupTaskUpsertRequest taskRequest : request.tasks()) {
            WriteTask task = persistedByKey.get(taskRequest.taskKey());
            if (task == null) {
                task = toRuntimeTask(taskRequest);
            }
            tasksByKey.put(taskRequest.taskKey(), task);
        }
        return tasksByKey;
    }

    private Map<String, RowPlan> resolveRowPlans(WriteTaskGroupUpsertRequest request) {
        Map<String, RowPlan> rowPlans = new HashMap<>();
        for (WriteTaskGroupTaskUpsertRequest task : request.tasks()) {
            rowPlans.put(task.taskKey(), toRowPlan(task));
        }
        return rowPlans;
    }

    private DatabaseResolvedDefinition resolveDatabaseDefinition(
            WriteTaskGroupUpsertRequest request,
            List<WriteTask> persistedTasks
    ) {
        Map<String, WriteTask> tasksByKey = resolveTasksByKey(request, persistedTasks);
        Map<String, RowPlan> rowPlans = resolveRowPlans(request);

        List<DatabaseResolvedRelation> resolvedRelations = new ArrayList<>();
        Map<String, List<DatabaseResolvedRelation>> incomingRelations = new LinkedHashMap<>();
        Set<String> relationNames = new HashSet<>();
        if (request.relations() != null) {
            for (WriteTaskGroupRelationUpsertRequest relation : request.relations()) {
                validateDatabaseRelation(relation, tasksByKey.keySet());
                if (!relationNames.add(relation.relationName().trim())) {
                    throw new IllegalArgumentException("Duplicate relation name: " + relation.relationName());
                }
                WriteTask parentTask = tasksByKey.get(relation.parentTaskKey());
                WriteTask childTask = tasksByKey.get(relation.childTaskKey());
                boolean rowDriver = Objects.equals(rowPlans.get(childTask.getTaskKey()).driverTaskKey(), relation.parentTaskKey());
                DatabaseResolvedRelation resolvedRelation = new DatabaseResolvedRelation(relation, parentTask, childTask, rowDriver);
                resolvedRelations.add(resolvedRelation);
                incomingRelations.computeIfAbsent(childTask.getTaskKey(), ignored -> new ArrayList<>()).add(resolvedRelation);
            }
        }

        List<String> topologicalOrder = topologicalSort(tasksByKey, resolvedRelations.stream()
                .map(relation -> new TaskEdge(relation.parentTask().getTaskKey(), relation.childTask().getTaskKey()))
                .toList());
        boolean requiresExistingRows = resolvedRelations.stream()
                .anyMatch(relation -> relation.request().sourceMode() != ReferenceSourceMode.CURRENT_BATCH);
        return new DatabaseResolvedDefinition(tasksByKey, incomingRelations, rowPlans, topologicalOrder, requiresExistingRows);
    }

    private KafkaResolvedDefinition resolveKafkaDefinition(
            WriteTaskGroupUpsertRequest request,
            List<WriteTask> persistedTasks
    ) {
        Map<String, WriteTask> tasksByKey = resolveTasksByKey(request, persistedTasks);
        Map<String, RowPlan> rowPlans = resolveRowPlans(request);

        List<KafkaResolvedRelation> resolvedRelations = new ArrayList<>();
        Map<String, List<KafkaResolvedRelation>> incomingRelations = new LinkedHashMap<>();
        Set<String> relationNames = new HashSet<>();
        if (request.relations() != null) {
            for (WriteTaskGroupRelationUpsertRequest relation : request.relations()) {
                validateKafkaRelation(relation, tasksByKey, rowPlans);
                if (!relationNames.add(relation.relationName().trim())) {
                    throw new IllegalArgumentException("Duplicate relation name: " + relation.relationName());
                }
                WriteTask parentTask = tasksByKey.get(relation.parentTaskKey());
                WriteTask childTask = tasksByKey.get(relation.childTaskKey());
                boolean rowDriver = Objects.equals(rowPlans.get(childTask.getTaskKey()).driverTaskKey(), relation.parentTaskKey());
                KafkaResolvedRelation resolvedRelation = new KafkaResolvedRelation(
                        relation,
                        parentTask,
                        childTask,
                        rowDriver,
                        parseKafkaRelationMappingConfig(relation, parentTask, childTask)
                );
                resolvedRelations.add(resolvedRelation);
                incomingRelations.computeIfAbsent(childTask.getTaskKey(), ignored -> new ArrayList<>()).add(resolvedRelation);
            }
        }

        List<String> topologicalOrder = topologicalSort(tasksByKey, resolvedRelations.stream()
                .map(relation -> new TaskEdge(relation.parentTask().getTaskKey(), relation.childTask().getTaskKey()))
                .toList());
        return new KafkaResolvedDefinition(tasksByKey, incomingRelations, rowPlans, topologicalOrder);
    }

    private void validateDatabaseRelation(
            WriteTaskGroupRelationUpsertRequest relation,
            Set<String> taskKeys
    ) {
        if (relation.relationMode() != WriteTaskRelationMode.DATABASE_COLUMNS) {
            throw new IllegalArgumentException("Database relationship must use DATABASE_COLUMNS mode");
        }
        if (!taskKeys.contains(relation.parentTaskKey())) {
            throw new IllegalArgumentException("Parent task does not exist: " + relation.parentTaskKey());
        }
        if (!taskKeys.contains(relation.childTaskKey())) {
            throw new IllegalArgumentException("Child task does not exist: " + relation.childTaskKey());
        }
        if (safeList(relation.parentColumns()).isEmpty() || safeList(relation.childColumns()).isEmpty()) {
            throw new IllegalArgumentException("Database relationship requires parent and child columns");
        }
        if (safeList(relation.parentColumns()).size() != safeList(relation.childColumns()).size()) {
            throw new IllegalArgumentException("Database relationship parent/child column counts must match");
        }
    }

    private void validateKafkaRelation(
            WriteTaskGroupRelationUpsertRequest relation,
            Map<String, WriteTask> tasksByKey,
            Map<String, RowPlan> rowPlans
    ) {
        if (relation.relationMode() != WriteTaskRelationMode.KAFKA_EVENT) {
            throw new IllegalArgumentException("Kafka relationship must use KAFKA_EVENT mode");
        }
        if (relation.mappingConfigJson() == null || relation.mappingConfigJson().isBlank()) {
            throw new IllegalArgumentException("Kafka relationship requires mappingConfigJson");
        }
        if (relation.sourceMode() != ReferenceSourceMode.CURRENT_BATCH) {
            throw new IllegalArgumentException("Kafka relationship currently only supports CURRENT_BATCH");
        }
        if (!tasksByKey.containsKey(relation.parentTaskKey())) {
            throw new IllegalArgumentException("Parent task does not exist: " + relation.parentTaskKey());
        }
        if (!tasksByKey.containsKey(relation.childTaskKey())) {
            throw new IllegalArgumentException("Child task does not exist: " + relation.childTaskKey());
        }
        RowPlan rowPlan = rowPlans.get(relation.childTaskKey());
        if (rowPlan != null
                && rowPlan.mode() == WriteTaskGroupRowPlanMode.CHILD_PER_PARENT
                && !Objects.equals(rowPlan.driverTaskKey(), relation.parentTaskKey())
                && relation.selectionStrategy() == com.datagenerator.task.domain.RelationSelectionStrategy.PARENT_DRIVEN) {
            throw new IllegalArgumentException("Kafka child_per_parent task must use its driver parent task");
        }
    }

    private KafkaRelationMappingConfig parseKafkaRelationMappingConfig(
            WriteTaskGroupRelationUpsertRequest relation,
            WriteTask parentTask,
            WriteTask childTask
    ) {
        try {
            KafkaRelationMappingConfig mappingConfig = objectMapper.readValue(
                    relation.mappingConfigJson(),
                    KafkaRelationMappingConfig.class
            );
            if (mappingConfig.fieldMappings() == null || mappingConfig.fieldMappings().isEmpty()) {
                throw new IllegalArgumentException("Kafka relationship must contain at least one field mapping");
            }
            List<String> parentPaths = availableKafkaPaths(parentTask);
            List<String> childPaths = availableKafkaPaths(childTask);
            for (KafkaRelationFieldMapping fieldMapping : mappingConfig.fieldMappings()) {
                if (fieldMapping.from() == null || fieldMapping.from().isBlank()) {
                    throw new IllegalArgumentException("Kafka relationship source path cannot be blank");
                }
                if (fieldMapping.to() == null || fieldMapping.to().isBlank()) {
                    throw new IllegalArgumentException("Kafka relationship target path cannot be blank");
                }
                if (!parentPaths.contains(fieldMapping.from())) {
                    throw new IllegalArgumentException("Kafka relationship source path does not exist in parent task: " + fieldMapping.from());
                }
                if (!childPaths.contains(fieldMapping.to())) {
                    throw new IllegalArgumentException("Kafka relationship target path does not exist in child task: " + fieldMapping.to());
                }
            }
            return mappingConfig;
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid Kafka relationship mappingConfigJson: " + exception.getMessage(), exception);
        }
    }

    private List<String> availableKafkaPaths(WriteTask task) {
        if (task.getPayloadSchemaJson() != null && !task.getPayloadSchemaJson().isBlank()) {
            KafkaPayloadSchemaNode payloadSchema = payloadSchemaService.parseAndValidate(task.getPayloadSchemaJson());
            return payloadSchemaService.collectScalarPaths(payloadSchema);
        }
        return task.getColumns().stream()
                .map(WriteTaskColumn::getColumnName)
                .filter(Objects::nonNull)
                .toList();
    }

    private WriteTask toRuntimeTask(WriteTaskGroupTaskUpsertRequest request) {
        WriteTask task = new WriteTask();
        task.setId(request.id());
        task.setTaskKey(request.taskKey());
        task.setName(request.name());
        task.setTableName(request.tableName());
        task.setBatchSize(request.batchSize());
        task.setDescription(request.description());
        task.setSeed(request.seed());
        task.setStatus(request.status());
        task.setTableMode(request.tableMode());
        task.setWriteMode(request.writeMode());
        task.setTargetConfigJson(request.targetConfigJson());
        task.setPayloadSchemaJson(request.payloadSchemaJson());
        task.setRowCount(request.rowPlan().rowCount() == null ? 1 : request.rowPlan().rowCount());
        task.replaceColumns(request.columns() == null ? List.of() : request.columns().stream()
                .map(this::toRuntimeColumn)
                .toList());
        return task;
    }

    private WriteTaskColumn toRuntimeColumn(WriteTaskGroupTaskColumnUpsertRequest request) {
        WriteTaskColumn column = new WriteTaskColumn();
        column.setColumnName(request.columnName());
        column.setDbType(request.dbType());
        column.setLengthValue(request.lengthValue());
        column.setPrecisionValue(request.precisionValue());
        column.setScaleValue(request.scaleValue());
        column.setNullableFlag(request.nullableFlag());
        column.setPrimaryKeyFlag(request.primaryKeyFlag());
        column.setForeignKeyFlag(request.foreignKeyFlag());
        column.setGeneratorType(request.generatorType());
        column.setGeneratorConfigJson(JsonSupport.writeMap(request.generatorConfig()));
        column.setSortOrder(request.sortOrder());
        return column;
    }

    private RowPlan toRowPlan(WriteTaskGroupTaskUpsertRequest task) {
        if (task.rowPlan() == null || task.rowPlan().mode() == null) {
            throw new IllegalArgumentException("Task " + task.taskKey() + " is missing row plan");
        }
        if (task.rowPlan().mode() == WriteTaskGroupRowPlanMode.FIXED && task.rowPlan().rowCount() == null) {
            throw new IllegalArgumentException("Task " + task.taskKey() + " requires a fixed row count");
        }
        if (task.rowPlan().mode() == WriteTaskGroupRowPlanMode.CHILD_PER_PARENT
                && (task.rowPlan().driverTaskKey() == null || task.rowPlan().driverTaskKey().isBlank())) {
            throw new IllegalArgumentException("Task " + task.taskKey() + " requires a driver parent task");
        }
        return new RowPlan(
                task.rowPlan().mode(),
                task.rowPlan().rowCount(),
                task.rowPlan().driverTaskKey(),
                task.rowPlan().minChildrenPerParent(),
                task.rowPlan().maxChildrenPerParent()
        );
    }

    private List<String> topologicalSort(Map<String, WriteTask> tasksByKey, List<TaskEdge> edges) {
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, List<String>> adjacency = new LinkedHashMap<>();
        tasksByKey.keySet().forEach(taskKey -> {
            indegree.put(taskKey, 0);
            adjacency.put(taskKey, new ArrayList<>());
        });
        for (TaskEdge edge : edges) {
            adjacency.get(edge.parentTaskKey()).add(edge.childTaskKey());
            indegree.put(edge.childTaskKey(), indegree.get(edge.childTaskKey()) + 1);
        }

        ArrayDeque<String> queue = new ArrayDeque<>();
        indegree.forEach((taskKey, degree) -> {
            if (degree == 0) {
                queue.add(taskKey);
            }
        });

        List<String> ordered = new ArrayList<>(tasksByKey.size());
        while (!queue.isEmpty()) {
            String taskKey = queue.removeFirst();
            ordered.add(taskKey);
            for (String child : adjacency.get(taskKey)) {
                int nextDegree = indegree.get(child) - 1;
                indegree.put(child, nextDegree);
                if (nextDegree == 0) {
                    queue.add(child);
                }
            }
        }
        if (ordered.size() != tasksByKey.size()) {
            throw new IllegalArgumentException("Relationship graph contains a cycle");
        }
        return ordered;
    }

    private boolean shouldAssignNull(WriteTaskGroupRelationUpsertRequest relation, Random random) {
        double nullRate = relation.nullRate() == null ? 0D : relation.nullRate();
        return nullRate > 0 && random.nextDouble() <= nullRate;
    }

    private Map<String, Object> pickRandom(List<Map<String, Object>> candidates, Random random) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        return candidates.get(random.nextInt(candidates.size()));
    }

    private boolean containsAllColumns(Map<String, Object> row, List<String> columns) {
        for (String column : columns) {
            if (row.get(column) == null) {
                return false;
            }
        }
        return true;
    }

    private long countPrimaryKeyDuplicates(WriteTask task, List<Map<String, Object>> rows) {
        List<String> primaryKeys = task.getColumns().stream()
                .filter(WriteTaskColumn::isPrimaryKeyFlag)
                .map(WriteTaskColumn::getColumnName)
                .toList();
        if (primaryKeys.isEmpty()) {
            return 0L;
        }
        Set<String> uniqueKeys = new HashSet<>();
        long duplicates = 0L;
        for (Map<String, Object> row : rows) {
            String compositeKey = primaryKeys.stream()
                    .map(column -> String.valueOf(row.get(column)))
                    .reduce((left, right) -> left + "|" + right)
                    .orElse("");
            if (!uniqueKeys.add(compositeKey)) {
                duplicates++;
            }
        }
        return duplicates;
    }

    private String normalizeKey(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value.trim();
    }

    private int sanitizePreviewCount(Integer previewCount) {
        if (previewCount == null || previewCount < 1) {
            return 5;
        }
        return Math.min(previewCount, 20);
    }

    private Object resolvePathValue(Object current, String keyPath) {
        if (keyPath == null || keyPath.isBlank()) {
            return null;
        }
        return resolvePathValue(current, keyPath.split("\\."), 0);
    }

    private Object resolvePathValue(Object current, String[] segments, int index) {
        if (current == null) {
            return null;
        }
        if (index >= segments.length) {
            return current;
        }

        String segment = segments[index];
        if (segment.endsWith("[]")) {
            Object next = resolveObjectSegment(current, segment.substring(0, segment.length() - 2));
            if (!(next instanceof List<?> listValue)) {
                return null;
            }
            ArrayList<Object> values = new ArrayList<>();
            for (Object item : listValue) {
                Object nested = resolvePathValue(item, segments, index + 1);
                if (nested instanceof List<?> nestedList) {
                    values.addAll(nestedList);
                } else if (nested != null) {
                    values.add(nested);
                }
            }
            return values;
        }

        Matcher matcher = INDEXED_SEGMENT.matcher(segment);
        if (matcher.matches()) {
            Object next = resolveObjectSegment(current, matcher.group(1));
            if (!(next instanceof List<?> listValue)) {
                return null;
            }
            int itemIndex = Integer.parseInt(matcher.group(2));
            if (itemIndex < 0 || itemIndex >= listValue.size()) {
                return null;
            }
            return resolvePathValue(listValue.get(itemIndex), segments, index + 1);
        }

        Object next = resolveObjectSegment(current, segment);
        return resolvePathValue(next, segments, index + 1);
    }

    private boolean assignPathValue(Map<String, Object> root, String path, Object value) {
        if (path == null || path.isBlank()) {
            return false;
        }
        return assignPathValue(root, path.split("\\."), 0, value);
    }

    @SuppressWarnings("unchecked")
    private boolean assignPathValue(Object current, String[] segments, int index, Object value) {
        if (current == null || index >= segments.length) {
            return false;
        }
        String segment = segments[index];

        if (segment.endsWith("[]")) {
            if (!(current instanceof Map<?, ?> mapValue)) {
                return false;
            }
            Object next = mapValue.get(segment.substring(0, segment.length() - 2));
            if (!(next instanceof List<?> listValue)) {
                return false;
            }
            if (index == segments.length - 1) {
                if (value instanceof List<?> sourceList && sourceList.size() == listValue.size()) {
                    for (int itemIndex = 0; itemIndex < listValue.size(); itemIndex++) {
                        ((List<Object>) listValue).set(itemIndex, deepCopyValue(sourceList.get(itemIndex)));
                    }
                } else {
                    for (int itemIndex = 0; itemIndex < listValue.size(); itemIndex++) {
                        ((List<Object>) listValue).set(itemIndex, deepCopyValue(value));
                    }
                }
                return true;
            }
            boolean assigned = false;
            for (Object item : listValue) {
                assigned |= assignPathValue(item, segments, index + 1, value);
            }
            return assigned;
        }

        Matcher matcher = INDEXED_SEGMENT.matcher(segment);
        if (matcher.matches()) {
            if (!(current instanceof Map<?, ?> mapValue)) {
                return false;
            }
            Object next = mapValue.get(matcher.group(1));
            if (!(next instanceof List<?> listValue)) {
                return false;
            }
            int itemIndex = Integer.parseInt(matcher.group(2));
            if (itemIndex < 0 || itemIndex >= listValue.size()) {
                return false;
            }
            if (index == segments.length - 1) {
                ((List<Object>) listValue).set(itemIndex, deepCopyValue(value));
                return true;
            }
            return assignPathValue(listValue.get(itemIndex), segments, index + 1, value);
        }

        if (!(current instanceof Map<?, ?> mapValue)) {
            return false;
        }
        Map<String, Object> mutableMap = (Map<String, Object>) mapValue;
        if (index == segments.length - 1) {
            mutableMap.put(segment, deepCopyValue(value));
            return true;
        }
        return assignPathValue(mutableMap.get(segment), segments, index + 1, value);
    }

    private Object resolveObjectSegment(Object current, String segment) {
        if (segment == null || segment.isBlank()) {
            return current;
        }
        if (!(current instanceof Map<?, ?> mapValue)) {
            return null;
        }
        return mapValue.get(segment);
    }

    private Object deepCopyValue(Object value) {
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        return objectMapper.convertValue(value, Object.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castRow(Map<?, ?> rawRow) {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        rawRow.forEach((key, value) -> row.put(String.valueOf(key), value));
        return row;
    }

    private List<String> safeList(List<String> values) {
        return values == null ? List.of() : values;
    }

    @SafeVarargs
    private final <T> T firstNonNull(T... values) {
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private String appendPath(String path, String segment) {
        if (path == null || path.isBlank()) {
            return segment;
        }
        return path + "." + segment;
    }

    private record DatabaseResolvedDefinition(
            Map<String, WriteTask> tasksByKey,
            Map<String, List<DatabaseResolvedRelation>> incomingRelations,
            Map<String, RowPlan> rowPlans,
            List<String> topologicalOrder,
            boolean requiresExistingRows
    ) {
    }

    private record DatabaseResolvedRelation(
            WriteTaskGroupRelationUpsertRequest request,
            WriteTask parentTask,
            WriteTask childTask,
            boolean rowDriver
    ) {
    }

    private record KafkaResolvedDefinition(
            Map<String, WriteTask> tasksByKey,
            Map<String, List<KafkaResolvedRelation>> incomingRelations,
            Map<String, RowPlan> rowPlans,
            List<String> topologicalOrder
    ) {
    }

    private record KafkaResolvedRelation(
            WriteTaskGroupRelationUpsertRequest request,
            WriteTask parentTask,
            WriteTask childTask,
            boolean rowDriver,
            KafkaRelationMappingConfig mappingConfig
    ) {
    }

    private record TaskEdge(String parentTaskKey, String childTaskKey) {
    }

    private record RowPlan(
            WriteTaskGroupRowPlanMode mode,
            Integer rowCount,
            String driverTaskKey,
            Integer minChildrenPerParent,
            Integer maxChildrenPerParent
    ) {
    }

    private record GeneratedRowsResult(
            List<Map<String, Object>> rows,
            int foreignKeyMissCount,
            int nullViolationCount,
            int blankStringCount
    ) {
    }

    private record ValidationSummary(long nullViolationCount, long blankStringCount) {
    }

    private record KafkaRelationMappingConfig(List<KafkaRelationFieldMapping> fieldMappings) {
    }

    private record KafkaRelationFieldMapping(String from, String to, boolean required) {
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

    private static final class JsonSupport {

        private JsonSupport() {
        }

        private static Map<String, Object> readMap(String json) {
            return JsonConfigSupport.readConfig(json, "generatorConfigJson");
        }

        private static String writeMap(Map<String, Object> value) {
            return JsonConfigSupport.writeJson(value == null ? Map.of() : value, "generatorConfig");
        }
    }
}

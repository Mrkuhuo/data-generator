package com.datagenerator.connection.application;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.connection.api.ConnectionTestResponse;
import com.datagenerator.connection.api.DatabaseColumnResponse;
import com.datagenerator.connection.api.DatabaseModelResponse;
import com.datagenerator.connection.api.DatabaseTableResponse;
import com.datagenerator.connection.api.DatabaseTableSchemaResponse;
import com.datagenerator.connection.api.TargetConnectionDraftTestRequest;
import com.datagenerator.connection.api.TargetConnectionUpsertRequest;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.connection.repository.TargetConnectionRepository;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TargetConnectionService {

    private final TargetConnectionRepository repository;
    private final ConnectionProbeService connectionProbeService;
    private final TableSchemaIntrospectionService tableSchemaIntrospectionService;
    private final ConnectionJdbcSupport connectionJdbcSupport;
    private final KafkaConnectionSupport kafkaConnectionSupport;

    public TargetConnectionService(
            TargetConnectionRepository repository,
            ConnectionProbeService connectionProbeService,
            TableSchemaIntrospectionService tableSchemaIntrospectionService,
            ConnectionJdbcSupport connectionJdbcSupport,
            KafkaConnectionSupport kafkaConnectionSupport
    ) {
        this.repository = repository;
        this.connectionProbeService = connectionProbeService;
        this.tableSchemaIntrospectionService = tableSchemaIntrospectionService;
        this.connectionJdbcSupport = connectionJdbcSupport;
        this.kafkaConnectionSupport = kafkaConnectionSupport;
    }

    public List<TargetConnection> findAll() {
        return repository.findAll(Sort.by(Sort.Direction.DESC, "updatedAt"));
    }

    public TargetConnection findById(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("未找到目标连接: " + id));
    }

    @Transactional
    public TargetConnection create(TargetConnectionUpsertRequest request) {
        TargetConnection connection = new TargetConnection();
        apply(connection, request, false);
        return repository.save(connection);
    }

    @Transactional
    public TargetConnection update(Long id, TargetConnectionUpsertRequest request) {
        TargetConnection connection = findById(id);
        apply(connection, request, true);
        return repository.save(connection);
    }

    @Transactional
    public void delete(Long id) {
        repository.deleteById(id);
    }

    @Transactional
    public ConnectionTestResponse test(Long id) {
        return connectionProbeService.test(findById(id));
    }

    public ConnectionTestResponse testDraft(TargetConnectionDraftTestRequest request) {
        TargetConnection connection = new TargetConnection();
        boolean keepPasswordWhenBlank = request.connectionId() != null;
        if (keepPasswordWhenBlank) {
            TargetConnection existing = findById(request.connectionId());
            connection.setId(existing.getId());
            connection.setPasswordValue(existing.getPasswordValue());
            connection.setUsername(existing.getUsername());
        }
        apply(connection, request.connection(), keepPasswordWhenBlank);
        return connectionProbeService.probe(connection);
    }

    public List<DatabaseTableResponse> listTables(Long id) {
        return tableSchemaIntrospectionService.listTables(findById(id));
    }

    public List<DatabaseColumnResponse> listColumns(Long id, String tableName) {
        return tableSchemaIntrospectionService.listColumns(findById(id), tableName);
    }

    public DatabaseTableSchemaResponse describeTable(Long id, String tableName) {
        return tableSchemaIntrospectionService.describeTable(findById(id), tableName);
    }

    public DatabaseModelResponse describeModel(Long id, List<String> tableNames) {
        return tableSchemaIntrospectionService.describeModel(findById(id), tableNames);
    }

    public long count() {
        return repository.count();
    }

    private void apply(TargetConnection connection, TargetConnectionUpsertRequest request, boolean keepPasswordWhenBlank) {
        connection.setName(requireText(request.name(), "name"));
        connection.setDbType(request.dbType());
        connection.setStatus(request.status());
        connection.setDescription(normalizeText(request.description()));

        if (request.dbType() == DatabaseType.KAFKA) {
            applyKafka(connection, request, keepPasswordWhenBlank);
            return;
        }

        connection.setHost(requireText(request.host(), "host"));
        connection.setPort(requirePort(request.port()));
        connection.setDatabaseName(requireText(request.databaseName(), "databaseName"));
        connection.setSchemaName(normalizeText(request.schemaName()));
        connection.setUsername(requireText(request.username(), "username"));
        connection.setJdbcParams(connectionJdbcSupport.normalizeParamsForStorage(request.dbType(), request.jdbcParams()));
        connection.setConfigJson(JsonConfigSupport.normalizeJson(request.configJson(), "configJson"));

        if (request.password() != null && !request.password().isBlank()) {
            connection.setPasswordValue(request.password());
        } else if (!keepPasswordWhenBlank || connection.getPasswordValue() == null || connection.getPasswordValue().isBlank()) {
            throw new IllegalArgumentException("password 不能为空");
        }
    }

    private void applyKafka(TargetConnection connection, TargetConnectionUpsertRequest request, boolean keepPasswordWhenBlank) {
        String normalizedConfigJson = JsonConfigSupport.normalizeJson(request.configJson(), "configJson");
        Map<String, Object> config = new LinkedHashMap<>(JsonConfigSupport.readConfig(normalizedConfigJson, "configJson"));
        String bootstrapServers = kafkaConnectionSupport.bootstrapServers(config);
        KafkaConnectionSupport.BootstrapEndpoint endpoint = kafkaConnectionSupport.firstEndpoint(bootstrapServers);

        connection.setHost(defaulted(normalizeText(request.host()), endpoint.host()));
        connection.setPort(request.port() == null ? endpoint.port() : request.port());
        connection.setDatabaseName(defaulted(normalizeText(request.databaseName()), "kafka"));
        connection.setSchemaName(null);
        connection.setJdbcParams(null);

        String username = defaulted(
                normalizeText(request.username()),
                JsonConfigSupport.optionalString(config, "username", "sasl.username")
        );
        connection.setUsername(username == null ? "" : username);

        if (request.password() != null && !request.password().isBlank()) {
            connection.setPasswordValue(request.password());
        } else if (!keepPasswordWhenBlank || connection.getPasswordValue() == null) {
            String password = JsonConfigSupport.optionalString(config, "password", "sasl.password");
            connection.setPasswordValue(password == null ? "" : password);
        }

        Map<String, Object> sanitized = kafkaConnectionSupport.sanitizeConfig(config);
        sanitized.put("bootstrapServers", bootstrapServers);
        connection.setConfigJson(JsonConfigSupport.writeJson(sanitized, "连接配置序列化失败"));
    }

    private String requireText(String value, String field) {
        String normalized = normalizeText(value);
        if (normalized == null) {
            throw new IllegalArgumentException(field + " 不能为空");
        }
        return normalized;
    }

    private Integer requirePort(Integer value) {
        if (value == null) {
            throw new IllegalArgumentException("port 不能为空");
        }
        return value;
    }

    private String normalizeText(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private String defaulted(String preferred, String fallback) {
        return preferred == null || preferred.isBlank() ? fallback : preferred;
    }
}

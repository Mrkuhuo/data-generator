package com.datagenerator.connection.application;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.connection.domain.TargetConnection;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

@Component
public class KafkaConnectionSupport {

    public Map<String, Object> readConfig(TargetConnection connection) {
        return JsonConfigSupport.readConfig(connection.getConfigJson(), "连接配置");
    }

    public String bootstrapServers(TargetConnection connection) {
        return bootstrapServers(readConfig(connection));
    }

    public String bootstrapServers(Map<String, Object> config) {
        return JsonConfigSupport.requireString(config, "bootstrapServers", "bootstrap.servers");
    }

    public BootstrapEndpoint firstEndpoint(String bootstrapServers) {
        String first = bootstrapServers.split(",")[0].trim();
        int schemeIndex = first.indexOf("://");
        if (schemeIndex >= 0) {
            first = first.substring(schemeIndex + 3);
        }

        if (first.startsWith("[")) {
            int closing = first.indexOf(']');
            if (closing > 0) {
                String host = first.substring(0, closing + 1);
                int port = parsePort(first.substring(closing + 1), 9092);
                return new BootstrapEndpoint(host, port);
            }
        }

        int colonIndex = first.lastIndexOf(':');
        if (colonIndex < 0) {
            return new BootstrapEndpoint(first, 9092);
        }
        String host = first.substring(0, colonIndex).trim();
        int port = parsePort(first.substring(colonIndex + 1), 9092);
        return new BootstrapEndpoint(host, port);
    }

    public Properties buildAdminProperties(TargetConnection connection) {
        Map<String, Object> config = readConfig(connection);
        Properties properties = buildBaseProperties(connection, config);
        properties.put(
                CommonClientConfigs.CLIENT_ID_CONFIG,
                resolveClientId(config, "mdg-admin-" + safeName(connection.getName()))
        );
        properties.put(
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                String.valueOf(defaulted(JsonConfigSupport.optionalInteger(config, "defaultApiTimeoutMs"), 5000))
        );
        properties.put(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                String.valueOf(defaulted(JsonConfigSupport.optionalInteger(config, "requestTimeoutMs"), 5000))
        );
        return properties;
    }

    public Properties buildProducerProperties(TargetConnection connection, Long executionId) {
        Map<String, Object> config = readConfig(connection);
        Properties properties = buildBaseProperties(connection, config);
        properties.put(
                CommonClientConfigs.CLIENT_ID_CONFIG,
                resolveClientId(config, "mdg-writer-" + (executionId == null ? "manual" : executionId))
        );
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(
                ProducerConfig.ACKS_CONFIG,
                defaulted(JsonConfigSupport.optionalString(config, "acks"), "all")
        );
        properties.put(
                ProducerConfig.RETRIES_CONFIG,
                String.valueOf(defaulted(JsonConfigSupport.optionalInteger(config, "retries"), 3))
        );
        properties.put(
                ProducerConfig.LINGER_MS_CONFIG,
                String.valueOf(defaulted(JsonConfigSupport.optionalInteger(config, "lingerMs"), 0))
        );
        properties.put(
                ProducerConfig.MAX_BLOCK_MS_CONFIG,
                String.valueOf(defaulted(JsonConfigSupport.optionalInteger(config, "maxBlockMs"), 10000))
        );
        properties.put(
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                String.valueOf(defaulted(JsonConfigSupport.optionalInteger(config, "deliveryTimeoutMs"), 15000))
        );

        String compressionType = JsonConfigSupport.optionalString(config, "compressionType", "compression.type");
        if (compressionType != null) {
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        }
        return properties;
    }

    public Map<String, Object> sanitizeConfig(Map<String, Object> rawConfig) {
        LinkedHashMap<String, Object> sanitized = new LinkedHashMap<>(rawConfig);
        sanitized.remove("password");
        sanitized.remove("sasl.password");
        Object sasl = sanitized.get("sasl");
        if (sasl instanceof Map<?, ?> rawSasl) {
            LinkedHashMap<String, Object> sanitizedSasl = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawSasl.entrySet()) {
                if (entry.getKey() == null || "password".equals(String.valueOf(entry.getKey()))) {
                    continue;
                }
                sanitizedSasl.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            sanitized.put("sasl", sanitizedSasl);
        }
        return sanitized;
    }

    public String resolveUsername(TargetConnection connection, Map<String, Object> config) {
        if (connection.getUsername() != null && !connection.getUsername().isBlank()) {
            return connection.getUsername().trim();
        }
        return JsonConfigSupport.optionalString(config, "username", "sasl.username");
    }

    public String resolvePassword(TargetConnection connection, Map<String, Object> config) {
        if (connection.getPasswordValue() != null && !connection.getPasswordValue().isBlank()) {
            return connection.getPasswordValue();
        }
        return JsonConfigSupport.optionalString(config, "password", "sasl.password");
    }

    private Properties buildBaseProperties(TargetConnection connection, Map<String, Object> config) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(config));
        properties.put(
                CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
                String.valueOf(defaulted(JsonConfigSupport.optionalInteger(config, "requestTimeoutMs"), 5000))
        );
        properties.put(
                CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                String.valueOf(defaulted(JsonConfigSupport.optionalInteger(config, "connectionsMaxIdleMs"), 5000))
        );

        String securityProtocol = JsonConfigSupport.optionalString(config, "securityProtocol", "security.protocol");
        if (securityProtocol != null) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        }

        String saslMechanism = JsonConfigSupport.optionalString(config, "saslMechanism", "sasl.mechanism");
        if (saslMechanism != null) {
            properties.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        }

        String username = resolveUsername(connection, config);
        String password = resolvePassword(connection, config);
        if (requiresSaslLogin(securityProtocol, saslMechanism) && username != null && password != null) {
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig(saslMechanism, username, password));
        }

        applyPassthroughProperties(properties, config);
        return properties;
    }

    private void applyPassthroughProperties(Properties properties, Map<String, Object> config) {
        Object customProperties = JsonConfigSupport.findValue(config, "properties");
        if (!(customProperties instanceof Map<?, ?> rawProperties)) {
            return;
        }
        for (Map.Entry<?, ?> entry : rawProperties.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                properties.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
        }
    }

    private String resolveClientId(Map<String, Object> config, String fallback) {
        return defaulted(JsonConfigSupport.optionalString(config, "clientId", "client.id"), fallback);
    }

    private boolean requiresSaslLogin(String securityProtocol, String saslMechanism) {
        if (saslMechanism != null && !saslMechanism.isBlank()) {
            return true;
        }
        if (securityProtocol == null || securityProtocol.isBlank()) {
            return false;
        }
        return securityProtocol.toUpperCase(Locale.ROOT).startsWith("SASL");
    }

    private String buildJaasConfig(String saslMechanism, String username, String password) {
        String mechanism = defaulted(saslMechanism, "PLAIN").toUpperCase(Locale.ROOT);
        String loginModule = switch (mechanism) {
            case "PLAIN" -> "org.apache.kafka.common.security.plain.PlainLoginModule";
            case "SCRAM-SHA-256", "SCRAM-SHA-512" -> "org.apache.kafka.common.security.scram.ScramLoginModule";
            default -> throw new IllegalArgumentException("暂不支持的 SASL 机制: " + mechanism);
        };
        return loginModule
                + " required username=\""
                + escape(username)
                + "\" password=\""
                + escape(password)
                + "\";";
    }

    private String safeName(String value) {
        if (value == null || value.isBlank()) {
            return "default";
        }
        return value.trim().replaceAll("[^a-zA-Z0-9._-]", "-");
    }

    private String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private int defaulted(Integer value, int fallback) {
        return value == null ? fallback : value;
    }

    private String defaulted(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private int parsePort(String rawPort, int fallback) {
        if (rawPort == null) {
            return fallback;
        }
        String normalized = rawPort.trim();
        if (normalized.startsWith(":")) {
            normalized = normalized.substring(1).trim();
        }
        if (normalized.isBlank()) {
            return fallback;
        }
        return Integer.parseInt(normalized);
    }

    public record BootstrapEndpoint(String host, int port) {
    }
}

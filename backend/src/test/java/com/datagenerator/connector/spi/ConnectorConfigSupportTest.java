package com.datagenerator.connector.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConnectorConfigSupportTest {

    @Test
    void shouldReadNestedConfigValuesAcrossSupportedTypes() {
        Map<String, Object> config = ConnectorConfigSupport.readConfig("""
                {
                  "target": {
                    "table": "synthetic_user_activity",
                    "batchSize": 500,
                    "enabled": true,
                    "columns": ["userId", "city"]
                  },
                  "bootstrapServers": "localhost:9092"
                }
                """, "runtime");

        assertThat(ConnectorConfigSupport.requireString(config, "bootstrapServers")).isEqualTo("localhost:9092");
        assertThat(ConnectorConfigSupport.requireString(config, "target.table")).isEqualTo("synthetic_user_activity");
        assertThat(ConnectorConfigSupport.optionalInteger(config, "target.batchSize")).isEqualTo(500);
        assertThat(ConnectorConfigSupport.optionalBoolean(config, "target.enabled")).isTrue();
        assertThat(ConnectorConfigSupport.optionalStringList(config, "target.columns")).isEqualTo(List.of("userId", "city"));
        assertThat(ConnectorConfigSupport.findValue(config, "target.table")).isEqualTo("synthetic_user_activity");
    }

    @Test
    void shouldHandleBlankAndCommaSeparatedInputs() {
        Map<String, Object> empty = ConnectorConfigSupport.readConfig("   ", "empty");
        Map<String, Object> config = Map.of("columns", "userId, city ,email");

        assertThat(empty).isEmpty();
        assertThat(ConnectorConfigSupport.optionalStringList(config, "columns"))
                .isEqualTo(List.of("userId", "city", "email"));
        assertThat(ConnectorConfigSupport.optionalString(empty, "missing")).isNull();
    }

    @Test
    void shouldFailFastForInvalidOrMissingRequiredFields() {
        assertThatThrownBy(() -> ConnectorConfigSupport.readConfig("{bad-json", "connector"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connector 不是合法的 JSON");

        assertThatThrownBy(() -> ConnectorConfigSupport.requireString(Map.of(), "jdbcUrl", "url"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("缺少配置字段");
    }
}

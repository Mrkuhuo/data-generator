package com.datagenerator.connection.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.datagenerator.connection.domain.TargetConnection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

class KafkaConnectionSupportTest {

    private final KafkaConnectionSupport support = new KafkaConnectionSupport();

    @Test
    void buildProducerProperties_shouldApplyFailFastDefaults() {
        TargetConnection connection = kafkaConnection("""
                {
                  "bootstrapServers": "localhost:9092"
                }
                """);

        var properties = support.buildProducerProperties(connection, 99L);

        assertThat(properties.get(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG)).isEqualTo("5000");
        assertThat(properties.get(ProducerConfig.MAX_BLOCK_MS_CONFIG)).isEqualTo("10000");
        assertThat(properties.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)).isEqualTo("15000");
    }

    @Test
    void buildProducerProperties_shouldHonorCustomMaxBlockMs() {
        TargetConnection connection = kafkaConnection("""
                {
                  "bootstrapServers": "localhost:9092",
                  "maxBlockMs": 2500
                }
                """);

        var properties = support.buildProducerProperties(connection, 99L);

        assertThat(properties.get(ProducerConfig.MAX_BLOCK_MS_CONFIG)).isEqualTo("2500");
    }

    private TargetConnection kafkaConnection(String configJson) {
        TargetConnection connection = new TargetConnection();
        connection.setName("kafka-demo");
        connection.setConfigJson(configJson);
        return connection;
    }
}

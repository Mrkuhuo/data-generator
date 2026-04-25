package com.datagenerator.dev;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

public final class EmbeddedKafkaDevServer {

    private EmbeddedKafkaDevServer() {
    }

    public static void main(String[] args) throws Exception {
        int port = 19092;
        String readyFile = null;

        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            if ("--port".equals(argument) && index + 1 < args.length) {
                port = Integer.parseInt(args[++index]);
            } else if ("--ready-file".equals(argument) && index + 1 < args.length) {
                readyFile = args[++index];
            }
        }

        EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, 1);
        broker.kafkaPorts(port);
        broker.brokerProperties(defaultBrokerProperties());
        broker.afterPropertiesSet();

        Runtime.getRuntime().addShutdownHook(new Thread(broker::destroy, "embedded-kafka-dev-shutdown"));

        String brokers = broker.getBrokersAsString();
        System.out.println("EMBEDDED_KAFKA_BROKERS=" + brokers);
        System.out.println("EMBEDDED_KAFKA_PORT=" + port);
        if (readyFile != null && !readyFile.isBlank()) {
            Files.writeString(
                    Path.of(readyFile),
                    brokers,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        }

        new CountDownLatch(1).await();
    }

    private static Map<String, String> defaultBrokerProperties() {
        LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        properties.put("auto.create.topics.enable", "true");
        properties.put("delete.topic.enable", "true");
        properties.put("num.partitions", "3");
        properties.put("offsets.topic.replication.factor", "1");
        properties.put("transaction.state.log.replication.factor", "1");
        properties.put("transaction.state.log.min.isr", "1");
        return properties;
    }
}

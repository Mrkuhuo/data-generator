package com.datagenerator.connector.spi.impl;

import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.domain.ConnectorType;
import com.datagenerator.connector.spi.ConnectorAdapter;
import com.datagenerator.connector.spi.ConnectorConfigSupport;
import com.datagenerator.connector.spi.ConnectorDeliveryRequest;
import com.datagenerator.connector.spi.ConnectorDeliveryResult;
import com.datagenerator.connector.spi.ConnectorTestResult;
import com.datagenerator.connector.spi.DeliveryStatus;
import com.datagenerator.job.domain.JobWriteStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class FileConnectorAdapter implements ConnectorAdapter {

    private final ObjectMapper objectMapper;

    public FileConnectorAdapter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public ConnectorType supports() {
        return ConnectorType.FILE;
    }

    @Override
    public ConnectorTestResult test(ConnectorInstance connector) {
        Map<String, Object> config = ConnectorConfigSupport.readConfig(connector);
        String pathValue = ConnectorConfigSupport.requireString(config, "path");
        String format = config.getOrDefault("format", "jsonl").toString();

        Path outputPath = Path.of(pathValue).toAbsolutePath().normalize();
        Path parent = outputPath.getParent();
        if (parent == null) {
            return new ConnectorTestResult(false, "INVALID_PATH", "Output path must include a parent directory", "{\"path\":\"" + outputPath + "\"}");
        }

        boolean parentExists = Files.exists(parent);
        if (!parentExists) {
            try {
                Files.createDirectories(parent);
                parentExists = Files.exists(parent);
            } catch (Exception exception) {
                Map<String, Object> failedDetails = new LinkedHashMap<>();
                failedDetails.put("path", outputPath.toString());
                failedDetails.put("format", format);
                failedDetails.put("error", exception.getMessage());
                return new ConnectorTestResult(false, "PATH_CREATE_FAILED", "Failed to create parent directory", ConnectorConfigSupport.writeDetails(failedDetails));
            }
        }
        boolean writable = parentExists && Files.isWritable(parent);

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("path", outputPath.toString());
        details.put("format", format);
        details.put("parentExists", parentExists);
        details.put("writable", writable);

        if (!writable) {
            return new ConnectorTestResult(false, "NOT_WRITABLE", "Parent directory is not writable", ConnectorConfigSupport.writeDetails(details));
        }
        return new ConnectorTestResult(true, "READY", "File output path is writable", ConnectorConfigSupport.writeDetails(details));
    }

    @Override
    public ConnectorDeliveryResult deliver(ConnectorDeliveryRequest request) {
        Map<String, Object> config = ConnectorConfigSupport.readConfig(request.connector());
        String pathValue = ConnectorConfigSupport.requireString(config, "path");
        String format = config.getOrDefault("format", "jsonl").toString().toLowerCase();
        Path outputPath = Path.of(pathValue).toAbsolutePath().normalize();

        try {
            Files.createDirectories(outputPath.getParent());

            return switch (format) {
                case "jsonl" -> writeJsonLines(request, outputPath);
                case "json" -> writeJson(request, outputPath);
                case "csv" -> writeCsv(request, outputPath);
                default -> new ConnectorDeliveryResult(
                        DeliveryStatus.FAILED,
                        0,
                        request.rows().size(),
                        "Unsupported file format: " + format,
                        "{\"format\":\"" + format + "\"}"
                );
            };
        } catch (Exception exception) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("path", outputPath.toString());
            details.put("error", exception.getMessage());
            return new ConnectorDeliveryResult(
                    DeliveryStatus.FAILED,
                    0,
                    request.rows().size(),
                    "File delivery failed",
                    ConnectorConfigSupport.writeDetails(details)
            );
        }
    }

    private ConnectorDeliveryResult writeJsonLines(ConnectorDeliveryRequest request, Path outputPath) throws Exception {
        List<String> lines = new ArrayList<>();
        for (Map<String, Object> row : request.rows()) {
            lines.add(objectMapper.writeValueAsString(row));
        }

        if (request.job().getWriteStrategy() == JobWriteStrategy.OVERWRITE) {
            Files.write(outputPath, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } else {
            appendLines(outputPath, lines);
        }

        return successResult(request.rows().size(), outputPath, "jsonl");
    }

    private ConnectorDeliveryResult writeJson(ConnectorDeliveryRequest request, Path outputPath) throws Exception {
        if (request.job().getWriteStrategy() != JobWriteStrategy.OVERWRITE) {
            return new ConnectorDeliveryResult(
                    DeliveryStatus.FAILED,
                    0,
                    request.rows().size(),
                    "JSON file output currently supports OVERWRITE only",
                    "{\"format\":\"json\",\"writeStrategy\":\"" + request.job().getWriteStrategy() + "\"}"
            );
        }

        objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputPath.toFile(), request.rows());
        return successResult(request.rows().size(), outputPath, "json");
    }

    private ConnectorDeliveryResult writeCsv(ConnectorDeliveryRequest request, Path outputPath) throws Exception {
        LinkedHashSet<String> headers = new LinkedHashSet<>();
        for (Map<String, Object> row : request.rows()) {
            headers.addAll(row.keySet());
        }

        boolean overwrite = request.job().getWriteStrategy() == JobWriteStrategy.OVERWRITE;
        boolean fileExists = Files.exists(outputPath);
        List<String> lines = new ArrayList<>();

        if (overwrite || !fileExists || Files.size(outputPath) == 0) {
            lines.add(String.join(",", headers));
        }

        for (Map<String, Object> row : request.rows()) {
            List<String> values = new ArrayList<>();
            for (String header : headers) {
                values.add(escapeCsvValue(row.get(header)));
            }
            lines.add(String.join(",", values));
        }

        if (overwrite) {
            Files.write(outputPath, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } else {
            appendLines(outputPath, lines);
        }

        return successResult(request.rows().size(), outputPath, "csv");
    }

    private String escapeCsvValue(Object value) {
        String normalized = value == null ? "" : value.toString();
        String escaped = normalized.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }

    private ConnectorDeliveryResult successResult(long rowCount, Path outputPath, String format) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("path", outputPath.toString());
        details.put("format", format);
        details.put("rows", rowCount);
        return new ConnectorDeliveryResult(
                DeliveryStatus.SUCCESS,
                rowCount,
                0,
                "Delivered " + rowCount + " rows to file output",
                ConnectorConfigSupport.writeDetails(details)
        );
    }

    private void appendLines(Path outputPath, List<String> lines) throws Exception {
        boolean fileExists = Files.exists(outputPath);
        boolean needsLeadingLineBreak = fileExists && Files.size(outputPath) > 0;
        String payload = String.join(System.lineSeparator(), lines);
        if (needsLeadingLineBreak) {
            payload = System.lineSeparator() + payload;
        }
        Files.writeString(outputPath, payload, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}

package com.datagenerator.connector.spi.impl;

import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.domain.ConnectorType;
import com.datagenerator.connector.spi.ConnectorAdapter;
import com.datagenerator.connector.spi.ConnectorConfigSupport;
import com.datagenerator.connector.spi.ConnectorDeliveryRequest;
import com.datagenerator.connector.spi.ConnectorDeliveryResult;
import com.datagenerator.connector.spi.ConnectorTestResult;
import com.datagenerator.connector.spi.DeliveryStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class HttpConnectorAdapter implements ConnectorAdapter {

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public HttpConnectorAdapter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Override
    public ConnectorType supports() {
        return ConnectorType.HTTP;
    }

    @Override
    public ConnectorTestResult test(ConnectorInstance connector) {
        Map<String, Object> config = ConnectorConfigSupport.readConfig(connector);
        String url = ConnectorConfigSupport.requireString(config, "url");
        String method = config.getOrDefault("method", "POST").toString().toUpperCase();

        try {
            URI uri = URI.create(url);
            if (uri.getScheme() == null || uri.getHost() == null) {
                throw new IllegalArgumentException("HTTP 连接器必须使用绝对 URL");
            }

            Map<String, Object> details = new LinkedHashMap<>();
            details.put("url", url);
            details.put("method", method);
            details.put("host", uri.getHost());

            return new ConnectorTestResult(true, "READY", "HTTP 连接器配置有效", ConnectorConfigSupport.writeDetails(details));
        } catch (Exception exception) {
            return new ConnectorTestResult(false, "INVALID_URL", exception.getMessage(), "{\"url\":\"" + url.replace("\"", "'") + "\"}");
        }
    }

    @Override
    public ConnectorDeliveryResult deliver(ConnectorDeliveryRequest request) {
        Map<String, Object> config = ConnectorConfigSupport.readConfig(request.connector());
        String url = ConnectorConfigSupport.requireString(config, "url");
        String method = config.getOrDefault("method", "POST").toString().toUpperCase();
        boolean batch = Boolean.parseBoolean(String.valueOf(config.getOrDefault("batch", false)));
        int timeoutMs = Integer.parseInt(String.valueOf(config.getOrDefault("timeoutMs", 5000)));
        Map<String, Object> headers = config.get("headers") instanceof Map<?, ?> rawHeaders
                ? castMap(rawHeaders)
                : Map.of();

        if (!List.of("POST", "PUT", "PATCH").contains(method)) {
            return new ConnectorDeliveryResult(
                    DeliveryStatus.FAILED,
                    0,
                    request.rows().size(),
                    "HTTP 投递目前仅支持 POST、PUT 或 PATCH",
                    "{\"method\":\"" + method + "\"}"
            );
        }

        long successCount = 0;
        List<String> errors = new ArrayList<>();

        try {
            if (batch) {
                HttpResponse<String> response = sendRequest(url, method, request.rows(), headers, timeoutMs);
                if (isSuccess(response.statusCode())) {
                    successCount = request.rows().size();
                } else {
                    errors.add("HTTP " + response.statusCode() + ": " + response.body());
                }
            } else {
                for (Map<String, Object> row : request.rows()) {
                    HttpResponse<String> response = sendRequest(url, method, row, headers, timeoutMs);
                    if (isSuccess(response.statusCode())) {
                        successCount++;
                    } else if (errors.size() < 5) {
                        errors.add("HTTP " + response.statusCode() + ": " + response.body());
                    }
                }
            }
        } catch (Exception exception) {
            errors.add(exception.getMessage());
        }

        long errorCount = request.rows().size() - successCount;
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("url", url);
        details.put("method", method);
        details.put("batch", batch);
        details.put("successCount", successCount);
        details.put("errorCount", errorCount);
        if (!errors.isEmpty()) {
            details.put("errors", errors);
        }

        if (successCount == request.rows().size()) {
            return new ConnectorDeliveryResult(
                    DeliveryStatus.SUCCESS,
                    successCount,
                    0,
                    "已向 HTTP 接口投递 " + successCount + " 条数据",
                    ConnectorConfigSupport.writeDetails(details)
            );
        }

        if (successCount > 0) {
            return new ConnectorDeliveryResult(
                    DeliveryStatus.PARTIAL_SUCCESS,
                    successCount,
                    errorCount,
                    "已投递 " + successCount + " 条数据，另有 " + errorCount + " 条 HTTP 失败",
                    ConnectorConfigSupport.writeDetails(details)
            );
        }

        return new ConnectorDeliveryResult(
                DeliveryStatus.FAILED,
                0,
                errorCount,
                "HTTP 投递失败",
                ConnectorConfigSupport.writeDetails(details)
        );
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Map<?, ?> rawHeaders) {
        return (Map<String, Object>) rawHeaders;
    }

    private HttpResponse<String> sendRequest(
            String url,
            String method,
            Object payload,
            Map<String, Object> headers,
            int timeoutMs
    ) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json");

        for (Map.Entry<String, Object> header : headers.entrySet()) {
            builder.header(header.getKey(), String.valueOf(header.getValue()));
        }

        String body = objectMapper.writeValueAsString(payload);
        HttpRequest request = builder
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private boolean isSuccess(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }
}

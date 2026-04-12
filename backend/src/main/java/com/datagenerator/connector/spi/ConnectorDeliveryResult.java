package com.datagenerator.connector.spi;

public record ConnectorDeliveryResult(
        DeliveryStatus status,
        long deliveredCount,
        long errorCount,
        String summary,
        String detailsJson
) {

    public static ConnectorDeliveryResult unsupported(String summary, String detailsJson) {
        return new ConnectorDeliveryResult(DeliveryStatus.UNSUPPORTED, 0, 0, summary, detailsJson);
    }
}


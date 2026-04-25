package com.datagenerator.connector.api;

import com.datagenerator.connector.domain.ConnectorStatus;
import com.datagenerator.connector.domain.ConnectorType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record ConnectorUpsertRequest(
        @NotBlank String name,
        @NotNull ConnectorType connectorType,
        @NotNull ConnectorStatus status,
        String description,
        @NotBlank String configJson
) {
}

package com.datagenerator.connection.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record TargetConnectionDraftTestRequest(
        Long connectionId,
        @NotNull @Valid TargetConnectionUpsertRequest connection
) {
}

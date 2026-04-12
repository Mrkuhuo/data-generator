package com.datagenerator.connector.spi.impl;

import com.datagenerator.connector.domain.ConnectorType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class PostgresqlConnectorAdapter extends AbstractJdbcConnectorAdapter {

    public PostgresqlConnectorAdapter(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public ConnectorType supports() {
        return ConnectorType.POSTGRESQL;
    }

    @Override
    protected String databaseLabel() {
        return "PostgreSQL";
    }

    @Override
    protected String identifierQuote() {
        return "\"";
    }
}

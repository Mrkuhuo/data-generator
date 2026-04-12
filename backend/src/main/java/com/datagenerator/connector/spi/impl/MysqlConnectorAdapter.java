package com.datagenerator.connector.spi.impl;

import com.datagenerator.connector.domain.ConnectorType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class MysqlConnectorAdapter extends AbstractJdbcConnectorAdapter {

    public MysqlConnectorAdapter(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public ConnectorType supports() {
        return ConnectorType.MYSQL;
    }

    @Override
    protected String databaseLabel() {
        return "MySQL";
    }

    @Override
    protected String identifierQuote() {
        return "`";
    }
}

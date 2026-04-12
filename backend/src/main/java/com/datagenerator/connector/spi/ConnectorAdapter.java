package com.datagenerator.connector.spi;

import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.domain.ConnectorType;

public interface ConnectorAdapter {

    ConnectorType supports();

    ConnectorTestResult test(ConnectorInstance connector);

    default ConnectorDeliveryResult deliver(ConnectorDeliveryRequest request) {
        return ConnectorDeliveryResult.unsupported(
                "Connector type " + supports() + " does not yet support runtime delivery",
                "{\"supported\":false}"
        );
    }
}

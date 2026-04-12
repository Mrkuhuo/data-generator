package com.datagenerator.connector.spi;

import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.domain.ConnectorType;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class ConnectorRegistry {

    private final Map<ConnectorType, ConnectorAdapter> adapters;

    public ConnectorRegistry(List<ConnectorAdapter> connectorAdapters) {
        adapters = new EnumMap<>(ConnectorType.class);
        for (ConnectorAdapter adapter : connectorAdapters) {
            adapters.put(adapter.supports(), adapter);
        }
    }

    public ConnectorTestResult test(ConnectorInstance connector) {
        ConnectorAdapter adapter = adapters.get(connector.getConnectorType());
        if (adapter == null) {
            return new ConnectorTestResult(
                    false,
                    "UNSUPPORTED",
                    "No connector adapter is registered for type " + connector.getConnectorType(),
                    "{\"registered\":false}"
            );
        }
        return adapter.test(connector);
    }

    public ConnectorDeliveryResult deliver(ConnectorDeliveryRequest request) {
        ConnectorAdapter adapter = adapters.get(request.connector().getConnectorType());
        if (adapter == null) {
            return ConnectorDeliveryResult.unsupported(
                    "No connector adapter is registered for type " + request.connector().getConnectorType(),
                    "{\"registered\":false}"
            );
        }
        return adapter.deliver(request);
    }
}

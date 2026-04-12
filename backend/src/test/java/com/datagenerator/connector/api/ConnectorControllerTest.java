package com.datagenerator.connector.api;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.connector.application.ConnectorService;
import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.domain.ConnectorRole;
import com.datagenerator.connector.domain.ConnectorStatus;
import com.datagenerator.connector.domain.ConnectorType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(ConnectorController.class)
class ConnectorControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private ConnectorService connectorService;

    @Test
    void create_shouldValidateConfigJson() throws Exception {
        mockMvc.perform(post("/api/connectors")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {
                                  "name": "Warehouse sink",
                                  "connectorType": "MYSQL",
                                  "connectorRole": "TARGET",
                                  "status": "READY",
                                  "description": "Primary relational warehouse sink",
                                  "configJson": " "
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message", containsString("configJson")));
    }

    @Test
    void createQuickstartKafka_shouldReturnConnector() throws Exception {
        ConnectorInstance connector = connector(21L, "Activity stream", ConnectorType.KAFKA);
        given(connectorService.createExampleKafkaConnector()).willReturn(connector);

        mockMvc.perform(post("/api/connectors/quickstart/kafka"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("Example Kafka connector created"))
                .andExpect(jsonPath("$.data.id").value(21))
                .andExpect(jsonPath("$.data.connectorType").value("KAFKA"));
    }

    @Test
    void test_shouldReturnConnectorProbeResult() throws Exception {
        ConnectorTestResponse response = new ConnectorTestResponse(
                8L,
                true,
                "SUCCESS",
                "Connector handshake succeeded",
                "{\"latencyMs\":42}"
        );
        given(connectorService.markTested(8L)).willReturn(response);

        mockMvc.perform(post("/api/connectors/8/test")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new ConnectorUpsertRequest(
                                "Warehouse sink",
                                ConnectorType.MYSQL,
                                ConnectorRole.TARGET,
                                ConnectorStatus.READY,
                                "Primary warehouse",
                                "{\"jdbcUrl\":\"jdbc:mysql://localhost:3306/demo\"}"
                        ))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("Connector test executed"))
                .andExpect(jsonPath("$.data.connectorId").value(8))
                .andExpect(jsonPath("$.data.status").value("SUCCESS"));
    }

    private ConnectorInstance connector(Long id, String name, ConnectorType type) {
        ConnectorInstance connector = new ConnectorInstance();
        connector.setId(id);
        connector.setName(name);
        connector.setConnectorType(type);
        connector.setConnectorRole(ConnectorRole.TARGET);
        connector.setStatus(ConnectorStatus.READY);
        connector.setConfigJson("{\"topic\":\"synthetic.user.activity\"}");
        return connector;
    }
}

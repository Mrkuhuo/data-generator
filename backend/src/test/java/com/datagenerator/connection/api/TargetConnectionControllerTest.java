package com.datagenerator.connection.api;

import static org.mockito.BDDMockito.given;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.connection.application.TargetConnectionService;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnectionStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(TargetConnectionController.class)
@AutoConfigureMockMvc(addFilters = false)
class TargetConnectionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private TargetConnectionService service;

    @Test
    void testDraft_shouldProbeCurrentFormConfiguration() throws Exception {
        TargetConnectionDraftTestRequest request = new TargetConnectionDraftTestRequest(
                8L,
                new TargetConnectionUpsertRequest(
                        "订单库",
                        DatabaseType.MYSQL,
                        "127.0.0.1",
                        3306,
                        "synthetic_demo_target",
                        null,
                        "root",
                        null,
                        "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
                        null,
                        TargetConnectionStatus.READY,
                        "用于测试当前表单"
                )
        );
        given(service.testDraft(request)).willReturn(new ConnectionTestResponse(
                8L,
                true,
                "READY",
                "连接成功",
                "{\"productName\":\"MySQL\"}"
        ));

        mockMvc.perform(post("/api/connections/test")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("当前配置连接测试已完成"))
                .andExpect(jsonPath("$.data.connectionId").value(8))
                .andExpect(jsonPath("$.data.status").value("READY"));
    }

    @Test
    void describeModel_shouldReturnSchemaModel() throws Exception {
        given(service.describeModel(8L, java.util.List.of("demo.customer", "demo.orders"))).willReturn(
                new DatabaseModelResponse(
                        java.util.List.of(
                                new DatabaseTableSchemaResponse(
                                        "demo.customer",
                                        java.util.List.of(new DatabaseColumnResponse("id", "BIGINT", null, null, null, false, true, false, null)),
                                        java.util.List.of()
                                )
                        ),
                        java.util.List.of(
                                new DatabaseForeignKeyResponse(
                                        "fk_orders_customer",
                                        "demo.customer",
                                        java.util.List.of("id"),
                                        "demo.orders",
                                        java.util.List.of("customer_id")
                                )
                        )
                )
        );

        mockMvc.perform(get("/api/connections/8/schema-model")
                        .param("tableNames", "demo.customer", "demo.orders"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.tables[0].tableName").value("demo.customer"))
                .andExpect(jsonPath("$.data.relations[0].constraintName").value("fk_orders_customer"));
    }
}

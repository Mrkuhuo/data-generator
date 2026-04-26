package com.datagenerator.system.api;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.system.application.DemoComplexKafkaJsonGroupResponse;
import com.datagenerator.system.application.DemoDataRebuildResponse;
import com.datagenerator.system.application.DemoDataRebuildService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(DemoDataController.class)
@AutoConfigureMockMvc(addFilters = false)
class DemoDataControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DemoDataRebuildService demoDataRebuildService;

    @Test
    void rebuild_shouldReturnRecreatedDemoMetadata() throws Exception {
        given(demoDataRebuildService.rebuild(1L)).willReturn(new DemoDataRebuildResponse(
                1L,
                9L,
                "\u6f14\u793a MySQL \u76ee\u6807\u5e93",
                DatabaseType.MYSQL,
                "127.0.0.1",
                3306,
                "synthetic_demo_target",
                "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
                18L,
                "\u4e2d\u6587\u8ba2\u5355\u6f14\u793a\u4efb\u52a1",
                "synthetic_demo_orders_cn"
        ));

        mockMvc.perform(post("/api/system/demo/rebuild")
                        .param("sourceConnectionId", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("\u6f14\u793a\u6570\u636e\u5df2\u91cd\u5efa"))
                .andExpect(jsonPath("$.data.connectionId").value(9))
                .andExpect(jsonPath("$.data.connectionName").value("\u6f14\u793a MySQL \u76ee\u6807\u5e93"))
                .andExpect(jsonPath("$.data.taskName").value("\u4e2d\u6587\u8ba2\u5355\u6f14\u793a\u4efb\u52a1"));
    }

    @Test
    void createComplexKafkaJsonGroup_shouldReturnCreatedGroupMetadata() throws Exception {
        given(demoDataRebuildService.createComplexKafkaJsonGroup(7L)).willReturn(
                new DemoComplexKafkaJsonGroupResponse(
                        7L,
                        "\u6f14\u793a Kafka \u8fde\u63a5",
                        31L,
                        "\u590d\u6742\u7236\u5b50 JSON \u6f14\u793a\u4efb\u52a1 20260425111530",
                        2026042501L,
                        "mdg.demo.parent.order-profile.20260425111530",
                        "mdg.demo.child.order-fulfillment.20260425111530",
                        2,
                        1
                )
        );

        mockMvc.perform(post("/api/system/demo/kafka-complex-group")
                        .param("connectionId", "7"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("\u590d\u6742\u7236\u5b50 JSON \u5173\u7cfb\u4efb\u52a1\u5df2\u521b\u5efa"))
                .andExpect(jsonPath("$.data.groupId").value(31))
                .andExpect(jsonPath("$.data.parentTopic").value("mdg.demo.parent.order-profile.20260425111530"))
                .andExpect(jsonPath("$.data.childTopic").value("mdg.demo.child.order-fulfillment.20260425111530"))
                .andExpect(jsonPath("$.data.taskCount").value(2))
                .andExpect(jsonPath("$.data.relationCount").value(1));
    }

    @Test
    void createPayloadKafkaJsonGroup_shouldReturnCreatedPayloadGroupMetadata() throws Exception {
        given(demoDataRebuildService.createPayloadKafkaJsonGroup(7L)).willReturn(
                new DemoComplexKafkaJsonGroupResponse(
                        7L,
                        "\u6f14\u793a Kafka \u8fde\u63a5",
                        32L,
                        "\u8d1f\u8f7d\u7236\u5b50 JSON \u6f14\u793a\u4efb\u52a1 20260425112030",
                        2026042502L,
                        "mdg.demo.payload.parent.order-envelope.20260425112030",
                        "mdg.demo.payload.child.shipment-envelope.20260425112030",
                        2,
                        1
                )
        );

        mockMvc.perform(post("/api/system/demo/kafka-payload-group")
                        .param("connectionId", "7"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("\u8d1f\u8f7d\u7236\u5b50 JSON \u5173\u7cfb\u4efb\u52a1\u5df2\u521b\u5efa"))
                .andExpect(jsonPath("$.data.groupId").value(32))
                .andExpect(jsonPath("$.data.parentTopic").value("mdg.demo.payload.parent.order-envelope.20260425112030"))
                .andExpect(jsonPath("$.data.childTopic").value("mdg.demo.payload.child.shipment-envelope.20260425112030"))
                .andExpect(jsonPath("$.data.taskCount").value(2))
                .andExpect(jsonPath("$.data.relationCount").value(1));
    }
}

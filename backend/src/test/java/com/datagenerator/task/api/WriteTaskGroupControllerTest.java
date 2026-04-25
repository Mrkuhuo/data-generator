package com.datagenerator.task.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.task.application.WriteTaskGroupScheduleSnapshot;
import com.datagenerator.task.application.WriteTaskGroupSchedulingService;
import com.datagenerator.task.application.WriteTaskGroupService;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.ReferenceSourceMode;
import com.datagenerator.task.domain.RelationReusePolicy;
import com.datagenerator.task.domain.RelationSelectionStrategy;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTaskGroup;
import com.datagenerator.task.domain.WriteTaskRelationMode;
import com.datagenerator.task.domain.WriteTaskRelationType;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(WriteTaskGroupController.class)
class WriteTaskGroupControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private WriteTaskGroupService service;

    @MockBean
    private WriteTaskGroupSchedulingService schedulingService;

    @Test
    void create_shouldReturnCreatedGroup() throws Exception {
        WriteTaskGroupResponse response = sampleResponse();
        WriteTaskGroup group = sampleGroup();

        given(service.create(any(WriteTaskGroupUpsertRequest.class))).willReturn(response);
        given(service.findById(12L)).willReturn(group);
        doNothing().when(schedulingService).scheduleOrUpdate(group);
        given(service.findResponseById(12L)).willReturn(response);
        given(schedulingService.readSnapshot(
                12L,
                WriteTaskScheduleType.CRON,
                WriteTaskStatus.READY,
                null,
                null
        )).willReturn(new WriteTaskGroupScheduleSnapshot("NORMAL", Instant.parse("2026-04-23T02:00:00Z"), null));

        mockMvc.perform(post("/api/write-task-groups")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sampleRequest())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("\u5173\u7cfb\u4efb\u52a1\u7ec4\u5df2\u521b\u5efa"))
                .andExpect(jsonPath("$.data.id").value(12))
                .andExpect(jsonPath("$.data.scheduleType").value("CRON"))
                .andExpect(jsonPath("$.data.schedulerState").value("NORMAL"))
                .andExpect(jsonPath("$.data.tasks[0].taskKey").value("customer"))
                .andExpect(jsonPath("$.data.relations[0].parentTaskKey").value("customer"));
    }

    @Test
    void executions_shouldReturnExecutionList() throws Exception {
        given(service.findExecutions(12L)).willReturn(List.of(sampleExecution()));

        mockMvc.perform(get("/api/write-task-groups/12/executions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data[0].triggerType").value("SCHEDULED"))
                .andExpect(jsonPath("$.data[0].status").value("SUCCESS"))
                .andExpect(jsonPath("$.data[0].insertedRowCount").value(300))
                .andExpect(jsonPath("$.data[0].tables[0].tableName").value("customer"));
    }

    private WriteTaskGroup sampleGroup() {
        WriteTaskGroup group = new WriteTaskGroup();
        group.setId(12L);
        group.setName("order-flow");
        group.setConnectionId(9L);
        group.setStatus(WriteTaskStatus.READY);
        group.setScheduleType(WriteTaskScheduleType.CRON);
        group.setCronExpression("0 0/5 * * * ?");
        return group;
    }

    private WriteTaskGroupUpsertRequest sampleRequest() {
        return new WriteTaskGroupUpsertRequest(
                "\u8ba2\u5355\u94fe\u8def",
                9L,
                "desc",
                20260422L,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.CRON,
                "0 0/5 * * * ?",
                null,
                null,
                null,
                null,
                List.of(
                        new WriteTaskGroupTaskUpsertRequest(
                                null,
                                "customer",
                                "customer",
                                "customer",
                                TableMode.USE_EXISTING,
                                WriteMode.APPEND,
                                200,
                                null,
                                null,
                                WriteTaskStatus.READY,
                                new WriteTaskGroupRowPlanRequest(
                                        com.datagenerator.task.domain.WriteTaskGroupRowPlanMode.FIXED,
                                        100,
                                        null,
                                        null,
                                        null
                                ),
                                null,
                                null,
                                List.of(
                                        new WriteTaskGroupTaskColumnUpsertRequest(
                                                "id",
                                                "BIGINT",
                                                null,
                                                null,
                                                null,
                                                false,
                                                true,
                                                false,
                                                ColumnGeneratorType.SEQUENCE,
                                                Map.of("start", 1, "step", 1),
                                                0
                                        )
                                )
                        )
                ),
                List.of(
                        new WriteTaskGroupRelationUpsertRequest(
                                null,
                                "fk_orders_customer",
                                "customer",
                                "customer",
                                WriteTaskRelationMode.DATABASE_COLUMNS,
                                WriteTaskRelationType.ONE_TO_MANY,
                                ReferenceSourceMode.CURRENT_BATCH,
                                RelationSelectionStrategy.PARENT_DRIVEN,
                                RelationReusePolicy.ALLOW_REPEAT,
                                List.of("id"),
                                List.of("id"),
                                0D,
                                null,
                                null,
                                null,
                                null,
                                0
                        )
                )
        );
    }

    private WriteTaskGroupResponse sampleResponse() {
        return new WriteTaskGroupResponse(
                12L,
                Instant.parse("2026-04-22T10:00:00Z"),
                Instant.parse("2026-04-22T10:00:00Z"),
                "\u8ba2\u5355\u94fe\u8def",
                9L,
                "desc",
                20260422L,
                "READY",
                "CRON",
                "0 0/5 * * * ?",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                List.of(
                        new WriteTaskGroupTaskResponse(
                                21L,
                                "customer",
                                "customer",
                                "customer",
                                "USE_EXISTING",
                                "APPEND",
                                200,
                                null,
                                null,
                                "READY",
                                new WriteTaskGroupRowPlanResponse(
                                        com.datagenerator.task.domain.WriteTaskGroupRowPlanMode.FIXED,
                                        100,
                                        null,
                                        null,
                                        null
                                ),
                                null,
                                null,
                                List.of(
                                        new WriteTaskGroupTaskColumnResponse(
                                                1L,
                                                "id",
                                                "BIGINT",
                                                null,
                                                null,
                                                null,
                                                false,
                                                true,
                                                false,
                                                ColumnGeneratorType.SEQUENCE,
                                                Map.of("start", 1, "step", 1),
                                                0
                                        )
                                )
                        )
                ),
                List.of(
                        new WriteTaskGroupRelationResponse(
                                31L,
                                "fk_orders_customer",
                                21L,
                                22L,
                                "customer",
                                "orders",
                                "DATABASE_COLUMNS",
                                "ONE_TO_MANY",
                                "CURRENT_BATCH",
                                "PARENT_DRIVEN",
                                "ALLOW_REPEAT",
                                List.of("id"),
                                List.of("customer_id"),
                                0D,
                                null,
                                1,
                                3,
                                null,
                                0
                        )
                )
        );
    }

    private WriteTaskGroupExecutionResponse sampleExecution() {
        return new WriteTaskGroupExecutionResponse(
                99L,
                12L,
                "SCHEDULED",
                "SUCCESS",
                Instant.parse("2026-04-22T10:10:00Z"),
                Instant.parse("2026-04-22T10:11:00Z"),
                2,
                2,
                2,
                0,
                300L,
                null,
                Map.of("insertedRowCount", 300),
                List.of(
                        new WriteTaskGroupTableExecutionResponse(
                                100L,
                                21L,
                                "customer",
                                "SUCCESS",
                                10L,
                                110L,
                                100L,
                                0L,
                                0L,
                                0L,
                                0L,
                                null,
                                Map.of("writtenRowCount", 100)
                        )
                )
        );
    }
}

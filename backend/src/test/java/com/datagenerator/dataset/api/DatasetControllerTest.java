package com.datagenerator.dataset.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.dataset.application.DatasetService;
import com.datagenerator.dataset.preview.DatasetPreviewService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(DatasetController.class)
@AutoConfigureMockMvc(addFilters = false)
class DatasetControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private DatasetService datasetService;

    @MockBean
    private DatasetPreviewService datasetPreviewService;

    @Test
    void preview_shouldReturnGeneratedRows() throws Exception {
        DatasetPreviewResponse response = new DatasetPreviewResponse(
                2,
                20260412L,
                List.of(
                        Map.of("userId", "u-1", "active", true),
                        Map.of("userId", "u-2", "active", false)
                )
        );
        given(datasetPreviewService.preview(eq(5L), any(DatasetPreviewRequest.class))).willReturn(response);

        mockMvc.perform(post("/api/datasets/5/preview")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new DatasetPreviewRequest(2, 20260412L))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("数据集预览已生成"))
                .andExpect(jsonPath("$.data.count").value(2))
                .andExpect(jsonPath("$.data.seed").value(20260412))
                .andExpect(jsonPath("$.data.rows[0].userId").value("u-1"));

        ArgumentCaptor<DatasetPreviewRequest> captor = ArgumentCaptor.forClass(DatasetPreviewRequest.class);
        verify(datasetPreviewService).preview(eq(5L), captor.capture());
        assertThat(captor.getValue().count()).isEqualTo(2);
        assertThat(captor.getValue().seed()).isEqualTo(20260412L);
    }

    @Test
    void create_shouldRejectBlankSchemaJson() throws Exception {
        mockMvc.perform(post("/api/datasets")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {
                                  "name": "Customer 360",
                                  "category": "crm",
                                  "version": "v1",
                                  "status": "READY",
                                  "description": "Synthetic customer profile stream",
                                  "schemaJson": " ",
                                  "sampleConfigJson": "{}"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message", containsString("schemaJson")));
    }

    @Test
    void preview_shouldMapIllegalArgumentsToBadRequest() throws Exception {
        given(datasetPreviewService.preview(eq(6L), any())).willThrow(new IllegalArgumentException("已归档的数据集不支持预览"));

        mockMvc.perform(post("/api/datasets/6/preview")
                        .contentType(APPLICATION_JSON)
                .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("已归档的数据集不支持预览"));
    }
}

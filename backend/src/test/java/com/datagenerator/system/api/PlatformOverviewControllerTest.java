package com.datagenerator.system.api;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.system.application.PlatformOverviewResponse;
import com.datagenerator.system.application.PlatformOverviewService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PlatformOverviewController.class)
class PlatformOverviewControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private PlatformOverviewService overviewService;

    @Test
    void getOverview_shouldReturnPlatformCounts() throws Exception {
        given(overviewService.getOverview()).willReturn(new PlatformOverviewResponse(5, 4, 3, 2, 6, 7, 8));

        mockMvc.perform(get("/api/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.connectorCount").value(5))
                .andExpect(jsonPath("$.data.datasetCount").value(4))
                .andExpect(jsonPath("$.data.jobCount").value(3))
                .andExpect(jsonPath("$.data.executionCount").value(2))
                .andExpect(jsonPath("$.data.connectionCount").value(6))
                .andExpect(jsonPath("$.data.writeTaskCount").value(7))
                .andExpect(jsonPath("$.data.writeExecutionCount").value(8));
    }
}

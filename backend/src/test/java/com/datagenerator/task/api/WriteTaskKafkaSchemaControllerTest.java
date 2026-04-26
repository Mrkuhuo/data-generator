package com.datagenerator.task.api;

import static org.mockito.BDDMockito.given;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.task.application.KafkaSchemaImportService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(WriteTaskKafkaSchemaController.class)
@AutoConfigureMockMvc(addFilters = false)
class WriteTaskKafkaSchemaControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KafkaSchemaImportService importService;

    @Test
    void importExample_shouldReturnSchemaResponse() throws Exception {
        given(importService.importExampleJson("{\"id\":1}"))
                .willReturn(new KafkaSchemaImportResponse(
                        "EXAMPLE_JSON",
                        "{\"type\":\"OBJECT\",\"children\":[]}",
                        List.of("id"),
                        List.of(new KafkaSchemaImportWarning("$.id", "NULL_VALUE", "example"))
                ));

        mockMvc.perform(post("/api/write-tasks/kafka/schema/example")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {
                                  "content": "{\\\"id\\\":1}"
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.schemaSource").value("EXAMPLE_JSON"))
                .andExpect(jsonPath("$.data.scalarPaths[0]").value("id"))
                .andExpect(jsonPath("$.data.warnings[0].code").value("NULL_VALUE"));
    }
}

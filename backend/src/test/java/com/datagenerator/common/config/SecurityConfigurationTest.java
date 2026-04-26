package com.datagenerator.common.config;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.system.api.PlatformOverviewController;
import com.datagenerator.system.application.PlatformOverviewResponse;
import com.datagenerator.system.application.PlatformOverviewService;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PlatformOverviewController.class)
@Import(SecurityConfiguration.class)
@TestPropertySource(properties = {
        "mdg.security.enabled=true",
        "mdg.security.username=admin",
        "mdg.security.password=123456"
})
class SecurityConfigurationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private PlatformOverviewService overviewService;

    @Test
    void apiRequests_shouldRequireAuthentication() throws Exception {
        mockMvc.perform(get("/api/overview"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void apiRequests_shouldAllowConfiguredBasicAuthCredentials() throws Exception {
        given(overviewService.getOverview()).willReturn(new PlatformOverviewResponse(0, 0, 0, 0, 1, 2, 3));

        mockMvc.perform(get("/api/overview")
                        .header("Authorization", "Basic " + basicToken("admin", "123456")))
                .andExpect(status().isOk());
    }

    private String basicToken(String username, String password) {
        return Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    }
}

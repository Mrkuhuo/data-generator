package com.datagenerator.system.api;

import com.datagenerator.common.web.ApiResponse;
import com.datagenerator.system.application.PlatformOverviewResponse;
import com.datagenerator.system.application.PlatformOverviewService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/overview")
public class PlatformOverviewController {

    private final PlatformOverviewService overviewService;

    public PlatformOverviewController(PlatformOverviewService overviewService) {
        this.overviewService = overviewService;
    }

    @GetMapping
    public ApiResponse<PlatformOverviewResponse> getOverview() {
        return ApiResponse.success(overviewService.getOverview());
    }
}

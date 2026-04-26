package com.datagenerator.dataset.api;

import com.datagenerator.common.web.ApiResponse;
import com.datagenerator.dataset.application.DatasetService;
import com.datagenerator.dataset.api.DatasetPreviewRequest;
import com.datagenerator.dataset.api.DatasetPreviewResponse;
import com.datagenerator.dataset.domain.DatasetDefinition;
import com.datagenerator.dataset.preview.DatasetPreviewService;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/datasets")
@ConditionalOnProperty(name = "mdg.legacy.enabled", havingValue = "true")
public class DatasetController {

    private final DatasetService datasetService;
    private final DatasetPreviewService datasetPreviewService;

    public DatasetController(DatasetService datasetService, DatasetPreviewService datasetPreviewService) {
        this.datasetService = datasetService;
        this.datasetPreviewService = datasetPreviewService;
    }

    @GetMapping
    public ApiResponse<List<DatasetDefinition>> list() {
        return ApiResponse.success(datasetService.findAll());
    }

    @GetMapping("/{id}")
    public ApiResponse<DatasetDefinition> detail(@PathVariable Long id) {
        return ApiResponse.success(datasetService.findById(id));
    }

    @PostMapping
    public ApiResponse<DatasetDefinition> create(@Valid @RequestBody DatasetUpsertRequest request) {
        return ApiResponse.success(datasetService.create(request), "数据集已创建");
    }

    @PostMapping("/quickstart")
    public ApiResponse<DatasetDefinition> createExample() {
        return ApiResponse.success(datasetService.createExample(), "示例数据集已创建");
    }

    @PutMapping("/{id}")
    public ApiResponse<DatasetDefinition> update(@PathVariable Long id, @Valid @RequestBody DatasetUpsertRequest request) {
        return ApiResponse.success(datasetService.update(id, request), "数据集已更新");
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        datasetService.delete(id);
        return ApiResponse.success(null, "数据集已删除");
    }

    @PostMapping("/{id}/preview")
    public ApiResponse<DatasetPreviewResponse> preview(@PathVariable Long id, @RequestBody(required = false) DatasetPreviewRequest request) {
        return ApiResponse.success(datasetPreviewService.preview(id, request), "数据集预览已生成");
    }
}

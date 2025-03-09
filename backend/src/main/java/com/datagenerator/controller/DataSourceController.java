package com.datagenerator.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.datagenerator.common.PageRequest;
import com.datagenerator.common.Result;
import com.datagenerator.entity.DataSource;
import com.datagenerator.model.ApiResponse;
import com.datagenerator.service.DataSourceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/data-sources")
@RequiredArgsConstructor
public class DataSourceController {

    private final DataSourceService dataSourceService;

    @GetMapping("/page")
    public Result<Page<DataSource>> page(PageRequest pageRequest) {
        Page<DataSource> page = new Page<>(pageRequest.getPageNum(), pageRequest.getPageSize());
        LambdaQueryWrapper<DataSource> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(DataSource::getDeleted, 0);
        if (pageRequest.getOrderBy() != null) {
            wrapper.orderBy(true, pageRequest.getAsc(), DataSource::getCreateTime);
        }
        return Result.success(dataSourceService.page(page, wrapper));
    }

    @GetMapping("/{id}")
    public Result<DataSource> getById(@PathVariable Long id) {
        return Result.success(dataSourceService.getById(id));
    }

    @PostMapping
    public Result<Boolean> save(@RequestBody DataSource dataSource) {
        return Result.success(dataSourceService.save(dataSource));
    }

    @PutMapping
    public Result<Boolean> update(@RequestBody DataSource dataSource) {
        return Result.success(dataSourceService.updateById(dataSource));
    }

    @DeleteMapping("/{id}")
    public Result<Boolean> delete(@PathVariable Long id) {
        DataSource dataSource = new DataSource();
        dataSource.setId(id);
        dataSource.setDeleted(1);
        return Result.success(dataSourceService.updateById(dataSource));
    }

    @PostMapping("/test")
    public ResponseEntity<?> testConnection(@RequestBody DataSource dataSource) {
        try {
            dataSourceService.testConnection(dataSource);
            return ResponseEntity.ok(new ApiResponse(200, "连接测试成功"));
        } catch (SQLException e) {
            // 返回错误状态码和具体的错误信息
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiResponse(500, e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiResponse(500, e.getMessage()));
        }
    }

    @GetMapping("/{id}/tables")
    public Result<List<String>> getTables(@PathVariable Long id) {
        return Result.success(dataSourceService.getTables(id));
    }

    @GetMapping("/{id}/topics")
    public Result<List<String>> getTopics(@PathVariable Long id) {
        return Result.success(dataSourceService.getTopics(id));
    }

    @GetMapping("/{id}/tables/{tableName}/columns")
    public Result<List<Map<String, String>>> getTableColumns(
            @PathVariable Long id,
            @PathVariable String tableName) {
        return Result.success(dataSourceService.getTableColumns(id, tableName));
    }

    /**
     * 获取表之间的依赖关系
     * @param id 数据源ID
     * @param tables 表名列表，逗号分隔
     * @return 表依赖关系，key为表名，value为该表依赖的表列表
     */
    @GetMapping("/{id}/table-dependencies")
    public Result<Map<String, List<String>>> getTableDependencies(
            @PathVariable Long id,
            @RequestParam String tables) {
        String[] tableArray = tables.split(",");
        return Result.success(dataSourceService.getTableDependencies(id, tableArray));
    }
} 
package com.datagenerator.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.datagenerator.entity.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface DataSourceService extends IService<DataSource> {
    /**
     * 测试数据源连接
     */
    void testConnection(DataSource dataSource) throws SQLException;

    /**
     * 获取数据源下的所有表
     */
    List<String> getTables(Long dataSourceId);

    /**
     * 获取数据源下的所有Kafka主题
     */
    List<String> getTopics(Long dataSourceId);

    /**
     * 获取表结构
     * @param dataSourceId 数据源ID
     * @param tableName 表名
     * @return 表结构信息列表，每个元素包含字段名、类型和注释
     */
    List<Map<String, String>> getTableColumns(Long dataSourceId, String tableName);

    /**
     * 获取表之间的依赖关系
     * @param dataSourceId 数据源ID
     * @param tables 表名数组
     * @return 表依赖关系，key为表名，value为该表依赖的表列表
     */
    Map<String, List<String>> getTableDependencies(Long dataSourceId, String[] tables);
} 
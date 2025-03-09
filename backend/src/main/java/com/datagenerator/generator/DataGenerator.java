package com.datagenerator.generator;

import java.util.List;
import java.util.Map;

public interface DataGenerator {
    /**
     * 生成数据
     * @param template 数据生成模板
     * @param count 生成数量
     * @return 生成的数据列表
     */
    List<Map<String, Object>> generate(String template, int count);

    /**
     * 获取字段列表
     * @return 字段列表
     */
    List<String> getFields();
} 
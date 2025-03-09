package com.datagenerator.generator.rule;

public interface DataRule {
    /**
     * 生成数据
     * @return 生成的数据
     */
    Object generate();

    /**
     * 获取规则类型
     * @return 规则类型
     */
    String getType();

    /**
     * 获取规则参数
     * @return 规则参数
     */
    Object getParams();
} 
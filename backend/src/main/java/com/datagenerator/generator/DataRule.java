package com.datagenerator.generator;

/**
 * 数据生成规则接口
 */
public interface DataRule {
    /**
     * 生成一个符合规则的值
     * @return 生成的值
     */
    Object generate();
} 
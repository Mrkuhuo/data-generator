package com.datagenerator.validator;

import java.util.Map;

public interface DataValidator {
    /**
     * 验证数据
     * @param data 待验证的数据
     * @return 验证结果
     */
    ValidationResult validate(Map<String, Object> data);

    /**
     * 获取验证器类型
     * @return 验证器类型
     */
    String getType();
} 
package com.datagenerator.validator.impl;

import com.datagenerator.validator.DataValidator;
import com.datagenerator.validator.ValidationResult;
import lombok.Data;

import java.util.Map;

@Data
public class RegexValidator implements DataValidator {
    private String field;
    private String pattern;
    private String message;

    @Override
    public ValidationResult validate(Map<String, Object> data) {
        ValidationResult result = new ValidationResult();
        
        if (field == null || !data.containsKey(field)) {
            return result;
        }
        
        Object value = data.get(field);
        if (!(value instanceof String)) {
            result.addError("字段 " + field + " 必须是字符串类型");
            return result;
        }
        
        String str = (String) value;
        if (!str.matches(pattern)) {
            result.addError(message != null ? message : "字段 " + field + " 格式不正确");
        }
        
        return result;
    }

    @Override
    public String getType() {
        return "regex";
    }
} 
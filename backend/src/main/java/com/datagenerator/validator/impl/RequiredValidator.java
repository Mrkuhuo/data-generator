package com.datagenerator.validator.impl;

import com.datagenerator.validator.DataValidator;
import com.datagenerator.validator.ValidationResult;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class RequiredValidator implements DataValidator {
    private List<String> requiredFields;

    @Override
    public ValidationResult validate(Map<String, Object> data) {
        ValidationResult result = new ValidationResult();
        
        if (requiredFields != null) {
            for (String field : requiredFields) {
                if (!data.containsKey(field) || data.get(field) == null) {
                    result.addError("字段 " + field + " 不能为空");
                }
            }
        }
        
        return result;
    }

    @Override
    public String getType() {
        return "required";
    }
} 
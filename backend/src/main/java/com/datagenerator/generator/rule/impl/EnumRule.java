package com.datagenerator.generator.rule.impl;

import com.datagenerator.generator.rule.DataRule;
import lombok.Data;

import java.util.List;

@Data
public class EnumRule implements DataRule {
    private List<Object> values;
    private boolean random = true;
    private int current = 0;
    private Object defaultValue;
    private boolean nullable = true;

    @Override
    public Object generate() {
        if (defaultValue != null) {
            return defaultValue;
        }
        
        if (nullable && Math.random() < 0.1) { // 10%的概率生成null
            return null;
        }
        
        if (values == null || values.isEmpty()) {
            return null;
        }
        if (random) {
            return values.get((int) (Math.random() * values.size()));
        } else {
            Object value = values.get(current);
            current = (current + 1) % values.size();
            return value;
        }
    }

    @Override
    public String getType() {
        return "enum";
    }

    @Override
    public Object getParams() {
        return this;
    }
} 
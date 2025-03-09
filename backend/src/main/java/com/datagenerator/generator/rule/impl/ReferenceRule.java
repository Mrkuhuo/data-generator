package com.datagenerator.generator.rule.impl;

import com.datagenerator.generator.rule.DataRule;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ReferenceRule implements DataRule {
    private String field;
    private String table;
    private String condition;
    private boolean random = true;
    private int current = 0;
    private List<Map<String, Object>> values;

    @Override
    public Object generate() {
        if (values == null || values.isEmpty()) {
            return null;
        }
        if (random) {
            return values.get((int) (Math.random() * values.size())).get(field);
        } else {
            Object value = values.get(current).get(field);
            current = (current + 1) % values.size();
            return value;
        }
    }

    @Override
    public String getType() {
        return "reference";
    }

    @Override
    public Object getParams() {
        return this;
    }
} 
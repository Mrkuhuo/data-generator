package com.datagenerator.generator.rule.impl;

import com.datagenerator.generator.rule.DataRule;
import lombok.Data;

@Data
public class FixedRule implements DataRule {
    private Object value;

    @Override
    public Object generate() {
        return value;
    }

    @Override
    public String getType() {
        return "fixed";
    }

    @Override
    public Object getParams() {
        return this;
    }
} 
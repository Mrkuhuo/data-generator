package com.datagenerator.generator.rule.impl;

import com.datagenerator.generator.rule.DataRule;
import lombok.Data;

@Data
public class SequenceRule implements DataRule {
    private long current = 0;
    private long step = 1;
    private long start = 0;

    @Override
    public Object generate() {
        long value = start + current * step;
        current++;
        return value;
    }

    @Override
    public String getType() {
        return "sequence";
    }

    @Override
    public Object getParams() {
        return this;
    }
} 
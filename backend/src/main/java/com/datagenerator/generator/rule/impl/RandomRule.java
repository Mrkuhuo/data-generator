package com.datagenerator.generator.rule.impl;

import com.datagenerator.generator.rule.DataRule;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RandomRule implements DataRule {
    private double min = 0;
    private double max = 100;
    @JsonAlias("isInteger")
    private boolean integer = false;
    private boolean nullable = true;
    private Number defaultValue;

    @Override
    public Object generate() {
        if (defaultValue != null) {
            return defaultValue;
        }
        
        if (nullable && Math.random() < 0.1) { // 10%的概率生成null
            return null;
        }
        double value = min + Math.random() * (max - min);
        return integer ? (long) value : value;
    }

    @Override
    public String getType() {
        return "random";
    }

    @Override
    public Object getParams() {
        return this;
    }
} 
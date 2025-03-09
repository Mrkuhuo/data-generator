package com.datagenerator.generator.rule;

import com.datagenerator.generator.rule.impl.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DataRuleFactory {
    
    private final ObjectMapper objectMapper;
    
    public DataRuleFactory() {
        this.objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    }
    
    public DataRule createRule(String type, Object params) {
        try {
            log.info("创建规则，类型: {}, 参数: {}", type, params);
            
            DataRule rule;
            switch (type.toLowerCase()) {
                case "sequence":
                    rule = objectMapper.convertValue(params, SequenceRule.class);
                    break;
                case "random":
                    rule = objectMapper.convertValue(params, RandomRule.class);
                    break;
                case "enum":
                    rule = objectMapper.convertValue(params, EnumRule.class);
                    break;
                case "date":
                    rule = objectMapper.convertValue(params, DateRule.class);
                    break;
                case "string":
                    rule = objectMapper.convertValue(params, StringRule.class);
                    break;
                case "reference":
                    rule = objectMapper.convertValue(params, ReferenceRule.class);
                    break;
                default:
                    log.error("不支持的数据生成规则类型: {}", type);
                    throw new IllegalArgumentException("不支持的数据生成规则类型: " + type);
            }
            
            log.info("规则创建成功: {}", rule);
            return rule;
        } catch (Exception e) {
            log.error("创建数据生成规则失败, 类型: {}, 参数: {}, 错误: {}", type, params, e.getMessage(), e);
            throw new RuntimeException("创建数据生成规则失败: " + e.getMessage(), e);
        }
    }
} 
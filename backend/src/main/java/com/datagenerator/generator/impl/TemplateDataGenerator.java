package com.datagenerator.generator.impl;

import com.datagenerator.generator.DataGenerator;
import com.datagenerator.generator.rule.DataRule;
import com.datagenerator.generator.rule.DataRuleFactory;
import com.datagenerator.service.DataGenerateService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
public class TemplateDataGenerator implements DataGenerator {
    
    private final ObjectMapper objectMapper;
    private final DataRuleFactory ruleFactory;
    private Map<String, DataRule> rules;
    private List<String> fields;
    
    public TemplateDataGenerator() {
        this.objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        this.ruleFactory = new DataRuleFactory();
    }
    
    @Override
    public List<Map<String, Object>> generate(String template, int count) {
        try {
            if (template == null || template.trim().isEmpty()) {
                throw new IllegalArgumentException("模板不能为空");
            }
            
            log.info("开始解析模板，模板内容: {}", template);
            
            // 解析模板
            Map<String, Object> templateMap;
            try {
                // 先尝试解析为JsonNode以便于调试
                JsonNode jsonNode = objectMapper.readTree(template);
                log.info("模板JSON解析结果: {}", jsonNode.toPrettyString());
                
                // 如果JSON解析成功，再转换为Map
                templateMap = objectMapper.convertValue(jsonNode, Map.class);
                log.info("模板Map解析结果: {}", templateMap);
            } catch (Exception e) {
                log.error("模板解析失败，请确保模板是有效的JSON格式。模板内容: {}, 错误: {}", template, e.getMessage(), e);
                throw new IllegalArgumentException("模板解析失败: " + e.getMessage());
            }
            
            // 创建规则
            rules = new HashMap<>();
            fields = new ArrayList<>();
            
            for (Map.Entry<String, Object> entry : templateMap.entrySet()) {
                String field = entry.getKey();
                Object value = entry.getValue();
                
                log.info("处理字段 {}, 值类型: {}, 值内容: {}", field, 
                    value != null ? value.getClass().getName() : "null", 
                    value);
                
                if (!(value instanceof Map)) {
                    String actualType = value != null ? value.getClass().getName() : "null";
                    String actualValue = String.valueOf(value);
                    log.error("字段 {} 的配置无效。期望类型: Map, 实际类型: {}, 实际值: {}", 
                        field, actualType, actualValue);
                    throw new IllegalArgumentException(String.format(
                        "字段 %s 的配置无效，应为对象类型，实际为: %s，值: %s", 
                        field, actualType, actualValue));
                }
                
                @SuppressWarnings("unchecked")
                Map<String, Object> ruleConfig = (Map<String, Object>) value;
                
                log.info("字段 {} 的规则配置: {}", field, ruleConfig);
                
                String type = (String) ruleConfig.get("type");
                if (type == null) {
                    log.error("字段 {} 缺少 type 属性，完整配置: {}", field, ruleConfig);
                    throw new IllegalArgumentException("字段 " + field + " 缺少 type 属性");
                }
                
                Object params = ruleConfig.get("params");
                if (params == null) {
                    log.error("字段 {} 缺少 params 属性，完整配置: {}", field, ruleConfig);
                    throw new IllegalArgumentException("字段 " + field + " 缺少 params 属性");
                }
                
                try {
                    log.info("为字段 {} 创建规则，类型: {}, 参数: {}", field, type, params);
                    DataRule rule = ruleFactory.createRule(type, params);
                    rules.put(field, rule);
                    fields.add(field);
                    log.info("成功创建字段 {} 的规则: {}", field, rule);
                } catch (Exception e) {
                    log.error("创建字段 {} 的规则失败。类型: {}, 参数: {}, 错误: {}", 
                        field, type, params, e.getMessage(), e);
                    throw new IllegalArgumentException(
                        String.format("创建字段 %s 的规则失败: %s", field, e.getMessage()));
                }
            }
            
            // 生成数据
            List<Map<String, Object>> data = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Map<String, Object> row = new HashMap<>();
                for (String field : fields) {
                    DataRule rule = rules.get(field);
                    Object value = rule.generate();
                    row.put(field, value);
                    if (i == 0) {
                        log.debug("字段 {} 生成的第一个值: {}", field, value);
                    }
                }
                data.add(row);
            }
            
            log.info("成功生成 {} 条数据", count);
            if (!data.isEmpty()) {
                log.debug("第一条数据示例: {}", data.get(0));
            }
            
            return data;
        } catch (Exception e) {
            log.error("生成数据失败: {}", e.getMessage(), e);
            throw new RuntimeException("生成数据失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public List<String> getFields() {
        return fields;
    }

    private Object generateValue(String fieldName, JsonNode fieldConfig) {
        String type = fieldConfig.path("type").asText();
        ObjectNode params = (ObjectNode) fieldConfig.path("params");

        try {
            switch (type.toLowerCase()) {
                case "foreignkey":
                    String referencedTable = fieldConfig.path("referencedTable").asText();
                    String referencedColumn = fieldConfig.path("referencedColumn").asText();
                    return DataGenerateService.getValidForeignKeyValue(referencedTable, referencedColumn);
                default:
                    // ... existing code ...
                    return null; // Placeholder return, actual implementation needed
            }
        } catch (Exception e) {
            log.error("生成字段 {} 的值失败: {}", fieldName, e.getMessage());
            throw new IllegalArgumentException("创建字段 " + fieldName + " 的规则失败: " + e.getMessage());
        }
    }
} 
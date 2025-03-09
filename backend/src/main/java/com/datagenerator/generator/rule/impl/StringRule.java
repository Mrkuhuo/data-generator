package com.datagenerator.generator.rule.impl;

import com.datagenerator.generator.rule.DataRule;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.regex.Pattern;

@Data
@Slf4j
@JsonIgnoreProperties(ignoreUnknown = true)
public class StringRule implements DataRule {
    private String prefix = "";
    private String suffix = "";
    private int minLength = 5;
    private int maxLength = 10;
    private String charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private String pattern;
    private boolean random = true;
    private int current = 0;
    private String defaultValue;
    private boolean nullable = true;

    @Override
    public Object generate() {
        try {
            if (defaultValue != null) {
                return defaultValue;
            }
            
            if (nullable && Math.random() < 0.1) { // 10%的概率生成null
                return null;
            }
            
            // 如果指定了模式，使用模式生成
            if (pattern != null && !pattern.isEmpty()) {
                return generateByPattern();
            }
            
            // 否则使用常规方式生成
            return generateByLength();
        } catch (Exception e) {
            log.error("生成字符串失败: {}", e.getMessage(), e);
            return generateDefault();
        }
    }
    
    private String generateByPattern() {
        try {
            if (pattern == null || pattern.isEmpty()) {
                return generateByLength();
            }
            
            // 检查是否是JSON模板
            if (pattern.startsWith("{") || pattern.startsWith("[")) {
                return processJsonTemplate(pattern);
            }
            
            // 如果是简单的正则表达式模式,直接返回
            return pattern;
        } catch (Exception e) {
            log.error("按模式生成字符串失败: {}", e.getMessage(), e);
            return generateDefault();
        }
    }
    
    private String processJsonTemplate(String template) {
        try {
            log.debug("处理JSON模板: {}", template);
            
            // 处理枚举值 ${enum:value1|value2|value3}
            java.util.regex.Pattern enumPattern = java.util.regex.Pattern.compile("\\$\\{enum:([^}]+)\\}");
            java.util.regex.Matcher enumMatcher = enumPattern.matcher(template);
            StringBuffer sb = new StringBuffer();
            while (enumMatcher.find()) {
                String values = enumMatcher.group(1);
                String[] options = values.split("\\|");
                int index = (int) (Math.random() * options.length);
                String replacement = options[index];
                // 使用quoteReplacement确保替换值中的特殊字符被正确处理
                replacement = java.util.regex.Matcher.quoteReplacement(replacement);
                enumMatcher.appendReplacement(sb, replacement);
            }
            enumMatcher.appendTail(sb);
            template = sb.toString();
            
            // 处理随机数 ${random:min-max}
            java.util.regex.Pattern randomPattern = java.util.regex.Pattern.compile("\\$\\{random:([0-9]+)-([0-9]+)\\}");
            java.util.regex.Matcher randomMatcher = randomPattern.matcher(template);
            sb = new StringBuffer();
            while (randomMatcher.find()) {
                long min = Long.parseLong(randomMatcher.group(1));
                long max = Long.parseLong(randomMatcher.group(2));
                long value = min + (long) (Math.random() * (max - min + 1));
                // 使用quoteReplacement确保替换值中的特殊字符被正确处理
                String replacement = java.util.regex.Matcher.quoteReplacement(String.valueOf(value));
                randomMatcher.appendReplacement(sb, replacement);
            }
            randomMatcher.appendTail(sb);
            template = sb.toString();
            
            // 处理字符串模式 ${string:pattern}
            java.util.regex.Pattern stringPattern = java.util.regex.Pattern.compile("\\$\\{string:([^}]+)\\}");
            java.util.regex.Matcher stringMatcher = stringPattern.matcher(template);
            sb = new StringBuffer();
            while (stringMatcher.find()) {
                String strPattern = stringMatcher.group(1);
                String replacement = "";
                
                // 简单处理一些常见的模式
                if (strPattern.contains("[a-zA-Z0-9]")) {
                    int minLen = 8;
                    int maxLen = 16;
                    if (strPattern.contains("{")) {
                        String[] parts = strPattern.split("\\{|\\}");
                        for (String part : parts) {
                            if (part.contains(",")) {
                                String[] range = part.split(",");
                                minLen = Integer.parseInt(range[0]);
                                maxLen = Integer.parseInt(range[1]);
                                break;
                            } else if (part.matches("\\d+")) {
                                minLen = maxLen = Integer.parseInt(part);
                                break;
                            }
                        }
                    }
                    
                    // 使用安全的方法生成随机字符串
                    replacement = generateRandomAlphanumeric(minLen + (int) (Math.random() * (maxLen - minLen + 1)));
                }
                // 处理特定前缀的模式
                else if (strPattern.startsWith("sku")) {
                    replacement = "sku" + String.format("%06d", (int) (Math.random() * 1000000));
                } else if (strPattern.startsWith("page")) {
                    replacement = "page" + String.format("%03d", (int) (Math.random() * 1000));
                } else if (strPattern.startsWith("act")) {
                    replacement = "act" + String.format("%03d", (int) (Math.random() * 1000));
                } else if (strPattern.startsWith("pos")) {
                    replacement = "pos" + String.format("%03d", (int) (Math.random() * 1000));
                } else if (strPattern.startsWith("ad")) {
                    replacement = "ad" + generateRandomAlphanumeric(6);
                } else if (strPattern.startsWith("err")) {
                    replacement = "err" + generateRandomAlphanumeric(6);
                } else {
                    replacement = generateRandomAlphanumeric(minLength + (int)(Math.random() * (maxLength - minLength + 1)));
                }
                
                // 使用quoteReplacement确保替换值中的特殊字符被正确处理
                replacement = java.util.regex.Matcher.quoteReplacement(replacement);
                stringMatcher.appendReplacement(sb, replacement);
            }
            stringMatcher.appendTail(sb);
            template = sb.toString();
            
            // 直接处理字符串值中的"}"符号
            // 这是一个更直接的方法，专门针对您提供的模板结构
            if (template.contains("\"")) {
                // 使用正则表达式匹配JSON字符串值
                java.util.regex.Pattern jsonStringPattern = java.util.regex.Pattern.compile("\"([^\"]*)\"");
                java.util.regex.Matcher jsonStringMatcher = jsonStringPattern.matcher(template);
                sb = new StringBuffer();
                while (jsonStringMatcher.find()) {
                    String stringValue = jsonStringMatcher.group(1);
                    // 移除字符串值中的"}"符号
                    stringValue = stringValue.replace("}", "");
                    // 使用quoteReplacement确保替换值中的特殊字符被正确处理
                    String replacement = "\"" + stringValue + "\"";
                    replacement = java.util.regex.Matcher.quoteReplacement(replacement);
                    jsonStringMatcher.appendReplacement(sb, replacement);
                }
                jsonStringMatcher.appendTail(sb);
                template = sb.toString();
            }
            
            log.debug("处理后的JSON模板: {}", template);
            return template;
        } catch (Exception e) {
            log.error("处理JSON模板失败: {}", e.getMessage(), e);
            // 出错时，尝试一个简单的修复：移除所有字符串值中的"}"符号
            try {
                // 使用正则表达式匹配JSON字符串值
                java.util.regex.Pattern jsonStringPattern = java.util.regex.Pattern.compile("\"([^\"]*)\"");
                java.util.regex.Matcher jsonStringMatcher = jsonStringPattern.matcher(template);
                StringBuffer sb = new StringBuffer();
                while (jsonStringMatcher.find()) {
                    String stringValue = jsonStringMatcher.group(1);
                    // 移除字符串值中的"}"符号
                    stringValue = stringValue.replace("}", "");
                    // 使用quoteReplacement确保替换值中的特殊字符被正确处理
                    String replacement = "\"" + stringValue + "\"";
                    replacement = java.util.regex.Matcher.quoteReplacement(replacement);
                    jsonStringMatcher.appendReplacement(sb, replacement);
                }
                jsonStringMatcher.appendTail(sb);
                return sb.toString();
            } catch (Exception ex) {
                log.error("简单修复也失败了: {}", ex.getMessage(), ex);
                return template;
            }
        }
    }
    
    /**
     * 生成只包含字母和数字的随机字符串
     */
    private String generateRandomAlphanumeric(int length) {
        StringBuilder sb = new StringBuilder();
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt((int) (Math.random() * chars.length())));
        }
        return sb.toString();
    }
    
    private String generateByLength() {
        int length = minLength;
        if (maxLength > minLength) {
            length += (int) (Math.random() * (maxLength - minLength + 1));
        }
        
        StringBuilder sb = new StringBuilder();
        
        if (prefix != null) {
            sb.append(prefix);
        }
        
        if (random) {
            Random random = new Random();
            for (int i = 0; i < length; i++) {
                sb.append(charset.charAt(random.nextInt(charset.length())));
            }
        } else {
            for (int i = 0; i < length; i++) {
                sb.append(charset.charAt(current % charset.length()));
                current = (current + 1) % charset.length();
            }
        }
        
        if (suffix != null) {
            sb.append(suffix);
        }
        
        return sb.toString();
    }
    
    private String generateDefault() {
        return "default_" + System.currentTimeMillis();
    }

    @Override
    public String getType() {
        return "string";
    }

    @Override
    public Object getParams() {
        return this;
    }
} 
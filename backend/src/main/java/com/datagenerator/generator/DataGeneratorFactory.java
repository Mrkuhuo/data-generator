package com.datagenerator.generator;

import com.datagenerator.generator.impl.TemplateDataGenerator;
import org.springframework.stereotype.Component;

@Component
public class DataGeneratorFactory {
    
    public DataGenerator createGenerator(String dataFormat) {
        if (dataFormat == null || dataFormat.trim().isEmpty()) {
            throw new IllegalArgumentException("数据格式不能为空");
        }
        
        switch (dataFormat.toUpperCase()) {
            case "JSON":
            case "AVRO":
            case "PROTOBUF":
                return new TemplateDataGenerator();
            default:
                throw new IllegalArgumentException("不支持的数据格式: " + dataFormat);
        }
    }
} 
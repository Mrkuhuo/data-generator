package com.datagenerator.dto;

import lombok.Data;

import java.util.List;

@Data
public class ChartDataDTO {
    
    /**
     * 时间点列表
     */
    private List<String> times;
    
    /**
     * 数据值列表
     */
    private List<Double> values;
    
    /**
     * 指标名称
     */
    private String metric;
    
    /**
     * 指标描述
     */
    private String description;
} 
package com.datagenerator.metadata;

import lombok.Data;

@Data
public class ColumnMetadata {
    private String name;
    private String dataType;
    private boolean nullable;
    private String maxLength;
    private String defaultValue;
    private String comment;
    private String columnKey;
    private String extra;
} 
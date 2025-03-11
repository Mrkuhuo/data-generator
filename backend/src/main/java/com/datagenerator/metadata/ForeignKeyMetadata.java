package com.datagenerator.metadata;

import lombok.Data;
import java.util.HashSet;
import java.util.Set;

@Data
public class ForeignKeyMetadata {
    private String columnName;
    private String referencedTable;
    private String referencedColumn;
    private Set<Object> validValues = new HashSet<>();
} 
package com.datagenerator.common;

import lombok.Data;

@Data
public class PageRequest {
    private Integer pageNum = 1;
    private Integer pageSize = 10;
    private String orderBy;
    private Boolean asc = true;
} 
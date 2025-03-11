package com.datagenerator.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("data_source")
public class DataSource {
    @TableId(type = IdType.AUTO)
    private Long id;
    
    private String name;
    private String type; // MYSQL, ORACLE, KAFKA等
    private String url;
    private String username;
    private String password;
    private String driverClassName;
    private String description;
    
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;
    
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
    
    @TableLogic
    private Integer deleted;
    
    // 手动添加getter方法，以防Lombok注解未被正确处理
    public String getUrl() {
        return url;
    }
    
    public String getUsername() {
        return username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public String getName() {
        return name;
    }
} 
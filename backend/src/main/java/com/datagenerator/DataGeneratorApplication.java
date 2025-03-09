package com.datagenerator;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@MapperScan("com.datagenerator.mapper")
public class DataGeneratorApplication {
    public static void main(String[] args) {
        SpringApplication.run(DataGeneratorApplication.class, args);
    }
} 
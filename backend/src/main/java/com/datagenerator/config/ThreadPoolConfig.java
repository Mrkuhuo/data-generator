package com.datagenerator.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class ThreadPoolConfig {

    @Bean(name = "threadPoolTaskScheduler")
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(10);
        scheduler.setThreadNamePrefix("task-scheduler-");
        scheduler.setErrorHandler(t -> {
            // 处理任务执行异常
            System.err.println("任务执行异常: " + t.getMessage());
        });
        scheduler.initialize();
        return scheduler;
    }
} 
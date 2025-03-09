package com.datagenerator.annotation;

import java.lang.annotation.*;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface PerformanceMonitor {
    /**
     * 接口名称
     */
    String name() default "";
    
    /**
     * 是否记录参数
     */
    boolean logParams() default true;
    
    /**
     * 是否记录返回值
     */
    boolean logResult() default true;
    
    /**
     * 是否记录异常
     */
    boolean logException() default true;
    
    /**
     * 是否记录执行时间
     */
    boolean logTime() default true;
} 
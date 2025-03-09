package com.datagenerator.aspect;

import com.datagenerator.annotation.PerformanceMonitor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.lang.reflect.Method;

@Slf4j
@Aspect
@Component
public class PerformanceMonitorAspect {

    @Resource
    private ObjectMapper objectMapper;

    @Around("@annotation(com.datagenerator.annotation.PerformanceMonitor)")
    public Object around(ProceedingJoinPoint point) throws Throwable {
        // 获取方法签名
        MethodSignature signature = (MethodSignature) point.getSignature();
        Method method = signature.getMethod();
        PerformanceMonitor monitor = method.getAnnotation(PerformanceMonitor.class);
        
        // 获取接口名称
        String name = monitor.name();
        if (name.isEmpty()) {
            name = method.getDeclaringClass().getSimpleName() + "." + method.getName();
        }
        
        // 记录开始时间
        long startTime = System.currentTimeMillis();
        
        try {
            // 记录请求参数
            if (monitor.logParams()) {
                log.info("接口[{}]请求参数：{}", name, objectMapper.writeValueAsString(point.getArgs()));
            }
            
            // 执行方法
            Object result = point.proceed();
            
            // 记录执行时间
            if (monitor.logTime()) {
                long endTime = System.currentTimeMillis();
                log.info("接口[{}]执行时间：{}ms", name, endTime - startTime);
            }
            
            // 记录返回结果
            if (monitor.logResult()) {
                log.info("接口[{}]返回结果：{}", name, objectMapper.writeValueAsString(result));
            }
            
            return result;
        } catch (Throwable e) {
            // 记录异常信息
            if (monitor.logException()) {
                log.error("接口[{}]发生异常：", name, e);
            }
            throw e;
        }
    }
} 
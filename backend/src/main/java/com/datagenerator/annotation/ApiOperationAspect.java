package com.datagenerator.annotation;

import io.swagger.v3.oas.annotations.Operation;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

@Aspect
@Component
public class ApiOperationAspect {

    @Around("@annotation(com.datagenerator.annotation.ApiOperation)")
    public Object around(ProceedingJoinPoint point) throws Throwable {
        MethodSignature signature = (MethodSignature) point.getSignature();
        Method method = signature.getMethod();
        ApiOperation apiOperation = method.getAnnotation(ApiOperation.class);
        
        // 添加Swagger注解
        Operation operation = method.getAnnotation(Operation.class);
        if (operation == null) {
            operation = method.getDeclaringClass().getAnnotation(Operation.class);
        }
        
        if (operation == null) {
            operation = method.getDeclaringClass().getMethod(method.getName(), method.getParameterTypes())
                    .getAnnotation(Operation.class);
        }
        
        if (operation == null) {
            operation = method.getDeclaringClass().getMethod(method.getName(), method.getParameterTypes())
                    .getDeclaringClass().getAnnotation(Operation.class);
        }
        
        return point.proceed();
    }
} 
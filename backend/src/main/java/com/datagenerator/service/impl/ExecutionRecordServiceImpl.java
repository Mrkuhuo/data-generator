package com.datagenerator.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.datagenerator.entity.ExecutionRecord;
import com.datagenerator.mapper.ExecutionRecordMapper;
import com.datagenerator.service.ExecutionRecordService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
public class ExecutionRecordServiceImpl extends ServiceImpl<ExecutionRecordMapper, ExecutionRecord> implements ExecutionRecordService {

    @Override
    public ExecutionRecord createRecord(Long taskId) {
        ExecutionRecord record = new ExecutionRecord();
        record.setTaskId(taskId);
        record.setStartTime(LocalDateTime.now());
        record.setStatus("RUNNING");
        record.setTotalCount(0L);
        record.setSuccessCount(0L);
        record.setErrorCount(0L);
        save(record);
        return record;
    }

    @Override
    public boolean updateStatus(Long recordId, Integer status, String errorMessage, Long recordsCount) {
        ExecutionRecord record = getById(recordId);
        if (record == null) {
            throw new RuntimeException("执行记录不存在");
        }
        
        // 根据状态码设置状态文本
        if (status == 1) {
            record.setStatus("SUCCESS"); // 成功
        } else if (status == 0) {
            record.setStatus("FAILED"); // 失败
        } else if (status == 2) {
            record.setStatus("STOPPED"); // 停止
        } else {
            record.setStatus("COMPLETED"); // 默认完成
        }
        
        record.setErrorMessage(errorMessage);
        record.setTotalCount(recordsCount);
        
        // 根据状态设置成功和失败记录数
        if ("SUCCESS".equals(record.getStatus()) || "COMPLETED".equals(record.getStatus())) {
            record.setSuccessCount(recordsCount);
            record.setErrorCount(0L);
        } else if ("FAILED".equals(record.getStatus())) {
            record.setSuccessCount(0L);
            record.setErrorCount(recordsCount);
        } else if ("STOPPED".equals(record.getStatus())) {
            // 停止状态下保持原有的成功和失败记录数
        }
        
        // 设置结束时间
        record.setEndTime(LocalDateTime.now());
        
        log.info("更新执行记录状态: recordId={}, status={}, totalCount={}", recordId, record.getStatus(), recordsCount);
        return updateById(record);
    }
} 
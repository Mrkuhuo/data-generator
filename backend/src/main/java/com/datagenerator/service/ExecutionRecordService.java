package com.datagenerator.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.datagenerator.entity.ExecutionRecord;

public interface ExecutionRecordService extends IService<ExecutionRecord> {
    /**
     * 创建执行记录
     */
    ExecutionRecord createRecord(Long taskId);

    /**
     * 更新执行记录状态
     */
    boolean updateStatus(Long recordId, Integer status, String errorMessage, Long recordsCount);
} 
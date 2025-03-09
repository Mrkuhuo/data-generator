package com.datagenerator.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.datagenerator.entity.AlertRecord;
import com.datagenerator.mapper.AlertRecordMapper;
import com.datagenerator.service.AlertRecordService;
import org.springframework.stereotype.Service;

@Service
public class AlertRecordServiceImpl extends ServiceImpl<AlertRecordMapper, AlertRecord> implements AlertRecordService {
} 
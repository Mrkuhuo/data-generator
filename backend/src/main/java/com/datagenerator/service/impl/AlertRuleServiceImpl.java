package com.datagenerator.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.datagenerator.entity.AlertRule;
import com.datagenerator.mapper.AlertRuleMapper;
import com.datagenerator.service.AlertRuleService;
import org.springframework.stereotype.Service;

@Service
public class AlertRuleServiceImpl extends ServiceImpl<AlertRuleMapper, AlertRule> implements AlertRuleService {
} 
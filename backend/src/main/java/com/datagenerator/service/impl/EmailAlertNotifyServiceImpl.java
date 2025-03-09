package com.datagenerator.service.impl;

import com.datagenerator.entity.SystemAlertRule;
import com.datagenerator.service.AlertNotifyService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Slf4j
@Service
public class EmailAlertNotifyServiceImpl implements AlertNotifyService {

    @Resource
    private JavaMailSender mailSender;

    @Value("${spring.mail.username}")
    private String from;

    @Override
    public void sendAlert(SystemAlertRule rule, String message) {
        try {
            SimpleMailMessage mailMessage = new SimpleMailMessage();
            mailMessage.setFrom(from);
            mailMessage.setTo(rule.getReceivers().split(","));
            mailMessage.setSubject("系统告警: " + rule.getName());
            mailMessage.setText(message);
            mailSender.send(mailMessage);
            log.info("邮件告警发送成功: {}", rule.getName());
        } catch (Exception e) {
            log.error("邮件告警发送失败: {}", rule.getName(), e);
        }
    }

    @Override
    public String getNotifyType() {
        return "email";
    }
} 
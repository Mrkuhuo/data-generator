package com.datagenerator.generator.rule.impl;

import com.datagenerator.generator.rule.DataRule;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

@Data
public class DateRule implements DataRule {
    private String type = "datetime"; // datetime, date, time
    private String startDate;
    private String endDate;
    private String format;
    private boolean random = true;
    private long current = 0;
    private long step = 86400000; // 1天
    private String defaultValue;
    private boolean nullable = true;

    @Override
    public Object generate() {
        try {
            if (defaultValue != null) {
                return defaultValue;
            }
            
            if (nullable && Math.random() < 0.1) { // 10%的概率生成null
                return null;
            }
            
            if (startDate == null || endDate == null) {
                return new Date();
            }

            long start = parseDate(startDate).getTime();
            long end = parseDate(endDate).getTime();
            long value;

            if (random) {
                value = start + (long) (Math.random() * (end - start));
            } else {
                value = start + current * step;
                if (value > end) {
                    value = start;
                }
                current++;
            }

            Date date = new Date(value);
            if (format != null) {
                return formatDate(date);
            }

            switch (type) {
                case "date":
                    return new java.sql.Date(value);
                case "time":
                    return new java.sql.Time(value);
                default:
                    return date;
            }
        } catch (Exception e) {
            return new Date();
        }
    }

    @Override
    public String getType() {
        return "date";
    }

    @Override
    public Object getParams() {
        return this;
    }

    private Date parseDate(String dateStr) {
        if (format != null) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            if (type.equals("date")) {
                return java.sql.Date.valueOf(LocalDate.parse(dateStr, formatter));
            } else if (type.equals("time")) {
                return java.sql.Time.valueOf(LocalTime.parse(dateStr, formatter));
            } else {
                return java.sql.Timestamp.valueOf(LocalDateTime.parse(dateStr, formatter));
            }
        }
        return new Date(dateStr);
    }

    private String formatDate(Date date) {
        if (format == null) {
            return date.toString();
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        if (type.equals("date")) {
            return formatter.format(date.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDate());
        } else if (type.equals("time")) {
            return formatter.format(date.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalTime());
        } else {
            return formatter.format(date.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDateTime());
        }
    }
} 
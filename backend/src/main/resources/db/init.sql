-- 创建数据库
CREATE DATABASE IF NOT EXISTS data_generator DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE data_generator;

-- 数据源表
CREATE TABLE IF NOT EXISTS data_source (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    name VARCHAR(100) NOT NULL COMMENT '数据源名称',
    type VARCHAR(20) NOT NULL COMMENT '数据源类型',
    url VARCHAR(500) NOT NULL COMMENT '连接URL',
    username VARCHAR(100) NOT NULL COMMENT '用户名',
    password VARCHAR(100) NOT NULL COMMENT '密码',
    driver_class_name VARCHAR(100) COMMENT '驱动类名',
    description VARCHAR(500) COMMENT '描述',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    deleted TINYINT NOT NULL DEFAULT 0 COMMENT '是否删除'
) COMMENT '数据源配置表';

-- 数据生成任务表
CREATE TABLE IF NOT EXISTS data_task (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    name VARCHAR(100) NOT NULL COMMENT '任务名称',
    data_source_id BIGINT NOT NULL COMMENT '数据源ID',
    target_type VARCHAR(20) NOT NULL COMMENT '目标类型',
    target_name VARCHAR(500) NOT NULL COMMENT '目标名称',
    write_mode VARCHAR(20) NOT NULL COMMENT '写入模式',
    data_format VARCHAR(20) NOT NULL COMMENT '数据格式',
    template TEXT COMMENT '数据生成模板',
    batch_size INT NOT NULL DEFAULT 1000 COMMENT '批量大小',
    frequency INT NOT NULL DEFAULT 1 COMMENT '生成频率(秒)',
    concurrent_num INT NOT NULL DEFAULT 1 COMMENT '并发数',
    status VARCHAR(20) NOT NULL DEFAULT 'STOPPED' COMMENT '任务状态',
    cron_expression VARCHAR(100) COMMENT '定时任务表达式',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    deleted TINYINT NOT NULL DEFAULT 0 COMMENT '是否删除',
    FOREIGN KEY (data_source_id) REFERENCES data_source(id)
) COMMENT '数据生成任务表';

-- 任务执行记录表
CREATE TABLE IF NOT EXISTS task_execution (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    task_id BIGINT NOT NULL COMMENT '任务ID',
    start_time DATETIME NOT NULL COMMENT '开始时间',
    end_time DATETIME COMMENT '结束时间',
    status VARCHAR(20) NOT NULL COMMENT '执行状态',
    total_count BIGINT NOT NULL DEFAULT 0 COMMENT '总记录数',
    success_count BIGINT NOT NULL DEFAULT 0 COMMENT '成功记录数',
    error_count BIGINT NOT NULL DEFAULT 0 COMMENT '失败记录数',
    error_message TEXT COMMENT '错误信息',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    deleted TINYINT NOT NULL DEFAULT 0 COMMENT '是否删除',
    FOREIGN KEY (task_id) REFERENCES data_task(id)
) COMMENT '任务执行记录表';
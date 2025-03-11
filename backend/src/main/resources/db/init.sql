-- MySQL dump 10.13  Distrib 5.7.42, for Win64 (x86_64)
--
-- Host: localhost    Database: data_generator
-- ------------------------------------------------------
-- Server version	5.7.42

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `data_source`
--

DROP TABLE IF EXISTS `data_source`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `data_source` (
                               `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
                               `name` varchar(100) NOT NULL COMMENT '数据源名称',
                               `type` varchar(20) NOT NULL COMMENT '数据源类型',
                               `url` varchar(500) NOT NULL COMMENT '连接URL',
                               `username` varchar(100) NOT NULL COMMENT '用户名',
                               `password` varchar(100) NOT NULL COMMENT '密码',
                               `driver_class_name` varchar(100) DEFAULT NULL COMMENT '驱动类名',
                               `description` varchar(500) DEFAULT NULL COMMENT '描述',
                               `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                               `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                               `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
                               PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COMMENT='数据源配置表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `data_task`
--

DROP TABLE IF EXISTS `data_task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `data_task` (
                             `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
                             `name` varchar(100) NOT NULL COMMENT '任务名称',
                             `data_source_id` bigint(20) NOT NULL COMMENT '数据源ID',
                             `target_type` varchar(20) NOT NULL COMMENT '目标类型',
                             `target_name` varchar(5000) NOT NULL COMMENT '目标名称',
                             `write_mode` varchar(20) NOT NULL COMMENT '写入模式',
                             `data_format` varchar(20) NOT NULL COMMENT '数据格式',
                             `template` text COMMENT '数据生成模板',
                             `batch_size` int(11) NOT NULL DEFAULT '1000' COMMENT '批量大小',
                             `frequency` int(11) NOT NULL DEFAULT '1' COMMENT '生成频率(秒)',
                             `concurrent_num` int(11) NOT NULL DEFAULT '1' COMMENT '并发数',
                             `status` varchar(20) NOT NULL DEFAULT 'STOPPED' COMMENT '任务状态',
                             `cron_expression` varchar(100) DEFAULT NULL COMMENT '定时任务表达式',
                             `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                             `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                             `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
                             PRIMARY KEY (`id`),
                             KEY `data_source_id` (`data_source_id`),
                             CONSTRAINT `data_task_ibfk_1` FOREIGN KEY (`data_source_id`) REFERENCES `data_source` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COMMENT='数据生成任务表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `system_info`
--

DROP TABLE IF EXISTS `system_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `system_info` (
                               `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
                               `cpu_usage` double DEFAULT NULL COMMENT 'CPU使用率',
                               `memory_usage` double DEFAULT NULL COMMENT '内存使用率',
                               `disk_usage` double DEFAULT NULL COMMENT '磁盘使用率',
                               `jvm_heap_usage` double DEFAULT NULL COMMENT 'JVM堆内存使用率',
                               `jvm_non_heap_usage` double DEFAULT NULL COMMENT 'JVM非堆内存使用率',
                               `uptime` bigint(20) DEFAULT NULL COMMENT '系统运行时间(毫秒)',
                               `create_time` datetime DEFAULT NULL COMMENT '创建时间',
                               PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1248 DEFAULT CHARSET=utf8mb4 COMMENT='系统信息表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `task_execution`
--

DROP TABLE IF EXISTS `task_execution`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task_execution` (
                                  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
                                  `task_id` bigint(20) NOT NULL COMMENT '任务ID',
                                  `start_time` datetime NOT NULL COMMENT '开始时间',
                                  `end_time` datetime DEFAULT NULL COMMENT '结束时间',
                                  `status` varchar(20) NOT NULL COMMENT '执行状态',
                                  `total_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '总记录数',
                                  `success_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '成功记录数',
                                  `error_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '失败记录数',
                                  `error_message` text COMMENT '错误信息',
                                  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                                  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                                  `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
                                  PRIMARY KEY (`id`),
                                  KEY `task_id` (`task_id`),
                                  CONSTRAINT `task_execution_ibfk_1` FOREIGN KEY (`task_id`) REFERENCES `data_task` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=217 DEFAULT CHARSET=utf8mb4 COMMENT='任务执行记录表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping routines for database 'data_generator'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-03-11 21:35:50

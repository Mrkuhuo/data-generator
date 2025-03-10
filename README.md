# 通用数据生成器 (Universal Data Generator)

## 项目概述
通用数据生成器是一个灵活且强大的工具，用于生成模拟数据并支持多种数据源。该工具可以帮助开发人员、测试人员和数据分析师快速生成符合特定业务场景的测试数据。目前版本支持MySQL和Kafka数据源。

## 核心功能

### 1. 多数据源支持
- **关系型数据库**
  - MySQL（当前版本已支持）
  - Oracle（计划支持）
  - PostgreSQL（计划支持）
  - SQL Server（计划支持）
  - 其他支持JDBC的数据库（计划支持）

- **消息队列**
  - Apache Kafka（当前版本已支持）
  - RabbitMQ（计划支持）
  - RocketMQ（计划支持）
  - 其他支持的消息中间件（计划支持）

### 2. 数据源配置
- 支持配置多个数据源连接
- 支持连接池管理
- 支持SSL/TLS加密连接
- 支持代理服务器配置

### 3. 数据生成规则
- **表/主题选择**
  - 支持选择指定数据库的所有表
  - 支持选择指定数据库的部分表
  - 支持选择Kafka主题
  - 支持创建新的Kafka主题

- **字段配置**
  - 支持自定义字段生成规则
  - 内置多种数据类型生成器：
    - 数值类型（整数、浮点数）
    - 字符串类型（随机字符串、姓名、地址等）
    - 日期时间类型
    - 布尔类型
    - JSON类型
    - 数组类型 
    - 自定义类型

- **数据关联**
  - 支持表间关联关系
  - 支持外键约束
  - 支持数据一致性维护

### 4. 数据输出控制
- **写入模式**
  - 覆盖模式：清空目标表后写入
  - 追加模式：保留现有数据，追加新数据
  - 更新模式：根据主键更新现有数据

- **批量控制**
  - 支持配置批量写入大小
  - 支持配置写入频率
  - 支持配置并发写入数

### 5. 数据格式配置
- **Kafka消息格式**
  - JSON格式（支持自定义结构）
  - Avro格式
  - Protobuf格式
  - 自定义格式

- **数据模板**
  - 支持自定义数据模板
  - 支持模板变量替换
  - 支持条件判断和循环

### 6. 任务管理
- 支持创建多个数据生成任务
- 支持任务调度（定时执行）
- 支持任务暂停/恢复
- 支持任务监控和统计

### 7. 监控和日志
- 实时监控数据生成进度
- 详细的执行日志记录
- 错误告警和通知
- 性能指标统计

## 技术架构

### 1. 核心组件
- 数据源连接管理器
- 数据生成引擎
- 任务调度器
- 监控系统
- 配置管理系统

### 2. 扩展性设计
- 插件化架构
- 自定义数据生成器接口
- 自定义数据源适配器
- 自定义输出格式处理器

## 使用场景

### 1. 测试数据生成
- 单元测试
- 集成测试
- 性能测试
- 压力测试

### 2. 开发环境搭建
- 快速构建开发环境
- 模拟生产环境数据
- 数据迁移测试

### 3. 数据分析
- 数据可视化测试
- 报表开发测试
- 数据挖掘测试

## 部署要求

### 1. 系统要求
- JDK 11或更高版本
- 最小内存：4GB
- 推荐内存：8GB或更高
- 磁盘空间：根据数据量配置

### 2. 依赖组件
- 数据库驱动
- 消息队列客户端
- 配置中心（可选）
- 监控系统（可选）

## 安全特性

### 1. 数据安全
- 敏感数据脱敏
- 数据加密传输
- 访问权限控制

### 2. 系统安全
- 用户认证
- 角色授权
- 操作审计
- 安全日志

## 后续规划

### 1. 功能增强
- 支持更多数据源
- 增强数据生成规则
- 优化性能
- 提供Web管理界面

### 2. 生态集成
- 支持容器化部署
- 支持云平台集成
- 支持CI/CD集成
- 支持监控系统集成

## 贡献指南
欢迎提交Issue和Pull Request来帮助改进项目。在提交代码前，请确保：
1. 代码符合项目规范
2. 添加必要的测试用例
3. 更新相关文档
4. 提供清晰的提交信息

## 许可证
本项目采用 MIT 许可证

# 数据生成器应用

这是一个用于生成模拟数据的应用程序，**当前版本仅支持MySQL和Kafka数据源**。

## 系统要求

- Docker
- Docker Compose

## 开发环境搭建

### 1. 数据库初始化

在开始使用应用前，需要先创建数据库和相关表结构：

```sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS data_generator DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

-- 使用数据库
USE data_generator;

-- 创建数据源配置表
CREATE TABLE IF NOT EXISTS `data_source` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `name` varchar(100) NOT NULL COMMENT '数据源名称',
  `type` varchar(50) NOT NULL COMMENT '数据源类型：MYSQL, KAFKA等',
  `url` varchar(500) NOT NULL COMMENT '连接URL',
  `username` varchar(100) DEFAULT NULL COMMENT '用户名',
  `password` varchar(100) DEFAULT NULL COMMENT '密码',
  `driver_class` varchar(200) DEFAULT NULL COMMENT '驱动类名',
  `properties` text DEFAULT NULL COMMENT '其他连接属性，JSON格式',
  `status` tinyint(1) DEFAULT 1 COMMENT '状态：0-禁用，1-启用',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据源配置表';

-- 创建任务配置表
CREATE TABLE IF NOT EXISTS `task_config` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `name` varchar(100) NOT NULL COMMENT '任务名称',
  `data_source_id` bigint(20) NOT NULL COMMENT '数据源ID',
  `target_table` varchar(100) DEFAULT NULL COMMENT '目标表名',
  `topic_name` varchar(100) DEFAULT NULL COMMENT 'Kafka主题名',
  `batch_size` int(11) DEFAULT 1000 COMMENT '批量大小',
  `total_count` bigint(20) DEFAULT 0 COMMENT '总记录数',
  `frequency` int(11) DEFAULT 0 COMMENT '频率(ms)',
  `write_mode` varchar(20) DEFAULT 'APPEND' COMMENT '写入模式：OVERWRITE, APPEND, UPDATE',
  `status` varchar(20) DEFAULT 'CREATED' COMMENT '状态：CREATED, RUNNING, PAUSED, COMPLETED, FAILED',
  `cron_expression` varchar(100) DEFAULT NULL COMMENT 'Cron表达式',
  `config_json` text DEFAULT NULL COMMENT '配置JSON',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_name` (`name`),
  KEY `idx_data_source_id` (`data_source_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务配置表';

-- 创建任务执行记录表
CREATE TABLE IF NOT EXISTS `task_execution` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `task_id` bigint(20) NOT NULL COMMENT '任务ID',
  `start_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
  `end_time` datetime DEFAULT NULL COMMENT '结束时间',
  `status` varchar(20) NOT NULL COMMENT '状态：RUNNING, COMPLETED, FAILED',
  `processed_count` bigint(20) DEFAULT 0 COMMENT '已处理记录数',
  `error_message` text DEFAULT NULL COMMENT '错误信息',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务执行记录表';

-- 创建字段配置表
CREATE TABLE IF NOT EXISTS `field_config` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `task_id` bigint(20) NOT NULL COMMENT '任务ID',
  `field_name` varchar(100) NOT NULL COMMENT '字段名',
  `field_type` varchar(50) NOT NULL COMMENT '字段类型',
  `generator_type` varchar(50) NOT NULL COMMENT '生成器类型',
  `generator_config` text DEFAULT NULL COMMENT '生成器配置，JSON格式',
  `is_primary_key` tinyint(1) DEFAULT 0 COMMENT '是否主键',
  `is_nullable` tinyint(1) DEFAULT 1 COMMENT '是否可为空',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_task_field` (`task_id`, `field_name`),
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='字段配置表';
```

您可以将上述SQL保存为`init.sql`文件，然后执行：

```bash
mysql -u用户名 -p密码 < init.sql
```

或者在MySQL客户端中直接执行这些SQL语句。

### 2. 后端开发环境启动

#### 环境要求
- JDK 11+
- Maven 3.6+
- MySQL 5.7+
- Kafka 2.8+

#### 步骤

1. 进入后端目录
```bash
cd backend
```

2. 编译项目
```bash
mvn clean package -DskipTests
```

3. 启动应用
```bash
java -jar target/data-generator-backend-1.0.0.jar
```

或者在开发IDE中直接运行`com.datagenerator.DataGeneratorApplication`类。

#### 配置说明

后端配置文件位于`backend/src/main/resources/application.yml`，您可以根据需要修改数据库连接、Kafka配置等。

### 3. 前端开发环境启动

#### 环境要求
- Node.js 14+
- npm 6+ 或 yarn 1.22+

#### 步骤

1. 进入前端目录
```bash
cd frontend
```

2. 安装依赖
```bash
npm install 
# 或
yarn install
```

3. 启动开发服务器
```bash
npm run serve
# 或
yarn serve
```

4. 构建生产版本
```bash
npm run build
# 或
yarn build
```

#### 配置说明

前端API配置文件位于`frontend/src/config/index.js`，您可以根据需要修改API地址等配置。

## 项目结构

- `frontend/`: 前端Vue应用
- `backend/`: 后端Spring Boot应用
- `docker-compose.yml`: Docker Compose配置文件
- `start.sh`/`start.bat`: 启动脚本
- `stop.sh`/`stop.bat`: 停止脚本

## 配置说明

### 数据库配置（MySQL）

默认的MySQL数据库配置：

- 数据库名：data_generator
- 用户名：datagenerator
- 密码：datagenerator123

如需修改，请编辑`docker-compose.yml`文件中的相关配置。

### Kafka配置

默认创建的主题：test

如需添加更多主题，请修改`docker-compose.yml`文件中的`KAFKA_CREATE_TOPICS`配置。

## 故障排除

如果应用启动失败，可以查看Docker日志：

```bash
docker-compose logs
```

针对特定服务的日志：

```bash
docker-compose logs backend
docker-compose logs frontend
docker-compose logs mysql
docker-compose logs kafka
```

### 常见问题

1. **数据库连接失败**
   - 检查数据库服务是否正常运行
   - 验证连接信息是否正确
   - 确认数据库用户是否有足够权限

2. **Kafka连接问题**
   - 检查Kafka服务是否正常运行
   - 验证主题是否已创建
   - 检查网络连接是否通畅

3. **前端无法连接后端API**
   - 确认后端服务是否正常运行
   - 检查前端配置中的API地址是否正确
   - 检查是否存在跨域问题

# 数据生成器项目部署文档

## 项目结构

```
data-generator/
├── backend/                # 后端项目目录
│   ├── src/               # 源代码
│   ├── pom.xml           # Maven配置文件
│   └── Dockerfile        # 后端Docker构建文件
├── frontend/              # 前端项目目录
│   ├── src/              # 源代码
│   ├── package.json      # npm配置文件
│   ├── nginx.conf        # nginx配置文件
│   └── Dockerfile        # 前端Docker构建文件
├── docker-compose.yml     # Docker Compose配置文件
├── deploy.sh             # 一键部署脚本
└── README.md             # 项目说明文档
```

## 环境要求

- Docker 20.10.0+
- Docker Compose 2.0.0+
- 操作系统：Linux/MacOS/Windows
- 内存：至少4GB RAM
- 磁盘空间：至少10GB可用空间

## 快速开始

1. 克隆项目：
```bash
git clone <项目地址>
cd data-generator
```

2. 运行部署脚本：
```bash
chmod +x deploy.sh  # 给脚本添加执行权限（Linux/MacOS）
./deploy.sh         # 运行部署脚本
```

3. 访问应用：
- 前端界面：http://localhost
- 后端API：http://localhost:8080
- 数据库：localhost:3306

## 配置说明

### 数据库配置
- 数据库：MySQL 8.0
- 用户名：data_generator
- 密码：data_generator
- 数据库名：data_generator

### 端口配置
- 前端：80
- 后端：8080
- MySQL：3306

## 目录说明

### 后端项目
- `backend/src/`: 后端源代码目录
- `backend/pom.xml`: Maven项目配置文件
- `backend/Dockerfile`: 后端Docker镜像构建文件

### 前端项目
- `frontend/src/`: 前端源代码目录
- `frontend/package.json`: npm项目配置文件
- `frontend/nginx.conf`: nginx服务器配置文件
- `frontend/Dockerfile`: 前端Docker镜像构建文件

## 部署说明

### 手动部署步骤

1. 构建镜像：
```bash
docker-compose build
```

2. 启动服务：
```bash
docker-compose up -d
```

3. 查看服务状态：
```bash
docker-compose ps
```

4. 查看服务日志：
```bash
docker-compose logs
```

### 停止服务

```bash
docker-compose down
```

## 常见问题

1. 端口冲突
- 问题：服务启动失败，提示端口被占用
- 解决：修改`docker-compose.yml`中的端口映射配置

2. 数据库连接失败
- 问题：后端服务无法连接到数据库
- 解决：检查数据库配置和网络连接

3. 前端访问后端API失败
- 问题：前端页面无法加载数据
- 解决：检查nginx配置中的代理设置

## 维护和更新

1. 更新应用：
```bash
git pull                # 获取最新代码
./deploy.sh            # 重新部署
```

2. 查看容器日志：
```bash
docker-compose logs -f  # 实时查看所有服务的日志
docker-compose logs backend  # 查看后端服务日志
docker-compose logs frontend # 查看前端服务日志
```

3. 备份数据：
```bash
docker exec data-generator-mysql mysqldump -u root -p data_generator > backup.sql
```

## 技术栈

- 后端：Spring Boot
- 前端：Vue.js
- 数据库：MySQL
- 服务器：Nginx
- 容器化：Docker & Docker Compose

## 注意事项

1. 生产环境部署
- 修改数据库密码
- 配置HTTPS
- 启用数据库备份
- 配置监控告警

2. 安全建议
- 定期更新依赖
- 限制数据库远程访问
- 使用安全的密码
- 配置防火墙规则

#!/bin/bash

echo "开始启动数据生成器应用..."

# 检查Docker是否安装
if ! command -v docker &> /dev/null; then
    echo "错误: Docker未安装，请先安装Docker和Docker Compose"
    exit 1
fi

# 检查Docker Compose是否安装
if ! command -v docker-compose &> /dev/null; then
    echo "错误: Docker Compose未安装，请先安装Docker Compose"
    exit 1
fi

# 构建并启动容器
echo "构建并启动容器..."
docker-compose up -d --build

# 等待服务启动
echo "等待服务启动..."
sleep 10

# 检查服务是否正常运行
if docker-compose ps | grep -q "Up"; then
    echo "服务已成功启动!"
    echo "前端访问地址: http://localhost"
    echo "后端API地址: http://localhost:8888"
    echo "Kafka地址: localhost:9092"
    echo "MySQL地址: localhost:3306"
else
    echo "服务启动失败，请检查日志:"
    docker-compose logs
fi 
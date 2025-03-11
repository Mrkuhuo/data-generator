#!/bin/bash

# 输出颜色设置
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 打印带颜色的信息
print_info() {
    echo -e "${GREEN}[INFO] $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

print_error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

# 检查Docker是否安装
if ! command -v docker &> /dev/null; then
    print_error "Docker未安装，请先安装Docker"
    exit 1
fi

# 检查Docker Compose是否安装
if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose未安装，请先安装Docker Compose"
    exit 1
fi

# 停止并删除现有容器
print_info "停止并删除现有容器..."
docker-compose down

# 删除旧的镜像
print_info "删除旧的镜像..."
docker rmi data-generator-frontend data-generator-backend 2>/dev/null || true

# 构建新的镜像
print_info "开始构建镜像..."
docker-compose build --no-cache

# 启动服务
print_info "启动服务..."
docker-compose up -d

# 检查服务状态
print_info "检查服务状态..."
sleep 10

if docker-compose ps | grep -q "Up"; then
    print_info "服务已成功启动！"
    echo -e "${GREEN}访问地址：${NC}"
    echo -e "  前端: ${GREEN}http://localhost${NC}"
    echo -e "  后端: ${GREEN}http://localhost:8080${NC}"
    echo -e "  数据库: ${GREEN}localhost:3306${NC}"
else
    print_error "服务启动失败，请检查日志："
    docker-compose logs
fi 
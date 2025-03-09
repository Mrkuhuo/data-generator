@echo off
echo 开始启动数据生成器应用...

REM 检查Docker是否安装
where docker >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo 错误: Docker未安装，请先安装Docker和Docker Compose
    exit /b 1
)

REM 检查Docker Compose是否安装
where docker-compose >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo 错误: Docker Compose未安装，请先安装Docker Compose
    exit /b 1
)

REM 构建并启动容器
echo 构建并启动容器...
docker-compose up -d --build

REM 等待服务启动
echo 等待服务启动...
timeout /t 10 /nobreak >nul

REM 检查服务是否正常运行
docker-compose ps
echo.
echo 如果上面显示的服务状态都是"Up"，则服务已成功启动!
echo 前端访问地址: http://localhost
echo 后端API地址: http://localhost:8888
echo Kafka地址: localhost:9092
echo MySQL地址: localhost:3306
echo.
echo 如果服务启动失败，请运行 "docker-compose logs" 查看详细日志 
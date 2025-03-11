#!/bin/bash

# 设置版本号
VERSION="1.0.0"
PACKAGE_NAME="data-generator-${VERSION}-bin"

echo "开始打包数据生成器应用 v${VERSION}..."

# 创建临时目录
mkdir -p ${PACKAGE_NAME}

# 复制必要的文件
echo "复制项目文件..."
cp -r backend ${PACKAGE_NAME}/
cp -r frontend ${PACKAGE_NAME}/
cp docker-compose.yml ${PACKAGE_NAME}/
cp start.sh ${PACKAGE_NAME}/
cp stop.sh ${PACKAGE_NAME}/
cp start.bat ${PACKAGE_NAME}/
cp stop.bat ${PACKAGE_NAME}/
cp README.md ${PACKAGE_NAME}/

# 创建bin目录
mkdir -p ${PACKAGE_NAME}/bin

# 创建启动脚本
cat > ${PACKAGE_NAME}/bin/startup.sh << 'EOF'
#!/bin/bash

# 获取脚本所在目录的绝对路径
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BASE_DIR=$(dirname "$SCRIPT_DIR")

# 切换到项目根目录
cd "$BASE_DIR"

# 执行启动脚本
bash start.sh
EOF

# 创建停止脚本
cat > ${PACKAGE_NAME}/bin/shutdown.sh << 'EOF'
#!/bin/bash

# 获取脚本所在目录的绝对路径
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BASE_DIR=$(dirname "$SCRIPT_DIR")

# 切换到项目根目录
cd "$BASE_DIR"

# 执行停止脚本
bash stop.sh
EOF

# 创建Windows启动脚本
cat > ${PACKAGE_NAME}/bin/startup.bat << 'EOF'
@echo off
rem 获取脚本所在目录
set "SCRIPT_DIR=%~dp0"
set "BASE_DIR=%SCRIPT_DIR%.."

rem 切换到项目根目录
cd /d "%BASE_DIR%"

rem 执行启动脚本
call start.bat
EOF

# 创建Windows停止脚本
cat > ${PACKAGE_NAME}/bin/shutdown.bat << 'EOF'
@echo off
rem 获取脚本所在目录
set "SCRIPT_DIR=%~dp0"
set "BASE_DIR=%SCRIPT_DIR%.."

rem 切换到项目根目录
cd /d "%BASE_DIR%"

rem 执行停止脚本
call stop.bat
EOF

# 添加执行权限
chmod +x ${PACKAGE_NAME}/start.sh
chmod +x ${PACKAGE_NAME}/stop.sh
chmod +x ${PACKAGE_NAME}/bin/startup.sh
chmod +x ${PACKAGE_NAME}/bin/shutdown.sh

# 创建压缩包
echo "创建压缩包..."
tar -czf ${PACKAGE_NAME}.tar.gz ${PACKAGE_NAME}

# 清理临时目录
echo "清理临时文件..."
rm -rf ${PACKAGE_NAME}

echo "打包完成: ${PACKAGE_NAME}.tar.gz"
echo "解压后，可以使用以下命令启动应用："
echo "Linux/Mac: ./bin/startup.sh"
echo "Windows: bin\\startup.bat" 
@echo off
setlocal enabledelayedexpansion

REM 设置版本号
set VERSION=1.0.0
set PACKAGE_NAME=data-generator-%VERSION%-bin

echo 开始打包数据生成器应用 v%VERSION%...

REM 创建临时目录
if exist %PACKAGE_NAME% rmdir /s /q %PACKAGE_NAME%
mkdir %PACKAGE_NAME%
mkdir %PACKAGE_NAME%\bin
mkdir %PACKAGE_NAME%\backend
mkdir %PACKAGE_NAME%\frontend

REM 复制必要的文件
echo 复制项目文件...
copy docker-compose.yml %PACKAGE_NAME%\
copy start.bat %PACKAGE_NAME%\
copy stop.bat %PACKAGE_NAME%\
copy start.sh %PACKAGE_NAME%\
copy stop.sh %PACKAGE_NAME%\
copy README.md %PACKAGE_NAME%\

REM 创建启动和停止脚本
echo 创建启动和停止脚本...

REM Windows启动脚本
echo @echo off > %PACKAGE_NAME%\bin\startup.bat
echo rem 获取脚本所在目录 >> %PACKAGE_NAME%\bin\startup.bat
echo set "SCRIPT_DIR=%%~dp0" >> %PACKAGE_NAME%\bin\startup.bat
echo set "BASE_DIR=%%SCRIPT_DIR%%.." >> %PACKAGE_NAME%\bin\startup.bat
echo. >> %PACKAGE_NAME%\bin\startup.bat
echo rem 切换到项目根目录 >> %PACKAGE_NAME%\bin\startup.bat
echo cd /d "%%BASE_DIR%%" >> %PACKAGE_NAME%\bin\startup.bat
echo. >> %PACKAGE_NAME%\bin\startup.bat
echo rem 执行启动脚本 >> %PACKAGE_NAME%\bin\startup.bat
echo call start.bat >> %PACKAGE_NAME%\bin\startup.bat

REM Windows停止脚本
echo @echo off > %PACKAGE_NAME%\bin\shutdown.bat
echo rem 获取脚本所在目录 >> %PACKAGE_NAME%\bin\shutdown.bat
echo set "SCRIPT_DIR=%%~dp0" >> %PACKAGE_NAME%\bin\shutdown.bat
echo set "BASE_DIR=%%SCRIPT_DIR%%.." >> %PACKAGE_NAME%\bin\shutdown.bat
echo. >> %PACKAGE_NAME%\bin\shutdown.bat
echo rem 切换到项目根目录 >> %PACKAGE_NAME%\bin\shutdown.bat
echo cd /d "%%BASE_DIR%%" >> %PACKAGE_NAME%\bin\shutdown.bat
echo. >> %PACKAGE_NAME%\bin\shutdown.bat
echo rem 执行停止脚本 >> %PACKAGE_NAME%\bin\shutdown.bat
echo call stop.bat >> %PACKAGE_NAME%\bin\shutdown.bat

REM 使用PowerShell创建Linux脚本
powershell -Command "Set-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value '#!/bin/bash' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value '' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value '# 获取脚本所在目录的绝对路径' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value 'SCRIPT_DIR=$(cd \"$(dirname \"$0\")\" && pwd)' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value 'BASE_DIR=$(dirname \"$SCRIPT_DIR\")' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value '' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value '# 切换到项目根目录' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value 'cd \"$BASE_DIR\"' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value '' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value '# 添加执行权限' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value 'chmod +x start.sh' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value '' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value '# 执行启动脚本' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\startup.sh' -Value './start.sh' -Encoding ASCII"

powershell -Command "Set-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value '#!/bin/bash' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value '' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value '# 获取脚本所在目录的绝对路径' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value 'SCRIPT_DIR=$(cd \"$(dirname \"$0\")\" && pwd)' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value 'BASE_DIR=$(dirname \"$SCRIPT_DIR\")' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value '' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value '# 切换到项目根目录' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value 'cd \"$BASE_DIR\"' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value '' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value '# 添加执行权限' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value 'chmod +x stop.sh' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value '' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value '# 执行停止脚本' -Encoding ASCII"
powershell -Command "Add-Content -Path '%PACKAGE_NAME%\bin\shutdown.sh' -Value './stop.sh' -Encoding ASCII"

REM 使用PowerShell复制目录并排除node_modules
echo 使用PowerShell复制目录...
powershell -Command "Get-ChildItem -Path 'backend' -Recurse -File | Where-Object { $_.FullName -notlike '*\node_modules\*' -and $_.FullName -notlike '*\target\*' -and $_.FullName -notlike '*\.git\*' -and $_.FullName -notlike '*\.idea\*' -and $_.FullName -notlike '*\.vscode\*' } | ForEach-Object { $destPath = $_.FullName.Replace((Get-Location).Path + '\backend', '%PACKAGE_NAME%\backend'); $destDir = Split-Path -Path $destPath -Parent; if (-not (Test-Path $destDir)) { New-Item -Path $destDir -ItemType Directory -Force | Out-Null }; Copy-Item -Path $_.FullName -Destination $destPath -Force }"

powershell -Command "Get-ChildItem -Path 'frontend' -Recurse -File | Where-Object { $_.FullName -notlike '*\node_modules\*' -and $_.FullName -notlike '*\.git\*' -and $_.FullName -notlike '*\.idea\*' -and $_.FullName -notlike '*\.vscode\*' } | ForEach-Object { $destPath = $_.FullName.Replace((Get-Location).Path + '\frontend', '%PACKAGE_NAME%\frontend'); $destDir = Split-Path -Path $destPath -Parent; if (-not (Test-Path $destDir)) { New-Item -Path $destDir -ItemType Directory -Force | Out-Null }; Copy-Item -Path $_.FullName -Destination $destPath -Force }"

REM 创建压缩包
echo 创建压缩包...
powershell -Command "Compress-Archive -Path '%PACKAGE_NAME%\*' -DestinationPath '%PACKAGE_NAME%.zip' -Force"

REM 清理临时目录
echo 清理临时文件...
rmdir /s /q %PACKAGE_NAME%

echo 打包完成!
echo 创建了文件: %PACKAGE_NAME%.zip
echo 解压后，可以使用以下命令启动应用：
echo Linux/Mac: ./bin/startup.sh
echo Windows: bin/startup.bat

endlocal 
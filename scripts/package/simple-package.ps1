# 设置版本号
$VERSION = "1.0.0"
$PACKAGE_NAME = "data-generator-$VERSION-bin"

Write-Host "开始打包数据生成器应用 v$VERSION..."

# 创建临时目录
if (Test-Path $PACKAGE_NAME) {
    Remove-Item -Path $PACKAGE_NAME -Recurse -Force
}
New-Item -Path $PACKAGE_NAME -ItemType Directory | Out-Null
New-Item -Path "$PACKAGE_NAME\bin" -ItemType Directory | Out-Null
New-Item -Path "$PACKAGE_NAME\backend" -ItemType Directory | Out-Null
New-Item -Path "$PACKAGE_NAME\frontend" -ItemType Directory | Out-Null

# 复制必要的文件
Write-Host "复制项目文件..."
Copy-Item -Path "docker-compose.yml" -Destination "$PACKAGE_NAME\" -Force
Copy-Item -Path "start.bat" -Destination "$PACKAGE_NAME\" -Force
Copy-Item -Path "stop.bat" -Destination "$PACKAGE_NAME\" -Force
Copy-Item -Path "start.sh" -Destination "$PACKAGE_NAME\" -Force
Copy-Item -Path "stop.sh" -Destination "$PACKAGE_NAME\" -Force
Copy-Item -Path "README.md" -Destination "$PACKAGE_NAME\" -Force

# 创建启动和停止脚本
Write-Host "创建启动和停止脚本..."

# Windows启动脚本
$startupBat = "@echo off`r`n"
$startupBat += "rem 获取脚本所在目录`r`n"
$startupBat += "set ""SCRIPT_DIR=%~dp0""`r`n"
$startupBat += "set ""BASE_DIR=%SCRIPT_DIR%..""`r`n"
$startupBat += "`r`n"
$startupBat += "rem 切换到项目根目录`r`n"
$startupBat += "cd /d ""%BASE_DIR%""`r`n"
$startupBat += "`r`n"
$startupBat += "rem 执行启动脚本`r`n"
$startupBat += "call start.bat`r`n"
[System.IO.File]::WriteAllText("$PACKAGE_NAME\bin\startup.bat", $startupBat, [System.Text.Encoding]::ASCII)

# Windows停止脚本
$shutdownBat = "@echo off`r`n"
$shutdownBat += "rem 获取脚本所在目录`r`n"
$shutdownBat += "set ""SCRIPT_DIR=%~dp0""`r`n"
$shutdownBat += "set ""BASE_DIR=%SCRIPT_DIR%..""`r`n"
$shutdownBat += "`r`n"
$shutdownBat += "rem 切换到项目根目录`r`n"
$shutdownBat += "cd /d ""%BASE_DIR%""`r`n"
$shutdownBat += "`r`n"
$shutdownBat += "rem 执行停止脚本`r`n"
$shutdownBat += "call stop.bat`r`n"
[System.IO.File]::WriteAllText("$PACKAGE_NAME\bin\shutdown.bat", $shutdownBat, [System.Text.Encoding]::ASCII)

# Linux启动脚本
$startupSh = "#!/bin/bash`n"
$startupSh += "`n"
$startupSh += "# 获取脚本所在目录的绝对路径`n"
$startupSh += "SCRIPT_DIR=\$(cd \"\$(dirname \"\$0\")\" && pwd)`n"
$startupSh += "BASE_DIR=\$(dirname \"\$SCRIPT_DIR\")`n"
$startupSh += "`n"
$startupSh += "# 切换到项目根目录`n"
$startupSh += "cd \"\$BASE_DIR\"`n"
$startupSh += "`n"
$startupSh += "# 添加执行权限`n"
$startupSh += "chmod +x start.sh`n"
$startupSh += "`n"
$startupSh += "# 执行启动脚本`n"
$startupSh += "./start.sh`n"
[System.IO.File]::WriteAllText("$PACKAGE_NAME\bin\startup.sh", $startupSh, [System.Text.Encoding]::ASCII)

# Linux停止脚本
$shutdownSh = "#!/bin/bash`n"
$shutdownSh += "`n"
$shutdownSh += "# 获取脚本所在目录的绝对路径`n"
$shutdownSh += "SCRIPT_DIR=\$(cd \"\$(dirname \"\$0\")\" && pwd)`n"
$shutdownSh += "BASE_DIR=\$(dirname \"\$SCRIPT_DIR\")`n"
$shutdownSh += "`n"
$shutdownSh += "# 切换到项目根目录`n"
$shutdownSh += "cd \"\$BASE_DIR\"`n"
$shutdownSh += "`n"
$shutdownSh += "# 添加执行权限`n"
$shutdownSh += "chmod +x stop.sh`n"
$shutdownSh += "`n"
$shutdownSh += "# 执行停止脚本`n"
$shutdownSh += "./stop.sh`n"
[System.IO.File]::WriteAllText("$PACKAGE_NAME\bin\shutdown.sh", $shutdownSh, [System.Text.Encoding]::ASCII)

# 复制backend目录，排除node_modules和target
Write-Host "复制backend目录..."
$excludeDirs = @("node_modules", "target", ".git", ".idea", ".vscode")

Get-ChildItem -Path "backend" -Recurse -File | ForEach-Object {
    $fullPath = $_.FullName
    $exclude = $false
    
    foreach ($dir in $excludeDirs) {
        if ($fullPath -match [regex]::Escape("\$dir\")) {
            $exclude = $true
            break
        }
    }
    
    if (-not $exclude) {
        $relativePath = $fullPath.Substring((Get-Location).Path.Length + 9) # +9 for "\backend\"
        $targetPath = "$PACKAGE_NAME\backend\$relativePath"
        $targetDir = Split-Path -Path $targetPath -Parent
        
        if (-not (Test-Path $targetDir)) {
            New-Item -Path $targetDir -ItemType Directory -Force | Out-Null
        }
        
        Copy-Item -Path $fullPath -Destination $targetPath -Force
    }
}

# 复制frontend目录，排除node_modules
Write-Host "复制frontend目录..."
Get-ChildItem -Path "frontend" -Recurse -File | ForEach-Object {
    $fullPath = $_.FullName
    $exclude = $false
    
    foreach ($dir in $excludeDirs) {
        if ($fullPath -match [regex]::Escape("\$dir\")) {
            $exclude = $true
            break
        }
    }
    
    if (-not $exclude) {
        $relativePath = $fullPath.Substring((Get-Location).Path.Length + 10) # +10 for "\frontend\"
        $targetPath = "$PACKAGE_NAME\frontend\$relativePath"
        $targetDir = Split-Path -Path $targetPath -Parent
        
        if (-not (Test-Path $targetDir)) {
            New-Item -Path $targetDir -ItemType Directory -Force | Out-Null
        }
        
        Copy-Item -Path $fullPath -Destination $targetPath -Force
    }
}

# 创建压缩包
Write-Host "创建压缩包..."
Compress-Archive -Path "$PACKAGE_NAME\*" -DestinationPath "$PACKAGE_NAME.zip" -Force

# 清理临时目录
Write-Host "清理临时文件..."
Remove-Item -Path $PACKAGE_NAME -Recurse -Force

Write-Host "打包完成!"
Write-Host "创建了文件: $PACKAGE_NAME.zip"
Write-Host "解压后，可以使用以下命令启动应用："
Write-Host "Linux/Mac: ./bin/startup.sh"
Write-Host "Windows: bin/startup.bat" 
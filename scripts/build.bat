@echo off
setlocal enabledelayedexpansion

REM Binlog CLI Windows 构建脚本
REM 用于构建不同平台的二进制文件

set PROJECT_NAME=binlog_cli
set VERSION=0.0.3
set BUILD_DIR=target\release
set DIST_DIR=dist

echo [INFO] Binlog CLI 构建脚本 v%VERSION%
echo.

REM 检查 Cargo 是否安装
where cargo >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Cargo 未安装，请先安装 Rust
    exit /b 1
)

REM 解析命令行参数
set CLEAN=false
set BUILD_TYPE=local
set TARGET=

:parse_args
if "%~1"=="" goto end_parse
if "%~1"=="-h" goto show_help
if "%~1"=="--help" goto show_help
if "%~1"=="-c" set CLEAN=true
if "%~1"=="--clean" set CLEAN=true
if "%~1"=="-l" set BUILD_TYPE=local
if "%~1"=="--local" set BUILD_TYPE=local
if "%~1"=="-a" set BUILD_TYPE=all
if "%~1"=="--all" set BUILD_TYPE=all
if "%~1"=="-t" (
    set BUILD_TYPE=target
    shift
    set TARGET=%~2
)
if "%~1"=="--target" (
    set BUILD_TYPE=target
    shift
    set TARGET=%~2
)
shift
goto parse_args

:end_parse

REM 清理构建目录
if "%CLEAN%"=="true" (
    echo [INFO] 清理构建目录...
    cargo clean
    if exist "%DIST_DIR%" rmdir /s /q "%DIST_DIR%"
    mkdir "%DIST_DIR%"
    echo [SUCCESS] 构建目录清理完成
    echo.
)

REM 执行构建
if "%BUILD_TYPE%"=="local" goto build_local
if "%BUILD_TYPE%"=="all" goto build_all
if "%BUILD_TYPE%"=="target" goto build_target

:build_local
echo [INFO] 构建本地版本...
cargo build --release --bin %PROJECT_NAME%
if %errorlevel% equ 0 (
    echo [SUCCESS] 本地版本构建完成: %BUILD_DIR%\%PROJECT_NAME%.exe
) else (
    echo [ERROR] 本地版本构建失败
    exit /b 1
)
goto end

:build_all
echo [INFO] 构建所有支持的目标平台...
echo [WARNING] 在 Windows 上，交叉编译可能需要额外配置
echo.

REM Windows 目标
call :build_single_target x86_64-pc-windows-gnu
call :build_single_target x86_64-pc-windows-msvc

REM Linux 目标 (需要交叉编译工具)
call :build_single_target x86_64-unknown-linux-gnu

echo [INFO] 所有目标构建完成
goto end

:build_target
if "%TARGET%"=="" (
    echo [ERROR] 请指定目标平台
    exit /b 1
)
call :build_single_target %TARGET%
goto end

:build_single_target
set CURRENT_TARGET=%~1
set BINARY_NAME=%PROJECT_NAME%
if "%CURRENT_TARGET:windows=%"!="%CURRENT_TARGET%" set BINARY_NAME=%PROJECT_NAME%.exe

echo [INFO] 构建目标: %CURRENT_TARGET%

REM 检查目标是否已安装
rustup target list --installed | findstr /C:"%CURRENT_TARGET%" >nul
if %errorlevel% neq 0 (
    echo [INFO] 安装目标平台: %CURRENT_TARGET%
    rustup target add %CURRENT_TARGET%
)

REM 构建
cargo build --release --target %CURRENT_TARGET% --bin %PROJECT_NAME%
if %errorlevel% equ 0 (
    REM 创建发布目录
    set RELEASE_DIR=%DIST_DIR%\%PROJECT_NAME%-%VERSION%-%CURRENT_TARGET%
    if not exist "!RELEASE_DIR!" mkdir "!RELEASE_DIR!"
    
    REM 复制二进制文件
    copy "target\%CURRENT_TARGET%\release\%BINARY_NAME%" "!RELEASE_DIR!\"
    
    REM 复制文档和配置文件
    copy "binlog_cli\README.md" "!RELEASE_DIR!\" >nul 2>nul
    copy "LICENSE" "!RELEASE_DIR!\" >nul 2>nul
    if exist "conf" xcopy /E /I "conf" "!RELEASE_DIR!\conf" >nul 2>nul
    
    REM 创建安装脚本
    call :create_install_script "!RELEASE_DIR!" "%CURRENT_TARGET%" "%BINARY_NAME%"
    
    REM 打包
    cd "%DIST_DIR%"
    if "%CURRENT_TARGET:windows=%"!="%CURRENT_TARGET%" (
        powershell -Command "Compress-Archive -Path '%PROJECT_NAME%-%VERSION%-%CURRENT_TARGET%' -DestinationPath '%PROJECT_NAME%-%VERSION%-%CURRENT_TARGET%.zip' -Force"
    ) else (
        tar -czf "%PROJECT_NAME%-%VERSION%-%CURRENT_TARGET%.tar.gz" "%PROJECT_NAME%-%VERSION%-%CURRENT_TARGET%"
    )
    cd ..
    
    echo [SUCCESS] 构建完成: %CURRENT_TARGET%
) else (
    echo [ERROR] 构建失败: %CURRENT_TARGET%
)
goto :eof

:create_install_script
set SCRIPT_DIR=%~1
set SCRIPT_TARGET=%~2
set SCRIPT_BINARY=%~3

if "%SCRIPT_TARGET:windows=%"!="%SCRIPT_TARGET%" (
    REM Windows 安装脚本
    echo @echo off > "%SCRIPT_DIR%\install.bat"
    echo echo Installing binlog_cli... >> "%SCRIPT_DIR%\install.bat"
    echo. >> "%SCRIPT_DIR%\install.bat"
    echo REM 检查管理员权限 >> "%SCRIPT_DIR%\install.bat"
    echo net session ^>nul 2^>^&1 >> "%SCRIPT_DIR%\install.bat"
    echo if %%errorLevel%% == 0 ^( >> "%SCRIPT_DIR%\install.bat"
    echo     echo Running with administrator privileges >> "%SCRIPT_DIR%\install.bat"
    echo ^) else ^( >> "%SCRIPT_DIR%\install.bat"
    echo     echo This script requires administrator privileges >> "%SCRIPT_DIR%\install.bat"
    echo     echo Please run as administrator >> "%SCRIPT_DIR%\install.bat"
    echo     pause >> "%SCRIPT_DIR%\install.bat"
    echo     exit /b 1 >> "%SCRIPT_DIR%\install.bat"
    echo ^) >> "%SCRIPT_DIR%\install.bat"
    echo. >> "%SCRIPT_DIR%\install.bat"
    echo REM 创建安装目录 >> "%SCRIPT_DIR%\install.bat"
    echo if not exist "C:\Program Files\binlog_cli" mkdir "C:\Program Files\binlog_cli" >> "%SCRIPT_DIR%\install.bat"
    echo. >> "%SCRIPT_DIR%\install.bat"
    echo REM 复制文件 >> "%SCRIPT_DIR%\install.bat"
    echo copy %SCRIPT_BINARY% "C:\Program Files\binlog_cli\" >> "%SCRIPT_DIR%\install.bat"
    echo copy README.md "C:\Program Files\binlog_cli\" >> "%SCRIPT_DIR%\install.bat"
    echo if exist conf xcopy /E /I conf "C:\Program Files\binlog_cli\conf" >> "%SCRIPT_DIR%\install.bat"
    echo. >> "%SCRIPT_DIR%\install.bat"
    echo REM 添加到 PATH >> "%SCRIPT_DIR%\install.bat"
    echo setx /M PATH "%%PATH%%;C:\Program Files\binlog_cli" >> "%SCRIPT_DIR%\install.bat"
    echo. >> "%SCRIPT_DIR%\install.bat"
    echo echo Installation completed successfully! >> "%SCRIPT_DIR%\install.bat"
    echo echo You may need to restart your command prompt to use binlog_cli >> "%SCRIPT_DIR%\install.bat"
    echo pause >> "%SCRIPT_DIR%\install.bat"
)
goto :eof

:show_help
echo Binlog CLI Windows 构建脚本
echo.
echo 用法: %~nx0 [选项]
echo.
echo 选项:
echo   -h, --help     显示帮助信息
echo   -c, --clean    清理构建目录
echo   -l, --local    只构建本地版本 (默认)
echo   -a, --all      构建所有目标平台
echo   -t, --target   构建指定目标平台
echo.
echo 支持的目标平台:
echo   - x86_64-pc-windows-gnu
echo   - x86_64-pc-windows-msvc
echo   - x86_64-unknown-linux-gnu (需要交叉编译工具)
echo.
echo 示例:
echo   %~nx0 --local                           # 构建本地版本
echo   %~nx0 --all                             # 构建所有平台
echo   %~nx0 --target x86_64-pc-windows-msvc   # 构建指定平台
echo   %~nx0 --clean --local                   # 清理后构建本地版本
goto end

:end
echo.
echo [SUCCESS] 构建脚本执行完成!
pause
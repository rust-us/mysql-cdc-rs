#!/bin/bash

# Binlog CLI 构建脚本
# 用于构建不同平台的二进制文件

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目信息
PROJECT_NAME="binlog_cli"
VERSION="0.0.3"
BUILD_DIR="target/release"
DIST_DIR="dist"

# 支持的目标平台
TARGETS=(
    "x86_64-unknown-linux-gnu"
    "x86_64-pc-windows-gnu" 
    "x86_64-apple-darwin"
    "aarch64-unknown-linux-gnu"
    "aarch64-apple-darwin"
)

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查依赖
check_dependencies() {
    print_info "检查构建依赖..."
    
    if ! command -v cargo &> /dev/null; then
        print_error "Cargo 未安装，请先安装 Rust"
        exit 1
    fi
    
    if ! command -v rustc &> /dev/null; then
        print_error "Rustc 未安装，请先安装 Rust"
        exit 1
    fi
    
    print_success "依赖检查完成"
}

# 清理构建目录
clean_build() {
    print_info "清理构建目录..."
    cargo clean
    rm -rf "$DIST_DIR"
    mkdir -p "$DIST_DIR"
    print_success "构建目录清理完成"
}

# 构建单个目标
build_target() {
    local target=$1
    local binary_name="$PROJECT_NAME"
    
    if [[ "$target" == *"windows"* ]]; then
        binary_name="${PROJECT_NAME}.exe"
    fi
    
    print_info "构建目标: $target"
    
    # 检查目标是否已安装
    if ! rustup target list --installed | grep -q "$target"; then
        print_info "安装目标平台: $target"
        rustup target add "$target"
    fi
    
    # 构建
    if cargo build --release --target "$target" --bin "$PROJECT_NAME"; then
        # 创建发布目录
        local release_dir="$DIST_DIR/${PROJECT_NAME}-${VERSION}-${target}"
        mkdir -p "$release_dir"
        
        # 复制二进制文件
        cp "target/$target/release/$binary_name" "$release_dir/"
        
        # 复制文档和配置文件
        cp README.md "$release_dir/"
        cp LICENSE "$release_dir/" 2>/dev/null || true
        cp -r conf "$release_dir/" 2>/dev/null || true
        
        # 创建安装脚本
        create_install_script "$release_dir" "$target" "$binary_name"
        
        # 打包
        cd "$DIST_DIR"
        if [[ "$target" == *"windows"* ]]; then
            zip -r "${PROJECT_NAME}-${VERSION}-${target}.zip" "${PROJECT_NAME}-${VERSION}-${target}/"
        else
            tar -czf "${PROJECT_NAME}-${VERSION}-${target}.tar.gz" "${PROJECT_NAME}-${VERSION}-${target}/"
        fi
        cd ..
        
        print_success "构建完成: $target"
    else
        print_error "构建失败: $target"
        return 1
    fi
}

# 创建安装脚本
create_install_script() {
    local release_dir=$1
    local target=$2
    local binary_name=$3
    
    if [[ "$target" == *"windows"* ]]; then
        # Windows 安装脚本
        cat > "$release_dir/install.bat" << 'EOF'
@echo off
echo Installing binlog_cli...

REM 检查管理员权限
net session >nul 2>&1
if %errorLevel% == 0 (
    echo Running with administrator privileges
) else (
    echo This script requires administrator privileges
    echo Please run as administrator
    pause
    exit /b 1
)

REM 创建安装目录
if not exist "C:\Program Files\binlog_cli" mkdir "C:\Program Files\binlog_cli"

REM 复制文件
copy binlog_cli.exe "C:\Program Files\binlog_cli\"
copy README.md "C:\Program Files\binlog_cli\"
if exist conf xcopy /E /I conf "C:\Program Files\binlog_cli\conf"

REM 添加到 PATH
setx /M PATH "%PATH%;C:\Program Files\binlog_cli"

echo Installation completed successfully!
echo You may need to restart your command prompt to use binlog_cli
pause
EOF
    else
        # Unix 安装脚本
        cat > "$release_dir/install.sh" << 'EOF'
#!/bin/bash

set -e

INSTALL_DIR="/usr/local/bin"
CONFIG_DIR="/etc/binlog_cli"

echo "Installing binlog_cli..."

# 检查权限
if [ "$EUID" -ne 0 ]; then
    echo "This script requires root privileges"
    echo "Please run with sudo: sudo ./install.sh"
    exit 1
fi

# 安装二进制文件
echo "Installing binary to $INSTALL_DIR"
cp binlog_cli "$INSTALL_DIR/"
chmod +x "$INSTALL_DIR/binlog_cli"

# 安装配置文件
if [ -d "conf" ]; then
    echo "Installing configuration files to $CONFIG_DIR"
    mkdir -p "$CONFIG_DIR"
    cp -r conf/* "$CONFIG_DIR/"
fi

# 创建符号链接 (如果需要)
if [ ! -L "/usr/bin/binlog_cli" ]; then
    ln -s "$INSTALL_DIR/binlog_cli" "/usr/bin/binlog_cli"
fi

echo "Installation completed successfully!"
echo "You can now use 'binlog_cli' command from anywhere"
EOF
        chmod +x "$release_dir/install.sh"
    fi
}

# 构建本地版本
build_local() {
    print_info "构建本地版本..."
    cargo build --release --bin "$PROJECT_NAME"
    print_success "本地版本构建完成: $BUILD_DIR/$PROJECT_NAME"
}

# 构建所有目标
build_all() {
    print_info "开始构建所有目标平台..."
    
    local success_count=0
    local total_count=${#TARGETS[@]}
    
    for target in "${TARGETS[@]}"; do
        if build_target "$target"; then
            ((success_count++))
        fi
    done
    
    print_info "构建完成: $success_count/$total_count 个目标成功"
    
    if [ $success_count -eq $total_count ]; then
        print_success "所有目标构建成功!"
    else
        print_warning "部分目标构建失败"
    fi
}

# 显示帮助信息
show_help() {
    echo "Binlog CLI 构建脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -h, --help     显示帮助信息"
    echo "  -c, --clean    清理构建目录"
    echo "  -l, --local    只构建本地版本"
    echo "  -a, --all      构建所有目标平台 (默认)"
    echo "  -t, --target   构建指定目标平台"
    echo ""
    echo "支持的目标平台:"
    for target in "${TARGETS[@]}"; do
        echo "  - $target"
    done
    echo ""
    echo "示例:"
    echo "  $0 --local                           # 构建本地版本"
    echo "  $0 --all                             # 构建所有平台"
    echo "  $0 --target x86_64-unknown-linux-gnu # 构建指定平台"
    echo "  $0 --clean --all                     # 清理后构建所有平台"
}

# 主函数
main() {
    local clean=false
    local build_type="all"
    local target=""
    
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -c|--clean)
                clean=true
                shift
                ;;
            -l|--local)
                build_type="local"
                shift
                ;;
            -a|--all)
                build_type="all"
                shift
                ;;
            -t|--target)
                build_type="target"
                target="$2"
                shift 2
                ;;
            *)
                print_error "未知选项: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # 检查依赖
    check_dependencies
    
    # 清理构建目录
    if [ "$clean" = true ]; then
        clean_build
    fi
    
    # 执行构建
    case $build_type in
        "local")
            build_local
            ;;
        "all")
            build_all
            ;;
        "target")
            if [ -z "$target" ]; then
                print_error "请指定目标平台"
                exit 1
            fi
            build_target "$target"
            ;;
    esac
    
    print_success "构建脚本执行完成!"
}

# 运行主函数
main "$@"
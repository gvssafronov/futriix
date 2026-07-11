#!/usr/bin/env bash
# Copyright 2026 Safronov Grigorii
#
# Licensed under the CDDL, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
# https://opensource.org/licenses/CDDL-1.0
#
# Универсальный скрипт сборки futriix для Linux и Illumos
# НЕ ТРЕБУЕТ GCC - использует CGO_ENABLED=0 для всех платформ

set -e

echo ""
echo "[BUILD] Building futriix database..."

# Определение ОС
OS=$(uname -s | tr '[:upper:]' '[:lower:]')

# Цвета для вывода
if [ -t 1 ]; then
    RED='\033[0;31m'
    BOLD_RED='\033[1;31m'
    BOLD_GREEN='\033[1;32m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    LIGHT_CYAN='\033[1;36m'
    NC='\033[0m'
else
    RED=''; BOLD_RED=''; BOLD_GREEN=''; GREEN=''; YELLOW=''; LIGHT_CYAN=''; NC=''
fi

# Функции
error_msg() { echo -e "${BOLD_RED}[ERROR] $1${NC}"; }
success_msg() { echo -e "${BOLD_GREEN}[OK] $1${NC}"; }
info_msg() { echo -e "${LIGHT_CYAN}[INFO] $1${NC}"; }
warning_msg() { echo -e "${YELLOW}[WARN] $1${NC}"; }
print_separator() { echo "================================================"; }

# Проверка Go
check_go() {
    info_msg "Checking Go installation..."
    if ! command -v go >/dev/null 2>&1; then
        error_msg "Go is not installed or not in PATH"
        exit 1
    fi
    
    GO_VERSION=$(go version | awk '{print $3}')
    success_msg "Go found: $GO_VERSION"
    
    go env -w GOPROXY=https://proxy.golang.org,direct 2>/dev/null || true
    info_msg "Go proxy: $(go env GOPROXY)"
}

# Подготовка vendor с видимым прогрессом
setup_vendor() {
    info_msg "Checking vendor directory..."
    
    NEED_VENDOR=false
    
    if [ ! -d "vendor" ] || [ ! -f "vendor/modules.txt" ]; then
        warning_msg "Vendor directory not found or incomplete"
        NEED_VENDOR=true
    elif [ "go.mod" -nt "vendor/modules.txt" ]; then
        warning_msg "go.mod is newer than vendor directory"
        NEED_VENDOR=true
    fi
    
    if [ "$NEED_VENDOR" = true ]; then
        info_msg "Creating/updating vendor directory..."
        echo ""
        info_msg "Step 1/2: Downloading modules..."
        go mod download -x 2>&1 | grep -E "^(GET|Downloading)" | head -20 || true
        echo ""
        info_msg "Step 2/2: Creating vendor directory..."
        go mod vendor
        success_msg "Vendor dependencies ready"
        echo ""
    else
        info_msg "Vendor directory is up to date"
    fi
}

# Сборка статического бинарника
build_static() {
    local target_os=$1
    local target_arch="amd64"
    local output_name="futriix-${target_os}"
    local output_path="bin/${output_name}"
    
    info_msg "Building for ${target_os} (static, no CGO)..."
    
    export GOOS="${target_os}"
    export GOARCH="${target_arch}"
    export CGO_ENABLED=0
    
    local build_tags=""
    if [ "${target_os}" = "illumos" ] || [ "${target_os}" = "solaris" ]; then
        build_tags="-tags=illumos"
        output_name="futriix-illumos"
        output_path="bin/futriix-illumos"
    elif [ "${target_os}" = "linux" ]; then
        output_name="futriix-linux"
        output_path="bin/futriix-linux"
    fi
    
    mkdir -p bin
    
    if go build -mod=vendor ${build_tags} -ldflags="-s -w" -o "${output_path}" ./cmd/futriis; then
        success_msg "Build successful for ${target_os}: ${output_path}"
        cp "${output_path}" "./${output_name}" 2>/dev/null || true
        info_msg "Binary copied to: ./${output_name}"
        
        SIZE=$(ls -lh "${output_path}" | awk '{print $5}')
        info_msg "Binary size: ${SIZE}"
        return 0
    else
        error_msg "Build failed for ${target_os}"
        return 1
    fi
}

# Основная функция
main() {
    print_separator
    echo -e "${GREEN}[BUILD] Building futriix database (static, no GCC required)${NC}"
    print_separator
    
    info_msg "Detected OS: ${OS}"
    
    check_go
    setup_vendor
    
    print_separator
    
    BUILD_SUCCESS=true
    
    case "${OS}" in
        linux)
            if ! build_static "linux"; then
                BUILD_SUCCESS=false
            fi
            ;;
        sunos|illumos)
            if ! build_static "illumos"; then
                BUILD_SUCCESS=false
            fi
            ;;
        darwin)
            info_msg "Detected macOS, cross-compiling for Linux..."
            if ! build_static "linux"; then
                BUILD_SUCCESS=false
            fi
            ;;
        *)
            warning_msg "Unknown OS: ${OS}, building for Linux..."
            if ! build_static "linux"; then
                BUILD_SUCCESS=false
            fi
            ;;
    esac
    
    print_separator
    
    if [ "$BUILD_SUCCESS" = true ]; then
        success_msg "Build complete!"
        echo ""
        info_msg "To run the database:"
        echo "  ./futriix-linux     # On Linux"
        echo "  ./futriix-illumos   # On Illumos/OpenIndiana"
        echo ""
        info_msg "Binary location: bin/"
        ls -lh bin/ 2>/dev/null || true
    else
        error_msg "Build failed"
        exit 1
    fi
}

main "$@"

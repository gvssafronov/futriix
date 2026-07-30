#!/usr/bin/env bash

# Copyright 2026 Safronov Grigorii
#
# Licensed under the CDDL, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
# https://opensource.org/licenses/CDDL-1.0
#
# Скрипт установки Golang для OpenIndiana Hipster

set -e

echo ""
echo "[INSTALL] Installing Golang on OpenIndiana Hipster..."

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

# Проверка прав root
check_root() {
    info_msg "Checking root privileges..."
    if [ "$(id -u)" -ne 0 ]; then
        error_msg "This script must be run as root (sudo)"
        exit 1
    fi
    success_msg "Root privileges confirmed"
}

# Скачивание дистрибутива
download_go() {
    local GO_URL="https://go.dev/dl/go1.26.5.illumos-amd64.tar.gz"
    local GO_TAR="go1.26.5.illumos-amd64.tar.gz"
    
    info_msg "Downloading Go distribution..."
    info_msg "URL: ${GO_URL}"
    
    if command -v wget >/dev/null 2>&1; then
        wget -q --show-progress "${GO_URL}" -O "/tmp/${GO_TAR}"
    elif command -v curl >/dev/null 2>&1; then
        curl -L --progress-bar "${GO_URL}" -o "/tmp/${GO_TAR}"
    else
        error_msg "Neither wget nor curl found. Please install one of them."
        exit 1
    fi
    
    if [ -f "/tmp/${GO_TAR}" ]; then
        success_msg "Download complete: /tmp/${GO_TAR}"
    else
        error_msg "Download failed"
        exit 1
    fi
}

# Установка прав на /usr/
set_usr_permissions() {
    info_msg "Setting permissions for /usr/ directory (777)..."
    
    if [ ! -d "/usr/" ]; then
        error_msg "/usr/ directory not found"
        exit 1
    fi
    
    chmod -R 777 /usr/
    success_msg "Permissions set: /usr/"
}

# Создание директории /usr/local
create_local_dir() {
    info_msg "Creating /usr/local directory..."
    
    if [ -d "/usr/local" ]; then
        warning_msg "/usr/local already exists"
    else
        mkdir -p /usr/local
        success_msg "Directory created: /usr/local"
    fi
}

# Распаковка Go
extract_go() {
    local GO_TAR="go1.26.5.illumos-amd64.tar.gz"
    
    info_msg "Extracting Go to /usr/local..."
    
    if [ -d "/usr/local/go" ]; then
        warning_msg "/usr/local/go already exists, removing..."
        rm -rf /usr/local/go
    fi
    
    tar -xzf "/tmp/${GO_TAR}" -C /usr/local/
    
    if [ -d "/usr/local/go" ]; then
        success_msg "Go extracted to /usr/local/go"
        rm -f "/tmp/${GO_TAR}"
        info_msg "Temporary file removed: /tmp/${GO_TAR}"
    else
        error_msg "Extraction failed"
        exit 1
    fi
}

# Обновление /etc/profile
update_profile() {
    local PROFILE_FILE="/etc/profile"
    
    info_msg "Updating ${PROFILE_FILE}..."
    
    if [ ! -f "${PROFILE_FILE}" ]; then
        error_msg "${PROFILE_FILE} not found"
        exit 1
    fi
    
    # Проверка, не добавлены ли уже строки
    if grep -q "/usr/local/go/bin" "${PROFILE_FILE}" 2>/dev/null; then
        warning_msg "Go paths already exist in ${PROFILE_FILE}"
    else
        echo "" >> "${PROFILE_FILE}"
        echo "# Go environment variables" >> "${PROFILE_FILE}"
        echo "export PATH=\$PATH:/usr/local/go/bin" >> "${PROFILE_FILE}"
        echo "export PATH=\"\$PATH:\$(go env GOPATH)/bin\"" >> "${PROFILE_FILE}"
        success_msg "Go paths added to ${PROFILE_FILE}"
    fi
}

# Проверка установки
verify_installation() {
    info_msg "Verifying Go installation..."
    
    export PATH=$PATH:/usr/local/go/bin
    
    if command -v go >/dev/null 2>&1; then
        GO_VERSION=$(go version)
        success_msg "Go installed successfully: ${GO_VERSION}"
        
        GOPATH=$(go env GOPATH)
        info_msg "GOPATH: ${GOPATH}"
    else
        error_msg "Go not found in PATH"
        exit 1
    fi
}

# Основная функция
main() {
    print_separator
    echo -e "${GREEN}[INSTALL] Installing Golang on OpenIndiana Hipster${NC}"
    print_separator
    
    info_msg "Detected OS: $(uname -s)"
    
    check_root
    
    print_separator
    info_msg "Step 1/6: Downloading Go distribution..."
    download_go
    
    print_separator
    info_msg "Step 2/6: Setting permissions for /usr/..."
    set_usr_permissions
    
    print_separator
    info_msg "Step 3/6: Creating /usr/local directory..."
    create_local_dir
    
    print_separator
    info_msg "Step 4/6: Extracting Go..."
    extract_go
    
    print_separator
    info_msg "Step 5/6: Updating /etc/profile..."
    update_profile
    
    print_separator
    info_msg "Step 6/6: Verifying installation..."
    verify_installation
    
    print_separator
    
    success_msg "Go installation complete!"
    echo ""
    info_msg "To apply changes, please reboot the system:"
    echo "  sudo reboot"
    echo ""
    info_msg "Or reload profile in current session:"
    echo "  source /etc/profile"
    echo ""
    info_msg "Go binary location: /usr/local/go/bin/go"
    
    print_separator
    
    echo -e "${YELLOW}[WARN] System reboot is required for changes to take effect${NC}"
    echo -e "${YELLOW}[WARN] Please run: sudo reboot${NC}"
}

# Запуск с обработкой ошибок
if ! main "$@"; then
    error_msg "Installation failed"
    exit 1
fi

/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/api/webui_credentials.go
// Назначение: Управление учётными данными для веб-интерфейса
// Хранит логин/пароль в скрытом файле .credentials в директории futriis
// Поддерживает множественных пользователей-администраторов

package api

import (
    "crypto/sha256"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"
)

// UserCredential представляет учётные данные одного пользователя
type UserCredential struct {
    Password  string `json:"password"`           // хранится в виде хеша
    Avatar    string `json:"avatar,omitempty"`   // base64 encoded avatar
    IsAdmin   bool   `json:"is_admin"`           // является ли администратором
    CreatedAt int64  `json:"created_at"`         // время создания
    LastLogin int64  `json:"last_login,omitempty"` // время последнего входа
}

// Credentials структура для хранения всех учётных данных
type Credentials struct {
    Users    map[string]*UserCredential `json:"users"`
    Settings map[string]interface{}     `json:"settings,omitempty"`
}

// CredentialManager управляет учётными данными веб-интерфейса
type CredentialManager struct {
    mu          sync.RWMutex
    credentials *Credentials
    credFile    string
    currentUser string
}

// NewCredentialManager создаёт новый менеджер учётных данных
func NewCredentialManager() *CredentialManager {
    // Определяем путь к директории futriis
    execPath, err := os.Executable()
    if err != nil {
        execPath = "."
    }
    futriisDir := filepath.Dir(execPath)
    
    // Ищем директорию futriis (поднимаемся вверх, если нужно)
    for {
        if _, err := os.Stat(filepath.Join(futriisDir, "futriis")); err == nil {
            futriisDir = filepath.Join(futriisDir, "futriis")
            break
        }
        parent := filepath.Dir(futriisDir)
        if parent == futriisDir {
            // Не нашли директорию futriis, создаём в текущей
            futriisDir = filepath.Join(execPath, "futriis")
            os.MkdirAll(futriisDir, 0700)
            break
        }
        futriisDir = parent
    }
    
    credFile := filepath.Join(futriisDir, ".credentials")
    
    return &CredentialManager{
        credentials: &Credentials{
            Users:    make(map[string]*UserCredential),
            Settings: make(map[string]interface{}),
        },
        credFile: credFile,
    }
}

// hashPassword создаёт хеш пароля
func (cm *CredentialManager) hashPassword(password string) string {
    hash := sha256.Sum256([]byte(password))
    return base64.StdEncoding.EncodeToString(hash[:])
}

// Load загружает учётные данные из файла
func (cm *CredentialManager) Load() error {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    data, err := os.ReadFile(cm.credFile)
    if err != nil {
        if os.IsNotExist(err) {
            return cm.createDefaultFile()
        }
        return err
    }
    
    var creds Credentials
    if err := json.Unmarshal(data, &creds); err != nil {
        return err
    }
    
    cm.credentials = &creds
    if cm.credentials.Users == nil {
        cm.credentials.Users = make(map[string]*UserCredential)
    }
    if cm.credentials.Settings == nil {
        cm.credentials.Settings = make(map[string]interface{})
    }
    
    return nil
}

// createDefaultFile создаёт файл с учётными данными по умолчанию
func (cm *CredentialManager) createDefaultFile() error {
    // Создаём пользователя admin по умолчанию
    cm.credentials.Users["admin"] = &UserCredential{
        Password:  cm.hashPassword("admin"),
        IsAdmin:   true,
        CreatedAt: time.Now().Unix(),
    }
    cm.credentials.Settings["version"] = "1.0"
    
    return cm.save()
}

// CreateDefault создаёт учётные данные по умолчанию (если файл не существует)
func (cm *CredentialManager) CreateDefault() error {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    // Проверяем, существует ли файл
    if _, err := os.Stat(cm.credFile); err == nil {
        return nil // файл уже существует
    }
    
    cm.credentials.Users["admin"] = &UserCredential{
        Password:  cm.hashPassword("admin"),
        IsAdmin:   true,
        CreatedAt: time.Now().Unix(),
    }
    cm.credentials.Settings["version"] = "1.0"
    
    return cm.save()
}

// save сохраняет учётные данные в файл
func (cm *CredentialManager) save() error {
    data, err := json.MarshalIndent(cm.credentials, "", "  ")
    if err != nil {
        return err
    }
    
    // Устанавливаем права доступа 0600 (только владелец может читать/писать)
    return os.WriteFile(cm.credFile, data, 0600)
}

// Validate проверяет учётные данные
func (cm *CredentialManager) Validate(username, password string) bool {
    cm.mu.RLock()
    defer cm.mu.RUnlock()
    
    if cm.credentials == nil || cm.credentials.Users == nil {
        return false
    }
    
    user, exists := cm.credentials.Users[username]
    if !exists {
        return false
    }
    
    return user.Password == cm.hashPassword(password)
}

// IsAdmin проверяет, является ли пользователь администратором
func (cm *CredentialManager) IsAdmin(username string) bool {
    cm.mu.RLock()
    defer cm.mu.RUnlock()
    
    if cm.credentials == nil || cm.credentials.Users == nil {
        return false
    }
    
    user, exists := cm.credentials.Users[username]
    if !exists {
        return false
    }
    
    return user.IsAdmin
}

// CreateUser создаёт нового пользователя (только для администраторов)
func (cm *CredentialManager) CreateUser(username, password string, isAdmin bool) error {
    if username == "" {
        return fmt.Errorf("username cannot be empty")
    }
    
    if len(password) < 4 {
        return fmt.Errorf("password must be at least 4 characters")
    }
    
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    if cm.credentials == nil {
        cm.credentials = &Credentials{
            Users:    make(map[string]*UserCredential),
            Settings: make(map[string]interface{}),
        }
    }
    
    if _, exists := cm.credentials.Users[username]; exists {
        return fmt.Errorf("user %s already exists", username)
    }
    
    cm.credentials.Users[username] = &UserCredential{
        Password:  cm.hashPassword(password),
        IsAdmin:   isAdmin,
        CreatedAt: time.Now().Unix(),
    }
    
    return cm.save()
}

// DeleteUser удаляет пользователя (только для администраторов)
func (cm *CredentialManager) DeleteUser(username string) error {
    if username == "admin" {
        return fmt.Errorf("cannot delete default admin user")
    }
    
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    if cm.credentials == nil || cm.credentials.Users == nil {
        return fmt.Errorf("no users found")
    }
    
    if _, exists := cm.credentials.Users[username]; !exists {
        return fmt.Errorf("user %s not found", username)
    }
    
    delete(cm.credentials.Users, username)
    return cm.save()
}

// ListUsers возвращает список всех пользователей (только для администраторов)
func (cm *CredentialManager) ListUsers() []map[string]interface{} {
    cm.mu.RLock()
    defer cm.mu.RUnlock()
    
    users := make([]map[string]interface{}, 0)
    
    for username, user := range cm.credentials.Users {
        users = append(users, map[string]interface{}{
            "username":   username,
            "is_admin":   user.IsAdmin,
            "created_at": user.CreatedAt,
            "last_login": user.LastLogin,
            "has_avatar": user.Avatar != "",
        })
    }
    
    return users
}

// ChangePassword изменяет пароль пользователя
func (cm *CredentialManager) ChangePassword(username, currentPassword, newPassword string) error {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    if cm.credentials == nil || cm.credentials.Users == nil {
        return fmt.Errorf("credentials not loaded")
    }
    
    user, exists := cm.credentials.Users[username]
    if !exists {
        return fmt.Errorf("user not found")
    }
    
    if user.Password != cm.hashPassword(currentPassword) {
        return fmt.Errorf("current password is incorrect")
    }
    
    if len(newPassword) < 4 {
        return fmt.Errorf("new password must be at least 4 characters")
    }
    
    user.Password = cm.hashPassword(newPassword)
    
    return cm.save()
}

// AdminChangePassword изменяет пароль пользователя (без проверки старого, только для админов)
func (cm *CredentialManager) AdminChangePassword(username, newPassword string) error {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    if cm.credentials == nil || cm.credentials.Users == nil {
        return fmt.Errorf("credentials not loaded")
    }
    
    user, exists := cm.credentials.Users[username]
    if !exists {
        return fmt.Errorf("user not found")
    }
    
    if len(newPassword) < 4 {
        return fmt.Errorf("password must be at least 4 characters")
    }
    
    user.Password = cm.hashPassword(newPassword)
    
    return cm.save()
}

// SetAvatar устанавливает аватар для пользователя
func (cm *CredentialManager) SetAvatar(username, avatarBase64 string) error {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    if cm.credentials == nil || cm.credentials.Users == nil {
        return fmt.Errorf("credentials not loaded")
    }
    
    user, exists := cm.credentials.Users[username]
    if !exists {
        return fmt.Errorf("user not found")
    }
    
    user.Avatar = avatarBase64
    
    return cm.save()
}

// GetAvatar возвращает аватар пользователя
func (cm *CredentialManager) GetAvatar(username string) (string, error) {
    cm.mu.RLock()
    defer cm.mu.RUnlock()
    
    if cm.credentials == nil || cm.credentials.Users == nil {
        return "", fmt.Errorf("credentials not loaded")
    }
    
    user, exists := cm.credentials.Users[username]
    if !exists {
        return "", fmt.Errorf("user not found")
    }
    
    return user.Avatar, nil
}

// DeleteAvatar удаляет аватар пользователя
func (cm *CredentialManager) DeleteAvatar(username string) error {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    if cm.credentials == nil || cm.credentials.Users == nil {
        return fmt.Errorf("credentials not loaded")
    }
    
    user, exists := cm.credentials.Users[username]
    if !exists {
        return fmt.Errorf("user not found")
    }
    
    user.Avatar = ""
    
    return cm.save()
}

// UpdateLastLogin обновляет время последнего входа пользователя
func (cm *CredentialManager) UpdateLastLogin(username string) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    if cm.credentials == nil || cm.credentials.Users == nil {
        return
    }
    
    user, exists := cm.credentials.Users[username]
    if !exists {
        return
    }
    
    user.LastLogin = time.Now().Unix()
    cm.save() // игнорируем ошибку, т.к. это не критично
}

// GetCurrentUsername возвращает имя текущего пользователя
func (cm *CredentialManager) GetCurrentUsername() string {
    cm.mu.RLock()
    defer cm.mu.RUnlock()
    
    if cm.credentials == nil {
        return ""
    }
    
    // Возвращаем последнего вошедшего пользователя или admin по умолчанию
    if cm.currentUser != "" {
        return cm.currentUser
    }
    
    return "admin"
}

// SetCurrentUsername устанавливает имя текущего пользователя
func (cm *CredentialManager) SetCurrentUsername(username string) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    cm.currentUser = username
}

// GetCredentialsFile возвращает путь к файлу с учётными данными
func (cm *CredentialManager) GetCredentialsFile() string {
    return cm.credFile
}

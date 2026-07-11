/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/acl/manager.go
// Назначение: Глобальный менеджер ACL для всей СУБД.
// Управляет пользователями, ролями и разрешениями на уровне БД и коллекций.
// Реализован с использованием sync.Map для wait-free доступа.

package acl

import (
    "fmt"
    "sync"
    "time"
    
    "github.com/google/uuid"
)

// PermissionType определяет тип разрешения
type PermissionType string

const (
    PermRead   PermissionType = "read"
    PermWrite  PermissionType = "write"
    PermDelete PermissionType = "delete"
    PermAdmin  PermissionType = "admin"
)

// User представляет пользователя системы
type User struct {
    ID        string    `msgpack:"id"`
    Username  string    `msgpack:"username"`
    Password  string    `msgpack:"password"`  // В реальной системе - хеш
    Roles     []string  `msgpack:"roles"`
    CreatedAt int64     `msgpack:"created_at"`
    LastLogin int64     `msgpack:"last_login"`
    Active    bool      `msgpack:"active"`
}

// Session представляет активную сессию пользователя
type Session struct {
    ID        string   `msgpack:"id"`
    Username  string   `msgpack:"username"`
    Roles     []string `msgpack:"roles"`
    CreatedAt int64    `msgpack:"created_at"`
    ExpiresAt int64    `msgpack:"expires_at"`
}

// Role представляет роль с набором разрешений
type Role struct {
    Name        string   `msgpack:"name"`
    Permissions []string `msgpack:"permissions"` // "database.collection:read" формат
}

// ACLManager управляет доступом к БД
type ACLManager struct {
    users       sync.Map // map[string]*User
    roles       sync.Map // map[string]*Role
    sessions    sync.Map // map[string]*Session - sessionID -> Session
    mu          sync.RWMutex
}

// NewACLManager создаёт новый менеджер ACL
func NewACLManager() *ACLManager {
    m := &ACLManager{}
    
    // Создаём роль администратора по умолчанию
    adminRole := &Role{
        Name:        "admin",
        Permissions: []string{"*:*"},
    }
    m.roles.Store("admin", adminRole)
    
    // Создаём пользователя admin по умолчанию
    adminUser := &User{
        ID:        uuid.New().String(),
        Username:  "admin",
        Password:  "admin", // В продакшене использовать хеш!
        Roles:     []string{"admin"},
        CreatedAt: time.Now().UnixMilli(),
        Active:    true,
    }
    m.users.Store("admin", adminUser)
    
    // Создаём роль guest с ограниченными правами
    guestRole := &Role{
        Name:        "guest",
        Permissions: []string{},
    }
    m.roles.Store("guest", guestRole)
    
    // Создаём пользователя guest по умолчанию
    guestUser := &User{
        ID:        uuid.New().String(),
        Username:  "guest",
        Password:  "guest",
        Roles:     []string{"guest"},
        CreatedAt: time.Now().UnixMilli(),
        Active:    true,
    }
    m.users.Store("guest", guestUser)
    
    return m
}

// CreateUser создаёт нового пользователя
func (m *ACLManager) CreateUser(username, password string, roles []string) error {
    if _, exists := m.users.Load(username); exists {
        return fmt.Errorf("user %s already exists", username)
    }
    
    user := &User{
        ID:        uuid.New().String(),
        Username:  username,
        Password:  password,
        Roles:     roles,
        CreatedAt: time.Now().UnixMilli(),
        Active:    true,
    }
    
    m.users.Store(username, user)
    return nil
}

// Authenticate аутентифицирует пользователя и создаёт сессию
func (m *ACLManager) Authenticate(username, password string) (string, error) {
    val, ok := m.users.Load(username)
    if !ok {
        return "", fmt.Errorf("user not found")
    }
    
    user := val.(*User)
    if !user.Active {
        return "", fmt.Errorf("user is disabled")
    }
    
    if user.Password != password {
        return "", fmt.Errorf("invalid password")
    }
    
    // Обновляем время последнего входа
    user.LastLogin = time.Now().UnixMilli()
    m.users.Store(username, user)
    
    // Создаём сессию (24 часа)
    sessionID := uuid.New().String()
    now := time.Now().Unix()
    session := &Session{
        ID:        sessionID,
        Username:  username,
        Roles:     user.Roles,
        CreatedAt: now,
        ExpiresAt: now + 86400, // 24 часа
    }
    m.sessions.Store(sessionID, session)
    
    return sessionID, nil
}

// Logout завершает сессию
func (m *ACLManager) Logout(sessionID string) {
    m.sessions.Delete(sessionID)
}

// CheckSession проверяет, активна ли сессия
func (m *ACLManager) CheckSession(sessionID string) bool {
    val, ok := m.sessions.Load(sessionID)
    if !ok {
        return false
    }
    
    session := val.(*Session)
    // Проверка на expiry
    if time.Now().Unix() > session.ExpiresAt {
        m.sessions.Delete(sessionID)
        return false
    }
    
    return true
}

// GetUsername возвращает имя пользователя по ID сессии
func (m *ACLManager) GetUsername(sessionID string) string {
    val, ok := m.sessions.Load(sessionID)
    if !ok {
        return ""
    }
    
    session := val.(*Session)
    return session.Username
}

// GetUserRoles возвращает роли пользователя по ID сессии
func (m *ACLManager) GetUserRoles(sessionID string) []string {
    val, ok := m.sessions.Load(sessionID)
    if !ok {
        return []string{}
    }
    
    session := val.(*Session)
    return session.Roles
}

// CheckPermission проверяет разрешение для сессии
func (m *ACLManager) CheckPermission(sessionID, database, collection, operation string) bool {
    val, ok := m.sessions.Load(sessionID)
    if !ok {
        return false
    }
    
    session := val.(*Session)
    
    for _, roleName := range session.Roles {
        roleVal, ok := m.roles.Load(roleName)
        if !ok {
            continue
        }
        
        role := roleVal.(*Role)
        for _, perm := range role.Permissions {
            if m.matchPermission(perm, database, collection, operation) {
                return true
            }
        }
    }
    
    return false
}

// matchPermission проверяет соответствие разрешения
func (m *ACLManager) matchPermission(perm, database, collection, operation string) bool {
    // Формат: "database.collection:operation" или "*:*" для всех
    // или "database.*:read" для всех коллекций в БД
    
    parts := splitPermission(perm)
    if len(parts) != 2 {
        return false
    }
    
    resource := parts[0] // "database.collection" или "database.*"
    op := parts[1]       // "read", "write", "delete", "admin"
    
    // Проверка операции
    if op != "*" && op != operation && operation != "admin" {
        return false
    }
    
    // Администратор имеет доступ ко всему
    if op == "admin" || op == "*" {
        return true
    }
    
    // Проверка ресурса
    if resource == "*:*" {
        return true
    }
    
    resourceParts := splitResource(resource)
    if len(resourceParts) != 2 {
        return false
    }
    
    dbPattern := resourceParts[0]
    collPattern := resourceParts[1]
    
    if dbPattern != "*" && dbPattern != database {
        return false
    }
    
    if collPattern != "*" && collPattern != collection {
        return false
    }
    
    return true
}

// GrantPermission выдаёт разрешение роли
func (m *ACLManager) GrantPermission(roleName, permission string) error {
    val, ok := m.roles.Load(roleName)
    if !ok {
        return fmt.Errorf("role not found")
    }
    
    role := val.(*Role)
    role.Permissions = append(role.Permissions, permission)
    m.roles.Store(roleName, role)
    
    return nil
}

// RevokePermission отзывает разрешение у роли
func (m *ACLManager) RevokePermission(roleName, permission string) error {
    val, ok := m.roles.Load(roleName)
    if !ok {
        return fmt.Errorf("role not found")
    }
    
    role := val.(*Role)
    newPermissions := make([]string, 0, len(role.Permissions))
    for _, p := range role.Permissions {
        if p != permission {
            newPermissions = append(newPermissions, p)
        }
    }
    role.Permissions = newPermissions
    m.roles.Store(roleName, role)
    
    return nil
}

// CreateRole создаёт новую роль
func (m *ACLManager) CreateRole(name string) error {
    if _, exists := m.roles.Load(name); exists {
        return fmt.Errorf("role %s already exists", name)
    }
    
    role := &Role{
        Name:        name,
        Permissions: []string{},
    }
    
    m.roles.Store(name, role)
    return nil
}

// DeleteRole удаляет роль
func (m *ACLManager) DeleteRole(name string) error {
    if _, exists := m.roles.LoadAndDelete(name); !exists {
        return fmt.Errorf("role not found")
    }
    return nil
}

// AddUserRole добавляет роль пользователю
func (m *ACLManager) AddUserRole(username, roleName string) error {
    val, ok := m.users.Load(username)
    if !ok {
        return fmt.Errorf("user not found")
    }
    
    user := val.(*User)
    // Проверяем, есть ли уже такая роль
    for _, r := range user.Roles {
        if r == roleName {
            return fmt.Errorf("user already has role %s", roleName)
        }
    }
    user.Roles = append(user.Roles, roleName)
    m.users.Store(username, user)
    
    return nil
}

// RemoveUserRole удаляет роль у пользователя
func (m *ACLManager) RemoveUserRole(username, roleName string) error {
    val, ok := m.users.Load(username)
    if !ok {
        return fmt.Errorf("user not found")
    }
    
    user := val.(*User)
    newRoles := make([]string, 0, len(user.Roles))
    for _, r := range user.Roles {
        if r != roleName {
            newRoles = append(newRoles, r)
        }
    }
    user.Roles = newRoles
    m.users.Store(username, user)
    
    return nil
}

// DisableUser отключает пользователя
func (m *ACLManager) DisableUser(username string) error {
    val, ok := m.users.Load(username)
    if !ok {
        return fmt.Errorf("user not found")
    }
    
    user := val.(*User)
    user.Active = false
    m.users.Store(username, user)
    
    return nil
}

// EnableUser включает пользователя
func (m *ACLManager) EnableUser(username string) error {
    val, ok := m.users.Load(username)
    if !ok {
        return fmt.Errorf("user not found")
    }
    
    user := val.(*User)
    user.Active = true
    m.users.Store(username, user)
    
    return nil
}

// DeleteUser удаляет пользователя
func (m *ACLManager) DeleteUser(username string) error {
    if _, exists := m.users.LoadAndDelete(username); !exists {
        return fmt.Errorf("user not found")
    }
    return nil
}

// ChangePassword изменяет пароль пользователя
func (m *ACLManager) ChangePassword(username, newPassword string) error {
    val, ok := m.users.Load(username)
    if !ok {
        return fmt.Errorf("user not found")
    }
    
    user := val.(*User)
    user.Password = newPassword
    m.users.Store(username, user)
    
    return nil
}

// ListUsers возвращает список всех пользователей
func (m *ACLManager) ListUsers() []string {
    users := make([]string, 0)
    m.users.Range(func(key, value interface{}) bool {
        users = append(users, key.(string))
        return true
    })
    return users
}

// ListRoles возвращает список всех ролей
func (m *ACLManager) ListRoles() []string {
    roles := make([]string, 0)
    m.roles.Range(func(key, value interface{}) bool {
        roles = append(roles, key.(string))
        return true
    })
    return roles
}

// GetUserInfo возвращает информацию о пользователе
func (m *ACLManager) GetUserInfo(username string) (*User, error) {
    val, ok := m.users.Load(username)
    if !ok {
        return nil, fmt.Errorf("user not found")
    }
    
    user := val.(*User)
    // Возвращаем копию, чтобы избежать модификации извне
    return &User{
        ID:        user.ID,
        Username:  user.Username,
        Roles:     user.Roles,
        CreatedAt: user.CreatedAt,
        LastLogin: user.LastLogin,
        Active:    user.Active,
    }, nil
}

// GetRolePermissions возвращает разрешения роли
func (m *ACLManager) GetRolePermissions(roleName string) ([]string, error) {
    val, ok := m.roles.Load(roleName)
    if !ok {
        return nil, fmt.Errorf("role not found")
    }
    
    role := val.(*Role)
    return role.Permissions, nil
}

// Helper functions
func splitPermission(perm string) []string {
    for i := 0; i < len(perm); i++ {
        if perm[i] == ':' {
            return []string{perm[:i], perm[i+1:]}
        }
    }
    return []string{perm, ""}
}

func splitResource(resource string) []string {
    for i := 0; i < len(resource); i++ {
        if resource[i] == '.' {
            return []string{resource[:i], resource[i+1:]}
        }
    }
    return []string{resource, "*"}
}

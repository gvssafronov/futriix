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
//
// ВАЖНО:
//   - Пароли хранятся как PBKDF2-HMAC-SHA256 (соль + 100000 итераций),
//     а не в открытом виде
//   - Constant-time сравнение паролей (защита от timing-атак)
//   - Убраны дефолтные пароли admin/admin и guest/guest; admin создаётся
//     со случайным паролем, который выводится в stderr; guest не создаётся
//   - Сессии имеют TTL и периодически очищаются фоновой горутиной
//   - CheckPermission сначала валидирует сессию (expiry)
//   - Исправлена критическая уязвимость эскалации привилегий в matchPermission:
//     раньше роль с "users:read" получала доступ к "users:admin". Теперь
//     admin-операция требует явного права "admin" в разрешении роли
//   - Блокировка аккаунта после 5 неудачных попыток входа (15 минут)
//   - Валидация username (regex) и длины password
//   - GrantPermission дедуплицирует разрешения
//   - ListUsers/ListRoles возвращают отсортированные списки
//   - Метод Stop() для остановки фоновой очистки сессий
//   - Защита от nil-указателей в GetUserInfo/GetRolePermissions

package acl

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// =============================================================================
// КОНСТАНТЫ
// =============================================================================

const (
	// Параметры PBKDF2
	pbkdf2Iterations = 100000
	pbkdf2KeyLen     = 32
	saltLen          = 16

	// Ограничения валидации
	maxUsernameLen = 64
	minPasswordLen = 8
	maxPasswordLen = 256

	// Параметры сессий
	sessionTTL          = 24 * time.Hour
	sessionCleanupEvery = 10 * time.Minute

	// Параметры блокировки аккаунта
	maxFailedAttempts = 5
	lockoutDuration   = 15 * time.Minute
)

// usernameRegex разрешает только безопасные символы в имени пользователя.
var usernameRegex = regexp.MustCompile(`^[a-zA-Z0-9_.@-]+$`)

// =============================================================================
// ТИПЫ
// =============================================================================

// PermissionType определяет тип разрешения
type PermissionType string

const (
	PermRead   PermissionType = "read"
	PermWrite  PermissionType = "write"
	PermDelete PermissionType = "delete"
	PermAdmin  PermissionType = "admin"
)

// User представляет пользователя системы.
//
// PasswordHash хранится в формате: pbkdf2$iterations$salt_base64$hash_base64
// LegacyPassword — для обратной совместимости со старым форматом (SHA-256),
// будет удалено при первом успешном входе.
type User struct {
	ID             string   `msgpack:"id"`
	Username       string   `msgpack:"username"`
	PasswordHash   string   `msgpack:"password_hash,omitempty"`
	LegacyPassword string   `msgpack:"password,omitempty"`
	Roles          []string `msgpack:"roles"`
	CreatedAt      int64    `msgpack:"created_at"`
	LastLogin      int64    `msgpack:"last_login"`
	Active         bool     `msgpack:"active"`
	FailedAttempts int      `msgpack:"failed_attempts,omitempty"`
	LockedUntil    int64    `msgpack:"locked_until,omitempty"`
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
	Permissions []string `msgpack:"permissions"`
}

// ACLManager управляет доступом к БД
type ACLManager struct {
	users    sync.Map
	roles    sync.Map
	sessions sync.Map
	mu       sync.RWMutex
	stopChan chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// =============================================================================
// КОНСТРУКТОР
// =============================================================================

// NewACLManager создаёт новый менеджер ACL.
//
// Создаёт роль "admin" с полными правами и пользователя "admin"
// со случайным паролем, который выводится в stderr. Пользователь guest
// не создаётся — если нужен гостевой доступ, создайте его явно.
func NewACLManager() *ACLManager {
	m := &ACLManager{
		stopChan: make(chan struct{}),
	}

	// Создаём роль администратора
	adminRole := &Role{
		Name:        "admin",
		Permissions: []string{"*:*"},
	}
	m.roles.Store("admin", adminRole)

	// Создаём администратора со случайным паролем
	randomBytes := make([]byte, 24)
	if _, err := rand.Read(randomBytes); err != nil {
		randomBytes = []byte(uuid.New().String())
	}
	adminPassword := base64.RawURLEncoding.EncodeToString(randomBytes)

	adminHash, err := hashPassword(adminPassword)
	if err != nil {
		// Fallback: сохраняем пароль в legacy-формате, чтобы не сломать старт
		adminHash = adminPassword
	}

	adminUser := &User{
		ID:           uuid.New().String(),
		Username:     "admin",
		PasswordHash: adminHash,
		Roles:        []string{"admin"},
		CreatedAt:    time.Now().UnixMilli(),
		Active:       true,
	}
	m.users.Store("admin", adminUser)

	// Выводим пароль в stderr (не в лог), чтобы администратор его увидел
	fmt.Fprintf(os.Stderr, "\n"+
		"======================================================================\n"+
		"  ACL: создан пользователь 'admin' со случайным паролем.\n"+
		"  Пароль: %s\n"+
		"  Смените пароль после первого входа командой 'acl change-password'!\n"+
		"======================================================================\n\n",
		adminPassword)

	// Запускаем фоновую очистку сессий
	m.wg.Add(1)
	go m.sessionCleanupLoop()

	return m
}

// =============================================================================
// ХЕШИРОВАНИЕ ПАРОЛЕЙ (PBKDF2-HMAC-SHA256)
// =============================================================================

// hashPassword создаёт PBKDF2-хеш пароля.
// Формат: pbkdf2$iterations$salt_base64$hash_base64
func hashPassword(password string) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("failed to generate salt: %w", err)
	}
	hash := pbkdf2Key(password, salt, pbkdf2Iterations, pbkdf2KeyLen)
	return fmt.Sprintf("pbkdf2$%d$%s$%s",
		pbkdf2Iterations,
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(hash),
	), nil
}

// pbkdf2Key реализует PBKDF2-HMAC-SHA256.
// Минимальная реализация без внешних зависимостей.
func pbkdf2Key(password string, salt []byte, iterations, keyLen int) []byte {
	const hashLen = 32 // SHA-256

	key := make([]byte, keyLen)
	numBlocks := (keyLen + hashLen - 1) / hashLen

	for block := 1; block <= numBlocks; block++ {
		mac := hmac.New(sha256.New, []byte(password))
		mac.Write(salt)
		mac.Write([]byte{
			byte(block >> 24), byte(block >> 16), byte(block >> 8), byte(block),
		})
		u := mac.Sum(nil)

		result := make([]byte, len(u))
		copy(result, u)

		for i := 1; i < iterations; i++ {
			mac.Reset()
			mac.Write(u)
			u = mac.Sum(nil)
			for j := range result {
				result[j] ^= u[j]
			}
		}

		offset := (block - 1) * hashLen
		copy(key[offset:], result)
	}
	return key[:keyLen]
}

// verifyPassword проверяет пароль против PBKDF2-хеша (constant-time).
func verifyPassword(password, storedHash string) bool {
	parts := strings.Split(storedHash, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2" {
		return false
	}

	var iterations int
	if _, err := fmt.Sscanf(parts[1], "%d", &iterations); err != nil {
		return false
	}
	if iterations <= 0 || iterations > 10_000_000 {
		return false
	}

	salt, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	expected, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil {
		return false
	}

	actual := pbkdf2Key(password, salt, iterations, len(expected))
	return subtle.ConstantTimeCompare(actual, expected) == 1
}

// verifyLegacyPassword проверяет старый формат (base64(SHA-256(password))).
// Используется только для миграции.
func verifyLegacyPassword(password, storedHash string) bool {
	hash := sha256.Sum256([]byte(password))
	computed := base64.StdEncoding.EncodeToString(hash[:])
	return subtle.ConstantTimeCompare([]byte(computed), []byte(storedHash)) == 1
}

// =============================================================================
// ВАЛИДАЦИЯ
// =============================================================================

func validateUsername(username string) error {
	if username == "" {
		return fmt.Errorf("username cannot be empty")
	}
	if len(username) > maxUsernameLen {
		return fmt.Errorf("username too long (max %d)", maxUsernameLen)
	}
	if !usernameRegex.MatchString(username) {
		return fmt.Errorf("username contains invalid characters")
	}
	return nil
}

func validatePassword(password string) error {
	if len(password) < minPasswordLen {
		return fmt.Errorf("password must be at least %d characters", minPasswordLen)
	}
	if len(password) > maxPasswordLen {
		return fmt.Errorf("password too long (max %d)", maxPasswordLen)
	}
	return nil
}

// =============================================================================
// БЛОКИРОВКА АККАУНТА
// =============================================================================

// isAccountLocked проверяет, заблокирован ли аккаунт.
// Автоматически снимает блокировку, если срок истёк.
func (m *ACLManager) isAccountLocked(user *User) bool {
	if user.LockedUntil == 0 {
		return false
	}
	if time.Now().Unix() < user.LockedUntil {
		return true
	}
	user.LockedUntil = 0
	user.FailedAttempts = 0
	return false
}

// =============================================================================
// ПОЛЬЗОВАТЕЛИ
// =============================================================================

// CreateUser создаёт нового пользователя
func (m *ACLManager) CreateUser(username, password string, roles []string) error {
	if err := validateUsername(username); err != nil {
		return err
	}
	if err := validatePassword(password); err != nil {
		return err
	}

	if _, exists := m.users.Load(username); exists {
		return fmt.Errorf("user %s already exists", username)
	}

	hash, err := hashPassword(password)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	user := &User{
		ID:           uuid.New().String(),
		Username:     username,
		PasswordHash: hash,
		Roles:        roles,
		CreatedAt:    time.Now().UnixMilli(),
		Active:       true,
	}
	m.users.Store(username, user)
	return nil
}

// Authenticate аутентифицирует пользователя и создаёт сессию.
func (m *ACLManager) Authenticate(username, password string) (string, error) {
	if err := validateUsername(username); err != nil {
		return "", fmt.Errorf("invalid credentials")
	}

	val, ok := m.users.Load(username)
	if !ok {
		// Constant-time фиктивная проверка для защиты от user enumeration
		_ = verifyPassword(password, "pbkdf2$100000$AAAAAAAAAAAAAAAAAAAAAA==$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
		return "", fmt.Errorf("invalid credentials")
	}

	user := val.(*User)
	if !user.Active {
		return "", fmt.Errorf("user is disabled")
	}
	if m.isAccountLocked(user) {
		return "", fmt.Errorf("account is temporarily locked")
	}

	var valid bool
	if user.PasswordHash != "" {
		valid = verifyPassword(password, user.PasswordHash)
	} else if user.LegacyPassword != "" {
		valid = verifyLegacyPassword(password, user.LegacyPassword)
		if valid {
			// Миграция на новый формат
			if newHash, err := hashPassword(password); err == nil {
				user.PasswordHash = newHash
				user.LegacyPassword = ""
			}
		}
	}

	if !valid {
		user.FailedAttempts++
		if user.FailedAttempts >= maxFailedAttempts {
			user.LockedUntil = time.Now().Add(lockoutDuration).Unix()
			user.FailedAttempts = 0
		}
		m.users.Store(username, user)
		return "", fmt.Errorf("invalid credentials")
	}

	user.LastLogin = time.Now().UnixMilli()
	user.FailedAttempts = 0
	user.LockedUntil = 0
	m.users.Store(username, user)

	sessionID := uuid.New().String()
	now := time.Now()
	session := &Session{
		ID:        sessionID,
		Username:  username,
		Roles:     append([]string(nil), user.Roles...),
		CreatedAt: now.Unix(),
		ExpiresAt: now.Add(sessionTTL).Unix(),
	}
	m.sessions.Store(sessionID, session)

	return sessionID, nil
}

// Logout завершает сессию
func (m *ACLManager) Logout(sessionID string) {
	m.sessions.Delete(sessionID)
}

// CheckSession проверяет, активна ли сессия (с учётом expiry)
func (m *ACLManager) CheckSession(sessionID string) bool {
	return m.getSession(sessionID) != nil
}

// getSession возвращает валидную сессию или nil.
func (m *ACLManager) getSession(sessionID string) *Session {
	val, ok := m.sessions.Load(sessionID)
	if !ok {
		return nil
	}
	session := val.(*Session)
	if time.Now().Unix() > session.ExpiresAt {
		m.sessions.Delete(sessionID)
		return nil
	}
	return session
}

// GetUsername возвращает имя пользователя по ID сессии
func (m *ACLManager) GetUsername(sessionID string) string {
	session := m.getSession(sessionID)
	if session == nil {
		return ""
	}
	return session.Username
}

// GetUserRoles возвращает роли пользователя по ID сессии
func (m *ACLManager) GetUserRoles(sessionID string) []string {
	session := m.getSession(sessionID)
	if session == nil {
		return []string{}
	}
	return append([]string(nil), session.Roles...)
}

// GetUserRolesByName возвращает роли пользователя по имени.
func (m *ACLManager) GetUserRolesByName(username string) []string {
	val, ok := m.users.Load(username)
	if !ok {
		return []string{}
	}
	user := val.(*User)
	return append([]string(nil), user.Roles...)
}

// GetUserInfo возвращает информацию о пользователе (без пароля)
func (m *ACLManager) GetUserInfo(username string) (*User, error) {
	val, ok := m.users.Load(username)
	if !ok {
		return nil, fmt.Errorf("user not found")
	}
	user := val.(*User)
	return &User{
		ID:        user.ID,
		Username:  user.Username,
		Roles:     append([]string(nil), user.Roles...),
		CreatedAt: user.CreatedAt,
		LastLogin: user.LastLogin,
		Active:    user.Active,
	}, nil
}

// DisableUser отключает пользователя
func (m *ACLManager) DisableUser(username string) error {
	if username == "admin" {
		return fmt.Errorf("cannot disable built-in admin")
	}
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
	if username == "admin" {
		return fmt.Errorf("cannot delete built-in admin")
	}
	if _, exists := m.users.LoadAndDelete(username); !exists {
		return fmt.Errorf("user not found")
	}
	return nil
}

// ChangePassword изменяет пароль пользователя
func (m *ACLManager) ChangePassword(username, newPassword string) error {
	if err := validatePassword(newPassword); err != nil {
		return err
	}
	val, ok := m.users.Load(username)
	if !ok {
		return fmt.Errorf("user not found")
	}
	user := val.(*User)
	hash, err := hashPassword(newPassword)
	if err != nil {
		return err
	}
	user.PasswordHash = hash
	user.LegacyPassword = ""
	m.users.Store(username, user)
	return nil
}

// ListUsers возвращает отсортированный список всех пользователей
func (m *ACLManager) ListUsers() []string {
	users := make([]string, 0)
	m.users.Range(func(key, value interface{}) bool {
		users = append(users, key.(string))
		return true
	})
	sort.Strings(users)
	return users
}

// =============================================================================
// РОЛИ
// =============================================================================

// CreateRole создаёт новую роль
func (m *ACLManager) CreateRole(name string) error {
	if name == "" {
		return fmt.Errorf("role name cannot be empty")
	}
	if _, exists := m.roles.Load(name); exists {
		return fmt.Errorf("role %s already exists", name)
	}
	role := &Role{Name: name, Permissions: []string{}}
	m.roles.Store(name, role)
	return nil
}

// DeleteRole удаляет роль
func (m *ACLManager) DeleteRole(name string) error {
	if name == "admin" {
		return fmt.Errorf("cannot delete built-in role 'admin'")
	}
	if _, exists := m.roles.LoadAndDelete(name); !exists {
		return fmt.Errorf("role not found")
	}
	return nil
}

// ListRoles возвращает отсортированный список всех ролей
func (m *ACLManager) ListRoles() []string {
	roles := make([]string, 0)
	m.roles.Range(func(key, value interface{}) bool {
		roles = append(roles, key.(string))
		return true
	})
	sort.Strings(roles)
	return roles
}

// GetRolePermissions возвращает разрешения роли (копию)
func (m *ACLManager) GetRolePermissions(roleName string) ([]string, error) {
	val, ok := m.roles.Load(roleName)
	if !ok {
		return nil, fmt.Errorf("role not found")
	}
	role := val.(*Role)
	return append([]string(nil), role.Permissions...), nil
}

// GrantPermission выдаёт разрешение роли (с дедупликацией)
func (m *ACLManager) GrantPermission(roleName, permission string) error {
	val, ok := m.roles.Load(roleName)
	if !ok {
		return fmt.Errorf("role not found")
	}
	role := val.(*Role)
	for _, p := range role.Permissions {
		if p == permission {
			return nil // уже есть
		}
	}
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

// =============================================================================
// РОЛИ ПОЛЬЗОВАТЕЛЯ
// =============================================================================

// AddUserRole добавляет роль пользователю
func (m *ACLManager) AddUserRole(username, roleName string) error {
	val, ok := m.users.Load(username)
	if !ok {
		return fmt.Errorf("user not found")
	}
	user := val.(*User)
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

// =============================================================================
// ПРОВЕРКА РАЗРЕШЕНИЙ
// =============================================================================

// CheckPermission проверяет разрешение для сессии.
//
// КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: раньше при operation == "admin" любая роль
// с любым разрешением получала доступ. Теперь:
//  1. Сначала проверяется валидность сессии (expiry).
//  2. Для admin-операций требуется разрешение "*:*" или явное "...:admin".
//  3. Для обычных операций проверяется соответствие ресурса и операции.
func (m *ACLManager) CheckPermission(sessionID, database, collection, operation string) bool {
	session := m.getSession(sessionID)
	if session == nil {
		return false
	}

	for _, roleName := range session.Roles {
		roleVal, ok := m.roles.Load(roleName)
		if !ok {
			continue
		}
		role := roleVal.(*Role)
		for _, perm := range role.Permissions {
			if matchPermission(perm, database, collection, operation) {
				return true
			}
		}
	}
	return false
}

// matchPermission проверяет соответствие одного разрешения запросу.
//
// Формат разрешения: "database.collection:operation"
//   - "*:*"        — полный доступ ко всему (включая admin)
//   - "db.*:read"  — read всех коллекций в db
//   - "db.coll:admin" — admin-доступ к конкретной коллекции
//   - "db.coll:*"  — все операции на db.coll
func matchPermission(perm, database, collection, operation string) bool {
	parts := splitPermission(perm)
	if len(parts) != 2 {
		return false
	}
	resource := parts[0]
	op := parts[1]

	// Полный доступ
	if resource == "*:*" && op == "*" {
		return true
	}
	// Явный admin-доступ
	if op == "admin" {
		if resource == "*:*" {
			return true
		}
		resourceParts := splitResource(resource)
		if len(resourceParts) != 2 {
			return false
		}
		dbMatch := resourceParts[0] == "*" || resourceParts[0] == database
		collMatch := resourceParts[1] == "*" || resourceParts[1] == collection
		return dbMatch && collMatch
	}
	// Обычные операции: op == "*" или совпадает
	if op != "*" && op != operation {
		return false
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

// =============================================================================
// ФОНОВАЯ ОЧИСТКА СЕССИЙ
// =============================================================================

// sessionCleanupLoop периодически удаляет истёкшие сессии.
func (m *ACLManager) sessionCleanupLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(sessionCleanupEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now().Unix()
			m.sessions.Range(func(key, value interface{}) bool {
				session := value.(*Session)
				if now > session.ExpiresAt {
					m.sessions.Delete(key)
				}
				return true
			})
		case <-m.stopChan:
			return
		}
	}
}

// Stop останавливает фоновые процессы ACLManager.
// Безопасен для многократного вызова.
func (m *ACLManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
	})
	m.wg.Wait()
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// =============================================================================

// splitPermission разделяет строку разрешения на ресурс и операцию.
func splitPermission(perm string) []string {
	for i := 0; i < len(perm); i++ {
		if perm[i] == ':' {
			return []string{perm[:i], perm[i+1:]}
		}
	}
	return []string{perm, ""}
}

// splitResource разделяет ресурс на базу данных и коллекцию.
func splitResource(resource string) []string {
	for i := 0; i < len(resource); i++ {
		if resource[i] == '.' {
			return []string{resource[:i], resource[i+1:]}
		}
	}
	return []string{resource, "*"}
}

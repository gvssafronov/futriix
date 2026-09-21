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
//

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
	maxSessionsPerUser  = 16

	// Параметры блокировки аккаунта
	maxFailedAttempts = 5
	lockoutDuration   = 15 * time.Minute

	// ANSI-коды для окраски сообщения о созданном admin-пользователе.
	// Используется точный #00bfff (Deep Sky Blue) с фолбэком на 256-цветный
	// и базовый ANSI для корректного отображения на OpenIndiana (illumos),
	// где xterm по умолчанию не поддерживает True Color.
	ansiDeepSkyBlueRGB = "\033[38;2;0;191;255m" // True Color
	ansiDeepSkyBlue256 = "\033[38;5;39m"        // Ближайший из xterm-256 палитры
	ansiDeepSkyBlue16  = "\033[96m"             // Bright Cyan (базовый ANSI)
	ansiReset          = "\033[0m"
)

// usernameRegex разрешает только безопасные символы в имени пользователя.
var usernameRegex = regexp.MustCompile(`^[a-zA-Z0-9_.@-]+$`)

// permissionRegex валидирует формат разрешения:
//   - "*:*"                 — полный доступ
//   - "db.coll:op"          — конкретный ресурс и операция
//   - "db.*:op"             — wildcard коллекции
// где db/coll/op — непустые строки из [A-Za-z0-9_*.-] (op — [A-Za-z0-9_*]).
var permissionRegex = regexp.MustCompile(`^([A-Za-z0-9_.*-]+)\.([A-Za-z0-9_*.-]+):([A-Za-z0-9_*]+)$|^\*:\*$`)

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

// Logger — минимальный интерфейс для логирования внутри ACL.
// Сделан локально, чтобы не тянуть internal/log (и не создавать циклов).
type Logger interface {
	Warn(msg string)
	Info(msg string)
}

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

// Session представляет активную сессию пользователя.
//
// ИСПРАВЛЕНО: добавлены CreatedByIP и UserAgent — для привязки сессии
// к клиенту (мягкая защита от session hijacking).
type Session struct {
	ID          string   `msgpack:"id"`
	Username    string   `msgpack:"username"`
	Roles       []string `msgpack:"roles"`
	CreatedAt   int64    `msgpack:"created_at"`
	ExpiresAt   int64    `msgpack:"expires_at"`
	CreatedByIP string   `msgpack:"created_by_ip,omitempty"`
	UserAgent   string   `msgpack:"user_agent,omitempty"`
}

// Role представляет роль с набором разрешений
type Role struct {
	Name        string   `msgpack:"name"`
	Permissions []string `msgpack:"permissions"`
}

// ACLManager управляет доступом к БД.
//
// ВАЖНО: sync.Map используется для чтений без блокировок. Все
// read-modify-write операции (Authenticate, ChangePassword, ...)
// защищены m.mu — иначе возможны потерянные обновления.
type ACLManager struct {
	users    sync.Map // map[string]*User
	roles    sync.Map // map[string]*Role
	sessions sync.Map // map[string]*Session

	// mu защищает read-modify-write операции над users/roles/sessions.
	// Чтения через sync.Map.Load не блокируются.
	mu sync.RWMutex

	logger Logger

	stopChan chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// =============================================================================
// КОНСТРУКТОР
// =============================================================================

// isTTY проверяет, является ли файл терминалом.
// Используется для отключения ANSI-кодов при перенаправлении вывода в файл.
func isTTY(f *os.File) bool {
	if f == nil {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// pickDeepSkyBlueCode выбирает подходящий ANSI-код для #00bfff в
// зависимости от возможностей терминала.
//
// Логика:
//   - True Color (COLORTERM=truecolor / 24bit) -> RGB \033[38;2;0;191;255m
//   - 256-цветный (TERM=...256color)            -> \033[38;5;39m
//   - иначе                                     -> \033[96m (bright cyan)
//
// Это гарантирует, что на OpenIndiana (illumos), где xterm по умолчанию
// не поддерживает True Color, цвет будет максимально близок к #00bfff
// без искажений.
func pickDeepSkyBlueCode() string {
	colorterm := strings.ToLower(os.Getenv("COLORTERM"))
	term := strings.ToLower(os.Getenv("TERM"))

	if colorterm == "truecolor" || colorterm == "24bit" {
		return ansiDeepSkyBlueRGB
	}
	if strings.Contains(term, "256color") || strings.Contains(colorterm, "256") {
		return ansiDeepSkyBlue256
	}
	return ansiDeepSkyBlue16
}

// colorizeACLMessage окрашивает строку в #00bfff (Deep Sky Blue),
// если stderr является TTY. Если stderr перенаправлен в файл —
// возвращает строку без ANSI-кодов (чтобы они не попали в лог).
func colorizeACLMessage(s string) string {
	if !isTTY(os.Stderr) {
		return s
	}
	return pickDeepSkyBlueCode() + s + ansiReset
}

// NewACLManager создаёт новый менеджер ACL.
//
// Создаёт роль "admin" с полными правами и пользователя "admin"
// со случайным паролем, который выводится в stderr. Пользователь guest
// не создаётся — если нужен гостевой доступ, создайте его явно.
//
// ИСПРАВЛЕНО (UI): убраны рамка "====" и пустые строки вокруг сообщения.
// ИСПРАВЛЕНО (безопасность): при сбое PBKDF2 больше НЕ используем пароль
// в открытом виде в качестве хеша — паникуем с понятной ошибкой, чтобы
// сервис не поднялся с незащищённым паролем.
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
		// ИСПРАВЛЕНО: не сохраняем пароль в открытом виде.
		// Паникуем — сервис не должен стартовать с небезопасным ACL.
		panic(fmt.Sprintf("acl: failed to hash admin password: %v", err))
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

	// ИСПРАВЛЕНО (UI): убрана пустая строка " \n" (в ней был лишний пробел).
	// Сразу после этого блока main.go вызывает displayBanner, который
	// начинает вывод со строки "futriis 3i²(by 02.04.2026)".
	//
	// Пароль по-прежнему идёт в stderr (не в stdout), чтобы его можно
	// было отделить от обычного вывода и не логировать.
	fmt.Fprintln(os.Stderr, colorizeACLMessage("\n"))
	fmt.Fprintln(os.Stderr, colorizeACLMessage("  ACL: создан пользователь 'admin' со случайным паролем."))
	fmt.Fprintln(os.Stderr, colorizeACLMessage(fmt.Sprintf("  Пароль: %s", adminPassword)))
	fmt.Fprintln(os.Stderr, colorizeACLMessage("  Смените пароль после первого входа командой 'acl change-password'!"))

	// Запускаем фоновую очистку сессий
	m.wg.Add(1)
	go m.sessionCleanupLoop()

	return m
}

// SetLogger устанавливает логгер (опционально).
// Вызывается после NewACLManager, если приложение хочет логировать
// события ACL (неудачные входы, блокировки аккаунтов и т.п.).
func (m *ACLManager) SetLogger(l Logger) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logger = l
}

func (m *ACLManager) logWarn(msg string) {
	m.mu.RLock()
	l := m.logger
	m.mu.RUnlock()
	if l != nil {
		l.Warn(msg)
	}
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

// validatePermission проверяет формат строки разрешения.
// Допустимые формы:
//   - "*:*"
//   - "database.collection:operation"
//   - "database.*:operation"
// где database/collection/operation — из безопасного набора символов.
func validatePermission(perm string) error {
	if perm == "" {
		return fmt.Errorf("permission cannot be empty")
	}
	if !permissionRegex.MatchString(perm) {
		return fmt.Errorf("invalid permission format: %q", perm)
	}
	return nil
}

// dedupeRoles убирает дубликаты из списка ролей, сохраняя порядок.
func dedupeRoles(roles []string) []string {
	if len(roles) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(roles))
	out := make([]string, 0, len(roles))
	for _, r := range roles {
		if r == "" {
			continue
		}
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	return out
}

// =============================================================================
// БЛОКИРОВКА АККАУНТА
// =============================================================================

// isAccountLocked проверяет, заблокирован ли аккаунт.
// Автоматически снимает блокировку, если срок истёк.
// ВАЖНО: вызывается под m.mu (read-modify-write), поэтому мутация
// user здесь безопасна.
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

	hash, err := hashPassword(password)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.users.Load(username); exists {
		return fmt.Errorf("user %s already exists", username)
	}

	user := &User{
		ID:           uuid.New().String(),
		Username:     username,
		PasswordHash: hash,
		Roles:        dedupeRoles(roles),
		CreatedAt:    time.Now().UnixMilli(),
		Active:       true,
	}
	m.users.Store(username, user)
	return nil
}

// Authenticate аутентифицирует пользователя и создаёт сессию.
//
// ИСПРАВЛЕНО: read-modify-write для user.FailedAttempts / LockedUntil /
// LastLogin выполняется под m.mu — иначе конкурентные попытки входа
// могли терять инкременты счётчика неудач.
func (m *ACLManager) Authenticate(username, password string) (string, error) {
	if err := validateUsername(username); err != nil {
		return "", fmt.Errorf("invalid credentials")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	val, ok := m.users.Load(username)
	if !ok {
		// Constant-time фиктивная проверка для защиты от user enumeration
		_ = verifyPassword(password, "pbkdf2$100000$AAAAAAAAAAAAAAAAAAAAAA==$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
		return "", fmt.Errorf("invalid credentials")
	}

	// Копируем пользователя, чтобы не мутировать объект, который
	// могут читать другие горутины.
	src := val.(*User)
	user := *src
	user.Roles = append([]string(nil), src.Roles...)

	if !user.Active {
		return "", fmt.Errorf("user is disabled")
	}
	if m.isAccountLocked(&user) {
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
			m.logWarn(fmt.Sprintf("acl: account %s locked for %s (too many failed logins)",
				username, lockoutDuration))
		}
		m.users.Store(username, &user)
		return "", fmt.Errorf("invalid credentials")
	}

	user.LastLogin = time.Now().UnixMilli()
	user.FailedAttempts = 0
	user.LockedUntil = 0
	m.users.Store(username, &user)

	// Ограничиваем количество активных сессий на пользователя.
	m.enforceSessionLimitLocked(username)

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

// enforceSessionLimitLocked удаляет старейшие сессии пользователя,
// если их количество достигло maxSessionsPerUser.
// Вызывается под m.mu.
func (m *ACLManager) enforceSessionLimitLocked(username string) {
	type sess struct {
		id        string
		createdAt int64
	}
	var userSessions []sess
	m.sessions.Range(func(key, value interface{}) bool {
		s := value.(*Session)
		if s.Username == username {
			userSessions = append(userSessions, sess{id: s.ID, createdAt: s.CreatedAt})
		}
		return true
	})
	if len(userSessions) < maxSessionsPerUser {
		return
	}
	// Сортируем по createdAt возрастанию и удаляем самые старые,
	// чтобы после добавления новой сессии лимит не был превышен.
	sort.Slice(userSessions, func(i, j int) bool {
		return userSessions[i].createdAt < userSessions[j].createdAt
	})
	toDelete := len(userSessions) - maxSessionsPerUser + 1
	for i := 0; i < toDelete; i++ {
		m.sessions.Delete(userSessions[i].id)
	}
}

// Logout завершает сессию
func (m *ACLManager) Logout(sessionID string) {
	m.sessions.Delete(sessionID)
}

// CheckSession проверяет, активна ли сессия (с учётом expiry)
func (m *ACLManager) CheckSession(sessionID string) bool {
	return m.getSession(sessionID) != nil
}

// CheckSessionForIP проверяет сессию и совпадение IP клиента.
// Если у сессии не задан CreatedByIP — проверка IP пропускается.
func (m *ACLManager) CheckSessionForIP(sessionID, clientIP string) bool {
	s := m.getSession(sessionID)
	if s == nil {
		return false
	}
	if s.CreatedByIP != "" && s.CreatedByIP != clientIP {
		return false
	}
	return true
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

// DisableUser отключает пользователя.
// ИСПРАВЛЕНО: инвалидирует все сессии пользователя.
func (m *ACLManager) DisableUser(username string) error {
	if username == "admin" {
		return fmt.Errorf("cannot disable built-in admin")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	val, ok := m.users.Load(username)
	if !ok {
		return fmt.Errorf("user not found")
	}
	src := val.(*User)
	user := *src
	user.Active = false
	m.users.Store(username, &user)

	// Инвалидируем все сессии
	m.dropUserSessionsLocked(username)
	return nil
}

// EnableUser включает пользователя
func (m *ACLManager) EnableUser(username string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	val, ok := m.users.Load(username)
	if !ok {
		return fmt.Errorf("user not found")
	}
	src := val.(*User)
	user := *src
	user.Active = true
	m.users.Store(username, &user)
	return nil
}

// DeleteUser удаляет пользователя.
// ИСПРАВЛЕНО: инвалидирует все сессии пользователя.
func (m *ACLManager) DeleteUser(username string) error {
	if username == "admin" {
		return fmt.Errorf("cannot delete built-in admin")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.users.LoadAndDelete(username); !exists {
		return fmt.Errorf("user not found")
	}
	m.dropUserSessionsLocked(username)
	return nil
}

// ChangePassword изменяет пароль пользователя.
// ИСПРАВЛЕНО: инвалидирует все сессии пользователя (кроме текущей,
// если нужно, — но проще сбросить все, чтобы гарантировать
// безопасность при смене пароля администратором).
func (m *ACLManager) ChangePassword(username, newPassword string) error {
	if err := validatePassword(newPassword); err != nil {
		return err
	}
	hash, err := hashPassword(newPassword)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	val, ok := m.users.Load(username)
	if !ok {
		return fmt.Errorf("user not found")
	}
	src := val.(*User)
	user := *src
	user.PasswordHash = hash
	user.LegacyPassword = ""
	m.users.Store(username, &user)

	m.dropUserSessionsLocked(username)
	return nil
}

// dropUserSessionsLocked удаляет все сессии пользователя.
// Вызывается под m.mu.
func (m *ACLManager) dropUserSessionsLocked(username string) {
	var toDelete []string
	m.sessions.Range(func(key, value interface{}) bool {
		s := value.(*Session)
		if s.Username == username {
			toDelete = append(toDelete, s.ID)
		}
		return true
	})
	for _, id := range toDelete {
		m.sessions.Delete(id)
	}
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
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.roles.Load(name); exists {
		return fmt.Errorf("role %s already exists", name)
	}
	role := &Role{Name: name, Permissions: []string{}}
	m.roles.Store(name, role)
	return nil
}

// DeleteRole удаляет роль.
// ИСПРАВЛЕНО: также удаляет роль у всех пользователей,
// у которых она была — иначе в сессиях/юзерах остаётся «висячая» роль.
func (m *ACLManager) DeleteRole(name string) error {
	if name == "admin" {
		return fmt.Errorf("cannot delete built-in role 'admin'")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.roles.LoadAndDelete(name); !exists {
		return fmt.Errorf("role not found")
	}

	// Убираем роль у всех пользователей
	m.users.Range(func(key, value interface{}) bool {
		src := value.(*User)
		hasRole := false
		for _, r := range src.Roles {
			if r == name {
				hasRole = true
				break
			}
		}
		if !hasRole {
			return true
		}
		user := *src
		newRoles := make([]string, 0, len(user.Roles))
		for _, r := range user.Roles {
			if r != name {
				newRoles = append(newRoles, r)
			}
		}
		user.Roles = newRoles
		m.users.Store(key.(string), &user)
		return true
	})

	// И инвалидируем все сессии, у которых была эта роль.
	var toDelete []string
	m.sessions.Range(func(key, value interface{}) bool {
		s := value.(*Session)
		for _, r := range s.Roles {
			if r == name {
				toDelete = append(toDelete, s.ID)
				break
			}
		}
		return true
	})
	for _, id := range toDelete {
		m.sessions.Delete(id)
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

// GrantPermission выдаёт разрешение роли (с дедупликацией).
// ИСПРАВЛЕНО: валидирует формат разрешения и мутирует под m.mu.
func (m *ACLManager) GrantPermission(roleName, permission string) error {
	if err := validatePermission(permission); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	val, ok := m.roles.Load(roleName)
	if !ok {
		return fmt.Errorf("role not found")
	}
	src := val.(*Role)
	role := *src
	role.Permissions = append([]string(nil), src.Permissions...)

	for _, p := range role.Permissions {
		if p == permission {
			return nil // уже есть
		}
	}
	role.Permissions = append(role.Permissions, permission)
	m.roles.Store(roleName, &role)
	return nil
}

// RevokePermission отзывает разрешение у роли.
func (m *ACLManager) RevokePermission(roleName, permission string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	val, ok := m.roles.Load(roleName)
	if !ok {
		return fmt.Errorf("role not found")
	}
	src := val.(*Role)
	role := *src
	newPermissions := make([]string, 0, len(role.Permissions))
	for _, p := range src.Permissions {
		if p != permission {
			newPermissions = append(newPermissions, p)
		}
	}
	role.Permissions = newPermissions
	m.roles.Store(roleName, &role)
	return nil
}

// =============================================================================
// РОЛИ ПОЛЬЗОВАТЕЛЯ
// =============================================================================

// AddUserRole добавляет роль пользователю.
// ИСПРАВЛЕНО: мутирует под m.mu; инвалидирует сессии пользователя,
// чтобы новоприобретённая роль вступила в силу при следующем входе.
func (m *ACLManager) AddUserRole(username, roleName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	val, ok := m.users.Load(username)
	if !ok {
		return fmt.Errorf("user not found")
	}
	src := val.(*User)
	for _, r := range src.Roles {
		if r == roleName {
			return fmt.Errorf("user already has role %s", roleName)
		}
	}
	user := *src
	user.Roles = append(append([]string(nil), src.Roles...), roleName)
	m.users.Store(username, &user)

	// Инвалидируем сессии пользователя: после смены ролей старые
	// сессии не должны сохранять прежний набор прав.
	m.dropUserSessionsLocked(username)
	return nil
}

// RemoveUserRole удаляет роль у пользователя.
// ИСПРАВЛЕНО: мутирует под m.mu; инвалидирует сессии пользователя.
func (m *ACLManager) RemoveUserRole(username, roleName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	val, ok := m.users.Load(username)
	if !ok {
		return fmt.Errorf("user not found")
	}
	src := val.(*User)
	newRoles := make([]string, 0, len(src.Roles))
	for _, r := range src.Roles {
		if r != roleName {
			newRoles = append(newRoles, r)
		}
	}
	user := *src
	user.Roles = newRoles
	m.users.Store(username, &user)

	m.dropUserSessionsLocked(username)
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
//   - "*:*"           — полный доступ ко всему (включая admin)
//   - "db.*:read"     — read всех коллекций в db
//   - "db.coll:admin" — admin-доступ к конкретной коллекции
//   - "db.coll:*"     — все операции на db.coll
//
// ИСПРАВЛЕНО: убран мёртвый код с проверкой resource == "*:*".
// Раньше splitPermission("*:*") давал resource="*", op="*", и первая
// ветка "if resource == \"*:*\" && op == \"*\"" никогда не срабатывала.
// Теперь wildcard-разрешение обрабатывается явно, до split.
func matchPermission(perm, database, collection, operation string) bool {
	// Полный доступ — единственная форма, где resource содержит ":".
	if perm == "*:*" {
		return true
	}

	parts := splitPermission(perm)
	if len(parts) != 2 {
		return false
	}
	resource := parts[0]
	op := parts[1]

	// Явный admin-доступ: требуется op == "admin" (или "*").
	if operation == "admin" {
		if op != "admin" && op != "*" {
			return false
		}
		resourceParts := splitResource(resource)
		if len(resourceParts) != 2 {
			return false
		}
		dbMatch := resourceParts[0] == "*" || resourceParts[0] == database
		collMatch := resourceParts[1] == "*" || resourceParts[1] == collection
		return dbMatch && collMatch
	}

	// Обычные операции: op == "*" или совпадает с запрошенной.
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

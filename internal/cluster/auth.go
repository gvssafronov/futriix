/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/cluster/auth.go
// Назначение: Аутентификация и авторизация между узлами кластера

package cluster

import (
    "crypto"
    "crypto/rand"
    "crypto/rsa"
    "crypto/sha256"
    "crypto/x509"
    "encoding/base64"
    "encoding/json"
    "encoding/pem"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"
)

// NodeAuthConfig конфигурация аутентификации узлов
type NodeAuthConfig struct {
    Enabled          bool          `json:"enabled"`
    TokenTTL         time.Duration `json:"token_ttl"`
    PrivateKeyPath   string        `json:"private_key_path"`
    PublicKeyPath    string        `json:"public_key_path"`
    AllowedNodes     []string      `json:"allowed_nodes"`
    RequireMTLS      bool          `json:"require_mtls"`
}

// NodeAuthToken представляет токен аутентификации узла
type NodeAuthToken struct {
    NodeID     string `json:"node_id"`
    IssuedAt   int64  `json:"issued_at"`
    ExpiresAt  int64  `json:"expires_at"`
    Signature  string `json:"signature"`
}

// NodeAuthenticator управляет аутентификацией между узлами
type NodeAuthenticator struct {
    config      *NodeAuthConfig
    privateKey  *rsa.PrivateKey
    publicKey   *rsa.PublicKey
    tokens      sync.Map
    mu          sync.RWMutex
    logger      LoggerInterface // Используем LoggerInterface из node.go
}

// NewNodeAuthenticator создаёт новый аутентификатор узлов
func NewNodeAuthenticator(config *NodeAuthConfig, logger LoggerInterface) (*NodeAuthenticator, error) {
    if config == nil {
        config = &NodeAuthConfig{
            Enabled:      true,
            TokenTTL:     24 * time.Hour,
            AllowedNodes: make([]string, 0),
            RequireMTLS:  false,
        }
    }
    
    na := &NodeAuthenticator{
        config: config,
        logger: logger,
    }
    
    if config.Enabled {
        if err := na.loadKeys(); err != nil {
            if err := na.generateKeys(); err != nil {
                return nil, err
            }
        }
    }
    
    return na, nil
}

// generateKeys генерирует RSA ключи для аутентификации
func (na *NodeAuthenticator) generateKeys() error {
    privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
    if err != nil {
        return err
    }
    
    na.privateKey = privateKey
    na.publicKey = &privateKey.PublicKey
    
    // Сохраняем ключи
    if err := na.saveKeys(); err != nil {
        return err
    }
    
    if na.logger != nil {
        na.logger.Info("Generated new RSA keys for node authentication")
    }
    
    return nil
}

// loadKeys загружает RSA ключи с диска
func (na *NodeAuthenticator) loadKeys() error {
    // Загружаем приватный ключ
    privData, err := os.ReadFile(na.config.PrivateKeyPath)
    if err != nil {
        return err
    }
    
    block, _ := pem.Decode(privData)
    if block == nil {
        return fmt.Errorf("failed to decode private key")
    }
    
    privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
    if err != nil {
        return err
    }
    
    var ok bool
    na.privateKey, ok = privateKey.(*rsa.PrivateKey)
    if !ok {
        return fmt.Errorf("invalid private key type")
    }
    
    // Загружаем публичный ключ
    pubData, err := os.ReadFile(na.config.PublicKeyPath)
    if err != nil {
        return err
    }
    
    pubBlock, _ := pem.Decode(pubData)
    if pubBlock == nil {
        return fmt.Errorf("failed to decode public key")
    }
    
    publicKey, err := x509.ParsePKIXPublicKey(pubBlock.Bytes)
    if err != nil {
        return err
    }
    
    na.publicKey, ok = publicKey.(*rsa.PublicKey)
    if !ok {
        return fmt.Errorf("invalid public key type")
    }
    
    return nil
}

// saveKeys сохраняет RSA ключи на диск
func (na *NodeAuthenticator) saveKeys() error {
    if na.privateKey == nil {
        return fmt.Errorf("no private key to save")
    }
    
    // Сохраняем приватный ключ
    privBytes, err := x509.MarshalPKCS8PrivateKey(na.privateKey)
    if err != nil {
        return err
    }
    
    privPEM := pem.EncodeToMemory(&pem.Block{
        Type:  "PRIVATE KEY",
        Bytes: privBytes,
    })
    
    if err := os.MkdirAll(filepath.Dir(na.config.PrivateKeyPath), 0700); err != nil {
        return err
    }
    
    if err := os.WriteFile(na.config.PrivateKeyPath, privPEM, 0600); err != nil {
        return err
    }
    
    // Сохраняем публичный ключ
    pubBytes, err := x509.MarshalPKIXPublicKey(na.publicKey)
    if err != nil {
        return err
    }
    
    pubPEM := pem.EncodeToMemory(&pem.Block{
        Type:  "PUBLIC KEY",
        Bytes: pubBytes,
    })
    
    if err := os.WriteFile(na.config.PublicKeyPath, pubPEM, 0644); err != nil {
        return err
    }
    
    return nil
}

// GenerateToken генерирует токен для узла
func (na *NodeAuthenticator) GenerateToken(nodeID string) (string, error) {
    if !na.config.Enabled {
        return "", nil
    }
    
    now := time.Now().UnixMilli()
    token := &NodeAuthToken{
        NodeID:    nodeID,
        IssuedAt:  now,
        ExpiresAt: now + na.config.TokenTTL.Milliseconds(),
    }
    
    // Подписываем токен
    data, err := json.Marshal(token)
    if err != nil {
        return "", err
    }
    
    hash := sha256.Sum256(data)
    signature, err := rsa.SignPKCS1v15(rand.Reader, na.privateKey, crypto.SHA256, hash[:])
    if err != nil {
        return "", err
    }
    
    token.Signature = base64.StdEncoding.EncodeToString(signature)
    
    // Сохраняем токен
    tokenData, err := json.Marshal(token)
    if err != nil {
        return "", err
    }
    
    tokenString := base64.StdEncoding.EncodeToString(tokenData)
    na.tokens.Store(nodeID, token)
    
    return tokenString, nil
}

// VerifyToken проверяет токен узла
func (na *NodeAuthenticator) VerifyToken(tokenString string) (string, error) {
    if !na.config.Enabled {
        return "", nil
    }
    
    data, err := base64.StdEncoding.DecodeString(tokenString)
    if err != nil {
        return "", fmt.Errorf("invalid token encoding: %v", err)
    }
    
    var token NodeAuthToken
    if err := json.Unmarshal(data, &token); err != nil {
        return "", fmt.Errorf("invalid token format: %v", err)
    }
    
    // Проверяем срок действия
    now := time.Now().UnixMilli()
    if token.ExpiresAt < now {
        return "", fmt.Errorf("token expired")
    }
    
    // Проверяем подпись
    sigData, err := base64.StdEncoding.DecodeString(token.Signature)
    if err != nil {
        return "", fmt.Errorf("invalid signature: %v", err)
    }
    
    tokenCopy := token
    tokenCopy.Signature = ""
    dataWithoutSig, _ := json.Marshal(tokenCopy)
    hash := sha256.Sum256(dataWithoutSig)
    
    if err := rsa.VerifyPKCS1v15(na.publicKey, crypto.SHA256, hash[:], sigData); err != nil {
        return "", fmt.Errorf("invalid signature: %v", err)
    }
    
    // Проверяем, разрешён ли узел
    if len(na.config.AllowedNodes) > 0 {
        allowed := false
        for _, node := range na.config.AllowedNodes {
            if node == token.NodeID {
                allowed = true
                break
            }
        }
        if !allowed {
            return "", fmt.Errorf("node %s not allowed", token.NodeID)
        }
    }
    
    return token.NodeID, nil
}

// AuthenticateRequest аутентифицирует запрос
func (na *NodeAuthenticator) AuthenticateRequest(data []byte, signature string) bool {
    if !na.config.Enabled {
        return true
    }
    
    sigData, err := base64.StdEncoding.DecodeString(signature)
    if err != nil {
        return false
    }
    
    hash := sha256.Sum256(data)
    err = rsa.VerifyPKCS1v15(na.publicKey, crypto.SHA256, hash[:], sigData)
    return err == nil
}

// SignRequest подписывает запрос
func (na *NodeAuthenticator) SignRequest(data []byte) (string, error) {
    if !na.config.Enabled {
        return "", nil
    }
    
    hash := sha256.Sum256(data)
    signature, err := rsa.SignPKCS1v15(rand.Reader, na.privateKey, crypto.SHA256, hash[:])
    if err != nil {
        return "", err
    }
    
    return base64.StdEncoding.EncodeToString(signature), nil
}

// AddAllowedNode добавляет разрешённый узел
func (na *NodeAuthenticator) AddAllowedNode(nodeID string) {
    na.mu.Lock()
    defer na.mu.Unlock()
    
    for _, n := range na.config.AllowedNodes {
        if n == nodeID {
            return
        }
    }
    na.config.AllowedNodes = append(na.config.AllowedNodes, nodeID)
}

// RemoveAllowedNode удаляет разрешённый узел
func (na *NodeAuthenticator) RemoveAllowedNode(nodeID string) {
    na.mu.Lock()
    defer na.mu.Unlock()
    
    newList := make([]string, 0)
    for _, n := range na.config.AllowedNodes {
        if n != nodeID {
            newList = append(newList, n)
        }
    }
    na.config.AllowedNodes = newList
}

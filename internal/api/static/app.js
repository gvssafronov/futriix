/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

/**
 * Futriis DB Dashboard - Web Interface JavaScript для веб-интерфейса Futriis DB Dashboard
 * @version 1.0.0
 * @description Обеспечивает полное управление СУБД: CRUD операции, ACL, индексы,
 *              транзакции, триггеры, ограничения (constraints), импорт/экспорт,
 *              управление кластером и аудит. Использует async/await, Fetch API,
 *              динамическую отрисовку DOM и модальные окна.
 * @author Futriis Team
 * @license CDDL-1.0
 */

// ======================== СОСТОЯНИЕ ПРИЛОЖЕНИЯ ========================

/** @type {string|null} ID текущей сессии */
let currentSession = null;

/** @type {string|null} Имя текущей базы данных */
let currentDatabase = null;

/** @type {string|null} Имя текущей коллекции */
let currentCollection = null;

/** @type {string|null} Имя текущего пользователя */
let currentUser = null;

/** @type {boolean} Флаг администратора */
let isAdmin = false;

// ======================== DOM ЭЛЕМЕНТЫ ========================

const DOM = {
    content: document.getElementById('contentArea'),
    title: document.getElementById('pageTitle'),
    connection: document.getElementById('connectionStatus'),
    userName: document.querySelector('#userName'),
    userRole: document.getElementById('userRole'),
    logoutBtn: document.getElementById('logoutBtn'),
    menuToggle: document.getElementById('menuToggle'),
    sidebar: document.querySelector('.sidebar'),
    modal: document.getElementById('modal'),
    modalTitle: document.getElementById('modalTitle'),
    modalBody: document.getElementById('modalBody'),
    modalConfirm: document.getElementById('modalConfirm'),
    modalCloseBtns: document.querySelectorAll('.modal-close'),
    changePasswordIcon: document.getElementById('changePasswordIcon'),
    userAvatar: document.getElementById('userAvatar'),
    notificationContainer: document.getElementById('notificationContainer')
};

// ======================== ИНИЦИАЛИЗАЦИЯ ========================

/**
 * Главная точка входа. Выполняется после загрузки DOM.
 */
document.addEventListener('DOMContentLoaded', async () => {
    await checkSession();
    initNavigation();
    initEventListeners();
    initAvatarUpload();
    initChangePassword();
});

// ======================== АУТЕНТИФИКАЦИЯ ========================

/**
 * Проверяет активность сессии на сервере
 */
async function checkSession() {
    try {
        const response = await fetch('/api/webui/session');
        const data = await response.json();

        if (data.success && data.data.authenticated) {
            currentUser = data.data.username;
            isAdmin = data.data.is_admin || false;
            DOM.userName.textContent = currentUser;
            DOM.userRole.textContent = isAdmin ? 'Администратор' : 'Пользователь';
            
            if (data.data.avatar) {
                updateAvatarDisplay(data.data.avatar);
            } else {
                loadUserAvatar();
            }

            updateConnectionStatus(data.data.connection_status);
            loadDashboard();
            startConnectionStatusMonitor();
        } else {
            showLoginModal();
        }
    } catch (error) {
        console.error('Session check failed:', error);
        showLoginModal();
    }
}

/**
 * Запускает периодическую проверку статуса подключения
 */
function startConnectionStatusMonitor() {
    setInterval(async () => {
        if (currentUser) {
            try {
                const response = await fetch('/api/webui/session');
                const data = await response.json();
                updateConnectionStatus(data.data?.connection_status || 'disconnected');
            } catch (error) {
                updateConnectionStatus('disconnected');
            }
        }
    }, 5000);
}

/**
 * Обновляет индикатор подключения
 * @param {string} status - Статус подключения ('connected' или 'disconnected')
 */
function updateConnectionStatus(status) {
    if (status === 'connected') {
        DOM.connection.className = 'connection-status online';
        DOM.connection.innerHTML = '<span>СУБД подключена</span>';
    } else {
        DOM.connection.className = 'connection-status offline';
        DOM.connection.innerHTML = '<span>СУБД не подключена</span>';
    }
}

/**
 * Отображает модальное окно для входа в систему
 */
function showLoginModal() {
    DOM.modalTitle.textContent = 'Вход в систему СУБД Futriis';
    DOM.modalBody.innerHTML = `
        <div class="form-group">
            <label for="username">Имя пользователя</label>
            <input type="text" id="username" class="form-control" placeholder="Введите имя пользователя">
        </div>
        <div class="form-group">
            <label for="password">Пароль</label>
            <input type="password" id="password" class="form-control" placeholder="Введите пароль">
        </div>
    `;
    DOM.modalConfirm.textContent = 'Войти';
    DOM.modal.classList.add('show');

    const confirmHandler = async () => {
        const username = document.getElementById('username').value;
        const password = document.getElementById('password').value;

        if (!username || !password) {
            showNotification('Пожалуйста, заполните все поля', 'error');
            return;
        }

        try {
            const response = await fetch('/api/webui/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password })
            });
            const data = await response.json();

            if (data.success) {
                currentUser = username;
                isAdmin = data.data.is_admin || false;
                DOM.userName.textContent = username;
                DOM.userRole.textContent = isAdmin ? 'Администратор' : 'Пользователь';
                
                if (data.data.avatar) {
                    updateAvatarDisplay(data.data.avatar);
                }
                
                DOM.modal.classList.remove('show');
                showNotification('Вход выполнен успешно', 'success');
                updateConnectionStatus('connected');
                startConnectionStatusMonitor();
                loadDashboard();
            } else {
                showNotification(data.error || 'Неверный логин и/или пароль', 'error');
            }
        } catch (error) {
            showNotification('Ошибка подключения к серверу', 'error');
        }
    };

    DOM.modalConfirm.onclick = confirmHandler;

    const handleEnter = (e) => {
        if (e.key === 'Enter') {
            confirmHandler();
            document.removeEventListener('keydown', handleEnter);
        }
    };
    document.addEventListener('keydown', handleEnter);
}

// ======================== АВАТАР ПОЛЬЗОВАТЕЛЯ ========================

/**
 * Загружает аватар пользователя с сервера
 */
async function loadUserAvatar() {
    try {
        const response = await fetch('/api/webui/user/info');
        const data = await response.json();
        
        if (data.success && data.data.avatar) {
            updateAvatarDisplay(data.data.avatar);
        }
    } catch (error) {
        console.error('Failed to load avatar:', error);
    }
}

/**
 * Обновляет отображение аватара
 * @param {string} avatarBase64 - Аватар в формате base64
 */
function updateAvatarDisplay(avatarBase64) {
    if (!DOM.userAvatar) return;
    DOM.userAvatar.innerHTML = `<img src="${avatarBase64}" alt="Avatar" style="width:40px;height:40px;border-radius:50%;object-fit:cover;">`;
}

/**
 * Инициализирует загрузку аватара
 */
function initAvatarUpload() {
    if (!DOM.userAvatar) return;
    
    DOM.userAvatar.style.cursor = 'pointer';
    DOM.userAvatar.addEventListener('click', () => {
        showAvatarUploadModal();
    });
}

/**
 * Показывает модальное окно загрузки аватара
 */
function showAvatarUploadModal() {
    const avatarModal = document.getElementById('avatarUploadModal');
    const fileInput = document.getElementById('avatarFile');
    const preview = document.getElementById('avatarPreview');
    const uploadBtn = document.getElementById('uploadAvatarBtn');
    
    if (!avatarModal) return;
    
    if (fileInput) fileInput.value = '';
    if (preview) preview.innerHTML = '';
    
    avatarModal.classList.add('show');
    
    if (fileInput) {
        fileInput.onchange = function() {
            if (this.files && this.files[0]) {
                const reader = new FileReader();
                reader.onload = function(e) {
                    if (preview) {
                        preview.innerHTML = `<img src="${e.target.result}" style="max-width:150px;max-height:150px;border-radius:50%;">`;
                    }
                };
                reader.readAsDataURL(this.files[0]);
            }
        };
    }
    
    if (uploadBtn) {
        uploadBtn.onclick = async () => {
            if (!fileInput || !fileInput.files || fileInput.files.length === 0) {
                showNotification('Выберите изображение', 'warning');
                return;
            }
            
            const formData = new FormData();
            formData.append('avatar', fileInput.files[0]);
            
            try {
                const response = await fetch('/api/webui/user/avatar', {
                    method: 'POST',
                    body: formData
                });
                const data = await response.json();
                
                if (data.success) {
                    updateAvatarDisplay(data.data.avatar);
                    avatarModal.classList.remove('show');
                    showNotification('Аватар успешно загружен', 'success');
                } else {
                    showNotification(data.error || 'Ошибка загрузки аватара', 'error');
                }
            } catch (error) {
                showNotification('Ошибка подключения', 'error');
            }
        };
    }
    
    const closeButtons = avatarModal.querySelectorAll('.modal-close');
    closeButtons.forEach(btn => {
        btn.onclick = () => {
            avatarModal.classList.remove('show');
        };
    });
}

// ======================== СМЕНА ПАРОЛЯ ========================

/**
 * Инициализирует смену пароля
 */
function initChangePassword() {
    if (DOM.changePasswordIcon) {
        DOM.changePasswordIcon.addEventListener('click', () => {
            showChangePasswordModal();
        });
    }
}

/**
 * Показывает модальное окно смены пароля
 */
function showChangePasswordModal() {
    const passwordModal = document.getElementById('changePasswordModal');
    const currentPasswordInput = document.getElementById('currentPassword');
    const newPasswordInput = document.getElementById('newPassword');
    const confirmPasswordInput = document.getElementById('confirmPassword');
    const changeBtn = document.getElementById('changePasswordBtn');
    
    if (!passwordModal) return;
    
    if (currentPasswordInput) currentPasswordInput.value = '';
    if (newPasswordInput) newPasswordInput.value = '';
    if (confirmPasswordInput) confirmPasswordInput.value = '';
    
    passwordModal.classList.add('show');
    
    if (changeBtn) {
        changeBtn.onclick = async () => {
            const currentPassword = currentPasswordInput?.value || '';
            const newPassword = newPasswordInput?.value || '';
            const confirmPassword = confirmPasswordInput?.value || '';
            
            if (!currentPassword) {
                showNotification('Введите текущий пароль', 'warning');
                return;
            }
            
            if (!newPassword) {
                showNotification('Введите новый пароль', 'warning');
                return;
            }
            
            if (newPassword !== confirmPassword) {
                showNotification('Новый пароль и подтверждение не совпадают', 'error');
                return;
            }
            
            if (newPassword.length < 4) {
                showNotification('Новый пароль должен содержать минимум 4 символа', 'error');
                return;
            }
            
            try {
                const response = await fetch('/api/webui/change-password', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        current_password: currentPassword,
                        new_password: newPassword
                    })
                });
                const data = await response.json();
                
                if (data.success) {
                    passwordModal.classList.remove('show');
                    showNotification('Пароль успешно изменён', 'success');
                } else {
                    showNotification(data.error || 'Ошибка смены пароля', 'error');
                }
            } catch (error) {
                showNotification('Ошибка подключения', 'error');
            }
        };
    }
    
    const closeButtons = passwordModal.querySelectorAll('.modal-close');
    closeButtons.forEach(btn => {
        btn.onclick = () => {
            passwordModal.classList.remove('show');
        };
    });
}

// ======================== НАВИГАЦИЯ ========================

/**
 * Инициализирует обработчики навигации
 */
function initNavigation() {
    document.querySelectorAll('.nav-link[data-section]').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const section = link.dataset.section;
            loadSection(section);
            setActiveNav(link);
        });
    });

    document.querySelectorAll('[data-action]').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const action = item.dataset.action;
            handleAction(action);
        });
    });

    document.querySelectorAll('.has-submenu > .nav-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const parent = link.closest('.has-submenu');
            parent.classList.toggle('open');
        });
    });
}

/**
 * Инициализирует глобальные обработчики событий
 */
function initEventListeners() {
    DOM.logoutBtn.addEventListener('click', async () => {
        await fetch('/api/webui/logout', { method: 'POST' });
        currentUser = null;
        isAdmin = false;
        updateConnectionStatus('disconnected');
        if (DOM.userAvatar) {
            DOM.userAvatar.innerHTML = '<i class="fas fa-user-circle" style="font-size: 40px;"></i>';
        }
        showLoginModal();
    });

    if (DOM.menuToggle) {
        DOM.menuToggle.addEventListener('click', () => {
            DOM.sidebar.classList.toggle('open');
        });
    }

    DOM.modalCloseBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            DOM.modal.classList.remove('show');
        });
    });

    DOM.modal.addEventListener('click', (e) => {
        if (e.target === DOM.modal) {
            DOM.modal.classList.remove('show');
        }
    });
}

/**
 * Загружает соответствующую секцию интерфейса
 * @param {string} section - Идентификатор секции
 */
async function loadSection(section) {
    const sections = {
        dashboard: loadDashboard,
        cluster: loadClusterManagement,
        logs: loadLogs,
        'plugins-list': loadPluginsList,
        settings: loadSettings,
        'acl-users': loadACLUsers,
        'acl-roles': loadACLRoles,
        'acl-permissions': loadACLPermissions,
        'tx-list': loadTransactionList,
        'indexes-list': loadIndexesList,
        'export-data': loadExportPage,
        'import-data': loadImportPage,
        'constraints-list': loadConstraintsList,
        'triggers-list': loadTriggersList,
        'trigger-log': loadTriggerLog
    };
    
    const loader = sections[section];
    if (loader) {
        await loader();
    } else {
        DOM.content.innerHTML = '<div class="info-message">Раздел в разработке</div>';
    }
}

/**
 * Устанавливает активный пункт навигации
 * @param {HTMLElement} activeLink - Активный элемент ссылки
 */
function setActiveNav(activeLink) {
    document.querySelectorAll('.nav-link').forEach(link => link.classList.remove('active'));
    activeLink.classList.add('active');
}

// ======================== ДАШБОРД ========================

/**
 * Загружает и отображает главную панель управления
 */
async function loadDashboard() {
    DOM.title.textContent = 'Панель управления';
    DOM.content.innerHTML = '<div class="loading-spinner"><i class="fas fa-spinner fa-pulse"></i><p>Загрузка данных...</p></div>';

    try {
        const [statsRes, dbsRes] = await Promise.all([
            fetch('/api/webui/stats'),
            fetch('/api/webui/databases')
        ]);
        const stats = await statsRes.json();
        const databases = await dbsRes.json();

        DOM.content.innerHTML = `
            <div class="dashboard-stats">
                <div class="stat-card"><div class="stat-icon"><i class="fas fa-database"></i></div><div class="stat-info"><h3>${stats.data.databases || 0}</h3><p>Базы данных</p></div></div>
                <div class="stat-card"><div class="stat-icon"><i class="fas fa-table"></i></div><div class="stat-info"><h3>${stats.data.collections || 0}</h3><p>Коллекции</p></div></div>
                <div class="stat-card"><div class="stat-icon"><i class="fas fa-file-alt"></i></div><div class="stat-info"><h3>${stats.data.documents || 0}</h3><p>Документы</p></div></div>
                <div class="stat-card"><div class="stat-icon"><i class="fas fa-hdd"></i></div><div class="stat-info"><h3>${stats.data.storage_used_mb?.toFixed(2) || 0} MB</h3><p>Использовано памяти</p></div></div>
            </div>
            <div class="data-table"><h3>Базы данных</h3>
            <table><thead><tr><th>Имя БД</th><th>Коллекции</th><th>Действия</th></tr></thead><tbody>
                ${databases.data.map(db => `<tr><td><strong>${escapeHtml(db.name)}</strong></td><td>${db.collections}</td><td><button class="btn btn-sm btn-primary" onclick="viewDatabase('${escapeHtml(db.name)}')"><i class="fas fa-eye"></i> Просмотр</button></td></tr>`).join('')}
            </tbody></table></div>
        `;
    } catch (error) {
        DOM.content.innerHTML = '<div class="error-message">Ошибка загрузки данных</div>';
        showNotification('Ошибка загрузки дашборда', 'error');
    }
}

// ======================== БАЗЫ ДАННЫХ И КОЛЛЕКЦИИ ========================

/**
 * Отображает список коллекций в выбранной базе данных
 * @param {string} dbName - Имя базы данных
 */
window.viewDatabase = async function(dbName) {
    currentDatabase = dbName;
    DOM.title.textContent = `База данных: ${dbName}`;
    DOM.content.innerHTML = '<div class="loading-spinner"><i class="fas fa-spinner fa-pulse"></i><p>Загрузка коллекций...</p></div>';

    try {
        const response = await fetch(`/api/webui/collections/${dbName}`);
        const data = await response.json();

        if (data.success) {
            DOM.content.innerHTML = `
                <div class="data-table">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                        <h3>Коллекции</h3>
                        <button class="btn btn-primary btn-sm" onclick="showCreateCollectionModal()"><i class="fas fa-plus"></i> Создать коллекцию</button>
                    </div>
                    <table><thead><tr><th>Имя коллекции</th><th>Документов</th><th>Размер</th><th>Индексы</th><th>Действия</th></tr></thead><tbody>
                        ${data.data.collections.map(coll => `
                            <tr>
                                <td><strong>${escapeHtml(coll.name)}</strong></td>
                                <td>${coll.count}</td>
                                <td>${(coll.size / 1024).toFixed(2)} KB</td>
                                <td>${coll.indexes.length}</td>
                                <td>
                                    <button class="btn btn-sm btn-primary" onclick="viewCollection('${escapeHtml(dbName)}', '${escapeHtml(coll.name)}')"><i class="fas fa-eye"></i></button>
                                    <button class="btn btn-sm btn-danger" onclick="deleteCollection('${escapeHtml(dbName)}', '${escapeHtml(coll.name)}')"><i class="fas fa-trash"></i></button>
                                </td>
                            </tr>
                        `).join('')}
                    </tbody></table>
                </div>
            `;
        } else {
            DOM.content.innerHTML = '<div class="error-message">Ошибка загрузки коллекций</div>';
        }
    } catch (error) {
        DOM.content.innerHTML = '<div class="error-message">Ошибка подключения</div>';
    }
};

/**
 * Отображает документы выбранной коллекции
 * @param {string} dbName - Имя базы данных
 * @param {string} collName - Имя коллекции
 */
window.viewCollection = async function(dbName, collName) {
    currentDatabase = dbName;
    currentCollection = collName;
    DOM.title.textContent = `Коллекция: ${dbName}.${collName}`;
    DOM.content.innerHTML = '<div class="loading-spinner"><i class="fas fa-spinner fa-pulse"></i><p>Загрузка документов...</p></div>';

    try {
        const response = await fetch(`/api/webui/documents/${dbName}/${collName}?limit=100`);
        const data = await response.json();

        if (data.success) {
            DOM.content.innerHTML = `
                <div style="margin-bottom: 16px; display: flex; gap: 12px;">
                    <button class="btn btn-primary" onclick="showInsertDocumentModal()"><i class="fas fa-plus"></i> Вставить документ</button>
                    <button class="btn btn-secondary" onclick="viewDatabase('${escapeHtml(dbName)}')"><i class="fas fa-arrow-left"></i> Назад</button>
                </div>
                <div class="data-table">
                    <h3>Документы (${data.data.total} всего)</h3>
                    <table><thead><tr><th>ID</th><th>Поля</th><th>Создан</th><th>Действия</th></tr></thead><tbody>
                        ${data.data.documents.map(doc => `
                            <tr>
                                <td><code>${escapeHtml(doc.id)}</code></td>
                                <td><pre style="max-width:400px; overflow-x:auto;">${escapeHtml(JSON.stringify(doc.fields, null, 2))}</pre></td>
                                <td>${new Date(doc.created_at).toLocaleString()}</td>
                                <td>
                                    <button class="btn btn-sm btn-secondary" onclick="showUpdateDocumentModal('${escapeHtml(doc.id)}', ${escapeHtml(JSON.stringify(doc.fields))})"><i class="fas fa-edit"></i></button>
                                    <button class="btn btn-sm btn-danger" onclick="deleteDocument('${escapeHtml(dbName)}', '${escapeHtml(collName)}', '${escapeHtml(doc.id)}')"><i class="fas fa-trash"></i></button>
                                </td>
                            </tr>
                        `).join('')}
                    </tbody></table>
                </div>
            `;
        } else {
            DOM.content.innerHTML = '<div class="error-message">Ошибка загрузки документов</div>';
        }
    } catch (error) {
        DOM.content.innerHTML = '<div class="error-message">Ошибка подключения</div>';
    }
};

/**
 * Удаляет коллекцию
 * @param {string} dbName - Имя БД
 * @param {string} collName - Имя коллекции
 */
window.deleteCollection = async function(dbName, collName) {
    if (!confirm(`Удалить коллекцию "${collName}"? Это действие необратимо.`)) return;
    try {
        const response = await fetch(`/api/db/${dbName}/${collName}`, { method: 'DELETE' });
        if (response.ok) {
            showNotification(`Коллекция "${collName}" удалена`, 'success');
            viewDatabase(dbName);
        } else {
            const error = await response.json();
            showNotification(error.error || 'Ошибка удаления коллекции', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
};

/**
 * Удаляет документ
 * @param {string} dbName - Имя БД
 * @param {string} collName - Имя коллекции
 * @param {string} docId - ID документа
 */
window.deleteDocument = async function(dbName, collName, docId) {
    if (!confirm(`Удалить документ "${docId}"?`)) return;
    try {
        const response = await fetch(`/api/webui/documents/${dbName}/${collName}?id=${encodeURIComponent(docId)}`, { method: 'DELETE' });
        const result = await response.json();
        if (result.success) {
            showNotification('Документ удалён', 'success');
            viewCollection(dbName, collName);
        } else {
            showNotification(result.error || 'Ошибка удаления документа', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
};

// ======================== ЛОГИ ОПЕРАЦИЙ ========================

/**
 * Загружает и отображает лог операций веб-интерфейса
 */
async function loadLogs() {
    if (!isAdmin) {
        DOM.content.innerHTML = '<div class="error-message">Доступ запрещён. Только для администраторов.</div>';
        return;
    }
    
    DOM.title.textContent = 'Лог операций';
    DOM.content.innerHTML = '<div class="loading-spinner"><i class="fas fa-spinner fa-pulse"></i><p>Загрузка логов...</p></div>';

    try {
        const response = await fetch('/api/webui/logs?limit=500');
        const data = await response.json();

        if (data.success) {
            const logs = data.data;
            if (logs.length === 0) {
                DOM.content.innerHTML = '<p>Лог операций пуст</p>';
                return;
            }
            
            DOM.content.innerHTML = `
                <div class="data-table">
                    <h3>Лог операций веб-интерфейса</h3>
                    <div style="margin-bottom: 16px;">
                        <button class="btn btn-sm btn-secondary" onclick="loadLogs()"><i class="fas fa-sync-alt"></i> Обновить</button>
                    </div>
                    <table><thead><tr>
                        <th>Время</th><th>Операция</th><th>Цель</th><th>Пользователь</th><th>Статус</th><th>Ошибка</th>
                    </tr></thead><tbody>
                        ${logs.map(log => `
                            <tr>
                                <td>${new Date(log.timestamp).toLocaleString()}</td>
                                <td><code>${escapeHtml(log.operation)}</code></td>
                                <td>${escapeHtml(log.target)}</td>
                                <td>${escapeHtml(log.user)}</td>
                                <td><span class="status-badge status-${log.status === 'success' ? 'active' : 'inactive'}">${log.status === 'success' ? 'Успех' : 'Ошибка'}</span></td>
                                <td>${log.error_msg ? `<span class="error-text">${escapeHtml(log.error_msg)}</span>` : '-'}</td>
                            </tr>
                        `).join('')}
                    </tbody></table>
                </div>
            `;
        } else {
            DOM.content.innerHTML = '<div class="error-message">Ошибка загрузки логов</div>';
        }
    } catch (error) {
        DOM.content.innerHTML = '<div class="error-message">Ошибка подключения</div>';
    }
}

// ======================== ПЛАГИНЫ ========================

/**
 * Загружает и отображает список плагинов
 */
async function loadPluginsList() {
    if (!isAdmin) {
        DOM.content.innerHTML = '<div class="error-message">Доступ запрещён. Только для администраторов.</div>';
        return;
    }
    
    DOM.title.textContent = 'Управление плагинами';
    DOM.content.innerHTML = '<div class="loading-spinner"><i class="fas fa-spinner fa-pulse"></i><p>Загрузка плагинов...</p></div>';

    try {
        const response = await fetch('/api/webui/plugins');
        const data = await response.json();

        if (data.success) {
            const plugins = data.data;
            DOM.content.innerHTML = `
                <div style="margin-bottom: 16px;">
                    <button class="btn btn-primary" onclick="showUploadPluginModal()"><i class="fas fa-upload"></i> Загрузить плагин</button>
                    <button class="btn btn-secondary" onclick="loadPluginsList()"><i class="fas fa-sync-alt"></i> Обновить</button>
                </div>
                <div class="data-table">
                    <h3>Плагины</h3>
                    <table><thead><tr>
                        <th>Имя</th><th>Версия</th><th>Автор</th><th>Описание</th><th>Загружен</th><th>Действия</th>
                    </tr></thead><tbody>
                        ${plugins.map(plugin => `
                            <tr>
                                <td><strong>${escapeHtml(plugin.name)}</strong></strong></td>
                                <td>${escapeHtml(plugin.version)}</td>
                                <td>${escapeHtml(plugin.author)}</td>
                                <td>${escapeHtml(plugin.description)}</td>
                                <td>${new Date(plugin.loaded_at).toLocaleString()}</td>
                                <td>
                                    <button class="btn btn-sm btn-success" onclick="startPlugin('${escapeHtml(plugin.name)}')"><i class="fas fa-play"></i> Старт</button>
                                    <button class="btn btn-sm btn-warning" onclick="stopPlugin('${escapeHtml(plugin.name)}')"><i class="fas fa-stop"></i> Стоп</button>
                                    <button class="btn btn-sm btn-danger" onclick="deletePlugin('${escapeHtml(plugin.name)}')"><i class="fas fa-trash"></i> Удалить</button>
                                </td>
                            </tr>
                        `).join('')}
                        ${plugins.length === 0 ? '<tr><td colspan="6">Нет загруженных плагинов</td></tr>' : ''}
                    </tbody></table>
                </div>
            `;
        } else {
            DOM.content.innerHTML = '<div class="error-message">Ошибка загрузки плагинов</div>';
        }
    } catch (error) {
        DOM.content.innerHTML = '<div class="error-message">Ошибка подключения</div>';
    }
}

/**
 * Показывает модальное окно загрузки плагина
 */
function showUploadPluginModal() {
    DOM.modalTitle.textContent = 'Загрузить плагин';
    DOM.modalBody.innerHTML = `
        <div class="form-group">
            <label>Файл плагина (.lua)</label>
            <input type="file" id="pluginFile" class="form-control" accept=".lua">
        </div>
        <div class="info-message">
            <i class="fas fa-info-circle"></i>
            Плагины должны быть написаны на Lua и содержать функции on_load, on_start, on_stop, on_unload
        </div>
    `;
    DOM.modalConfirm.textContent = 'Загрузить';
    DOM.modal.classList.add('show');
    
    DOM.modalConfirm.onclick = async () => {
        const fileInput = document.getElementById('pluginFile');
        if (!fileInput || !fileInput.files || fileInput.files.length === 0) {
            showNotification('Выберите файл плагина', 'warning');
            return;
        }
        
        const formData = new FormData();
        formData.append('plugin', fileInput.files[0]);
        
        try {
            const response = await fetch('/api/webui/plugin/upload', {
                method: 'POST',
                body: formData
            });
            const data = await response.json();
            
            if (data.success) {
                DOM.modal.classList.remove('show');
                showNotification('Плагин загружен', 'success');
                loadPluginsList();
            } else {
                showNotification(data.error || 'Ошибка загрузки плагина', 'error');
            }
        } catch (error) {
            showNotification('Ошибка подключения', 'error');
        }
    };
}

/**
 * Запускает плагин
 * @param {string} pluginName - Имя плагина
 */
window.startPlugin = async function(pluginName) {
    try {
        const response = await fetch(`/api/webui/plugin/${pluginName}/start`, { method: 'POST' });
        const data = await response.json();
        if (data.success) {
            showNotification(`Плагин ${pluginName} запущен`, 'success');
            loadPluginsList();
        } else {
            showNotification(data.error || 'Ошибка запуска', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
};

/**
 * Останавливает плагин
 * @param {string} pluginName - Имя плагина
 */
window.stopPlugin = async function(pluginName) {
    try {
        const response = await fetch(`/api/webui/plugin/${pluginName}/stop`, { method: 'POST' });
        const data = await response.json();
        if (data.success) {
            showNotification(`Плагин ${pluginName} остановлен`, 'success');
            loadPluginsList();
        } else {
            showNotification(data.error || 'Ошибка остановки', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
};

/**
 * Удаляет плагин
 * @param {string} pluginName - Имя плагина
 */
window.deletePlugin = async function(pluginName) {
    if (!confirm(`Удалить плагин "${pluginName}"?`)) return;
    try {
        const response = await fetch(`/api/webui/plugin/${pluginName}/delete`, { method: 'DELETE' });
        const data = await response.json();
        if (data.success) {
            showNotification(`Плагин ${pluginName} удалён`, 'success');
            loadPluginsList();
        } else {
            showNotification(data.error || 'Ошибка удаления', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
};

// ======================== ОСТАЛЬНЫЕ СЕКЦИИ ========================

/**
 * Загружает страницу управления кластером
 */
async function loadClusterManagement() {
    DOM.title.textContent = 'Управление кластером';
    DOM.content.innerHTML = '<div class="loading-spinner"><i class="fas fa-spinner fa-pulse"></i><p>Загрузка информации о кластере...</p></div>';

    try {
        const [statusRes, nodesRes] = await Promise.all([
            fetch('/api/webui/cluster/status'),
            fetch('/api/webui/cluster/nodes')
        ]);
        const status = await statusRes.json();
        const nodes = await nodesRes.json();

        DOM.content.innerHTML = `
            <div class="dashboard-stats">
                <div class="stat-card"><div class="stat-icon"><i class="fas fa-heartbeat"></i></div><div class="stat-info"><h3 style="color: ${status.data.health === 'healthy' ? '#28a745' : status.data.health === 'degraded' ? '#ffc107' : '#dc3545'}">${status.data.health === 'healthy' ? 'Здоров' : status.data.health === 'degraded' ? 'Деградирован' : 'Критический'}</h3><p>Состояние кластера</p></div></div>
                <div class="stat-card"><div class="stat-icon"><i class="fas fa-server"></i></div><div class="stat-info"><h3>${status.data.active_nodes}/${status.data.total_nodes}</h3><p>Активные узлы</p></div></div>
                <div class="stat-card"><div class="stat-icon"><i class="fas fa-copy"></i></div><div class="stat-info"><h3>${status.data.replication_factor}</h3><p>Фактор репликации</p></div></div>
            </div>
            <div class="data-table"><h3>Узлы кластера</h3></table><thead><tr><th>ID узла</th><th>Адрес</th><th>Статус</th><th>Последний контакт</th></tr></thead><tbody>
                ${nodes.data.map(node => `<tr><td><code>${escapeHtml(node.id)}</code></td><td>${escapeHtml(node.ip)}:${node.port}</td><td><span class="status-badge status-${node.status}">${node.status}</span></td><td>${new Date(node.last_seen * 1000).toLocaleString()}</td></tr>`).join('')}
            </tbody></table></div>
        `;
    } catch (error) {
        DOM.content.innerHTML = '<div class="error-message">Ошибка загрузки информации о кластере</div>';
    }
}

/**
 * Загружает страницу настроек
 */
function loadSettings() {
    DOM.title.textContent = 'Настройки';
    DOM.content.innerHTML = `
        <div class="settings-panel"><h3>Настройки интерфейса</h3>
        <div class="form-group"><label>Тема оформления</label><select class="form-control" id="themeSelect"><option value="dark">Тёмная</option><option value="light">Светлая</option></select></div>
        <button class="btn btn-primary" onclick="saveSettings()">Сохранить настройки</button></div>
    `;
}

/**
 * Сохраняет настройки интерфейса
 */
function saveSettings() {
    const theme = document.getElementById('themeSelect')?.value;
    if (theme) {
        localStorage.setItem('theme', theme);
        showNotification('Настройки сохранены', 'success');
    }
}

// ======================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========================

/**
 * Экранирует HTML-спецсимволы для предотвращения XSS
 * @param {any} str - Входная строка
 * @returns {string} Экранированная строка
 */
function escapeHtml(str) {
    if (!str) return '';
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

/**
 * Отображает всплывающее уведомление
 * @param {string} message - Текст уведомления
 * @param {string} type - Тип уведомления (success, error, warning, info)
 */
function showNotification(message, type = 'info') {
    const container = DOM.notificationContainer || document.getElementById('notificationContainer');
    if (!container) return;
    
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    const icons = { success: '✅', error: '❌', warning: '⚠️', info: 'ℹ️' };
    notification.innerHTML = `${icons[type] || icons.info} ${escapeHtml(message)}`;
    container.appendChild(notification);
    setTimeout(() => {
        notification.style.animation = 'slideOutRight 0.3s ease';
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}

/**
 * Обрабатывает быстрые действия из меню
 * @param {string} action - Идентификатор действия
 */
function handleAction(action) {
    const actions = {
        'create-db': showCreateDatabaseModal,
        'create-collection': showCreateCollectionModal,
        'insert-doc': showInsertDocumentModal,
        'update-doc': () => showUpdateDocumentModal(),
        'acl-create-user': showCreateUserModal,
        'acl-create-role': showCreateRoleModal,
        'tx-start-session': startSession,
        'tx-start': startTransaction,
        'tx-commit': commitTransaction,
        'tx-abort': abortTransaction,
        'index-create': () => {
            if (document.getElementById('indexDbSelect')?.value && document.getElementById('indexCollSelect')?.value) {
                showCreateIndexModal();
            } else {
                showNotification('Сначала выберите БД и коллекцию на странице индексов', 'warning');
            }
        },
        'plugin-upload': showUploadPluginModal,
        'constraint-add-required': () => showConstraintModal('required'),
        'constraint-add-unique': () => showConstraintModal('unique'),
        'constraint-add-min': () => showConstraintModal('min'),
        'constraint-add-max': () => showConstraintModal('max'),
        'constraint-add-enum': () => showConstraintModal('enum'),
        'constraint-add-regex': () => showConstraintModal('regex')
    };
    
    const handler = actions[action];
    if (handler) {
        handler();
    } else {
        showNotification('Неизвестное действие', 'warning');
    }
}

// ======================== МОДАЛЬНЫЕ ОКНА ДЛЯ CRUD ========================

/**
 * Показывает модальное окно создания базы данных
 */
function showCreateDatabaseModal() {
    DOM.modalTitle.textContent = 'Создать базу данных';
    DOM.modalBody.innerHTML = `<div class="form-group"><label>Имя базы данных</label><input type="text" id="dbName" class="form-control" placeholder="my_database"></div>`;
    DOM.modalConfirm.textContent = 'Создать';
    DOM.modal.classList.add('show');
    
    DOM.modalConfirm.onclick = async () => {
        const dbName = document.getElementById('dbName').value;
        if (!dbName) {
            showNotification('Введите имя базы данных', 'error');
            return;
        }
        try {
            const response = await fetch('/api/db/' + dbName, { method: 'POST' });
            if (response.ok) {
                DOM.modal.classList.remove('show');
                showNotification(`База данных "${dbName}" создана`, 'success');
                loadDashboard();
            } else {
                const error = await response.json();
                showNotification(error.error || 'Ошибка создания БД', 'error');
            }
        } catch (error) {
            showNotification('Ошибка подключения', 'error');
        }
    };
}

/**
 * Показывает модальное окно создания коллекции
 */
function showCreateCollectionModal() {
    if (!currentDatabase) {
        showNotification('Сначала выберите базу данных', 'warning');
        return;
    }
    DOM.modalTitle.textContent = 'Создать коллекцию';
    DOM.modalBody.innerHTML = `
        <div class="form-group"><label>База данных</label><input type="text" class="form-control" value="${escapeHtml(currentDatabase)}" disabled></div>
        <div class="form-group"><label>Имя коллекции</label><input type="text" id="collName" class="form-control" placeholder="my_collection"></div>
    `;
    DOM.modalConfirm.textContent = 'Создать';
    DOM.modal.classList.add('show');
    
    DOM.modalConfirm.onclick = async () => {
        const collName = document.getElementById('collName').value;
        if (!collName) {
            showNotification('Введите имя коллекции', 'error');
            return;
        }
        try {
            const response = await fetch(`/api/db/${currentDatabase}/${collName}`, { method: 'POST' });
            if (response.ok) {
                DOM.modal.classList.remove('show');
                showNotification(`Коллекция "${collName}" создана`, 'success');
                viewDatabase(currentDatabase);
            } else {
                const error = await response.json();
                showNotification(error.error || 'Ошибка создания коллекции', 'error');
            }
        } catch (error) {
            showNotification('Ошибка подключения', 'error');
        }
    };
}

/**
 * Показывает модальное окно вставки документа
 */
function showInsertDocumentModal() {
    if (!currentDatabase || !currentCollection) {
        showNotification('Сначала выберите базу данных и коллекцию', 'warning');
        return;
    }
    DOM.modalTitle.textContent = 'Вставить документ';
    DOM.modalBody.innerHTML = `
        <div class="form-group"><label>База данных</label><input type="text" class="form-control" value="${escapeHtml(currentDatabase)}" disabled></div>
        <div class="form-group"><label>Коллекция</label><input type="text" class="form-control" value="${escapeHtml(currentCollection)}" disabled></div>
        <div class="form-group"><label>Данные документа (JSON)</label><textarea id="docData" class="form-control" rows="8" placeholder='{"name": "Example", "value": 123}'></textarea></div>
    `;
    DOM.modalConfirm.textContent = 'Вставить';
    DOM.modal.classList.add('show');
    
    DOM.modalConfirm.onclick = async () => {
        const docData = document.getElementById('docData').value;
        if (!docData) {
            showNotification('Введите данные документа', 'error');
            return;
        }
        try {
            const data = JSON.parse(docData);
            const response = await fetch(`/api/webui/documents/${currentDatabase}/${currentCollection}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });
            const result = await response.json();
            if (result.success) {
                DOM.modal.classList.remove('show');
                showNotification('Документ вставлен', 'success');
                viewCollection(currentDatabase, currentCollection);
            } else {
                showNotification(result.error || 'Ошибка вставки документа', 'error');
            }
        } catch (error) {
            showNotification(error instanceof SyntaxError ? 'Неверный формат JSON' : 'Ошибка подключения', 'error');
        }
    };
}

/**
 * Показывает модальное окно обновления документа
 * @param {string} docId - ID документа
 * @param {Object} currentFields - Текущие поля документа
 */
function showUpdateDocumentModal(docId = '', currentFields = null) {
    if (!currentDatabase || !currentCollection) {
        showNotification('Сначала выберите базу данных и коллекцию', 'warning');
        return;
    }
    DOM.modalTitle.textContent = 'Обновить документ';
    DOM.modalBody.innerHTML = `
        <div class="form-group"><label>База данных</label><input type="text" class="form-control" value="${escapeHtml(currentDatabase)}" disabled></div>
        <div class="form-group"><label>Коллекция</label><input type="text" class="form-control" value="${escapeHtml(currentCollection)}" disabled></div>
        <div class="form-group"><label>ID документа</label><input type="text" id="updateDocId" class="form-control" value="${escapeHtml(docId)}" ${docId ? 'disabled' : ''} placeholder="document_id"></div>
        <div class="form-group"><label>Обновления (JSON)</label><textarea id="updateData" class="form-control" rows="8" placeholder='{"field1": "new value"}'>${escapeHtml(currentFields ? JSON.stringify(currentFields, null, 2) : '')}</textarea></div>
    `;
    DOM.modalConfirm.textContent = 'Обновить';
    DOM.modal.classList.add('show');
    
    DOM.modalConfirm.onclick = async () => {
        const updateDocId = document.getElementById('updateDocId').value;
        const updateData = document.getElementById('updateData').value;
        if (!updateDocId) {
            showNotification('Введите ID документа', 'error');
            return;
        }
        if (!updateData) {
            showNotification('Введите данные для обновления', 'error');
            return;
        }
        try {
            const data = JSON.parse(updateData);
            const response = await fetch(`/api/webui/documents/${currentDatabase}/${currentCollection}?id=${encodeURIComponent(updateDocId)}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });
            const result = await response.json();
            if (result.success) {
                DOM.modal.classList.remove('show');
                showNotification('Документ обновлён', 'success');
                viewCollection(currentDatabase, currentCollection);
            } else {
                showNotification(result.error || 'Ошибка обновления документа', 'error');
            }
        } catch (error) {
            showNotification(error instanceof SyntaxError ? 'Неверный формат JSON' : 'Ошибка подключения', 'error');
        }
    };
}

// ======================== ТРАНЗАКЦИИ ========================

/**
 * Загружает и отображает список активных транзакций
 */
async function loadTransactionList() {
    DOM.title.textContent = 'Активные транзакции';
    DOM.content.innerHTML = '<div class="loading-spinner"><i class="fas fa-spinner fa-pulse"></i><p>Загрузка транзакций...</p></div>';

    try {
        const response = await fetch('/api/webui/transactions');
        const data = await response.json();

        if (data.success) {
            DOM.content.innerHTML = `
                <div style="margin-bottom:16px;display:flex;gap:12px;flex-wrap:wrap;">
                    <button class="btn btn-primary" onclick="startSession()"><i class="fas fa-play"></i> Начать сессию</button>
                    <button class="btn btn-success" onclick="startTransaction()"><i class="fas fa-play-circle"></i> Начать транзакцию</button>
                    <button class="btn btn-success" onclick="commitTransaction()"><i class="fas fa-check-circle"></i> Зафиксировать</button>
                    <button class="btn btn-danger" onclick="abortTransaction()"><i class="fas fa-times-circle"></i> Отменить</button>
                    <button class="btn btn-secondary" onclick="loadTransactionList()"><i class="fas fa-sync-alt"></i> Обновить</button>
                </div>
                <div class="data-table"><h3>Транзакции</h3>
                <table><thead><tr><th>ID</th><th>Статус</th><th>Начало</th><th>Операций</th><th>Действия</th></tr></thead><tbody>
                    ${data.data.map(tx => `<tr>
                        <td><code>${escapeHtml(tx.id)}</code></td>
                        <td><span class="status-badge status-${tx.status}">${escapeHtml(tx.status)}</span></td>
                        <td>${new Date(tx.start_time).toLocaleString()}</td>
                        <td>${tx.operation_count || 0}</td>
                        <td>
                            <button class="btn btn-sm btn-info" onclick="loadTransactionDetails('${escapeHtml(tx.id)}')"><i class="fas fa-info-circle"></i> Детали</button>
                            ${tx.status === 'active' ? `<button class="btn btn-sm btn-success" onclick="commitTransactionById('${escapeHtml(tx.id)}')"><i class="fas fa-check"></i> Commit</button><button class="btn btn-sm btn-danger" onclick="abortTransactionById('${escapeHtml(tx.id)}')"><i class="fas fa-times"></i> Abort</button>` : ''}
                        </td>
                    </tr>`).join('') || '<tr><td colspan="5">Нет активных транзакций</td></tr>'}
                </tbody></table></div>
            `;
        } else {
            DOM.content.innerHTML = '<div class="error-message">Ошибка загрузки транзакций</div>';
        }
    } catch (error) {
        DOM.content.innerHTML = '<div class="error-message">Ошибка подключения</div>';
    }
}

/**
 * Загружает детали транзакции
 * @param {string} txId - ID транзакции
 */
async function loadTransactionDetails(txId) {
    DOM.modalTitle.textContent = `Детали транзакции ${txId}`;
    DOM.modalConfirm.textContent = 'Закрыть';
    DOM.modalBody.innerHTML = '<div class="loading-spinner"><i class="fas fa-spinner fa-pulse"></i><p>Загрузка деталей...</p></div>';
    DOM.modal.classList.add('show');

    const originalConfirmHandler = DOM.modalConfirm.onclick;
    DOM.modalConfirm.onclick = () => {
        DOM.modal.classList.remove('show');
        DOM.modalConfirm.onclick = originalConfirmHandler;
    };

    try {
        const response = await fetch(`/api/webui/transaction/${txId}/details`);
        const data = await response.json();
        if (data.success && data.data) {
            const tx = data.data;
            DOM.modalBody.innerHTML = `<div><strong>ID:</strong> <code>${escapeHtml(tx.id)}</code></div>
                <div><strong>Статус:</strong> <span class="status-badge status-${tx.status}">${escapeHtml(tx.status)}</span></div>
                <div><strong>Время начала:</strong> ${new Date(tx.start_time).toLocaleString()}</div>
                <div><strong>Количество операций:</strong> ${tx.operation_count}</div>
                <hr><h4>Операции</h4>
                ${tx.operations?.length ? `<table class="data-table"><thead><tr><th>Тип</th><th>БД</th><th>Коллекция</th><th>ID документа</th></tr></thead><tbody>${tx.operations.map(op => `<tr><td>${escapeHtml(op.type)}</td><td>${escapeHtml(op.database)}</td><td>${escapeHtml(op.collection)}</td><td><code>${escapeHtml(op.document_id)}</code></td></tr>`).join('')}</tbody></table>` : '<p>Нет операций</p>'}</div>`;
        } else {
            DOM.modalBody.innerHTML = `<div class="error-message">Ошибка загрузки деталей</div>`;
        }
    } catch (error) {
        DOM.modalBody.innerHTML = '<div class="error-message">Ошибка подключения</div>';
    }
}

/**
 * Начинает новую сессию транзакций
 */
async function startSession() {
    try {
        const response = await fetch('/api/webui/transactions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'start_session' })
        });
        const data = await response.json();
        if (data.success) {
            showNotification('Сессия начата', 'success');
            loadTransactionList();
        } else {
            showNotification(data.error || 'Ошибка', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
}

/**
 * Начинает новую транзакцию
 */
async function startTransaction() {
    try {
        const response = await fetch('/api/webui/transactions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'start_transaction' })
        });
        const data = await response.json();
        if (data.success) {
            showNotification('Транзакция начата', 'success');
            loadTransactionList();
        } else {
            showNotification(data.error || 'Ошибка', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
}

/**
 * Фиксирует текущую транзакцию
 */
async function commitTransaction() {
    try {
        const response = await fetch('/api/webui/transaction/commit', { method: 'POST' });
        const data = await response.json();
        if (data.success) {
            showNotification('Транзакция зафиксирована', 'success');
            loadTransactionList();
        } else {
            showNotification(data.error || 'Ошибка', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
}

/**
 * Отменяет текущую транзакцию
 */
async function abortTransaction() {
    try {
        const response = await fetch('/api/webui/transaction/abort', { method: 'POST' });
        const data = await response.json();
        if (data.success) {
            showNotification('Транзакция отменена', 'success');
            loadTransactionList();
        } else {
            showNotification(data.error || 'Ошибка', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
}

/**
 * Фиксирует транзакцию по ID
 * @param {string} txId - ID транзакции
 */
async function commitTransactionById(txId) {
    if (!confirm(`Зафиксировать транзакцию ${txId}?`)) return;
    try {
        const response = await fetch(`/api/webui/transaction/${txId}/commit`, { method: 'POST' });
        const data = await response.json();
        if (data.success) {
            showNotification(`Транзакция ${txId} зафиксирована`, 'success');
            loadTransactionList();
        } else {
            showNotification(data.error || 'Ошибка фиксации', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
}

/**
 * Отменяет транзакцию по ID
 * @param {string} txId - ID транзакции
 */
async function abortTransactionById(txId) {
    if (!confirm(`Отменить транзакцию ${txId}?`)) return;
    try {
        const response = await fetch(`/api/webui/transaction/${txId}/abort`, { method: 'POST' });
        const data = await response.json();
        if (data.success) {
            showNotification(`Транзакция ${txId} отменена`, 'success');
            loadTransactionList();
        } else {
            showNotification(data.error || 'Ошибка отмены', 'error');
        }
    } catch (error) {
        showNotification('Ошибка подключения', 'error');
    }
}

// ======================== ОСТАЛЬНЫЕ ФУНКЦИИ (ЗАГЛУШКИ ДЛЯ ПОЛНОТЫ) ========================

// ACL функции
async function loadACLUsers() { DOM.content.innerHTML = '<div class="info-message">Раздел в разработке</div>'; }
async function loadACLRoles() { DOM.content.innerHTML = '<div class="info-message">Раздел в разработке</div>'; }
async function loadACLPermissions() { DOM.content.innerHTML = '<div class="info-message">Раздел в разработке</div>'; }

// Индексы
async function loadIndexesList() { DOM.content.innerHTML = '<div class="info-message">Раздел в разработке</div>'; }

// Импорт/Экспорт
async function loadExportPage() { DOM.content.innerHTML = '<div class="info-message">Раздел в разработке</div>'; }
async function loadImportPage() { DOM.content.innerHTML = '<div class="info-message">Раздел в разработке</div>'; }

// Ограничения
async function loadConstraintsList() { DOM.content.innerHTML = '<div class="info-message">Раздел в разработке</div>'; }

// Триггеры
async function loadTriggersList() { DOM.content.innerHTML = '<div class="info-message">Раздел в разработке</div>'; }
async function loadTriggerLog() { DOM.content.innerHTML = '<div class="info-message">Раздел в разработке</div>'; }

// Модальные окна для ACL и ограничений
function showCreateUserModal() { showNotification('Функция в разработке', 'info'); }
function showCreateRoleModal() { showNotification('Функция в разработке', 'info'); }
function showCreateIndexModal() { showNotification('Функция в разработке', 'info'); }
function showConstraintModal(type) { showNotification(`Добавление ограничения "${type}" в разработке`, 'info'); }

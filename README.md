<!-- Improved compatibility of К началу link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a id="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->

<!-- PROJECT LOGO -->
<br />
<div align="center">
<!--  <a href="https://github.com/othneildrew/Best-README-Template"> -->
<img src="Logo.png" height=100 alt="Logo.png"></img>
  </a>

  <p align="center">
  <h3> <b>futriix-это легковесная, распределённая wait-free и lock-free дружественная in-memory СУБД, 
реализованная на Go с поддержкой плагинов на языке lua для операционных систем на базе Solaris (ядра Illumos)</b> <br></h3>
    <br />
    <br />
  <!--   <a href="">Сообщить об ошибке</a> 
    &middot;
 <!--    <a href="">Предложение новой функциональности</a> -->
  </p>
</div>

  ## Краткая документация проекта futriix

<!-- TABLE OF CONTENTS -->
<br>
<!-- <details> -->
  <summary><b>Содержание</b></summary></br>
  <ol>
     <li>
    <a href="#о-проекте">О проекте</a>
    <li><a href="#лицензия">Лицензия</a></li>
    <li><a href="#глоссарий">Глоссарий</a></li>
    <li><a href="#системные-требования">Системные требования</a></li>
    <li><a href="#подготовка-и-компиляция">Подготовка и компиляция</a></li>
    <li><a href="#логирование">Логирование</a></li>
    <li><a href="#тестирование">Тестирование</a></li>
    <li><a href="#crud-операции">CRUD операции</a></li>
    <li><a href="#индексы">Индексы</a></li>
    <li><a href="#транзакции">Транзакции</a></li>
    <li><a href="#кластеризация-и-шардинг">Кластеризация и шардинг</a></li>
    <li><a href="#ограничения">Ограничения</a></li>
    <li><a href="#import-export">Import-Export</a></li>
    <li><a href="#lua-плагины">Lua-плагины</a></li>
    <li><a href="acl">ACL</a></li>
    <li><a href="#http-api">HTTP API</a></li>
    <li><a href="#триггеры">Триггеры</a></li>
    <li><a href="#сжатие-данных">Сжатие данных</a></li>
    <li><a href="#графический-интерфейс">Графический интерфейс</a></li>
    <li><a href="#дорожная-карта">Дорожная карта</a></li>
    <li><a href="#контакты">Контакты</a></li>
  </ol>
<!-- </details> -->


## О проекте

> [!CAUTION]
> **ALPHA VERSION**<br><br>**Категорически не использовать в продакшене, так как это экспериментальная версия!!!**  

futriix - это легковесная, распределённая, использующая алгоритмы неблокирующей синхронизации - `wait-free` и `lock-free` in-memory СУБД, реализованная на языке Go с поддержкой плагинов на языке lua использующая алгоритм консенсуса Raft.
Данная субд была разработана, в первую очередь для эксплуатации в операционных системах на базе ядра Illumos и операционной системы Solaris: OpenIndiana Hipster, Oracle Solaris в замкнутых программных средах.
Но также она совместима с популярными Linux-дистрибутивами (Debian,Ubuntu,Fedora), т.е. операционными системами построенными на базе ядра Linux.


<p align="right">(<a href="#readme-top">К началу</a>)</p>

## Лицензия

Проект распространяется под лицензией **`CDDL 1.0`**. Подробнсти в файлах `LICENSE`.
Эта лицензия позволяет вам производить копирование, модификацию, распространение, включение в другие проекты, получение патентных прав, распространение бинарных файлов с доступом к их исходному коду. Она запрещает вам добавление новых ограничений, скрытие изменений, удаление оригинальных уведомлений, несоблюдение условий CDDL 1.0 при перераспределении, неправильное связывание с другими лицензиями.

Все дополнительное программное обеспечение (включая скрипт компиляции проекта `build.sh`) предоставляются "как есть", без гарантий и обязательств со стороны разработчиков. Разработчики не несут ответственности за прямой или косвенный ущерб, вызванный использованием открытого кода Futriix и futriix или технических решений, использующих этот код.

<p align="right">(<a href="#readme-top">К началу</a>)</p>

## Глоссарий

* **База Данных(БД)** - это структурированное, организованное хранилище данных, которое позволяет удобно собирать, хранить, управлять и извлекать информацию.
* **Система Управления Базами Данных(СУБД)** - это программное обеспечение, которое позволяет создавать, управлять и взаимодействовать с базами данных
* **Таппл (Tapple)** - аналог базы данных в РСУБД
* **Слайс (Slice)** - аналог таблицы
* **Кортеж (Tuple)** - аналог записи в таблице
* **Мультимодельная СУБД** - это СУБД, которая объединяет в себе поддержку нескольких моделей данных (реляционной, документной, графовой, ключ-значение и др.) в рамках единого интегрированного ядра.
* **Резидентная СУБД** - это СУБД, которая работает непрерывно в оперативной памяти (RAM).
* **Инстанс** - это запущенный экземляр базы данных.
* **Узел (хост,нода,шард)** - это отдельный сервер (физический или виртуальный), который является частью кластера или распределенной системы и выполняет часть общей работы.
* **Слайс (от англ. "slice"-слой)** - это логический и физически изолированный фрагмент коллекции документов, полученный в результате горизонтального партиционирования (шардирования) и размещенный на определенном узле кластера с целью масштабирования производительности и объема данных.
* **Репликасет** - это группа серверов СУБД, объединенных в отказоустойчивую конфигурацию, где один узел выполняет роль первичного (принимающего операции записи), а один или несколько других - роль вторичных (синхронизирующих свои данные с первичным и обслуживающих чтение), с автоматическим переизбранием первичного узла в случае его сбоя.
* **Временные ряды (time series)** - это это упорядоченная во времени последовательность данных, собранная в регулярные промежутки времени из какого-либо источниика (цены на акции, данные температуры, объёмы продаж и.т.д.).
* **OLTP (Online Transactional Processing-Онлайн обработка транзакций)**- это технология обработки транзакций в режиме реального времени. Её основная задача заключается в обеспечении быстрого и надёжного выполнения операций, которые происходят ежесекундно в бизнесе. Они обеспечивают быстрое выполнение операций вставки, обновления и удаления данных, поддерживая целостность и надежность транзакций.
* **OLAP (Online Analytical Processing - Оперативная аналитическая обработка)** — это технология, которая работает с историческими массивами информации, извлекая из них закономерности и производя анализ больших объемов данных, поддерживает многоразмерные запросы и сложные аналитические операции. Данная технология оптимизирована для выполнения сложных запросов и предоставления сводной информации для принятия управленческих решений.
* **HTAP (Hybrid Transactional and Analytical Processing - Гибридная транзакционно-аналитическая обработка)**- это технология, которая заключаются в эффективном совмещении операционных и аналитических запросов, т.е. классов OLTP и OLAP.
* **Кластер** - это группа компьютеров, объединённых высокоскоростными каналами связи для решения сложных вычислительных задач и представляющая с точки зрения пользователя группу серверов, объединенных для работы как единая система.
* **WUI (от англ. Web-User-Interface "веб интерфейс пользователя")** - это термин проекта futriix, означающий веб-интерфейс (интерфейс работающий в веб-браузере)
* **workflow (англ. workflow — «поток работы»)** — это принцип организации рабочих процессов, в соответствии с которым повторяющиеся задачи представлены как последовательность стандартных шагов.
* **wait-free (дословно с англ. wait-free — «свободный от ожидания»)**-класс неблокирующих алгоритмов, в которых каждая операция должна завершаться за конечное число шагов независимо от активности других потоков.
* **ЗПС (Замкнутая Программная Среда)** - Это локальная сеть предприятия или организации как правило без доступа к сети "Интернет", своего рода "фильтр" содержащий в себе перечень программного обеспечения (ПО), которому разрешено работать на компьютере, в то время как ПО, которого нет в этом списке, будет запрещено к исполнению.
* Команды, выполняемые с привилегиями суперпользователя (root), отмечены символом приглашения **«#»**
* Команды, выполняемые с правами обычного пользователя(user), отмечены символом приглашения **«$»**

<p align="right">(<a href="#readme-top">К началу</a>)</p>


## Системные требования

> [!WARNING]
> - Процессор: Intel или AMD
> - Оперативная память: 4ГБ (Для Linux) 8ГБ (Для Illumos sytems) 
> - Только Unix-подобная ОС (Solaris, OpenIndiana, Linux)
> - Утилиты: curl или wget, zip и unzip
> - Go 1.25.6 или выше

> [!CAUTION]
> **Важно: Windows и MacOS X не поддерживаются!**

<p align="right">(<a href="#readme-top">К началу</a>)</p>


## Подготовка и компиляция

1. Клонируйте репозиторий:

```bash
$ git clone https://github.com/futriix/db.git
$ cd futriix
```

> [!IMPORTANT]
> **Важно: Шаги с 1.1 по 1.3(включительно) необходимы тогда и только тогда, когда вы используете ЗПС**
> **Данный шаг позволит скачать вам все зависимости в архиве "vendor.zip"**


**1.1 Скачайте локальные зависимости в архиве "vendor.zip"**

```bash
$ wget https://futriix.ru:8083/fm/?r=/download&path=L3dlYi9mdXRyaWl4LnJ1L3B1YmxpY19odG1sL2Rvd25sb2Fkcy92ZW5kb3Iuemlw
```

**1.2 Поместите архив `vendor.zip` в один каталог с проектом и распакуйте его командой:**

```bash
$ unzip vendor.zip
```
**1.3 Скомпилируйте проект с помощью специального скрипта для ЗПС `build_vendor.sh`**

```bash
$ ./build_vendor.sh
```

2. Скомпилируйте и запустите:

```bash
# Стандартная сборка для ОС на базе Linux
$ ./build.sh

# Сборка для операционных систем на базе Illumos
$ cd scripts/
$ ./build_illumos.sh

# Показать справку
$./build.sh --help

$ ./futriix
```
<p align="right">(<a href="#readme-top">К началу</a>)</p>


### Логирование

В субд **"futriix"** используется два журнала для ведение логов: `"futriix.log"`-основной журнал, в котором ведутся логи при работе в субд через терминал, и `"webui.log"`-основной журнал, в котором ведутся логи при работе в субд через веб-интрефейс.

**futriix.log** — основной системный журнал, фиксирующий все события жизненного цикла СУБД: запуск/остановку сервера, инициализацию компонентов (транзакции, Raft-координатор, ACL), состояние кластера и критические ошибки выполнения запросов.

**webui.log** — специализированный журнал веб-интерфейса, регистрирующий только действия пользователей через Web UI: успешные и неудачные попытки входа, управление аватарами, создание/удаление триггеров и индексов, а также операции импорта/экспорта данных.

Оба журнала используют структурированный JSON-формат (для webui.log) и текстовый формат с временными метками (для futriis.log), что обеспечивает удобный парсинг и интеграцию с системами мониторинга.

Журналы автоматически ротируются и ограничены по размеру (по умолчанию 10000 записей для webui.log), предотвращая неконтролируемый рост дискового пространства при длительной работе сервера.

<p align="right">(<a href="#readme-top">К началу</a>)</p>


### Тестирование

Разработанный набор из пяти тестов (регрессионный, smoke-тест, функциональный, интеграционный и нагрузочный) на языке Lua обеспечивает комплексную проверку всех ключевых компонентов СУБД: CRUD-операций, индексов, транзакций, ограничений целостности, ACL, триггеров, MVCC-версионирования, а также взаимодействия API с хранилищем и кластерной координации. 

Регрессионный тест гарантирует, что изменения кода не нарушили существующую функциональность, smoke-тест выполняет быструю проверку доступности и базовой работоспособности системы.
Функциональный и интеграционный тесты проверяют корректность реализации бизнес-требований и взаимодействие между компонентами, а нагрузочный тест оценивает производительность (латентность, пропускную способность) под различными сценариями использования.

Команды для запуска тестов приведены ниже:

> [!IMPORTANT]  
> 1. Перед запуском тестов убедитесь, что СУБД запущена и HTTP API доступен на порту 8080
> 2. Load test может занять несколько минут при больших объёмах данных


```bash
# Установка зависимостей для Lua-тестирования
sudo apt install lua5.3 lua-socket

# Запуск регрессионного теста
lua test_regression.lua

# Запуск smoke-теста
lua test_smoke.lua

# Запуск функционального теста
lua test_functional.lua

# Запуск интеграционного теста
lua test_integration.lua

# Запуск нагрузочного теста
lua test_performance.lua

# Запуск всех тестов последовательно
for test in test_regression.lua test_smoke.lua test_functional.lua test_integration.lua test_performance.lua; do
    echo "=== Running $test ==="
    lua "$test"
    echo ""
done
```
<p align="right">(<a href="#readme-top">К началу</a>)</p>
    
### Примеры команд субд

```sh
# Создание новой базы данных
futriix:~> create database company
✓ Database 'company' created

futriix:~> create database shop
✓ Database 'shop' created

# Переключение на базу данных
futriix:~> use company
✓ Switched to database 'company'

futriix:~> use shop
✓ Switched to database 'shop'

# Просмотр всех баз данных
futriix:~> show databases
Databases:
   company
 * shop
   test

# Удаление базы данных
futriix:~> drop database test
✓ Database 'test' dropped
```

<p align="right">(<a href="#readme-top">К началу</a>)</p>

## CRUD операции

```sh
# Создание новой базы данных
futriix:~> create database company
✓ Database 'company' created

futriix:~> create database shop
✓ Database 'shop' created

# Переключение на базу данных
futriix:~> use company
✓ Switched to database 'company'

futriix:~> use shop
✓ Switched to database 'shop'

# Просмотр всех баз данных
futriix:~> show databases
Databases:
   company
 * shop
   test

# Удаление базы данных
futriix:~> drop database test
✓ Database 'test' dropped
```
```sh
# Создание коллекции
futriix:~> use company
✓ Switched to database 'company'

futriix:~> create collection employees
✓ Collection 'employees' created in database 'company'

futriix:~> create collection departments
✓ Collection 'departments' created in database 'company'

futriix:~> create collection projects
✓ Collection 'projects' created in database 'company'

# Просмотр всех коллекций
futriix:~> show collections
Collections in database 'company':
  - employees
  - departments
  - projects

# Удаление коллекции
futriix:~> drop collection projects
✓ Collection 'projects' dropped from database 'company'

```

```sh
# Вставка документа (простой формат key=value)
futriix:~> insert employees name=John Doe,position=Developer,age=30,department=IT
✓ Document inserted with ID: 550e8400-e29b-41d4-a716-446655440000

futriix:~> insert employees name=Jane Smith,position=Manager,age=35,department=HR
✓ Document inserted with ID: 550e8400-e29b-41d4-a716-446655440001

futriix:~> insert employees name=Bob Johnson,position=Designer,age=28,department=Design
✓ Document inserted with ID: 550e8400-e29b-41d4-a716-446655440002

# Поиск документа по ID
futriix:~> find employees 550e8400-e29b-41d4-a716-446655440000
Document found:
{
  "name": "John Doe",
  "position": "Developer",
  "age": 30,
  "department": "IT"
}

# Поиск по индексу
futriix:~> findbyindex employees name_idx "John Doe"
Found 1 document(s):
  [1] ID: 550e8400-e29b-41d4-a716-446655440000
{
  "name": "John Doe",
  "position": "Developer",
  "age": 30,
  "department": "IT"
}

# Обновление документа
futriix:~> update employees 550e8400-e29b-41d4-a716-446655440000 age=31,position=Senior Developer
✓ Document '550e8400-e29b-41d4-a716-446655440000' updated

# Подсчёт количества документов
futriix:~> count employees
Collection 'employees' has 3 document(s)

# Удаление документа
futriix:~> delete employees 550e8400-e29b-41d4-a716-446655440002
✓ Document '550e8400-e29b-41d4-a716-446655440002' deleted

futriix:~> count employees
Collection 'employees' has 2 document(s)

```
<p align="right">(<a href="#readme-top">К началу</a>)</p>


## Индексы

```sh
# Создание обычного индекса
futriix:~> create index employees name_idx name
✓ Index 'name_idx' created on collection 'employees'

# Создание уникального индекса
futriix:~> create index employees email_idx email unique
✓ Index 'email_idx' created on collection 'employees'

# Создание составного индекса
futriix:~> create index employees dept_age_idx department,age
✓ Index 'dept_age_idx' created on collection 'employees'

# Просмотр всех индексов
futriix:~> show indexes employees
Indexes on collection 'employees':
  - _id_
  - name_idx
  - email_idx
  - dept_age_idx

# Удаление индекса
futriix:~> drop index employees dept_age_idx
✓ Index 'dept_age_idx' dropped from collection 'employees'
```

<p align="right">(<a href="#readme-top">К началу</a>)</p>

## Транзакции

```sh
# Начало сессии
futriix:~> db.startSession()
✓ Session started: session_12345

# Начало транзакции в рамках сессии
futriix:~> session.startTransaction()
✓ Transaction started: TX_67890

# Выполнение операций в транзакции
futriix:~> insert employees name=New User,position=Trainee,age=22
✓ Document inserted with ID: 550e8400-e29b-41d4-a716-446655440005

futriix:~> update employees 550e8400-e29b-41d4-a716-446655440005 status=active
✓ Document '550e8400-e29b-41d4-a716-446655440005' updated

# Подтверждение транзакции
futriix:~> session.commitTransaction()
✓ Transaction committed successfully

# Откат транзакции (при ошибке)
futriix:~> session.startTransaction()
✓ Transaction started: TX_67891

futriix:~> insert employees name=Test User,position=Test,age=25
✓ Document inserted with ID: 550e8400-e29b-41d4-a716-446655440006

futriix:~> session.abortTransaction()
✓ Transaction aborted, changes rolled back
```

<p align="right">(<a href="#readme-top">К началу</a>)</p>


## Кластеризация и шардинг

```sh
# Просмотр статуса кластера
futriix:~> status
=== Cluster Status ===
✓ Role: LEADER
  Cluster Name: production
  Node: 192.168.1.100:8080
  Raft Port: 7000

# В режиме follower
futriix:~> status
=== Cluster Status ===
⚠ Role: FOLLOWER
  Cluster Name: production
  Node: 192.168.1.101:8080
  Raft Port: 7000

# Просмотр всех узлов кластера
futriix:~> nodes
=== Cluster Nodes ===
  * 192.168.1.100:8080
    192.168.1.101:8080
    192.168.1.102:8080

```
<p align="right">(<a href="#readme-top">К началу</a>)</p>

## Ограничения

```sh
# Добавление обязательного поля
futriix:~> add required employees email
✓ Required field 'email' added to collection 'employees'

# Добавление ограничения уникальности
futriix:~> add unique employees phone
✓ Unique constraint added for field 'phone' on collection 'employees'

# Добавление минимального значения
futriix:~> add min employees age 18
✓ Min constraint added for field 'age' on collection 'employees' (min: 18.00)

# Добавление максимального значения
futriix:~> add max employees age 65
✓ Max constraint added for field 'age' on collection 'employees' (max: 65.00)

# Добавление enum-ограничения (допустимые значения)
futriix:~> add enum employees status active,inactive,on_leave
✓ Enum constraint added for field 'status' on collection 'employees' (allowed: [active inactive on_leave])
```
<p align="right">(<a href="#readme-top">К началу</a>)</p>


## Import-Export

```sh
# Экспорт базы данных в файл MessagePack
futriix:~> export "company" "company_backup.msgpack"
✓ Database 'company' exported to company_backup.msgpack

# Экспорт с автоматическим добавлением расширения
futriix:~> export "shop" "shop_backup"
✓ Database 'shop' exported to shop_backup.msgpack

# Импорт базы данных из файла
futriix:~> import "company" "company_backup.msgpack"
Importing data from company_backup.msgpack to database 'company'...
✓ Database 'company' imported successfully from company_backup.msgpack
  Collections imported: 2
  Documents imported: 150
  Documents skipped (already exist): 0
  Documents failed: 0

# Импорт в новую базу данных
futriix:~> import "company_restore" "company_backup.msgpack"
Created database 'company_restore'
✓ Database 'company_restore' imported successfully from company_backup.msgpack
  Collections imported: 2
  Documents imported: 150
```

<p align="right">(<a href="#readme-top">К началу</a>)</p>

## Lua-плагины

```sh
# Просмотр информации о системе плагинов
futriix:~> plugin status
=== Plugin System Status ===
  Enabled: true
  Plugins Directory: ./plugins
  Loaded Plugins: 3
  Total Executions: 125

# Список загруженных плагинов
futriix:~> plugin list
=== Loaded Plugins ===
  validation (v1.0.0) by admin - Document validation rules
     Status: RUNNING
  audit (v2.1.0) by security - Audit trail logger
     Status: RUNNING
  notify (v1.2.0) by devops - Email and webhook notifications
     Status: RUNNING

# Загрузка плагина из файла
futriix:~> plugin load email_notifier ./plugins/email_notifier.lua
✓ Plugin 'email_notifier' loaded successfully
  Version: 1.0.0
  Author: admin
  Description: Send email notifications on database events

# Запуск/остановка плагина
futriix:~> plugin start email_notifier
✓ Plugin 'email_notifier' started

futriix:~> plugin stop email_notifier
✓ Plugin 'email_notifier' stopped

# Выгрузка плагина
futriix:~> plugin unload email_notifier
✓ Plugin 'email_notifier' unloaded
```

**Пример плагина валидации документов**

**В директорию `plugins` добавляем файл `validation.lua`, следдующего содержания:

```sh
-- Метаданные плагина
version = "1.0.0"
author = "admin"
description = "Document validation rules for employees collection"

-- Функция инициализации
function on_load()
    plugin_log("info", "Validation plugin loaded")
    return true
end

-- Функция запуска
function on_start()
    plugin_log("info", "Validation plugin started")
    return true
end

-- Функция остановки
function on_stop()
    plugin_log("info", "Validation plugin stopped")
    return true
end

-- Функция выгрузки
function on_unload()
    plugin_log("info", "Validation plugin unloaded")
    return true
end

-- Обработчик событий
function on_event(event)
    plugin_log("debug", "Received event: " .. event.type)
    
    if event.type == "BEFORE_INSERT" then
        return validate_document(event.data)
    end
    
    return true
end

-- Функция валидации документа
function validate_document(doc)
    -- Проверка обязательных полей
    if doc.name == nil or doc.name == "" then
        plugin_log("error", "Document missing required field: name")
        return false, "Field 'name' is required"
    end
    
    -- Проверка возраста
    if doc.age ~= nil then
        if doc.age < 18 then
            plugin_log("warn", "Age validation failed: " .. doc.age)
            return false, "Employee must be at least 18 years old"
        end
        if doc.age > 65 then
            plugin_log("warn", "Age validation failed: " .. doc.age)
            return false, "Employee cannot be older than 65 years"
        end
    end
    
    -- Проверка email
    if doc.email ~= nil then
        if string.match(doc.email, "^[%w._-]+@[%w._-]+%.[%w]+$") == nil then
            plugin_log("error", "Invalid email format: " .. doc.email)
            return false, "Invalid email format"
        end
    end
    
    -- Проверка зарплаты
    if doc.salary ~= nil then
        if doc.salary < 30000 then
            plugin_log("warn", "Salary below minimum: " .. doc.salary)
            return false, "Salary must be at least 30000"
        end
    end
    
    plugin_log("info", "Document validation passed for: " .. doc.name)
    return true
end

-- Пользовательская функция для массовой валидации
function validate_collection(collection_name)
    local coll = get_collection("company", collection_name)
    if coll == nil then
        plugin_log("error", "Collection not found: " .. collection_name)
        return 0
    end
    
    -- Здесь можно реализовать массовую валидацию
    plugin_log("info", "Validating collection: " .. collection_name)
    return 0
end
```

**ИСпользование плагина валидации документов**

```sh
# Создание базы данных и коллекции
futriix:~> create database company
✓ Database 'company' created

futriix:~> use company
✓ Switched to database 'company'

futriix:~> create collection employees
✓ Collection 'employees' created in database 'company'

# Загрузка и запуск плагина валидации
futriix:~> plugin load validation ./plugins/validation.lua
✓ Plugin 'validation' loaded successfully
  Version: 1.0.0
  Author: admin
  Description: Document validation rules for employees collection

futriix:~> plugin start validation
✓ Plugin 'validation' started

# Вставка валидного документа
futriix:~> insert employees name=John Doe,age=25,email=john@company.com,salary=45000
✓ Document inserted with ID: emp_001

# Вставка невалидного документа (возраст < 18)
futriix:~> insert employees name=Jane Smith,age=16,email=jane@company.com,salary=20000
Error: Employee must be at least 18 years old

# Вставка невалидного документа (некорректный email)
futriix:~> insert employees name=Bob Johnson,age=30,email=invalid-email,salary=50000
Error: Invalid email format

# Выполнение пользовательской функции плагина
futriix:~> plugin call validation validate_collection employees
✓ Function returned: 0
```

**Управление плагинами через HTTP API**

```sh
# Получение списка плагинов через API
curl -X GET "http://localhost:8080/api/plugin/list" \
  -H "X-Session-ID: abc123"

# Загрузка плагина через API
curl -X POST http://localhost:8080/api/plugin/load \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: abc123" \
  -d '{"name":"validation","path":"./plugins/validation.lua"}'

# Запуск плагина через API
curl -X POST "http://localhost:8080/api/plugin/start/validation" \
  -H "X-Session-ID: abc123"

# Выполнение функции плагина через API
curl -X POST http://localhost:8080/api/plugin/call \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: abc123" \
  -d '{"plugin":"validation","function":"validate_collection","args":["employees"]}'
```

<p align="right">(<a href="#readme-top">К началу</a>)</p>

## ACL

```sh
# Вход в систему
futriix:~> acl login admin admin
✓ Logged in as 'admin' with role 'admin'

# Выход из системы
futriix:~> acl logout
✓ Logged out

# Назначение прав доступа (после входа как admin)
futriix:~> acl login admin admin
✓ Logged in as 'admin' with role 'admin'

futriix:~> use company
✓ Switched to database 'company'

# Назначение прав на чтение
futriix:~> acl grant employees reader r
✓ Permissions 'r' granted to role 'reader' on collection 'employees'

# Назначение прав на чтение и запись
futriix:~> acl grant employees editor rw
✓ Permissions 'rw' granted to role 'editor' on collection 'employees'

# Назначение полных прав (администратор коллекции)
futriix:~> acl grant employees admin rwda
✓ Permissions 'rwda' granted to role 'admin' on collection 'employees'

# Пример использования разных прав:
# r  - read (чтение)
# w  - write (запись)
# d  - delete (удаление)
# a  - admin (администратор)

```
<p align="right">(<a href="#readme-top">К началу</a>)</p>

## HTTP API

```sh
# Аутентификация
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}'
# Response: {"success":true,"data":{"session_id":"abc123"}}

# Вставка документа
curl -X POST http://localhost:8080/api/db/company/employees \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: abc123" \
  -d '{"name":"API User","position":"Integrator","age":28}'
# Response: {"success":true,"data":{"status":"inserted"}}

# Получение документа по ID
curl -X GET "http://localhost:8080/api/db/company/employees/550e8400-e29b-41d4-a716-446655440000" \
  -H "X-Session-ID: abc123"
# Response: {"success":true,"data":{"_id":"550e8400-...","fields":{...}}}

# Получение всех документов с пагинацией
curl -X GET "http://localhost:8080/api/db/company/employees?limit=10&offset=0" \
  -H "X-Session-ID: abc123"

# Поиск по индексу
curl -X GET "http://localhost:8080/api/db/company/employees?index=name_idx&value=John%20Doe" \
  -H "X-Session-ID: abc123"

# Обновление документа
curl -X PUT http://localhost:8080/api/db/company/employees/550e8400-e29b-41d4-a716-446655440000 \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: abc123" \
  -d '{"age":31,"position":"Senior Developer"}'
# Response: {"success":true,"data":{"status":"updated"}}

# Удаление документа
curl -X DELETE "http://localhost:8080/api/db/company/employees/550e8400-e29b-41d4-a716-446655440000" \
  -H "X-Session-ID: abc123"
# Response: {"success":true,"data":{"status":"deleted"}}

# Создание индекса через API
curl -X POST http://localhost:8080/api/index/company/employees/create \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: abc123" \
  -d '{"name":"email_idx","fields":["email"],"unique":true}'

# Просмотр индексов
curl -X GET "http://localhost:8080/api/index/company/employees/list" \
  -H "X-Session-ID: abc123"

# Статус кластера через API
curl -X GET "http://localhost:8080/api/cluster/status" \
  -H "X-Session-ID: abc123"

# Создание пользователя через API
curl -X POST http://localhost:8080/api/acl/user/newuser \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: abc123" \
  -d '{"password":"secret","roles":["reader"]}'

# Назначение прав через API
curl -X POST "http://localhost:8080/api/acl/grant/reader/rw" \
  -H "X-Session-ID: abc123"

# Создание триггера через API
curl -X POST http://localhost:8080/api/trigger/company/employees/create \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: abc123" \
  -d '{"name":"audit","event":"AFTER_INSERT","action":"log"}'
```

<p align="right">(<a href="#readme-top">К началу</a>)</p>


## Триггеры

```sh
# Создание триггера для логирования вставок
futriix:~> create trigger employees audit_log AFTER_INSERT log
✓ Trigger 'audit_log' created on collection 'employees' for event AFTER_INSERT

# Создание триггера с автоматической установкой timestamp
futriix:~> create trigger employees set_timestamp BEFORE_INSERT modify --set updated_at $$NOW
✓ Trigger 'set_timestamp' created on collection 'employees' for event BEFORE_INSERT

# Создание триггера с условием (запрет удаления активных пользователей)
futriix:~> create trigger employees protect_active BEFORE_DELETE abort --condition status eq active
✓ Trigger 'protect_active' created on collection 'employees' for event BEFORE_DELETE

# Создание триггера с обновлением аудита
futriix:~> create trigger employees audit BEFORE_UPDATE modify --set modified_by $$USER --set modified_at $$NOW
✓ Trigger 'audit' created on collection 'employees' for event BEFORE_UPDATE

# Просмотр всех триггеров коллекции
futriix:~> show triggers employees
=== Triggers on collection 'employees': ===
  audit_log (AFTER_INSERT) - enabled [log]
      Operations:
        - log:  = 
  set_timestamp (BEFORE_INSERT) - enabled [modify]
      Operations:
        - set: updated_at = $$NOW
  protect_active (BEFORE_DELETE) - enabled [abort]
      Condition: status eq active
  audit (BEFORE_UPDATE) - enabled [modify]
      Operations:
        - set: modified_by = $$USER
        - set: modified_at = $$NOW

# Включение/отключение триггера
futriix:~> disable trigger employees BEFORE_INSERT set_timestamp
✓ Trigger 'set_timestamp' disabled

futriix:~> enable trigger employees BEFORE_INSERT set_timestamp
✓ Trigger 'set_timestamp' enabled

# Просмотр лога выполнения триггеров
futriix:~> trigger log
=== Trigger Execution Log ===
[1] 2026-04-12 10:30:45 - Trigger: audit_log, Event: AFTER_INSERT, Collection: employees, Document: 550e8400-...
[2] 2026-04-12 10:31:20 - Trigger: protect_active, Event: BEFORE_DELETE, Collection: employees, Document: 550e8400-...

# Удаление триггера
futriix:~> drop trigger employees BEFORE_INSERT set_timestamp
✓ Trigger 'set_timestamp' dropped from collection 'employees'
```
<p align="right">(<a href="#readme-top">К началу</a>)</p>


## Сжатие данных

Сжатие данных в СУБД futriix предназначено для уменьшения объёма хранимых документов в оперативной памяти и на диске, что позволяет эффективнее использовать доступные ресурсы при работе с большими объёмами информации. 

Алгоритм Brotli, разработанный компанией Google, обеспечивает сжатие с коэффициентом, на 20–26% лучшим по сравнению с классическим Gzip при сопоставимой скорости распаковки, что делает его оптимальным выбором для систем с интенсивными операциями чтения.

Основные преимущества Brotli включают: использование предопределённого словаря часто встречающихся последовательностей байт, адаптивное кодирование с переменной длиной кода, поддержку 11 уровней сжатия (от быстрого до максимально плотного) и высокую скорость распаковки, критически важную для быстрого доступа к документам. В futriix сжатие применяется автоматически при превышении порогового размера документа (настраивается через compression.MinSize), при этом каждый документ хранит флаг Compressed и оригинальный размер для последующего контроля эффективности.

```sh
# Просмотр конфигурации сжатия
futriix:~> compression config
=== Compression Configuration ===
  Enabled:    true
  Algorithm:  snappy
  Level:      3
  Min Size:   1 KB

Available Algorithms:
  snappy  - Fast compression/decompression, good balance (default)
  lz4     - Extremely fast, lower compression ratio
  zstd    - High compression ratio, slower

# Просмотр статистики сжатия
futriix:~> compression stats
=== Compression Statistics ===
  Total Documents:      1250
  Compressed Documents: 890
  Compression Rate:     71.20%
  Size Reduction:       45.30%
  Original Size:        15.2 MB
  Compressed Size:      8.3 MB
  Algorithm:            snappy
  Compression Level:    3
  Min Size Threshold:   1 KB

# Ручное сжатие коллекции
futriix:~> compress collection employees
Compressing collection 'employees'...
✓ Compressed 45 documents in collection 'employees'

# Просмотр информации о сжатии документа
futriix:~> doc compression employees 550e8400-e29b-41d4-a716-446655440000
=== Compression Info for Document: 550e8400-e29b-41d4-a716-446655440000 ===
  Compressed:     true
  Ratio:          35.20%
  Original Size:  2.5 KB
  Current Size:   1.6 KB
```

<p align="right">(<a href="#readme-top">К началу</a>)</p>


## Графический интерфейс

В субд futriix для упрощения администрирования реализован **WUI (Web User Inerface) Веб-интерфейс**, с помощью которого через веб-браузер можно управлять субд, быстро просто и удобно. На фото ниже, приведён пример загруженного веб-интерфейса субд по умолчанию. 

<img src="wui.png" height=400 weight=400 alt="wui.png"></img>



<p align="right">(<a href="#readme-top">К началу</a>)</p>


## Дорожная карта

- [x] Реализовать поддержку хранимых процедур
- [x] Реализовать поддержку триггеров (обратных вызовов)
- [x] Реализовать поддержку многопоточности
- [x] Реализовать неблокирующие чтение/запись
- [x] Реализовать неблокирующие транзакции
- [x] Реализовать constraints (Ограничения)
- [x] Реализовать мульти-мастер асинхронную репликацию через файл конфигурации
- [x] Реализовать логирование
- [x] Реализовать поддержку синхронной мастер-мастер репликации
- [x] Реализовать базовую поддержку протокола Raft
- [x] Реализовать базовую поддержку WebUI
- [x] Реализовать поддержку первичных индексов
- [x] Реализовать поддержку протокола MessagePack
- [x] Добавить механизм сторонних модулей на языке lua, расширяющих базовый функционал сервера
- [x] Реализовать поддержку HTTP-restfull API
- [x] Реализовать сжатия данных в субд
- [x] Реализовать импорт и экспорт дампа субд в формате "MessagePack"
- [x] Исправить ошибки записи журнала логов (в журнал лога кроме текущего времени добавить текущий год)
- [x] В веб-интерфейсе, слева от надписи "admin" в левой нижней части экрана, реализована возможность добавлять фото (маленького размера) 
- [x] В веб-интерфейсе "вшитые в исходный код" логин и пароль (admin; admin)-удалены
- [x] В веб-интерфейсе,  реализована возможность смены логина и пароля для авторизации в нём, при этом логин и пароль (по умолчанию admin; admin) храни в скрытом файле ".credentials", расположенном в каталоге "futriix"
- [x] В файле "/internal/compression/compression.go" заменена библиотека "lz4" на альтернативную, которая имеет стандартное версионирование на "GitHub"
- [x] В веб-интерфейс добавлена возможность читать файл логов "futriix.log", в котором отображаются все  операции, выполненные в веб-интерфейсе за сеанс (например, создана база данных, или удалена коллекция), включая те операции, которые не были выполнены в виду какой-либо ошибки
- [x] В веб-интерфейсе, добавлена возможность добавления нового пользователя администратора, данные которого (логин и пароль) будут хранится в скрытом файле ".credentionals", расположенном в каталоге "futriix". 
- [x] В веб-интерфейсе, добавлена возможность управлять плагинами (включать, отключать)
- [x] Скрипты сборки "build.sh" и "vendor_build.sh" переписаны таким образом, чтобы проект не зависел от компилятора "gcc", т.е. напиши реализацию так чтобы его не нужно было устанавливать отдельно в операционной системе "OpenIndiana Hipster"
- [x] Библиотека  "raft-boltdb" заменена на встроенное файловое хранилище
- [x] Библиотека компрессии "lz4" заменена на "Brotli"
- [ ] Реализовать полноценный графический веб-интерфейс для управления кластером
- [ ] Реализовать автоматический шардинг с консистентным хэшированием
- [ ] Реализовать полноценную поддержку алгоритма Raft (с автоматическим перевывыбором лидера, с доменом отказа)
- [ ] Реализовать полноценную поддержку кластеризации (с обработкой состояний "split-brain", автоматической ребалансировкой кластера)
- [ ] Интегрировать интеллектуального помощник FutBot в веб-интерфейс
- [ ] Интеграцию с мониторинговыми системами (Prometheus, Grafana)
- [ ] Реализовать полноценную систему бекапирования с возможностью определения корректности созданного бекапа и кроссдацентровых решений по автоматическому копироваю бекапа в другой дацентр
- [ ] Реализовать полноценную систему авторизации на основе RBAC-модели
- [ ] Реализовать коннекторы к современным языкам программирования (C, C++, Java, Python, Go)
- [ ] Реализовать утилиту тестирования сервера на количество запросов на чтение/запись



См. [Открытые проблемы](https://source.futriix.ru/gvsafronov/futriixw/issues) полный список предлагаемых функций (и известных проблем).

<p align="right">(<a href="#readme-top">К началу</a>)</p>


## Контакты

Григорий Сафронов - [E-mail](gvsafronov@yandex.ru) 

<p align="right">(<a href="#readme-top">К началу</a>)</p>

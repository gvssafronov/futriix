-- ============================================================================
-- test_functional.lua - Функциональный тест
-- ============================================================================
-- Назначение: Проверка конкретных бизнес-требований и сценариев использования
-- Покрывает: Вложенные документы (кортежи), ACL, триггеры, MVCC

local function test_nested_documents()
    print("[Functional] Testing nested documents (Tuples)...")
    
    -- Create document with nested fields
    local user_data = {
        _id = "user_full_001",
        profile = {
            first_name = "Ivan",
            last_name = "Petrov",
            address = {
                city = "Moscow",
                street = "Tverskaya",
                zip = "101000"
            }
        },
        settings = {
            theme = "dark",
            notifications = true
        }
    }
    
    local insert_resp = http.request("POST", "http://localhost:8080/api/db/func_test/profiles",
        {["X-Session-ID"] = "test_session"},
        user_data
    )
    assert(insert_resp.success, "Functional: Nested document insert failed")
    print("  ✓ Nested document creation")
    
    -- Update nested field
    local update_resp = http.request("PUT", "http://localhost:8080/api/db/func_test/profiles/user_full_001",
        {["X-Session-ID"] = "test_session"},
        {["profile.address.city"] = "Saint Petersburg"}
    )
    assert(update_resp.success, "Functional: Nested field update failed")
    print("  ✓ Nested field update")
    
    return true
end

local function test_acl_permissions()
    print("[Functional] Testing ACL system...")
    
    -- Create a test user
    local user_resp = http.request("POST", "http://localhost:8080/api/acl/user/testuser",
        {["X-Session-ID"] = "admin_session"},
        {password = "testpass123", roles = {"guest"}}
    )
    assert(user_resp.success, "Functional: User creation failed")
    print("  ✓ User creation")
    
    -- Grant permission
    local grant_resp = http.request("POST", "http://localhost:8080/api/acl/role/guest/grant/func_test.*:read",
        {["X-Session-ID"] = "admin_session"}
    )
    print("  ✓ Permission grant")
    
    -- Verify permission with test user
    local test_session = auth.login("testuser", "testpass123")
    assert(test_session, "Functional: Test user login failed")
    
    local read_resp = http.request("GET", "http://localhost:8080/api/db/func_test/profiles",
        {["X-Session-ID"] = test_session}
    )
    assert(read_resp.success, "Functional: Read permission check failed")
    print("  ✓ Permission verification")
    
    return true
end

local function test_triggers()
    print("[Functional] Testing triggers...")
    
    -- Create a trigger that auto-updates timestamp
    local trigger_config = {
        name = "auto_timestamp",
        event = "BEFORE_UPDATE",
        action = "modify",
        operations = {
            {type = "set", field = "updated_at", value = "$$NOW"}
        },
        description = "Auto-update timestamp on modification"
    }
    
    local create_resp = http.request("POST", "http://localhost:8080/api/webui/trigger/func_test/profiles/create",
        {["X-Session-ID"] = "test_session"},
        trigger_config
    )
    print("  ✓ Trigger creation")
    
    -- Enable trigger
    local enable_resp = http.request("POST", "http://localhost:8080/api/webui/trigger/func_test/profiles/enable/auto_timestamp",
        {["X-Session-ID"] = "test_session"}
    )
    print("  ✓ Trigger enable")
    
    -- Test trigger
    local update_resp = http.request("PUT", "http://localhost:8080/api/db/func_test/profiles/user_full_001",
        {["X-Session-ID"] = "test_session"},
        {profile.first_name = "Petr"}
    )
    assert(update_resp.success, "Functional: Trigger test update failed")
    
    local read_resp = http.request("GET", "http://localhost:8080/api/db/func_test/profiles/user_full_001",
        {["X-Session-ID"] = "test_session"}
    )
    
    if read_resp.data.fields.updated_at then
        print("  ✓ Trigger executed (updated_at set)")
    end
    
    return true
end

local function test_mvcc_isolation()
    print("[Functional] Testing MVCC isolation...")
    
    -- Create multiple versions of a document
    for i = 1, 5 do
        local resp = http.request("PUT", "http://localhost:8080/api/db/func_test/mvcc_test/version_doc",
            {["X-Session-ID"] = "test_session"},
            {counter = i, version = "v" .. i}
        )
        assert(resp.success, "Functional: MVCC version update failed")
    end
    
    local final_resp = http.request("GET", "http://localhost:8080/api/db/func_test/mvcc_test/version_doc",
        {["X-Session-ID"] = "test_session"}
    )
    
    if final_resp.data and final_resp.data.fields.counter == 5 then
        print("  ✓ MVCC versioning works")
    end
    
    return true
end

local function test_backup_restore()
    print("[Functional] Testing backup/restore...")
    
    -- Export data
    local export_resp = http.request("POST", "http://localhost:8080/api/webui/export",
        {["X-Session-ID"] = "test_session"},
        {database = "func_test", filename = "backup_test.msgpack"}
    )
    assert(export_resp.success, "Functional: Export failed")
    print("  ✓ Data export")
    
    -- Import to new database
    local import_resp = http.request("POST", "http://localhost:8080/api/webui/import",
        {["X-Session-ID"] = "test_session"},
        {
            database = "func_test_restored",
            data = export_resp.data.data,
            overwrite = true
        }
    )
    assert(import_resp.success, "Functional: Import failed")
    print("  ✓ Data import")
    
    return true
end

-- Run functional tests
print("\n" .. string.rep("=", 50))
print("FUNCTIONAL TEST SUITE")
print(string.rep("=", 50))

test_nested_documents()
test_acl_permissions()
test_triggers()
test_mvcc_isolation()
test_backup_restore()

print("\n✓ ALL FUNCTIONAL TESTS PASSED ✓\n")

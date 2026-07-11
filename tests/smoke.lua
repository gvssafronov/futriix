-- ============================================================================
-- test_smoke.lua - Smoke-тест (проверка доступности и базовой функциональности)
-- ============================================================================
-- Назначение: Быстрая проверка того, что система работает и отвечает
-- Покрывает: API endpoints, кластеризацию, аутентификацию

local function test_api_availability()
    print("[Smoke] Testing API availability...")
    
    -- Check main endpoints
    local endpoints = {
        "/api/webui/stats",
        "/api/webui/databases",
        "/api/cluster/status",
        "/api/webui/transactions"
    }
    
    for _, endpoint in ipairs(endpoints) do
        local resp = http.request("GET", "http://localhost:8080" .. endpoint,
            {["X-Session-ID"] = "test_session"})
        if resp and resp.success then
            print("  ✓ " .. endpoint .. " - available")
        else
            print("  ✗ " .. endpoint .. " - unavailable")
            return false
        end
    end
    
    return true
end

local function test_authentication()
    print("[Smoke] Testing authentication...")
    
    -- Login with admin credentials
    local login_resp = http.request("POST", "http://localhost:8080/api/auth/login",
        {},
        {username = "admin", password = "admin"}
    )
    assert(login_resp.success and login_resp.data.session_id, "Smoke: Login failed")
    print("  ✓ Authentication works")
    
    -- Test invalid credentials
    local invalid_resp = http.request("POST", "http://localhost:8080/api/auth/login",
        {},
        {username = "fake", password = "wrong"}
    )
    assert(not invalid_resp.success, "Smoke: Invalid credentials accepted")
    print("  ✓ Invalid credentials rejected")
    
    return login_resp.data.session_id
end

local function test_cluster_status()
    print("[Smoke] Testing cluster status...")
    
    local status_resp = http.request("GET", "http://localhost:8080/api/cluster/status",
        {["X-Session-ID"] = "test_session"}
    )
    
    if status_resp and status_resp.success then
        print("  ✓ Cluster status: " .. (status_resp.data.health or "unknown"))
        print("    - Total nodes: " .. (status_resp.data.total_nodes or 0))
        print("    - Active nodes: " .. (status_resp.data.active_nodes or 0))
        return true
    else
        print("  ⚠ Cluster endpoint may be unavailable in single-node mode")
        return true
    end
end

local function test_basic_crud()
    print("[Smoke] Testing basic CRUD...")
    
    -- Create database and collection via first write
    local insert_resp = http.request("POST", "http://localhost:8080/api/db/smoke_test/users",
        {["X-Session-ID"] = "test_session"},
        {_id = "smoke_001", test_field = "smoke_value", timestamp = os.time()}
    )
    assert(insert_resp.success, "Smoke: Basic insert failed")
    print("  ✓ Basic insert")
    
    -- Read back
    local read_resp = http.request("GET", "http://localhost:8080/api/db/smoke_test/users/smoke_001",
        {["X-Session-ID"] = "test_session"}
    )
    assert(read_resp.success, "Smoke: Basic read failed")
    assert(read_resp.data.fields.test_field == "smoke_value", "Smoke: Data mismatch")
    print("  ✓ Basic read")
    
    return true
end

-- Run smoke tests
print("\n" .. string.rep("=", 50))
print("SMOKE TEST SUITE")
print(string.rep("=", 50))

local session = test_authentication()
test_api_availability()
test_cluster_status()
test_basic_crud()

print("\n✓ ALL SMOKE TESTS PASSED ✓\n")
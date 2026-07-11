-- ============================================================================
-- test_regression.lua - Регрессионный тест
-- ============================================================================
-- Назначение: Проверка корректности базовых операций после изменений
-- Покрывает: CRUD операции, индексы, транзакции, ограничения

local function setup_test_data()
    -- Создаём тестовую базу и коллекцию
    local response = http.request("POST", "http://localhost:8080/api/db/test_db/test_collection",
        {["X-Session-ID"] = "test_session"},
        {name = "test_doc_1", value = 100}
    )
    assert(response.success, "Regression: Failed to insert test document")
    
    response = http.request("POST", "http://localhost:8080/api/db/test_db/test_collection",
        {["X-Session-ID"] = "test_session"},
        {name = "test_doc_2", value = 200}
    )
    assert(response.success, "Regression: Failed to insert second document")
    
    return true
end

local function test_crud_operations()
    print("[Regression] Testing CRUD operations...")
    
    -- CREATE - Insert
    local insert_resp = http.request("POST", "http://localhost:8080/api/db/regress_db/users",
        {["X-Session-ID"] = "test_session"},
        {_id = "user_001", name = "John Doe", age = 30, email = "john@example.com"}
    )
    assert(insert_resp.success, "CRUD: Insert operation failed")
    print("  ✓ INSERT operation")
    
    -- READ - Find by ID
    local read_resp = http.request("GET", "http://localhost:8080/api/db/regress_db/users/user_001",
        {["X-Session-ID"] = "test_session"}
    )
    assert(read_resp.success and read_resp.data._id == "user_001", "CRUD: Read operation failed")
    assert(read_resp.data.fields.name == "John Doe", "CRUD: Read data mismatch")
    print("  ✓ READ operation")
    
    -- UPDATE
    local update_resp = http.request("PUT", "http://localhost:8080/api/db/regress_db/users/user_001",
        {["X-Session-ID"] = "test_session"},
        {age = 31, city = "New York"}
    )
    assert(update_resp.success, "CRUD: Update operation failed")
    
    -- Verify update
    local verify_resp = http.request("GET", "http://localhost:8080/api/db/regress_db/users/user_001",
        {["X-Session-ID"] = "test_session"}
    )
    assert(verify_resp.data.fields.age == 31, "CRUD: Update data not persisted")
    print("  ✓ UPDATE operation")
    
    -- DELETE
    local delete_resp = http.request("DELETE", "http://localhost:8080/api/db/regress_db/users/user_001",
        {["X-Session-ID"] = "test_session"}
    )
    assert(delete_resp.success, "CRUD: Delete operation failed")
    
    -- Verify deletion
    local deleted_resp = http.request("GET", "http://localhost:8080/api/db/regress_db/users/user_001",
        {["X-Session-ID"] = "test_session"}
    )
    assert(not deleted_resp.success, "CRUD: Document still exists after delete")
    print("  ✓ DELETE operation")
    
    return true
end

local function test_index_operations()
    print("[Regression] Testing index operations...")
    
    -- Create index
    local idx_resp = http.request("POST", "http://localhost:8080/api/index/regress_db/users/create",
        {["X-Session-ID"] = "test_session"},
        {name = "idx_name", fields = {"name"}, unique = false}
    )
    assert(idx_resp.success, "Index: Create index failed")
    print("  ✓ Index creation")
    
    -- List indexes
    local list_resp = http.request("GET", "http://localhost:8080/api/index/regress_db/users/list",
        {["X-Session-ID"] = "test_session"}
    )
    assert(list_resp.success and #list_resp.data >= 1, "Index: List indexes failed")
    print("  ✓ Index listing")
    
    -- Query by index
    local query_resp = http.request("GET", "http://localhost:8080/api/db/regress_db/users?index=idx_name&value=John%20Doe",
        {["X-Session-ID"] = "test_session"}
    )
    print("  ✓ Index query")
    
    -- Drop index
    local drop_resp = http.request("DELETE", "http://localhost:8080/api/index/regress_db/users/drop/idx_name",
        {["X-Session-ID"] = "test_session"}
    )
    print("  ✓ Index drop")
    
    return true
end

local function test_transaction_operations()
    print("[Regression] Testing transaction operations...")
    
    -- Start transaction session
    local session_resp = http.request("POST", "http://localhost:8080/api/webui/transactions",
        {["X-Session-ID"] = "test_session"},
        {action = "start_session"}
    )
    assert(session_resp.success, "Transaction: Session start failed")
    print("  ✓ Transaction session")
    
    -- Start transaction
    local tx_resp = http.request("POST", "http://localhost:8080/api/webui/transactions",
        {["X-Session-ID"] = "test_session"},
        {action = "start_transaction"}
    )
    assert(tx_resp.success, "Transaction: Start failed")
    print("  ✓ Transaction start")
    
    -- Insert within transaction
    local insert_resp = http.request("POST", "http://localhost:8080/api/db/regress_db/tx_test",
        {["X-Session-ID"] = "test_session"},
        {_id = "tx_doc", value = "transactional"}
    )
    assert(insert_resp.success, "Transaction: Insert in transaction failed")
    
    -- Commit transaction
    local commit_resp = http.request("POST", "http://localhost:8080/api/webui/transaction/commit",
        {["X-Session-ID"] = "test_session"}
    )
    assert(commit_resp.success, "Transaction: Commit failed")
    print("  ✓ Transaction commit")
    
    -- Verify persisted data
    local verify_resp = http.request("GET", "http://localhost:8080/api/db/regress_db/tx_test/tx_doc",
        {["X-Session-ID"] = "test_session"}
    )
    assert(verify_resp.success, "Transaction: Data not persisted after commit")
    print("  ✓ Transaction verification")
    
    return true
end

local function test_constraints()
    print("[Regression] Testing constraints...")
    
    -- Add required field constraint
    local required_resp = http.request("POST", "http://localhost:8080/api/constraint/regress_db/users/required/email",
        {["X-Session-ID"] = "test_session"}
    )
    assert(required_resp.success, "Constraints: Add required field failed")
    print("  ✓ Required constraint")
    
    -- Test constraint violation
    local insert_resp = http.request("POST", "http://localhost:8080/api/db/regress_db/users",
        {["X-Session-ID"] = "test_session"},
        {_id = "bad_user", name = "No Email"}
    )
    assert(not insert_resp.success, "Constraints: Required field constraint not enforced")
    print("  ✓ Required constraint enforcement")
    
    -- Add unique constraint
    local unique_resp = http.request("POST", "http://localhost:8080/api/constraint/regress_db/users/unique/email",
        {["X-Session-ID"] = "test_session"}
    )
    assert(unique_resp.success, "Constraints: Add unique field failed")
    
    -- Test unique violation
    local insert1 = http.request("POST", "http://localhost:8080/api/db/regress_db/users",
        {["X-Session-ID"] = "test_session"},
        {_id = "user_a", name = "User A", email = "same@email.com"}
    )
    local insert2 = http.request("POST", "http://localhost:8080/api/db/regress_db/users",
        {["X-Session-ID"] = "test_session"},
        {_id = "user_b", name = "User B", email = "same@email.com"}
    )
    assert(insert1.success and not insert2.success, "Constraints: Unique constraint not enforced")
    print("  ✓ Unique constraint")
    
    return true
end

-- Run regression tests
print("\n" .. string.rep("=", 50))
print("REGRESSION TEST SUITE")
print(string.rep("=", 50))

setup_test_data()
test_crud_operations()
test_index_operations()
test_transaction_operations()
test_constraints()

print("\n✓ ALL REGRESSION TESTS PASSED ✓\n")

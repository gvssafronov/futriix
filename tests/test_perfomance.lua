-- ============================================================================
-- test_performance.lua - Нагрузочный тест
-- ============================================================================
-- Назначение: Оценка производительности под нагрузкой
-- Покрывает: Пропускную способность, латентность, параллелизм

local function measure_latency(operation, fn)
    local start_time = os.clock()
    local result = fn()
    local end_time = os.clock()
    local latency_ms = (end_time - start_time) * 1000
    
    if not result then
        print("  ✗ " .. operation .. " failed")
        return nil
    end
    
    print(string.format("  ✓ %s: %.2f ms", operation, latency_ms))
    return latency_ms
end

local function performance_insert_test(count)
    print(string.format("\n[Performance] Insert %d documents...", count))
    
    local latencies = {}
    local successes = 0
    
    for i = 1, count do
        local latency = measure_latency("Insert " .. i, function()
            local resp = http.request("POST", "http://localhost:8080/api/db/perf_test/benchmark",
                {["X-Session-ID"] = "test_session"},
                {
                    _id = "perf_" .. i,
                    value = math.random(1, 1000000),
                    text = string.rep("x", 100),
                    timestamp = os.time()
                }
            )
            if resp and resp.success then
                successes = successes + 1
                return true
            end
            return false
        end)
        
        if latency then
            table.insert(latencies, latency)
        end
    end
    
    -- Calculate statistics
    local total_ms = 0
    for _, lat in ipairs(latencies) do
        total_ms = total_ms + lat
    end
    
    local avg_latency = total_ms / #latencies
    local throughput = (successes / (total_ms / 1000))
    
    print(string.format("\n  Results: %d/%d successful", successes, count))
    print(string.format("  Average latency: %.2f ms", avg_latency))
    print(string.format("  Throughput: %.2f ops/sec", throughput))
    
    return {successes = successes, avg_latency = avg_latency, throughput = throughput}
end

local function performance_read_test(doc_count)
    print(string.format("\n[Performance] Read %d documents...", doc_count))
    
    local latencies = {}
    local successes = 0
    
    for i = 1, doc_count do
        local latency = measure_latency("Read " .. i, function()
            local resp = http.request("GET", "http://localhost:8080/api/db/perf_test/benchmark/perf_" .. i,
                {["X-Session-ID"] = "test_session"}
            )
            if resp and resp.success then
                successes = successes + 1
                return true
            end
            return false
        end)
        
        if latency then
            table.insert(latencies, latency)
        end
    end
    
    local total_ms = 0
    for _, lat in ipairs(latencies) do
        total_ms = total_ms + lat
    end
    
    local avg_latency = total_ms / #latencies
    local throughput = (successes / (total_ms / 1000))
    
    print(string.format("\n  Results: %d/%d successful", successes, doc_count))
    print(string.format("  Average latency: %.2f ms", avg_latency))
    print(string.format("  Throughput: %.2f ops/sec", throughput))
    
    return {successes = successes, avg_latency = avg_latency, throughput = throughput}
end

local function performance_index_query_test()
    print("\n[Performance] Index query test...")
    
    -- Create index
    local idx_resp = http.request("POST", "http://localhost:8080/api/index/perf_test/benchmark/create",
        {["X-Session-ID"] = "test_session"},
        {name = "value_idx", fields = {"value"}, unique = false}
    )
    
    local latencies = {}
    local queries = 100
    
    for i = 1, queries do
        local search_value = math.random(1, 1000000)
        
        local latency = measure_latency("Query " .. i, function()
            local resp = http.request("GET", 
                "http://localhost:8080/api/db/perf_test/benchmark?index=value_idx&value=" .. search_value,
                {["X-Session-ID"] = "test_session"}
            )
            return resp and resp.success
        end)
        
        if latency then
            table.insert(latencies, latency)
        end
    end
    
    local total_ms = 0
    for _, lat in ipairs(latencies) do
        total_ms = total_ms + lat
    end
    
    local avg_latency = total_ms / #latencies
    print(string.format("\n  Average index query latency: %.2f ms", avg_latency))
    
    return avg_latency
end

local function performance_concurrent_test()
    print("\n[Performance] Concurrent operations test...")
    print("  ⚠ This test simulates concurrent operations")
    print("  Note: Lua concurrency is simulated via sequential requests")
    
    local concurrency_levels = {10, 50, 100}
    local results = {}
    
    for _, level in ipairs(concurrency_levels) do
        print(string.format("\n  Testing concurrency level: %d", level))
        
        local start_time = os.clock()
        local success_count = 0
        
        for i = 1, level do
            local resp = http.request("POST", "http://localhost:8080/api/db/perf_test/concurrent",
                {["X-Session-ID"] = "test_session"},
                {
                    _id = "concurrent_" .. i .. "_" .. os.time(),
                    thread_id = i,
                    timestamp = os.time()
                }
            )
            if resp and resp.success then
                success_count = success_count + 1
            end
        end
        
        local duration = os.clock() - start_time
        local throughput = success_count / duration
        
        table.insert(results, {
            level = level,
            duration = duration,
            throughput = throughput,
            success_rate = (success_count / level) * 100
        })
        
        print(string.format("    Duration: %.2f sec", duration))
        print(string.format("    Throughput: %.2f ops/sec", throughput))
        print(string.format("    Success rate: %.1f%%", (success_count / level) * 100))
    end
    
    return results
end

local function performance_mixed_workload_test()
    print("\n[Performance] Mixed workload (70% read, 20% write, 10% update)...")
    
    local operations = 200
    local reads = 0
    local writes = 0
    local updates = 0
    
    local start_time = os.clock()
    
    for i = 1, operations do
        local rand = math.random(1, 100)
        
        if rand <= 70 then
            -- Read operation
            local doc_id = "perf_" .. math.random(1, 100)
            local resp = http.request("GET", "http://localhost:8080/api/db/perf_test/benchmark/" .. doc_id,
                {["X-Session-ID"] = "test_session"}
            )
            if resp and resp.success then reads = reads + 1 end
            
        elseif rand <= 90 then
            -- Write operation
            local resp = http.request("POST", "http://localhost:8080/api/db/perf_test/benchmark",
                {["X-Session-ID"] = "test_session"},
                {
                    _id = "mixed_" .. i .. "_" .. os.time(),
                    data = "mixed workload test",
                    timestamp = os.time()
                }
            )
            if resp and resp.success then writes = writes + 1 end
            
        else
            -- Update operation
            local doc_id = "perf_" .. math.random(1, 100)
            local resp = http.request("PUT", "http://localhost:8080/api/db/perf_test/benchmark/" .. doc_id,
                {["X-Session-ID"] = "test_session"},
                {updated = true, update_time = os.time()}
            )
            if resp and resp.success then updates = updates + 1 end
        end
    end
    
    local duration = os.clock() - start_time
    
    print(string.format("\n  Results:"))
    print(string.format("    Total operations: %d", operations))
    print(string.format("    Reads: %d (%.1f%%)", reads, (reads/operations)*100))
    print(string.format("    Writes: %d (%.1f%%)", writes, (writes/operations)*100))
    print(string.format("    Updates: %d (%.1f%%)", updates, (updates/operations)*100))
    print(string.format("    Total duration: %.2f sec", duration))
    print(string.format("    Overall throughput: %.2f ops/sec", operations/duration))
    
    return {reads = reads, writes = writes, updates = updates, duration = duration}
end

-- Run performance tests
print("\n" .. string.rep("=", 50))
print("PERFORMANCE TEST SUITE")
print(string.rep("=", 50))

-- Warm-up
print("\n[Performance] Warming up...")
for i = 1, 10 do
    http.request("POST", "http://localhost:8080/api/db/perf_test/benchmark",
        {["X-Session-ID"] = "test_session"},
        {_id = "warmup_" .. i, data = "warmup"}
    )
end

local insert_results = performance_insert_test(100)
local read_results = performance_read_test(100)
local index_latency = performance_index_query_test()
local concurrent_results = performance_concurrent_test()
local mixed_results = performance_mixed_workload_test()

print("\n" .. string.rep("=", 50))
print("PERFORMANCE SUMMARY")
print(string.rep("=", 50))
print(string.format("Insert throughput: %.2f ops/sec", insert_results.throughput or 0))
print(string.format("Read throughput: %.2f ops/sec", read_results.throughput or 0))
print(string.format("Index query latency: %.2f ms", index_latency or 0))
print(string.format("Mixed workload throughput: %.2f ops/sec", mixed_results.duration and 200/mixed_results.duration or 0))

print("\n✓ PERFORMANCE TESTS COMPLETED ✓\n")

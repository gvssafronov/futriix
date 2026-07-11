--
 -- Copyright 2026 Safronov Grigorii
 --
 --Licensed under the CDDL, Version 1.0 (the "License");
 --you may not use this file except in compliance with the License.
 --
 -- You may obtain a copy of the License at
 --https://opensource.org/licenses/CDDL-1.0
 --
-- example.lua- Пример модуля скрипта на lua для извелечения коллекции в субд и вставки её в документ

version = "1.0.0"
author = "futriis"
description = "Example plugin"

function on_load()
    plugin_log("info", "Example plugin loaded")
    return true
end

function on_start()
    plugin_log("info", "Example plugin started")
    
    -- Получаем коллекцию
    local coll = get_collection("test_db", "test_coll")
    if coll then
        -- Вставляем документ
        coll:insert({name = "test", value = 42})
        plugin_log("info", "Document inserted")
    end
    
    return true
end

function process_data(data)
    plugin_log("debug", "Processing: " .. data)
    return {processed = true, original = data}
end

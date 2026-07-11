/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/serializer/msgpack.go
// Назначение: Сериализация и десериализация документов в формате MessagePack.
// Используется библиотека vmihailenco/msgpack для высокой производительности.

package serializer

import (
    "github.com/vmihailenco/msgpack/v5"
)

func Marshal(v interface{}) ([]byte, error) {
    return msgpack.Marshal(v)
}

func Unmarshal(data []byte, v interface{}) error {
    return msgpack.Unmarshal(data, v)
}

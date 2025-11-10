// Copyright (c) 2023 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package common

import (
	"crypto/rand"
	"encoding/binary"
	"os"
	"strconv"
	"strings"
)

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return fallback
}

func GetEnvInt(key string, fallback int) int {
	str := GetEnv(key, strconv.Itoa(fallback))
	val, err := strconv.Atoi(str)
	if err != nil {
		return fallback
	}

	return val
}

// GenerateRandomInt generate a random int that is not determined
func GenerateRandomInt() int {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		// Fallback to 0 if crypto/rand fails (should never happen)
		return 0
	}

	// Convert bytes to uint64, then modulo to get value in range [0, 10000)
	randomValue := binary.BigEndian.Uint64(b[:])
	// Modulo first to ensure result fits in int safely
	result := randomValue % 10000
	return int(result) // #nosec G115 - result is guaranteed to be < 10000, safe to convert
}

// MakeTraceID create new traceID
// example: service_1234
func MakeTraceID(identifiers ...string) string {
	strInt := strconv.Itoa(GenerateRandomInt())
	var builder strings.Builder
	for _, i := range identifiers {
		builder.WriteString(i)
		builder.WriteString("_")
	}
	builder.WriteString(strInt)

	return builder.String()
}

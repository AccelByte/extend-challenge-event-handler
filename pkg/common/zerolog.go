// Copyright (c) 2023 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package common

import (
	"os"
	"strings"

	"github.com/rs/zerolog"
)

// NewZerologLogger creates a new zerolog logger with the specified level.
// Supports level strings: debug, info, warn, error
func NewZerologLogger(levelStr string) zerolog.Logger {
	// Parse log level
	var level zerolog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = zerolog.DebugLevel
	case "info":
		level = zerolog.InfoLevel
	case "warn", "warning":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	default:
		level = zerolog.InfoLevel
	}

	// Set global log level
	zerolog.SetGlobalLevel(level)

	// Create logger with timestamp
	logger := zerolog.New(os.Stdout).
		With().
		Timestamp().
		Logger()

	return logger
}

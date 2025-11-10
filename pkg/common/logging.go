// Copyright (c) 2023 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package common

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/rs/zerolog"
)

// InterceptorLogger adapts zerolog logger to interceptor logger.
// This code is referenced from https://github.com/grpc-ecosystem/go-grpc-middleware/
func InterceptorLogger(l *zerolog.Logger) logging.Logger {
	return logging.LoggerFunc(func(_ context.Context, lvl logging.Level, msg string, fields ...any) {
		var event *zerolog.Event

		switch lvl {
		case logging.LevelDebug:
			event = l.Debug()
		case logging.LevelInfo:
			event = l.Info()
		case logging.LevelWarn:
			event = l.Warn()
		case logging.LevelError:
			event = l.Error()
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}

		// Add fields to the event
		i := logging.Fields(fields).Iterator()
		for i.Next() {
			k, v := i.At()
			event = event.Interface(k, v)
		}

		event.Msg(msg)
	})
}

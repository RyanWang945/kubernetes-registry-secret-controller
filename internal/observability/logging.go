package observability

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/go-logr/logr"
)

func LogLevel(value string) (slog.Level, error) {
	switch value {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("log level must be debug, info, warn or error")
	}
}

func ConfigureLogging(output io.Writer, value string) (logr.Logger, error) {
	level, err := LogLevel(value)
	if err != nil {
		return logr.Logger{}, err
	}
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
	return logr.FromSlogHandler(handler), nil
}

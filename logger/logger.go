package logger

import (
	"net/http"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.Logger
	zap.AtomicLevel
}

var logger Logger

func init() {
	level := zap.InfoLevel
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "warn":
		level = zap.WarnLevel
	case "error":
		level = zap.ErrorLevel
	case "fatal":
		level = zap.FatalLevel
	}

	atomicLevel := zap.NewAtomicLevelAt(level)

	zapLogger := zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(zapcore.EncoderConfig{
				TimeKey:        "ts",
				LevelKey:       "level",
				NameKey:        "logger",
				CallerKey:      "caller",
				MessageKey:     "message",
				StacktraceKey:  "stacktrace",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    zapcore.LowercaseLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.SecondsDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			}),
			zapcore.AddSync(os.Stderr),
			atomicLevel,
		),
	)

	logger = Logger{
		zapLogger,
		atomicLevel,
	}

	Info("logger initialized", zap.String("log_level", level.String()))
}

func Debug(msg string, args ...zap.Field) {
	logger.Debug(msg, args...)
}

func Info(msg string, args ...zap.Field) {
	logger.Info(msg, args...)
}

func Warn(msg string, args ...zap.Field) {
	logger.Warn(msg, args...)
}

func Error(msg string, args ...zap.Field) {
	logger.Error(msg, args...)

}

func Panic(msg string, args ...zap.Field) {
	logger.Panic(msg, args...)

}

func Fatal(msg string, args ...zap.Field) {
	logger.Fatal(msg, args...)

}

// Handler returns http handler to view/change log level in runtime,
// see go.uber.org/zap@v1.18.1/http_handler.go for usage
func Handler() http.Handler {
	return logger.AtomicLevel
}

func SetLevel(level zapcore.Level) {
	logger.SetLevel(level)
}

package utils

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"talaria/config"

	"gopkg.in/natefinch/lumberjack.v2"
)

type LogLevel int32

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelError
)

var currentLogLevel int32 = int32(LogLevelInfo)

func ParseLogLevel(raw string) (LogLevel, error) {
	level := strings.ToUpper(strings.TrimSpace(raw))
	if level == "" {
		return LogLevelInfo, nil
	}
	switch level {
	case "DEBUG":
		return LogLevelDebug, nil
	case "INFO":
		return LogLevelInfo, nil
	case "ERROR":
		return LogLevelError, nil
	default:
		return LogLevelInfo, fmt.Errorf("invalid log level %q (expected DEBUG, INFO, or ERROR)", raw)
	}
}

func SetLogLevel(level LogLevel) {
	atomic.StoreInt32(&currentLogLevel, int32(level))
}

func GetLogLevel() LogLevel {
	return LogLevel(atomic.LoadInt32(&currentLogLevel))
}

func shouldLog(level LogLevel) bool {
	return level >= GetLogLevel()
}

func logf(level LogLevel, prefix string, format string, args ...any) {
	if !shouldLog(level) {
		return
	}
	log.Printf("[%s] %s", prefix, fmt.Sprintf(format, args...))
}

func Debugf(format string, args ...any) {
	logf(LogLevelDebug, "DEBUG", format, args...)
}

func Infof(format string, args ...any) {
	logf(LogLevelInfo, "INFO", format, args...)
}

func Errorf(format string, args ...any) {
	logf(LogLevelError, "ERROR", format, args...)
}

func Fatalf(format string, args ...any) {
	logf(LogLevelError, "ERROR", format, args...)
	os.Exit(1)
}

// SetupLogger configures the default logger.  If cfg.Filename is empty the
// logger writes to stdout; otherwise it uses a rotating lumberjack file.
func SetupLogger(cfg *config.LogConfig) {
	var w io.Writer
	if cfg != nil && cfg.Filename != "" {
		w = &lumberjack.Logger{
			Filename:   cfg.Filename,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,
			Compress:   cfg.Compress,
		}
	} else {
		w = os.Stdout
	}
	log.SetOutput(w)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	level := LogLevelInfo
	if cfg != nil {
		parsed, err := ParseLogLevel(cfg.Level)
		if err != nil {
			log.Printf("[ERROR] invalid GlobalLog.Level %q; defaulting to INFO", cfg.Level)
		} else {
			level = parsed
		}
	}
	SetLogLevel(level)
}

package utils

import (
	"io"
	"log"
	"os"

	"talaria/config"

	"gopkg.in/natefinch/lumberjack.v2"
)

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
}

package log

import (
	"io"
	"log"
	"os"
)

var (
	logger = NewLogger()
)

// Logger is logger
type Logger struct {
	debug bool
}

// NewLogger returns a logger object
func NewLogger() *Logger {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)
	return &Logger{
		debug: false,
	}
}

// SetDebug enables debug mode
func (l *Logger) SetDebug(debug bool) {
	l.debug = debug
}

// Debug prints a debug log
func (l *Logger) Debug(v ...interface{}) {
	if l.debug {
		log.Println(v...)
	}
}

// Debugf prints a formatted debug log
func (l *Logger) Debugf(format string, v ...interface{}) {
	if l.debug {
		log.Printf(format+"\n", v...)
	}
}

// Println prints a stdout log
func (l *Logger) Println(v ...interface{}) {
	log.Println(v...)
}

// Printf prints a formatted stdout log
func (l *Logger) Printf(format string, v ...interface{}) {
	log.Printf(format+"\n", v...)
}

// SetFlags set log package's flags
func SetFlags(flag int) {
	log.SetFlags(flag)
}

// SetOutput set output destination
func SetOutput(w io.Writer) {
	log.SetOutput(w)
}

// SetDebug enables debug mode
func SetDebug(debug bool) {
	logger.SetDebug(debug)
}

// Debug prints a debug log
func Debug(args ...interface{}) {
	logger.Debug(args...)
}

// Debugf prints a formatted debug log
func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

// Println prints a stdout log
func Println(args ...interface{}) {
	logger.Println(args...)
}

// Printf prints a formatted stdout log
func Printf(format string, args ...interface{}) {
	logger.Printf(format, args...)
}

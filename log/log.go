package log

import (
	"log"
	"io"
	"os"
)

var (
	logger = NewLogger()
)

type Logger struct {
	debug bool
}

func NewLogger() *Logger {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)
	return &Logger{
		debug:  false,
	}
}

func (l *Logger) SetDebug(debug bool) {
	l.debug = debug
}

func (l *Logger) Debug(v ...interface{}) {
	if l.debug {
		log.Println(v...)
	}
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	if l.debug {
		log.Printf(format+"\n", v...)
	}
}

func (l *Logger) Println(v ...interface{}) {
	log.Println(v...)
}

func (l *Logger) Printf(format string, v ...interface{}) {
	log.Printf(format+"\n", v...)
}

func SetFlags(flag int) {
	log.SetFlags(flag)
}

func SetOutput(w io.Writer) {
	log.SetOutput(w)
}

func SetDebug(debug bool) {
	logger.SetDebug(debug)
}

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func Println(args ...interface{}) {
	logger.Println(args...)
}

func Printf(format string, args ...interface{}) {
	logger.Printf(format, args...)
}

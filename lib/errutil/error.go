// Package errutil provides utilities for pkg/errors.
package errutil

import (
	"github.com/yuuki/diamondb/lib/log"

	"github.com/pkg/errors"
)

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// PrintStackTrace prints the stack trace by pkg/errors
func PrintStackTrace(err error) {
	if err, ok := prevCause(err).(stackTracer); ok {
		log.Printf("%+v", err.StackTrace())
	}
}

// prevCause returns the previous 'cause' error
func prevCause(err error) error {
	type causer interface {
		Cause() error
	}

	prev := err
	for err != nil {
		cause, ok := err.(causer)
		if !ok {
			break
		}
		prev = err
		err = cause.Cause()
	}
	return prev
}

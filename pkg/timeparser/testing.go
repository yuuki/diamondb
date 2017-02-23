package timeparser

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Test that time is roughly the same.
func TestTimeNearlyEqual(t *testing.T, got time.Time, expected time.Time) {
	gy, gm, gd := got.Date()
	gh, gmin, _ := got.Clock()

	ey, em, ed := expected.Date()
	eh, emin, _ := expected.Clock()

	if gy != ey || gm != em || gd != ed || gh != eh || gmin != emin {
		var caller string
		if _, file, line, ok := runtime.Caller(1); ok {
			// Truncate file name at last file name separator.
			if index := strings.LastIndex(file, "/"); index >= 0 {
				file = file[index+1:]
			} else if index = strings.LastIndex(file, "\\"); index >= 0 {
				file = file[index+1:]
			}
			caller = fmt.Sprintf("%s:%d:\n", file, line)
		}
		t.Fatalf("\n\r\t%sExpected: %+v\nActual:   %+v", caller, expected, got)
	}
}

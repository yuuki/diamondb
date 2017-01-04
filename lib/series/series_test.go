package series

import (
	"reflect"
	"testing"
)

func TestNewSeries(t *testing.T) {
	name := "server1.loadavg5"
	values := NewFloat64PointerSlice([]float64{0.1, 0.2, 0.3})
	start, step := int64(10000), 60

	s := NewSeries(name, values, start, step)

	if s.Name() != "server1.loadavg5" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", name, s.Name())
	}
	if !reflect.DeepEqual(s.Values(), values) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", values, s.Values())
	}
	if s.Start() != start {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", start, s.Start())
	}
	if s.End() != 10120 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 10120, s.End())
	}
	if s.Step() != step {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", step, s.Step())
	}
	if s.Len() != 3 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 3, s.Len())
	}
}

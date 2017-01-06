package storage

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestNewSeriesPoint(t *testing.T) {
	points := datapoints{
		newDataPoint(1000, 0.1),
		newDataPoint(1120, 0.3),
		newDataPoint(1060, 0.2),
	}
	s := newSeriesPoint("server1.loadavg5", points, 60)
	if s.Name() != "server1.loadavg5" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "server1.loadavg5", s.Name())
	}
	expected := datapoints{
		newDataPoint(1000, 0.1),
		newDataPoint(1060, 0.2),
		newDataPoint(1120, 0.3),
	}
	if diff := pretty.Compare(s.Points(), expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
	if s.Step() != 60 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 60, s.Step())
	}
	if s.Start() != 1000 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 1000, s.Start())
	}
	if s.End() != 1120 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 1120, s.End())
	}
	if s.Len() != 3 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 3, s.Len())
	}
}

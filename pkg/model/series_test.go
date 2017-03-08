package model

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestNewSeries(t *testing.T) {
	tests := []struct {
		desc           string
		inValues       []float64
		inStart        int64
		expectedValues []float64
		expectedStart  int64
		expectedEnd    int64
	}{
		{
			"normal",
			[]float64{0.1, 0.2, 0.3},
			960,
			[]float64{0.1, 0.2, 0.3},
			960,
			1080,
		},
		{
			"zero length vlaues",
			[]float64{},
			960,
			[]float64{},
			-1,
			-1,
		},
	}

	for _, tc := range tests {
		s := NewSeries("server1.loadavg5", tc.inValues, tc.expectedStart, 60)

		if s.Name() != "server1.loadavg5" {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", "server1.loadavg5", s.Name())
		}
		if diff := pretty.Compare(s.Values(), tc.expectedValues); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
		if s.Start() != tc.expectedStart {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", tc.expectedStart, s.Start())
		}
		if s.End() != tc.expectedEnd {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", tc.expectedEnd, s.End())
		}
		if s.Step() != 60 {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", 60, s.Step())
		}
		if s.Len() != len(tc.expectedValues) {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", len(tc.expectedValues), s.Len())
		}
	}
}

func TestSeriesAlias(t *testing.T) {
	s := NewSeries("server1.loadavg5", []float64{}, 100, 60)
	if s.Alias() != "server1.loadavg5" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "server1.loadavg5", s.Alias())
	}
	s.SetAlias("func(server1.loadavg5)")
	if s.Alias() != "func(server1.loadavg5)" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "func(server1.loadavg5)", s.Alias())
	}
	s = NewSeries("server1.loadavg5", []float64{}, 100, 60).SetAliasWith("func2(server1.loadavg5)")
	if s.Alias() != "func2(server1.loadavg5)" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "func2(server1.loadavg5)", s.Alias())
	}
}

func TestSeriesSetName(t *testing.T) {
	s := NewSeries("server1.loadavg5", []float64{}, 100, 60)
	s.SetName("server10.loadavg5")
	if s.Name() != "server10.loadavg5" {
		t.Fatalf("failed to SetName. got %s, expected 'server10.loadavg5'", s.Name())
	}
}

func TestMarshalJSON(t *testing.T) {
	s := NewSeries("server1.loadavg5", []float64{0.1, 0.2, 0.3, math.NaN(), 0.5}, 1000, 60)
	s.SetAlias("func(server1.loadavg5)")
	j, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("%s", err)
	}
	expected := "{\"target\":\"func(server1.loadavg5)\",\"datapoints\":[[0.1,1000],[0.2,1060],[0.3,1120],[null,1180],[0.5,1240]]}"
	if got := fmt.Sprintf("%s", j); got != expected {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, got)
	}
}

func TestSeriesPoints(t *testing.T) {
	tests := []struct {
		desc     string
		s        *Series
		expected DataPoints
	}{
		{
			"normal",
			NewSeries("server1.loadavg5", []float64{0.1, 0.2, 0.3, 0.4, 0.5}, 0, 60),
			DataPoints{
				NewDataPoint(0, 0.1),
				NewDataPoint(60, 0.2),
				NewDataPoint(120, 0.3),
				NewDataPoint(180, 0.4),
				NewDataPoint(240, 0.5),
			},
		},
		{
			"zero length values",
			NewSeries("server1.loadavg5", []float64{}, 0, 60),
			DataPoints{},
		},
	}

	for _, tc := range tests {
		got := tc.s.Points()
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
	}
}

package series

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

var testNewSeriesPointTests = []struct {
	desc           string
	input          DataPoints
	expectedPoints DataPoints
	expectedValues []float64
	expectedStart  int64
	expectedEnd    int64
}{
	{
		"not aligned and not sorted timestamp",
		DataPoints{
			NewDataPoint(1000, 0.1),
			NewDataPoint(1120, 0.3),
			NewDataPoint(1060, 0.2),
		},
		DataPoints{
			// Align timestamp by step
			NewDataPoint(960, 0.1),
			NewDataPoint(1020, 0.2),
			NewDataPoint(1080, 0.3),
		},
		[]float64{0.1, 0.2, 0.3},
		960,
		1080,
	},
	{
		"duplicate timestamps after aligned",
		DataPoints{
			NewDataPoint(1000, 0.1),
			NewDataPoint(1120, 0.3),
			NewDataPoint(1060, 0.2),
			NewDataPoint(1070, 0.4),
		},
		DataPoints{
			// Align timestamp by step
			NewDataPoint(960, 0.1),
			NewDataPoint(1020, 0.4),
			NewDataPoint(1080, 0.3),
		},
		[]float64{0.1, 0.4, 0.3},
		960,
		1080,
	},
}

func TestNewSeriesPoint(t *testing.T) {
	for _, tc := range testNewSeriesPointTests {
		s := NewSeriesPoint("server1.loadavg5", tc.input, 60)

		if s.Name() != "server1.loadavg5" {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", "server1.loadavg5", s.Name())
		}
		if diff := pretty.Compare(s.Points(), tc.expectedPoints); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
		if diff := pretty.Compare(s.Values(), tc.expectedValues); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
		if s.Step() != 60 {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", 60, s.Step())
		}
		if s.Start() != tc.expectedStart {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", tc.expectedStart, s.Start())
		}
		if s.End() != tc.expectedEnd {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", tc.expectedEnd, s.End())
		}
		if s.Len() != len(tc.expectedPoints) {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", len(tc.expectedPoints), s.Len())
		}
	}
}

package series

import "testing"

func TestFormatedName_Uniq(t *testing.T) {
	ss := SeriesSlice{
		NewSeries("server1.cpu.system", nil, 0, 0),
		NewSeries("server2.cpu.system", nil, 0, 0),
		NewSeries("server3.cpu.system", nil, 0, 0),
	}
	name := ss.FormatedName()
	expected := "server1.cpu.system,server2.cpu.system,server3.cpu.system"
	if name != expected {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, name)
	}
}

func TestFormatSeries_NotUniq(t *testing.T) {
	ss := SeriesSlice{
		NewSeries("server2.cpu.system", nil, 0, 0),
		NewSeries("server1.cpu.system", nil, 0, 0),
		NewSeries("server3.cpu.system", nil, 0, 0),
		NewSeries("server1.cpu.system", nil, 0, 0),
	}
	name := ss.FormatedName()
	expected := "server1.cpu.system,server2.cpu.system,server3.cpu.system"
	if name != expected {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, name)
	}
}

var testNormalizeTests = []struct {
	ss    SeriesSlice
	start int64
	end   int64
	step  int
}{
	{
		SeriesSlice{}, 0, 0, 0,
	},
	{
		SeriesSlice{NewSeries(
			"server1.loadavg5",
			NewFloat64PointerSlice([]float64{0.0, 0.0, 0.0, 0.0, 0.0}),
			int64(1),
			1,
		)},
		int64(1), int64(5), 1,
	},
	{
		GenerateSeriesSlice(),
		int64(0), int64(99), 1,
	},
}

func TestNormalize(t *testing.T) {
	for i, nt := range testNormalizeTests {
		start, end, step := nt.ss.Normalize()
		if start != nt.start {
			t.Fatalf("\nExpected: %+v\nActual:   %+v (#%d)", nt.start, start, i)
		}
		if end != nt.end {
			t.Fatalf("\nExpected: %+v\nActual:   %+v (#%d)", nt.end, end, i)
		}
		if step != nt.step {
			t.Fatalf("\nExpected: %+v\nActual:   %+v (#%d)", nt.step, step, i)
		}
	}
}

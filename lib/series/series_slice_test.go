package series

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

var testFormatedNameTests = []struct {
	desc string
	ss   SeriesSlice
	name string
}{
	{
		"case1: input unique series names",
		SeriesSlice{
			NewSeries("server1.cpu.system", nil, 0, 0),
			NewSeries("server2.cpu.system", nil, 0, 0),
			NewSeries("server3.cpu.system", nil, 0, 0),
		},
		"server1.cpu.system,server2.cpu.system,server3.cpu.system",
	},
	{
		"case2: input not unique series names",
		SeriesSlice{
			NewSeries("server2.cpu.system", nil, 0, 0),
			NewSeries("server1.cpu.system", nil, 0, 0),
			NewSeries("server3.cpu.system", nil, 0, 0),
			NewSeries("server1.cpu.system", nil, 0, 0),
		},
		"server1.cpu.system,server2.cpu.system,server3.cpu.system",
	},
}

func TestFormatedName(t *testing.T) {
	for _, tc := range testFormatedNameTests {
		name := tc.ss.FormattedName()
		if name != tc.name {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", tc.name, name)
		}
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
			[]float64{0.0, 0.0, 0.0, 0.0, 0.0},
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

var testSeriesSliceZipTests = []struct {
	desc string
	ss   SeriesSlice
	rows [][]float64
}{
	{
		"each of series's length is the same",
		SeriesSlice{
			NewSeries("server1.cpu.system", []float64{0.1, 0.2}, 1000, 60),
			NewSeries("server2.cpu.system", []float64{0.1, 0.2}, 1000, 60),
			NewSeries("server3.cpu.system", []float64{0.1, 0.2}, 1000, 60),
		},
		[][]float64{{0.1, 0.1, 0.1}, {0.2, 0.2, 0.2}},
	},
	{
		"each of series's length is different",
		SeriesSlice{
			NewSeries("server1.cpu.system", []float64{0.1, 0.2}, 1000, 60),
			NewSeries("server2.cpu.system", []float64{0.1, 0.2, 0.3}, 1000, 60),
			NewSeries("server3.cpu.system", []float64{0.1, 0.2}, 1000, 60),
		},
		[][]float64{{0.1, 0.1, 0.1}, {0.2, 0.2, 0.2}},
	},
}

func TestSeriesSliceZip(t *testing.T) {
	for _, tc := range testSeriesSliceZipTests {
		iter := tc.ss.Zip()
		i := 0
		for row := iter(); row != nil; row = iter() {
			if diff := pretty.Compare(row, tc.rows[i]); diff != "" {
				t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
			}
			i++
		}
	}
}

package query

import (
	"math"
	"testing"

	"github.com/kylelemons/godebug/pretty"
	"github.com/pkg/errors"

	. "github.com/yuuki/diamondb/lib/series"
)

var testDoSumSeriesTests = []struct {
	desc string
	args funcArgs
	err  error
}{
	{
		"the number of arguments is one",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
				},
			},
		},
		nil,
	},
	{
		"the number of arguments is two",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
				},
			},
			&funcArg{
				expr: SeriesListExpr{Literal: "server2.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server2.loadavg5", []float64{0.1}, 100, 1),
				},
			},
		},
		nil,
	},
	{
		"the type of the arguments is different",
		funcArgs{&funcArg{expr: StringExpr{Literal: "server1.loadavg5"}}},
		errors.New("invalid argument type `seriesList` to function `sumSeries`"),
	},
}

func TestDoSumSeries(t *testing.T) {
	for _, tc := range testDoSumSeriesTests {
		_, err := doSumSeries(tc.args)
		if tc.err != nil {
			if diff := pretty.Compare(err.Error(), tc.err.Error()); diff != "" {
				t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
			}
			continue
		}
		if err != nil {
			t.Fatalf("err should be nil")
		}
	}
}

var testDoAverageSeriesTests = []struct {
	desc string
	args funcArgs
	err  error
}{
	{
		"the number of arguments is one",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
				},
			},
		},
		nil,
	},
	{
		"the number of arguments is two",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
				},
			},
			&funcArg{
				expr: SeriesListExpr{Literal: "server2.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server2.loadavg5", []float64{0.1}, 100, 1),
				},
			},
		},
		nil,
	},
	{
		"the type of the arguments is different",
		funcArgs{&funcArg{expr: StringExpr{Literal: "server1.loadavg5"}}},
		errors.New("invalid argument type `seriesList` to function `averageSeries`"),
	},
}

func TestDoAverageSeries(t *testing.T) {
	for _, tc := range testDoAverageSeriesTests {
		_, err := doAverageSeries(tc.args)
		if tc.err != nil {
			if diff := pretty.Compare(err.Error(), tc.err.Error()); diff != "" {
				t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
			}
			continue
		}
		if err != nil {
			t.Fatalf("err should be nil")
		}
	}
}

var testDoMinSeriesTests = []struct {
	desc string
	args funcArgs
	err  error
}{
	{
		"the number of arguments is one",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
				},
			},
		},
		nil,
	},
	{
		"the number of arguments is two",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
				},
			},
			&funcArg{
				expr: SeriesListExpr{Literal: "server2.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server2.loadavg5", []float64{0.1}, 100, 1),
				},
			},
		},
		nil,
	},
	{
		"the type of the arguments is different",
		funcArgs{&funcArg{expr: StringExpr{Literal: "server1.loadavg5"}}},
		errors.New("invalid argument type `seriesList` to function `minSeries`"),
	},
}

func TestDoMinSeries(t *testing.T) {
	for _, tc := range testDoMinSeriesTests {
		_, err := doMinSeries(tc.args)
		if tc.err != nil {
			if diff := pretty.Compare(err.Error(), tc.err.Error()); diff != "" {
				t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
			}
			continue
		}
		if err != nil {
			t.Fatalf("err should be nil")
		}
	}
}

var testDoMaxSeriesTests = []struct {
	desc string
	args funcArgs
	err  error
}{
	{
		"the number of arguments is one",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
				},
			},
		},
		nil,
	},
	{
		"the number of arguments is two",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
				},
			},
			&funcArg{
				expr: SeriesListExpr{Literal: "server2.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server2.loadavg5", []float64{0.1}, 100, 1),
				},
			},
		},
		nil,
	},
	{
		"the type of the arguments is different",
		funcArgs{&funcArg{expr: StringExpr{Literal: "server1.loadavg5"}}},
		errors.New("invalid argument type `seriesList` to function `maxSeries`"),
	},
}

func TestDoMaxSeries(t *testing.T) {
	for _, tc := range testDoMaxSeriesTests {
		_, err := doMaxSeries(tc.args)
		if tc.err != nil {
			if diff := pretty.Compare(err.Error(), tc.err.Error()); diff != "" {
				t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
			}
			continue
		}
		if err != nil {
			t.Fatalf("err should be nil")
		}
	}
}

var testDoMultiplySeriesTests = []struct {
	desc string
	args funcArgs
	err  error
}{
	{
		"the number of arguments is one",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
				},
			},
		},
		nil,
	},
	{
		"the number of arguments is two",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
				},
			},
			&funcArg{
				expr: SeriesListExpr{Literal: "server2.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server2.loadavg5", []float64{0.1}, 100, 1),
				},
			},
		},
		nil,
	},
	{
		"the type of the arguments is different",
		funcArgs{&funcArg{expr: StringExpr{Literal: "server1.loadavg5"}}},
		errors.New("invalid argument type `seriesList` to function `multiplySeries`"),
	},
}

func TestDoMultiplySeries(t *testing.T) {
	for _, tc := range testDoMultiplySeriesTests {
		_, err := doMultiplySeries(tc.args)
		if tc.err != nil {
			if diff := pretty.Compare(err.Error(), tc.err.Error()); diff != "" {
				t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
			}
			continue
		}
		if err != nil {
			t.Fatalf("err should be nil")
		}
	}
}

func TestDoGroup(t *testing.T) {
	args := funcArgs{
		&funcArg{
			expr: SeriesListExpr{Literal: "server{1,2}.loadavg5"},
			seriesSlice: SeriesSlice{
				NewSeries("server1.loadavg5", []float64{0.1}, 0, 1),
				NewSeries("server2.loadavg5", []float64{0.2}, 0, 1),
			},
		},
		&funcArg{
			expr: SeriesListExpr{Literal: "server{2,3}.loadavg5"},
			seriesSlice: SeriesSlice{
				NewSeries("server2.loadavg5", []float64{0.2}, 0, 1),
				NewSeries("server3.loadavg5", []float64{0.3}, 0, 1),
			},
		},
	}
	got, err := doGroup(args)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := SeriesSlice{
		NewSeries("server1.loadavg5", []float64{0.1}, 0, 1),
		NewSeries("server2.loadavg5", []float64{0.2}, 0, 1),
		NewSeries("server2.loadavg5", []float64{0.2}, 0, 1),
		NewSeries("server3.loadavg5", []float64{0.3}, 0, 1),
	}
	if diff := pretty.Compare(got, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestAlias(t *testing.T) {
	ss := SeriesSlice{
		NewSeries("server1.loadavg5", []float64{10.0}, 0, 60),
		NewSeries("server2.loadavg5", []float64{11.0}, 0, 60),
	}
	got := alias(ss, "server.loadavg5")
	for _, s := range got {
		if s.Alias() != "server.loadavg5" {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", "server.loadavg5", s.Alias())
		}
	}
}

func TestOffset(t *testing.T) {
	tests := []struct {
		desc                string
		inputSeriesSlice    SeriesSlice
		inputFactor         float64
		expectedSeriesSlice SeriesSlice
	}{
		{
			"positive factor",
			SeriesSlice{
				NewSeries("server1.loadavg5", []float64{1.0, 2.0, 3.0, 4.0}, 1, 1),
				NewSeries("server2.loadavg5", []float64{math.NaN(), 5.0}, 4, 1),
			},
			0.5,
			SeriesSlice{
				NewSeries("offset(server1.loadavg5,0.5)", []float64{1.5, 2.5, 3.5, 4.5}, 1, 1),
				NewSeries("offset(server2.loadavg5,0.5)", []float64{math.NaN(), 5.5}, 4, 1),
			},
		},
		{
			"negative factor",
			SeriesSlice{
				NewSeries("server1.loadavg5", []float64{1.0, 2.0, 3.0, 4.0}, 1, 1),
				NewSeries("server2.loadavg5", []float64{math.NaN(), 5.0}, 4, 1),
			},
			-0.5,
			SeriesSlice{
				NewSeries("offset(server1.loadavg5,-0.5)", []float64{0.5, 1.5, 2.5, 3.5}, 1, 1),
				NewSeries("offset(server2.loadavg5,-0.5)", []float64{math.NaN(), 4.5}, 4, 1),
			},
		},
	}

	for _, tc := range tests {
		got := offset(tc.inputSeriesSlice, tc.inputFactor)
		if diff := pretty.Compare(got, tc.expectedSeriesSlice); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestSumSeries(t *testing.T) {
	series := sumSeries(GenerateSeriesSlice())
	vals := make([]float64, 100)
	for i := 0; i < 100; i++ {
		vals[i] = float64(i+1) * 2
	}
	expected := NewSeries(
		"sumSeries(server0.loadavg5,server1.loadavg5)",
		vals, 0, 1,
	)
	if diff := pretty.Compare(series, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestAverageSeries(t *testing.T) {
	series := averageSeries(GenerateSeriesSlice())
	vals := make([]float64, 100)
	vals[0] = 1.0
	for i := 1; i < 100; i++ {
		vals[i] = float64(i+1) * 3 / 3
	}
	expected := NewSeries(
		"averageSeries(server0.loadavg5,server1.loadavg5)",
		vals, 0, 1,
	)
	if diff := pretty.Compare(series, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestMinSeries(t *testing.T) {
	series := minSeries(GenerateSeriesSlice())
	vals := make([]float64, 100)
	for i := 0; i < 100; i++ {
		vals[i] = float64(i + 1)
	}
	expected := NewSeries(
		"minSeries(server0.loadavg5,server1.loadavg5)",
		vals, 0, 1,
	)
	if diff := pretty.Compare(series, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestMaxSeries(t *testing.T) {
	series := maxSeries(GenerateSeriesSlice())
	vals := make([]float64, 100)
	for i := 0; i < 100; i++ {
		vals[i] = float64(i + 1)
	}
	expected := NewSeries(
		"maxSeries(server0.loadavg5,server1.loadavg5)",
		vals, 0, 1,
	)
	if diff := pretty.Compare(series, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestMultiplySeries(t *testing.T) {
	series := multiplySeries(GenerateSeriesSlice())
	vals := make([]float64, 100)
	vals[0] = 1.0 * 1.0 * 1.0
	for i := 1; i < 100; i++ {
		vals[i] = math.Pow(float64(i+1), 2)
	}
	expected := NewSeries(
		"multiplySeries(server0.loadavg5,server1.loadavg5)",
		vals, 0, 1,
	)
	if diff := pretty.Compare(series, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestDivideSeries(t *testing.T) {
	vals1 := make([]float64, 100)
	vals1[0] = 0.0
	for i := 1; i < 100; i++ {
		vals1[i] = 2.0
	}
	divisorSeries := NewSeries("server10.loadavg5", vals1, 0, 1)

	ss := divideSeries(GenerateSeriesSlice(), divisorSeries)

	vals2 := make([]float64, 100)
	vals2[0] = math.NaN()
	for i := 1; i < 100; i++ {
		vals2[i] = float64(i+1) / 2.0
	}
	expected := SeriesSlice{
		NewSeries("divideSeries(server0.loadavg5,server10.loadavg5)", vals2, 0, 1),
		NewSeries("divideSeries(server1.loadavg5,server10.loadavg5)", vals2, 0, 1),
	}
	if diff := pretty.Compare(ss, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

var testDoSummarizeTests = []struct {
	desc string
	args funcArgs
	err  error
}{
	{
		"case1: correct two arguments",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1, 0.2}, 0, 1),
				},
			},
			&funcArg{
				expr: StringExpr{Literal: "20s"},
			},
		},
		nil,
	},
	{
		"case2: correct three arguments",
		funcArgs{
			&funcArg{
				expr: SeriesListExpr{Literal: "server1.loadavg5"},
				seriesSlice: SeriesSlice{
					NewSeries("server1.loadavg5", []float64{0.1, 0.2}, 0, 1),
				},
			},
			&funcArg{expr: StringExpr{Literal: "20s"}},
			&funcArg{expr: StringExpr{Literal: "avg"}},
		},
		nil,
	},
}

func TestDoSummarize(t *testing.T) {
	for _, tc := range testDoSummarizeTests {
		_, err := doSummarize(tc.args)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
	}
}

var testSummarizeTests = []struct {
	desc                string
	inputSeriesSlice    SeriesSlice
	interval            string
	function            string
	expectedSeriesSlice SeriesSlice
}{
	{
		"case1: function is sum",
		GenerateSeriesSlice(),
		"20s",
		"sum",
		SeriesSlice{
			NewSeries("summarize(server0.loadavg5, \"20s\", \"sum\")",
				[]float64{210, 610, 1010, 1410, 1810, math.NaN()}, 0, 20),
			NewSeries("summarize(server1.loadavg5, \"20s\", \"sum\")",
				[]float64{210, 610, 1010, 1410, 1810, math.NaN()}, 0, 20),
		},
	},
	{
		"case2: function is avg",
		GenerateSeriesSlice(),
		"20s",
		"avg",
		SeriesSlice{
			NewSeries("summarize(server0.loadavg5, \"20s\", \"avg\")",
				[]float64{10.5, 30.5, 50.5, 70.5, 90.5, math.NaN()}, 0, 20),
			NewSeries("summarize(server1.loadavg5, \"20s\", \"avg\")",
				[]float64{10.5, 30.5, 50.5, 70.5, 90.5, math.NaN()}, 0, 20),
		},
	},
	{
		"case3: function is last",
		GenerateSeriesSlice(),
		"20s",
		"last",
		SeriesSlice{
			NewSeries("summarize(server0.loadavg5, \"20s\", \"last\")",
				[]float64{20, 40, 60, 80, 100, math.NaN()}, 0, 20),
			NewSeries("summarize(server1.loadavg5, \"20s\", \"last\")",
				[]float64{20, 40, 60, 80, 100, math.NaN()}, 0, 20),
		},
	},
	{
		"case4: function is max",
		GenerateSeriesSlice(),
		"20s",
		"max",
		SeriesSlice{
			NewSeries("summarize(server0.loadavg5, \"20s\", \"max\")",
				[]float64{20, 40, 60, 80, 100, math.NaN()}, 0, 20),
			NewSeries("summarize(server1.loadavg5, \"20s\", \"max\")",
				[]float64{20, 40, 60, 80, 100, math.NaN()}, 0, 20),
		},
	},
	{
		"case5: function is min",
		GenerateSeriesSlice(),
		"20s",
		"min",
		SeriesSlice{
			NewSeries("summarize(server0.loadavg5, \"20s\", \"min\")",
				[]float64{1, 21, 41, 61, 81, math.NaN()}, 0, 20),
			NewSeries("summarize(server1.loadavg5, \"20s\", \"min\")",
				[]float64{1, 21, 41, 61, 81, math.NaN()}, 0, 20),
		},
	},
	{
		"case6: interval is not divisible",
		GenerateSeriesSlice(),
		"21s",
		"max",
		SeriesSlice{
			NewSeries("summarize(server0.loadavg5, \"21s\", \"max\")",
				[]float64{21, 42, 63, 84, 100, math.NaN()}, 0, 21),
			NewSeries("summarize(server1.loadavg5, \"21s\", \"max\")",
				[]float64{21, 42, 63, 84, 100, math.NaN()}, 0, 21),
		},
	},
}

func TestSummarize(t *testing.T) {
	for _, tc := range testSummarizeTests {
		got, err := summarize(tc.inputSeriesSlice, tc.interval, tc.function)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if diff := pretty.Compare(got, tc.expectedSeriesSlice); diff != "" {
			t.Fatalf("desc: %s, diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

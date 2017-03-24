package query

import (
	"math"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/pkg/errors"

	. "github.com/yuuki/diamondb/pkg/model"
	"github.com/yuuki/diamondb/pkg/storage"
)

func TestDoScale(t *testing.T) {
	var tests = []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"引数1個",
			[]*funcArg{
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries(
							"server1.loadavg5", []float64{0.1}, 100, 1,
						),
					},
				},
			},
			errors.New("scale: wrong number of arguments (1 for 2)"),
		},
		{
			"seriesSliceExpr * numberExpr",
			[]*funcArg{
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries(
							"server1.loadavg5", []float64{0.1}, 100, 1,
						),
					},
				},
				{expr: NumberExpr{Literal: 1}},
			},
			nil,
		},
		{
			"seriesSliceExpr(2) * numberExpr",
			[]*funcArg{
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries(
							"server1.loadavg5", []float64{0.1}, 100, 1,
						),
						NewSeries(
							"server2.loadavg5", []float64{0.1}, 100, 1,
						),
					},
				},
				{expr: NumberExpr{Literal: 1}},
			},
			nil,
		},
		{
			"型が違う",
			[]*funcArg{
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries(
							"server1.loadavg5", []float64{0.1}, 100, 1,
						),
					},
				},
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries(
							"server1.loadavg5", []float64{0.1}, 100, 1,
						),
					},
				},
			},
			errors.New("scale: invalid argument type (server1.loadavg5) as factor"),
		},
	}
	for _, tc := range tests {
		_, err := doScale(tc.args)
		if tc.err != nil {
			if diff := pretty.Compare(errors.Cause(err).Error(), errors.Cause(tc.err).Error()); diff != "" {
				t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
			}
		}
	}
}

func TestScale(t *testing.T) {
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
				NewSeries("scale(server1.loadavg5,0.5)", []float64{0.5, 1.0, 1.5, 2.0}, 1, 1),
				NewSeries("scale(server2.loadavg5,0.5)", []float64{math.NaN(), 2.5}, 4, 1),
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
				NewSeries("scale(server1.loadavg5,-0.5)", []float64{-0.5, -1.0, -1.5, -2.0}, 1, 1),
				NewSeries("scale(server2.loadavg5,-0.5)", []float64{math.NaN(), -2.5}, 4, 1),
			},
		},
	}
	for _, tc := range tests {
		got := scale(tc.inputSeriesSlice, tc.inputFactor)
		if diff := pretty.Compare(got, tc.expectedSeriesSlice); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestDoSumSeries(t *testing.T) {
	tests := []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"the number of arguments is one",
			[]*funcArg{
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
			[]*funcArg{
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
			[]*funcArg{&funcArg{expr: StringExpr{Literal: "server1.loadavg5"}}},
			errors.New("sumSeries: invalid argument type (server1.loadavg5)"),
		},
	}

	for _, tc := range tests {
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

func TestDoAverageSeries(t *testing.T) {
	tests := []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"the number of arguments is one",
			[]*funcArg{
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
			[]*funcArg{
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
			[]*funcArg{&funcArg{expr: StringExpr{Literal: "server1.loadavg5"}}},
			errors.New("averageSeries: invalid argument type (server1.loadavg5)"),
		},
	}

	for _, tc := range tests {
		_, err := doAverageSeries(tc.args)
		if tc.err != nil {
			if diff := pretty.Compare(err.Error(), tc.err.Error()); diff != "" {
				t.Fatalf("desc: %s, diff: (-actual +expected)\n%s", tc.desc, diff)
			}
			continue
		}
		if err != nil {
			t.Fatalf("err should be nil")
		}
	}
}

func TestDoMinSeries(t *testing.T) {
	tests := []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"the number of arguments is one",
			[]*funcArg{
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
			[]*funcArg{
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
			[]*funcArg{&funcArg{expr: StringExpr{Literal: "server1.loadavg5"}}},
			errors.New("minSeries: invalid argument type (server1.loadavg5)"),
		},
	}

	for _, tc := range tests {
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

func TestDoMaxSeries(t *testing.T) {
	tests := []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"the number of arguments is one",
			[]*funcArg{
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
			[]*funcArg{
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
			[]*funcArg{&funcArg{expr: StringExpr{Literal: "server1.loadavg5"}}},
			errors.New("maxSeries: invalid argument type (server1.loadavg5)"),
		},
	}

	for _, tc := range tests {
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

func TestDoMultiplySeries(t *testing.T) {
	tests := []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"the number of arguments is one",
			[]*funcArg{
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
			[]*funcArg{
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
			[]*funcArg{&funcArg{expr: StringExpr{Literal: "server1.loadavg5"}}},
			errors.New("multiplySeries: invalid argument type (server1.loadavg5)"),
		},
	}

	for _, tc := range tests {
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

func TestDoPercentileOfSeries(t *testing.T) {
	tests := []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"the number of arguments is one",
			[]*funcArg{
				&funcArg{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
					},
				},
			},
			errors.New("percentileOfSeries: wrong number of arguments (1 for 2,3)"),
		},
		{
			"SeriesListExpr + NumberExpr",
			[]*funcArg{
				&funcArg{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
					},
				},
				&funcArg{expr: NumberExpr{Literal: 10}},
			},
			nil,
		},
		{
			"the type of the arguments is different",
			[]*funcArg{
				&funcArg{expr: StringExpr{Literal: "hoge"}},
				&funcArg{expr: StringExpr{Literal: "foo"}},
			},
			errors.New("percentileOfSeries: invalid argument type (hoge)"),
		},
		{
			"the type of the arguments is different",
			[]*funcArg{
				&funcArg{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
					},
				},
				&funcArg{expr: StringExpr{Literal: "hoge"}},
			},
			errors.New("percentileOfSeries: invalid argument type (hoge)"),
		},
	}

	for _, tc := range tests {
		_, err := doPercentileOfSeries(tc.args)
		if tc.err != nil {
			if diff := pretty.Compare(err.Error(), tc.err.Error()); diff != "" {
				t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
			}
			continue
		}
		if err != nil {
			t.Fatalf("err should be nil: %s", err)
		}
	}
}

func TestDoGroup(t *testing.T) {
	args := []*funcArg{
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

func TestDoOffset(t *testing.T) {
	tests := []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"one argument",
			[]*funcArg{
				&funcArg{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
					},
				},
			},
			errors.New("offset: wrong number of arguments (1 for 2)"),
		},
		{
			"seriesListExpr + numberExpr",
			[]*funcArg{
				&funcArg{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
					},
				},
				&funcArg{expr: NumberExpr{Literal: 1}},
			},
			nil,
		},
		{
			"seriesSliceExpr(2) + numberExpr",
			[]*funcArg{
				&funcArg{
					expr: SeriesListExpr{Literal: "server{1,2}.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{0.1}, 100, 1),
						NewSeries("server2.loadavg5", []float64{0.1}, 100, 1),
					},
				},
				&funcArg{expr: NumberExpr{Literal: 1}},
			},
			nil,
		},
		{
			"seriesSliceExpr + seriesSliceExpr",
			[]*funcArg{
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
			errors.New("offset: invalid argument type (server2.loadavg5)"),
		},
	}

	for _, tc := range tests {
		_, err := doOffset(tc.args)
		if tc.err != nil {
			if diff := pretty.Compare(errors.Cause(err).Error(), errors.Cause(tc.err).Error()); diff != "" {
				t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
			}
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

func TestDoSummarize(t *testing.T) {
	tests := []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"case1: correct two arguments",
			[]*funcArg{
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
			[]*funcArg{
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

	for _, tc := range tests {
		_, err := doSummarize(tc.args)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
	}
}

func TestSummarize(t *testing.T) {
	tests := []struct {
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

	for _, tc := range tests {
		got, err := summarize(tc.inputSeriesSlice, tc.interval, tc.function)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if diff := pretty.Compare(got, tc.expectedSeriesSlice); diff != "" {
			t.Fatalf("desc: %s, diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestPercentileOfSeries(t *testing.T) {
	ss := SeriesSlice{
		NewSeries("server1.loadavg5", []float64{1.0, 2.0, 3.0}, 1, 1),
		NewSeries("server2.loadavg5", []float64{4.0, 5.0, 6.0}, 1, 1),
		NewSeries("server3.loadavg5", []float64{7.0, 8.0, 9.0}, 1, 1),
	}
	got := percentileOfSeries(ss, 30, false)
	expected := NewSeries(
		"percentileOfSeries(server1.loadavg5,server2.loadavg5,server3.loadavg5)",
		[]float64{4.0, 5.0, 6.0}, 1, 1,
	)
	if diff := pretty.Compare(got, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestDoSumSeriesWithWildcards(t *testing.T) {
	tests := []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"normal (SeriesListExpr + NumberExpr)",
			[]*funcArg{
				&funcArg{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{}, 0, 1),
					},
				},
				&funcArg{
					expr: NumberExpr{Literal: 1},
				},
			},
			nil,
		},
		{
			"normal (SeriesListExpr + NumberExpr x 3)",
			[]*funcArg{
				&funcArg{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{}, 0, 1),
					},
				},
				&funcArg{
					expr: NumberExpr{Literal: 1},
				},
				&funcArg{
					expr: NumberExpr{Literal: 2},
				},
				&funcArg{
					expr: NumberExpr{Literal: 3},
				},
			},
			nil,
		},
		{
			"too few arguments",
			[]*funcArg{
				&funcArg{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{}, 0, 1),
					},
				},
			},
			errors.New("sumSeriesWithWildcards: wrong number of arguments (1 for 2+)"),
		},
		{
			"the type of SeriesSlice is wrong",
			[]*funcArg{
				&funcArg{
					expr: NumberExpr{Literal: 1},
				},
				&funcArg{
					expr: NumberExpr{Literal: 2},
				},
			},
			errors.New("sumSeriesWithWildcards: invalid argument type (1)"),
		},
		{
			"the type of position is wrong",
			[]*funcArg{
				&funcArg{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{}, 0, 1),
					},
				},
				&funcArg{
					expr: StringExpr{Literal: "1"},
				},
			},
			errors.New("sumSeriesWithWildcards: invalid argument type (1)"),
		},
	}

	for _, tc := range tests {
		_, err := doSumSeriesWithWildcards(tc.args)
		if err != tc.err {
			if diff := pretty.Compare(errors.Cause(err).Error(), errors.Cause(tc.err).Error()); diff != "" {
				t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
			}
		}
	}
}

func TestSumSeriesWithWildcards(t *testing.T) {
	tests := []struct {
		desc                string
		inputSeriesSlice    SeriesSlice
		inputPositions      []int
		expectedSeriesSlice SeriesSlice
	}{
		{
			"position 0",
			SeriesSlice{
				NewSeries("server0.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("server0.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
			[]int{0},
			SeriesSlice{
				NewSeries("loadavg5", []float64{
					float64(0.1) + float64(1.1),
					float64(0.2) + float64(1.2),
					float64(0.3) + float64(1.3),
				}, 0, 1),
			},
		},
		{
			"position last",
			SeriesSlice{
				NewSeries("server0.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("server1.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
			[]int{1},
			SeriesSlice{
				NewSeries("server0", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("server1", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
		},
		{
			"position 1",
			SeriesSlice{
				NewSeries("roleA.server0.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("roleA.server1.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
			[]int{1},
			SeriesSlice{
				NewSeries("roleA.loadavg5", []float64{
					float64(0.1) + float64(1.1),
					float64(0.2) + float64(1.2),
					float64(0.3) + float64(1.3),
				}, 0, 1),
			},
		},
		{
			"position 1 + two series that position 0 is different",
			SeriesSlice{
				NewSeries("roleA.server0.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("roleB.server1.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
			[]int{1},
			SeriesSlice{
				NewSeries("roleA.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("roleB.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
		},
		{
			"position {0, last}",
			SeriesSlice{
				NewSeries("roleA.server0.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("roleA.server1.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
			[]int{0, 2},
			SeriesSlice{
				NewSeries("server0", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("server1", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
		},
		{
			"position 0,1",
			SeriesSlice{
				NewSeries("roleA.server0.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("roleA.server1.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
			[]int{0, 1},
			SeriesSlice{
				NewSeries("loadavg5", []float64{
					float64(0.1) + float64(1.1),
					float64(0.2) + float64(1.2),
					float64(0.3) + float64(1.3),
				}, 0, 1),
			},
		},
		{
			"position all",
			SeriesSlice{
				NewSeries("roleA.server0.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("roleA.server1.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
			[]int{0, 1, 2},
			SeriesSlice{
				NewSeries("", []float64{
					float64(0.1) + float64(1.1),
					float64(0.2) + float64(1.2),
					float64(0.3) + float64(1.3),
				}, 0, 1),
			},
		},
		{
			"position is out of range",
			SeriesSlice{
				NewSeries("roleA.server0.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("roleB.server1.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
			[]int{100},
			SeriesSlice{
				NewSeries("roleA.server0.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("roleB.server1.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
		},
		{
			"position includes out of range",
			SeriesSlice{
				NewSeries("roleA.server0.loadavg5", []float64{0.1, 0.2, 0.3}, 0, 1),
				NewSeries("roleA.server1.loadavg5", []float64{1.1, 1.2, 1.3}, 0, 1),
			},
			[]int{0, 1, 100},
			SeriesSlice{
				NewSeries("loadavg5", []float64{
					float64(0.1) + float64(1.1),
					float64(0.2) + float64(1.2),
					float64(0.3) + float64(1.3),
				}, 0, 1),
			},
		},
	}

	for _, tc := range tests {
		got := sumSeriesWithWildcards(tc.inputSeriesSlice, tc.inputPositions)
		if diff := pretty.Compare(got, tc.expectedSeriesSlice); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestDoLinearRegression(t *testing.T) {
	tests := []struct {
		desc string
		args []*funcArg
		err  error
	}{
		{
			"SeriesListExpr",
			[]*funcArg{
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{}, 1, 1),
					},
				},
			},
			nil,
		},
		{
			"SeriesListExpr + StringExpr",
			[]*funcArg{
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{}, 1, 1),
					},
				},
				{
					expr: StringExpr{Literal: "10"}, // unix time
				},
			},
			nil,
		},
		{
			"SeriesListExpr + StringExpr + StringExpr",
			[]*funcArg{
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{}, 1, 1),
					},
				},
				{
					expr: StringExpr{Literal: "10"}, // unix time
				},
				{
					expr: StringExpr{Literal: "50"}, // unix time
				},
			},
			nil,
		},
		{
			"the number of argument is 4",
			[]*funcArg{
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{}, 1, 1),
					},
				},
				{
					expr: StringExpr{Literal: "10"}, // unix time
				},
				{
					expr: StringExpr{Literal: "50"}, // unix time
				},
				{
					expr: StringExpr{Literal: "-1"},
				},
			},
			errors.New("linearRegression: wrong number of arguments (4 for 1,2,3)"),
		},
		{
			"the type of the arguments is different (1)",
			[]*funcArg{
				{
					expr: NumberExpr{Literal: 1.0},
				},
			},
			errors.New("linearRegression: invalid argument type (1) as SeriesSlice"),
		},
		{
			"the type of the arguments is different (2)",
			[]*funcArg{
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{}, 1, 1),
					},
				},
				{
					expr: NumberExpr{Literal: 1.0},
				},
			},
			errors.New("linearRegression: invalid argument type (1) as startSourceAt"),
		},
		{
			"the type of the arguments is different (3)",
			[]*funcArg{
				{
					expr: SeriesListExpr{Literal: "server1.loadavg5"},
					seriesSlice: SeriesSlice{
						NewSeries("server1.loadavg5", []float64{}, 1, 1),
					},
				},
				{
					expr: StringExpr{Literal: "10"},
				},
				{
					expr: NumberExpr{Literal: 1.0},
				},
			},
			errors.New("linearRegression: invalid argument type (1) as endSourceAt"),
		},
	}

	ff := &storage.FakeReadWriter{
		FakeFetch: func(name string, start, end time.Time) (SeriesSlice, error) {
			return SeriesSlice{}, nil
		},
	}

	for _, tc := range tests {
		_, err := doLinerRegression(ff, tc.args, time.Unix(100, 0), time.Unix(200, 0))
		if err != tc.err {
			if diff := pretty.Compare(errors.Cause(err).Error(), errors.Cause(tc.err).Error()); diff != "" {
				t.Errorf("desc: %s, diff: (-actual +expected)\n%s", tc.desc, diff)
			}
		}
	}
}

func TestLinearRegression_EvalTargetsErr(t *testing.T) {
	ff := &storage.FakeReadWriter{
		FakeFetch: func(name string, start, end time.Time) (SeriesSlice, error) {
			return nil, errors.New("something occurs")
		},
	}
	_, err := linearRegression(ff, SeriesSlice{
		NewSeries("server1.loadavg5", []float64{}, 1, 1),
	}, time.Unix(0, 0), time.Unix(1, 0))
	if err == nil {
		t.Fatalf("should raise error %v", err)
	}
}

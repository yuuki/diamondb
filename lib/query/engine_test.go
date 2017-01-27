package query

import (
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/pkg/errors"

	. "github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage"
)

func TestEvalTarget_Func(t *testing.T) {
	fakefetcher := &storage.FakeFetcher{
		FakeFetch: func(name string, start, end time.Time) (SeriesSlice, error) {
			return SeriesSlice{
				NewSeries("server1.loadavg5", []float64{10.0, 11.0}, 1000, 60),
			}, nil
		},
	}

	seriesSlice, err := EvalTarget(
		fakefetcher,
		"alias(server1.loadavg5,\"server01.loadavg5\")",
		time.Unix(0, 0),
		time.Unix(120, 0),
	)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	s1 := NewSeries("server1.loadavg5", []float64{10.0, 11.0}, 1000, 60).SetAliasWith(
		"server01.loadavg5",
	)
	expected := SeriesSlice{s1}
	if diff := pretty.Compare(seriesSlice, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestEvalTarget_FuncNest(t *testing.T) {
	fakefetcher := &storage.FakeFetcher{
		FakeFetch: func(name string, start, end time.Time) (SeriesSlice, error) {
			return SeriesSlice{
				NewSeries("server1.loadavg5", []float64{10.0, 11.0}, 1000, 60),
			}, nil
		},
	}

	seriesSlice, err := EvalTarget(
		fakefetcher,
		"alias(alias(server1.loadavg5,\"server01.loadavg5\"),\"server001.loadavg5\")",
		time.Unix(0, 0),
		time.Unix(120, 0),
	)

	if err != nil {
		t.Fatalf("err: %s", err)
	}
	s1 := NewSeries("server1.loadavg5", []float64{10.0, 11.0}, 1000, 60).SetAliasWith(
		"server001.loadavg5",
	)
	expected := SeriesSlice{s1}
	if diff := pretty.Compare(seriesSlice, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestEvalTarget_GroupSeries(t *testing.T) {
	expected := SeriesSlice{
		NewSeries("server1.loadavg5", []float64{10.0, 11.0}, 1000, 60),
		NewSeries("server2.loadavg5", []float64{12.0, 13.0}, 1000, 60),
	}
	fakefetcher := &storage.FakeFetcher{
		FakeFetch: func(name string, start, end time.Time) (SeriesSlice, error) {
			if name != "server1.loadavg5,server2.loadavg5" {
				return nil, errors.Errorf("unexpected name: %s", name)
			}
			return expected, nil
		},
	}
	got, err := EvalTarget(
		fakefetcher,
		"server{1,2}.loadavg5",
		time.Unix(0, 0),
		time.Unix(120, 0),
	)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if diff := pretty.Compare(got, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

var testEvalTargetFuncTests = []struct {
	desc          string
	target        string
	mockSeriesMap SeriesSlice
	expected      SeriesSlice
}{
	{
		"the number of arguments is one",
		"sumSeries(server1.loadavg5)",
		SeriesSlice{NewSeries("server1.loadavg5", []float64{10.0}, 1000, 60)},
		SeriesSlice{NewSeries("server1.loadavg5", []float64{10.0}, 1000, 60)},
	},
	{
		"the number of arguments is two",
		"sumSeries(server1.loadavg5,server2.loadavg5)",
		SeriesSlice{
			NewSeries("server1.loadavg5", []float64{10.0}, 1000, 60),
			NewSeries("server2.loadavg5", []float64{10.0}, 1000, 60),
		},
		SeriesSlice{
			NewSeries("sumSerie(server1.loadavg5,server2.loadavg5)", []float64{20.0}, 1000, 60),
		},
	},
	{
		"the type of arguments is group series.",
		"sumSeries(server{1,2}.loadavg5)",
		SeriesSlice{
			NewSeries("server1.loadavg5", []float64{10.0}, 1000, 60),
			NewSeries("server2.loadavg5", []float64{10.0}, 1000, 60),
		},
		SeriesSlice{
			// The original specification is "sumSeries(server{1,2}.loadavg5)"
			NewSeries("sumSeries(server1.loadavg5,server2.loadavg5)", []float64{20.0}, 100, 60),
		},
	},
	{
		"function is nested",
		"sumSeries(sumSeries(server1.loadavg5))",
		SeriesSlice{
			NewSeries("server1.loadavg5", []float64{10.0}, 1000, 60),
		},
		SeriesSlice{
			NewSeries(
				"sumSeries(sumSeries(server1.loadavg5))",
				[]float64{10.0}, 1000, 60,
			),
		},
	},
}

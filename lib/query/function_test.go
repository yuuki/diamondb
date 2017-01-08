package query

import (
	"math"
	"testing"

	"github.com/kylelemons/godebug/pretty"
	. "github.com/yuuki/diamondb/lib/series"
)

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

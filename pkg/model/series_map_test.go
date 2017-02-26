package model

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestSeriesMapSortedNames(t *testing.T) {
	sm := SeriesMap{
		"server3.loadavg5": NewSeriesPoint("server2.loadavg5", DataPoints{}, 60),
		"server1.loadavg5": NewSeriesPoint("server1.loadavg5", DataPoints{}, 60),
		"server2.loadavg5": NewSeriesPoint("server2.loadavg5", DataPoints{}, 60),
	}
	expected := []string{"server1.loadavg5", "server2.loadavg5", "server3.loadavg5"}
	if diff := pretty.Compare(sm.SortedNames(), expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestSeriesMapMerge(t *testing.T) {
	sm1 := SeriesMap{
		"server1.loadavg5": NewSeriesPoint("server1.loadavg5", DataPoints{}, 60),
		"server2.loadavg5": NewSeriesPoint("server2.loadavg5", DataPoints{}, 60),
	}
	sm2 := SeriesMap{
		"server1.loadavg5": NewSeriesPoint("server1.loadavg5", DataPoints{}, 60),
		"server3.loadavg5": NewSeriesPoint("server3.loadavg5", DataPoints{}, 60),
		"server4.loadavg5": NewSeriesPoint("server4.loadavg5", DataPoints{}, 60),
	}
	expected := SeriesMap{
		"server1.loadavg5": NewSeriesPoint("server1.loadavg5", DataPoints{}, 60),
		"server2.loadavg5": NewSeriesPoint("server2.loadavg5", DataPoints{}, 60),
		"server3.loadavg5": NewSeriesPoint("server3.loadavg5", DataPoints{}, 60),
		"server4.loadavg5": NewSeriesPoint("server4.loadavg5", DataPoints{}, 60),
	}
	sm := sm1.Merge(sm2)
	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestMergePointsToMap(t *testing.T) {
	sm1 := SeriesMap{
		"server1.loadavg5": NewSeriesPoint("server1.loadavg5", DataPoints{
			NewDataPoint(1000, 0.1),
			NewDataPoint(1060, 0.2),
			NewDataPoint(1120, 0.3),
		}, 60),
	}
	sm2 := SeriesMap{
		"server1.loadavg5": NewSeriesPoint("server1.loadavg5", DataPoints{
			NewDataPoint(1120, 0.1),
			NewDataPoint(1180, 0.2),
			NewDataPoint(1240, 0.3),
		}, 60),
		"server2.loadavg5": NewSeriesPoint("server2.loadavg5", DataPoints{
			NewDataPoint(1120, 0.1),
		}, 60),
	}

	sm := sm1.MergePointsToMap(sm2)

	expected := SeriesMap{
		"server1.loadavg5": NewSeriesPoint("server1.loadavg5", DataPoints{
			NewDataPoint(1000, 0.1),
			NewDataPoint(1060, 0.2),
			NewDataPoint(1120, 0.1),
			NewDataPoint(1180, 0.2),
			NewDataPoint(1240, 0.3),
		}, 60),
		"server2.loadavg5": NewSeriesPoint("server2.loadavg5", DataPoints{
			NewDataPoint(1120, 0.1),
		}, 60),
	}

	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestMergePointsToSlice(t *testing.T) {
	sm1 := SeriesMap{
		"server1.loadavg5": NewSeriesPoint("server1.loadavg5", DataPoints{
			NewDataPoint(1000, 0.1),
			NewDataPoint(1060, 0.2),
			NewDataPoint(1120, 0.3),
		}, 60),
	}
	sm2 := SeriesMap{
		"server1.loadavg5": NewSeriesPoint("server1.loadavg5", DataPoints{
			NewDataPoint(1120, 0.1),
			NewDataPoint(1180, 0.2),
			NewDataPoint(1240, 0.3),
		}, 60),
		"server2.loadavg5": NewSeriesPoint("server2.loadavg5", DataPoints{
			NewDataPoint(1120, 0.1),
		}, 60),
	}

	sl := sm1.MergePointsToSlice(sm2)

	expected := SeriesSlice{
		NewSeries("server1.loadavg5", []float64{0.1, 0.2, 0.1, 0.2, 0.3}, 960, 60),
		NewSeries("server2.loadavg5", []float64{0.1}, 1080, 60),
	}
	if diff := pretty.Compare(sl, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

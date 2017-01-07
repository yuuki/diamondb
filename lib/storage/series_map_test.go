package storage

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
	"github.com/yuuki/diamondb/lib/series"
)

func TestSeriesMapSortedNames(t *testing.T) {
	sm := seriesMap{
		"server3.loadavg5": newSeriesPoint("server2.loadavg5", datapoints{}, 60),
		"server1.loadavg5": newSeriesPoint("server1.loadavg5", datapoints{}, 60),
		"server2.loadavg5": newSeriesPoint("server2.loadavg5", datapoints{}, 60),
	}
	expected := []string{"server1.loadavg5", "server2.loadavg5", "server3.loadavg5"}
	if diff := pretty.Compare(sm.SortedNames(), expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestSeriesMapMerge(t *testing.T) {
	sm1 := seriesMap{
		"server1.loadavg5": newSeriesPoint("server1.loadavg5", datapoints{}, 60),
		"server2.loadavg5": newSeriesPoint("server2.loadavg5", datapoints{}, 60),
	}
	sm2 := seriesMap{
		"server1.loadavg5": newSeriesPoint("server1.loadavg5", datapoints{}, 60),
		"server3.loadavg5": newSeriesPoint("server3.loadavg5", datapoints{}, 60),
		"server4.loadavg5": newSeriesPoint("server4.loadavg5", datapoints{}, 60),
	}
	expected := seriesMap{
		"server1.loadavg5": newSeriesPoint("server1.loadavg5", datapoints{}, 60),
		"server2.loadavg5": newSeriesPoint("server2.loadavg5", datapoints{}, 60),
		"server3.loadavg5": newSeriesPoint("server3.loadavg5", datapoints{}, 60),
		"server4.loadavg5": newSeriesPoint("server4.loadavg5", datapoints{}, 60),
	}
	sm := sm1.Merge(sm2)
	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestMergePointsToMap(t *testing.T) {
	sm1 := seriesMap{
		"server1.loadavg5": newSeriesPoint("server1.loadavg5", datapoints{
			newDataPoint(1000, 0.1),
			newDataPoint(1060, 0.2),
			newDataPoint(1120, 0.3),
		}, 60),
	}
	sm2 := seriesMap{
		"server1.loadavg5": newSeriesPoint("server1.loadavg5", datapoints{
			newDataPoint(1120, 0.1),
			newDataPoint(1180, 0.2),
			newDataPoint(1240, 0.3),
		}, 60),
		"server2.loadavg5": newSeriesPoint("server2.loadavg5", datapoints{
			newDataPoint(1120, 0.1),
		}, 60),
	}

	sm := sm1.MergePointsToMap(sm2)

	expected := seriesMap{
		"server1.loadavg5": newSeriesPoint("server1.loadavg5", datapoints{
			newDataPoint(1000, 0.1),
			newDataPoint(1060, 0.2),
			newDataPoint(1120, 0.3), // TODO remove duplicated timestamp
			newDataPoint(1120, 0.1),
			newDataPoint(1180, 0.2),
			newDataPoint(1240, 0.3),
		}, 60),
		"server2.loadavg5": newSeriesPoint("server2.loadavg5", datapoints{
			newDataPoint(1120, 0.1),
		}, 60),
	}

	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestMergePointsToSlice(t *testing.T) {
	sm1 := seriesMap{
		"server1.loadavg5": newSeriesPoint("server1.loadavg5", datapoints{
			newDataPoint(1000, 0.1),
			newDataPoint(1060, 0.2),
			newDataPoint(1120, 0.3),
		}, 60),
	}
	sm2 := seriesMap{
		"server1.loadavg5": newSeriesPoint("server1.loadavg5", datapoints{
			newDataPoint(1120, 0.1),
			newDataPoint(1180, 0.2),
			newDataPoint(1240, 0.3),
		}, 60),
		"server2.loadavg5": newSeriesPoint("server2.loadavg5", datapoints{
			newDataPoint(1120, 0.1),
		}, 60),
	}

	sl := sm1.MergePointsToSlice(sm2)

	expected := series.SeriesSlice{
		series.NewSeries("server1.loadavg5", []float64{0.1, 0.2, 0.3, 0.1, 0.2, 0.3}, 1000, 60),
		series.NewSeries("server2.loadavg5", []float64{0.1}, 1120, 60),
	}
	if diff := pretty.Compare(sl, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

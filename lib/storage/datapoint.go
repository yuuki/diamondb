package storage

import (
	"fmt"
	"sort"
	"strconv"
)

type datapoint struct {
	timestamp int64
	value     float64
}

func newDataPoint(t int64, v float64) *datapoint {
	return &datapoint{
		timestamp: t,
		value:     v,
	}
}

func (d *datapoint) Timestamp() int64 {
	return d.timestamp
}

func (d *datapoint) Value() float64 {
	return d.value
}

type datapoints []*datapoint

func (ds datapoints) Len() int {
	return len(ds)
}

func (ds datapoints) Swap(i, j int) {
	ds[i], ds[j] = ds[j], ds[i]
}

func (ds datapoints) Less(i, j int) bool {
	return ds[i].Timestamp() < ds[j].Timestamp()
}

func (ds datapoints) Sort() datapoints {
	sort.Sort(ds)
	return ds
}

func (ds datapoints) Deduplicate() datapoints {
	deduplicated := make(map[string]float64, ds.Len())
	for _, d := range ds {
		deduplicated[fmt.Sprintf("%d", d.Timestamp())] = d.Value()
	}
	points := make(datapoints, 0, len(deduplicated))
	for ts, v := range deduplicated {
		t, _ := strconv.ParseInt(ts, 10, 64)
		points = append(points, newDataPoint(t, v))
	}
	return points.Sort()
}

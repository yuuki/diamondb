package series

import (
	"fmt"
	"sort"
	"strconv"
)

type DataPoint struct {
	timestamp int64
	value     float64
}

func NewDataPoint(t int64, v float64) *DataPoint {
	return &DataPoint{
		timestamp: t,
		value:     v,
	}
}

func (d *DataPoint) Timestamp() int64 {
	return d.timestamp
}

func (d *DataPoint) Value() float64 {
	return d.value
}

type DataPoints []*DataPoint

func (ds DataPoints) Len() int {
	return len(ds)
}

func (ds DataPoints) Swap(i, j int) {
	ds[i], ds[j] = ds[j], ds[i]
}

func (ds DataPoints) Less(i, j int) bool {
	return ds[i].Timestamp() < ds[j].Timestamp()
}

func (ds DataPoints) Sort() DataPoints {
	sort.Sort(ds)
	return ds
}

func (ds DataPoints) Deduplicate() DataPoints {
	deduplicated := make(map[string]float64, ds.Len())
	for _, d := range ds {
		deduplicated[fmt.Sprintf("%d", d.Timestamp())] = d.Value()
	}
	points := make(DataPoints, 0, len(deduplicated))
	for ts, v := range deduplicated {
		t, _ := strconv.ParseInt(ts, 10, 64)
		points = append(points, NewDataPoint(t, v))
	}
	return points.Sort()
}

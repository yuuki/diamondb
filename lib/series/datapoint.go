package series

import (
	"fmt"
	"sort"
	"strconv"
)

// DataPoint represents a pair of metric timestamp and value.
type DataPoint struct {
	timestamp int64 // UNIX Timestamp
	value     float64
}

// NewDataPoint returns the pointer of the DataPoint object.
func NewDataPoint(t int64, v float64) *DataPoint {
	return &DataPoint{
		timestamp: t,
		value:     v,
	}
}

// Timestamp returns timestamp.
func (d *DataPoint) Timestamp() int64 {
	return d.timestamp
}

// Value returns value.
func (d *DataPoint) Value() float64 {
	return d.value
}

// DataPoints represents the slice of pointer of DataPoint
type DataPoints []*DataPoint

// Len returns DataPoints length.
func (ds DataPoints) Len() int {
	return len(ds)
}

// Swap is for Sort interface.
func (ds DataPoints) Swap(i, j int) {
	ds[i], ds[j] = ds[j], ds[i]
}

// Less is for Sort interface.
func (ds DataPoints) Less(i, j int) bool {
	return ds[i].Timestamp() < ds[j].Timestamp()
}

// Sort sorts DataPoints in ascending order of timestamps.
func (ds DataPoints) Sort() DataPoints {
	sort.Sort(ds)
	return ds
}

// Deduplicate eliminates duplications of DataPoints with the same timestamp.
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

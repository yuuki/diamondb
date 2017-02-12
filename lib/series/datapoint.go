package series

import (
	"encoding/json"
	"math"
	"sort"
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

// MarshalJSON marshals DataPoint into JSON.
func (d *DataPoint) MarshalJSON() ([]byte, error) {
	if math.IsNaN(d.Value()) {
		return json.Marshal([]interface{}{nil, d.timestamp})
	}
	return json.Marshal([]interface{}{d.value, d.timestamp})
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
	deduplicated := make(map[int64]float64, ds.Len())
	for _, d := range ds {
		key := d.Timestamp()
		if _, ok := deduplicated[key]; ok {
			// Don't overwrite with NaN value
			if math.IsNaN(d.Value()) {
				continue
			}
		}
		deduplicated[key] = d.Value()
	}
	points := make(DataPoints, 0, len(deduplicated))
	for t, v := range deduplicated {
		points = append(points, NewDataPoint(t, v))
	}
	return points.Sort()
}

// AlignTimestamp aligns each timestamp into multiples of step with DataPoints.
func (ds DataPoints) AlignTimestamp(step int) DataPoints {
	for _, d := range ds {
		d.timestamp -= d.timestamp % int64(step)
	}
	return ds
}

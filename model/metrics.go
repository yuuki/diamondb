package model

import (
	"fmt"
	"time"
)

type DataPoint struct {
	Timestamp	int32
	Value		float64
}

type Metric struct {
	Name		string
	DataPoints	[]*DataPoint
	Step            time.Duration  // seconds
	Start           int32
	End		int32
}

func NewDataPoint(ts int32, value float64) *DataPoint {
	return &DataPoint{Timestamp: ts, Value: value}
}

func (d *DataPoint) String() string {
	return fmt.Sprintf("datapoint timestamp=%d, value=%f", d.Timestamp, d.Value)
}

func NewMetric(name string, datapoint []*DataPoint) *Metric {
	return &Metric{Name: name, DataPoints: datapoint}
}

func (m *Metric) Count() int {
	return len(m.DataPoints)
}

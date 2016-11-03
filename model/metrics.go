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

type ViewMetric struct {
	Target		string		`json:"target"`
	DataPoints	[][]interface{}	`json:"datapoints"`
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


/*
An example of json response
{
    "target": "server1.cpu.softirq.percentage",
    "datapoints": [
      [
        0.244669050464,
        1474725188
      ],
      [
        0.236104685209,
        1474725248
      ],
}
*/

// AsResponse converts Metric into ViewMetric type
func (m *Metric) AsResponse() *ViewMetric {
	datapoints := make([][]interface{}, 0, len(m.DataPoints))
	for _, dp := range m.DataPoints {
		p := make([]interface{}, 2)
		p[0], p[1] = dp.Value, dp.Timestamp
		datapoints = append(datapoints, p)
	}
	return &ViewMetric{Target: m.Name, DataPoints: datapoints}
}

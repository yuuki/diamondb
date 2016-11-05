package model

import (
	"fmt"
	"time"
	"sort"
)

type DataPoint struct {
	Timestamp	int32
	Value		float64
}

type ByTimestamp []*DataPoint

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

func (d ByTimestamp) Len() int {
	return len(d)
}

func (d ByTimestamp) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d ByTimestamp) Less(i, j int) bool {
	return d[i].Timestamp < d[j].Timestamp
}

func NewMetric(name string, datapoints []*DataPoint, step int) *Metric {
	if len(datapoints) < 1 {
		return &Metric{
			Name: name,
			DataPoints: datapoints,
			Step: time.Duration(step)*time.Second,
		}
	}

	// Stable (Insertion Sort) is faster than Sort
	// because datapoints is expected to roughly be sorted
	sort.Stable(ByTimestamp(datapoints))
	start, end := datapoints[0].Timestamp, datapoints[len(datapoints)-1].Timestamp
	return &Metric{
		Name: name,
		DataPoints: datapoints,
		Step: time.Duration(step)*time.Second,
		Start: start,
		End: end,
	}
}

func NewEmptyMetric() *Metric {
	return &Metric{Name: "", DataPoints: []*DataPoint{}}
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

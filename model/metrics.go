package model

import (
	"fmt"
	"sort"
)

type DataPoint struct {
	Timestamp	uint64
	Value		float64
}

type ByTimestamp []*DataPoint

type Metric struct {
	Name		string
	DataPoints	[]*DataPoint
	Step            int  // seconds
	Start           uint64
	End		uint64
}

type ViewMetric struct {
	Target		string		`json:"target"`
	DataPoints	[][]interface{}	`json:"datapoints"`
}

func NewDataPoint(ts uint64, value float64) *DataPoint {
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
			Step: step,
		}
	}

	// Stable (Insertion Sort) is faster than Sort
	// because datapoints is expected to roughly be sorted
	sort.Stable(ByTimestamp(datapoints))
	start, end := datapoints[0].Timestamp, datapoints[len(datapoints)-1].Timestamp

	return &Metric{
		Name: name,
		DataPoints: datapoints,
		Step: step,
		Start: start,
		End: end,
	}
}

func NewEmptyMetric() *Metric {
	return &Metric{Name: "", DataPoints: []*DataPoint{}}
}

func (m *Metric) FilledWithNil() *Metric {
	for i := 0; i < len(m.DataPoints); i++ {
		p := m.DataPoints[i]
		if p.Timestamp > (m.Start + uint64(m.Step*i)) {
			m.insertDatapoint(i, nil)
		}
	}
	return m
}

func (m *Metric) insertDatapoint(i int, p *DataPoint) {
	m.DataPoints = append(m.DataPoints, &DataPoint{})
	copy(m.DataPoints[i+1:], m.DataPoints[i:])
	m.DataPoints[i] = p
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
	for i, dp := range m.DataPoints {
		p := make([]interface{}, 2)
		if dp == nil {
			p[0], p[1] = nil, m.Start + uint64(m.Step*i)
		} else {
			p[0], p[1] = dp.Value, dp.Timestamp
		}
		datapoints = append(datapoints, p)
	}
	return &ViewMetric{Target: m.Name, DataPoints: datapoints}
}

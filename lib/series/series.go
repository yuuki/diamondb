package series

import "math"

type Series interface {
	Name() string
	Values() []float64
	Start() int64
	End() int64
	Step() int
	Len() int
	SetAlias(s string)
	SetAliasWith(s string) Series
	Alias() string
	AsResp() *SeriesResp
	Points() DataPoints
}

type series struct {
	name   string
	values []float64
	start  int64
	step   int
	alias  string
}

func NewSeries(name string, values []float64, start int64, step int) Series {
	return &series{
		name:   name,
		values: values,
		start:  start,
		step:   step,
	}
}

func (s *series) Name() string {
	return s.name
}

func (s *series) Values() []float64 {
	return s.values
}

func (s *series) Start() int64 {
	return s.start
}

func (s *series) End() int64 {
	return s.Start() + int64(s.Step()*(s.Len()-1))
}

func (s *series) Step() int {
	return s.step
}

func (s *series) Len() int {
	return len(s.Values())
}

func (s *series) SetAlias(a string) {
	s.alias = a
}

func (s *series) SetAliasWith(a string) Series {
	s.alias = a
	return s
}

func (s *series) Alias() string {
	if s.alias == "" {
		return s.Name()
	}
	return s.alias
}

func (s *series) Points() DataPoints {
	points := make(DataPoints, 0, s.Len())
	end := s.End()
	vals := s.Values()
	i := 0
	for t := s.Start(); t <= end; t += int64(s.Step()) {
		points = append(points, NewDataPoint(t, vals[i]))
		i += 1
	}
	return points
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

type SeriesResp struct {
	Target     string          `json:"target"`
	DataPoints [][]interface{} `json:"datapoints"`
}

func (s *series) AsResp() *SeriesResp {
	points := make([][]interface{}, 0, s.Len())
	for i, v := range s.Values() {
		timestamp := s.Start() + int64(s.Step()*i)
		if math.IsNaN(v) {
			points = append(points, []interface{}{nil, timestamp})
		} else {
			points = append(points, []interface{}{v, timestamp})
		}
	}
	return &SeriesResp{Target: s.Alias(), DataPoints: points}
}

package model

import "encoding/json"

// Series represents time series.
type Series struct {
	name   string
	values []float64
	start  int64 // timestamp of start.
	step   int   // the interval seconds of values.
	alias  string
}

// NewSeries returns the Series object.
func NewSeries(name string, values []float64, start int64, step int) *Series {
	return &Series{
		name:   name,
		values: values,
		start:  start,
		step:   step,
	}
}

// Name returns the name.
func (s *Series) Name() string {
	return s.name
}

// Values returns the values.
func (s *Series) Values() []float64 {
	return s.values
}

// Start returns the start timestamp.
func (s *Series) Start() int64 {
	return s.start
}

// End returns the end timestamp.
func (s *Series) End() int64 {
	if s.Len() == 0 {
		return -1
	}
	return s.Start() + int64(s.Step()*(s.Len()-1))
}

// Step returns the step.
func (s *Series) Step() int {
	return s.step
}

// Len returns the length of series.
func (s *Series) Len() int {
	return len(s.Values())
}

// SetName sets the name
func (s *Series) SetName(name string) {
	s.name = name
}

// SetAlias set alias with a.
func (s *Series) SetAlias(a string) {
	s.alias = a
}

// SetAliasWith set alias with a and return the pointer of series.
func (s *Series) SetAliasWith(a string) *Series {
	s.alias = a
	return s
}

// Alias returns the alias.
func (s *Series) Alias() string {
	if s.alias == "" {
		return s.Name()
	}
	return s.alias
}

// Points returns DataPoints converted from values.
func (s *Series) Points() DataPoints {
	if s.Len() == 0 {
		return DataPoints{}
	}
	points := make(DataPoints, 0, s.Len())
	end := s.End()
	vals := s.Values()
	i := 0
	for t := s.Start(); t <= end; t += int64(s.Step()) {
		points = append(points, NewDataPoint(t, vals[i]))
		i++
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

// jsonMarshallableSeries represents the JSON response structure for Series.
type jsonMarshallableSeries struct {
	Target     string     `json:"target"`
	DataPoints DataPoints `json:"datapoints"`
}

// MarshalJSON marshals Series as JSON.
func (s *Series) MarshalJSON() ([]byte, error) {
	return json.Marshal(&jsonMarshallableSeries{
		Target:     s.Alias(),
		DataPoints: s.Points(),
	})
}

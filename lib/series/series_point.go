package series

import (
	"math"
)

// SeriesPoint represents a series having not only value but DataPoints.
type SeriesPoint struct {
	name   string
	points DataPoints
	step   int
}

// NewSeriesPoint creates a new SeriesPoint. The points is sorted by the timestamp and
// deduplicated with the same timestamp.
func NewSeriesPoint(name string, points DataPoints, step int) *SeriesPoint {
	points = points.AlignTimestamp(step).Sort().Deduplicate()
	return &SeriesPoint{
		name:   name,
		points: points,
		step:   step,
	}
}

// Name returns the name.
func (s *SeriesPoint) Name() string {
	return s.name
}

// Points returns the points.
func (s *SeriesPoint) Points() DataPoints {
	return s.points
}

// Values convertes the values with []float64 format.
func (s *SeriesPoint) Values() []float64 {
	points := s.Points()
	vals := make([]float64, points.Len())
	for i := range vals {
		vals[i] = math.NaN() // NaN reprensents 'lack of data point'
	}
	for i, p := range points {
		if p.Timestamp() == (s.Start() + int64(s.Step()*i)) {
			vals[i] = p.Value()
		}
	}
	return vals
}

// Start returns the unix timestamp of the beginning of the data points.
func (s *SeriesPoint) Start() int64 {
	return s.Points()[0].Timestamp()
}

// End returns the unix timestamp of the end of the data points.
func (s *SeriesPoint) End() int64 {
	return s.Start() + int64(s.Step()*(s.Len()-1))
}

// Step returns the step.
func (s *SeriesPoint) Step() int {
	return s.step
}

// Len returns the length of the points.
func (s *SeriesPoint) Len() int {
	return s.Points().Len()
}

// ToSeries converts s into Series.
func (s *SeriesPoint) ToSeries() Series {
	return NewSeries(s.Name(), s.Values(), s.Start(), s.Step())
}

package series

import (
	"math"
)

type SeriesPoint struct {
	name   string
	points DataPoints
	step   int
}

func NewSeriesPoint(name string, points DataPoints, step int) *SeriesPoint {
	return &SeriesPoint{
		name:   name,
		points: points.Sort().Deduplicate(),
		step:   step,
	}
}

func (s *SeriesPoint) Name() string {
	return s.name
}

func (s *SeriesPoint) Points() DataPoints {
	return s.points
}

func (s *SeriesPoint) Values() []float64 {
	points := s.Points()
	vals := make([]float64, points.Len())
	for i, _ := range vals {
		vals[i] = math.NaN() // NaN reprensents 'lack of data point'
	}
	for i, p := range points {
		if p.Timestamp() == (s.Start() + int64(s.Step()*i)) {
			vals[i] = p.Value()
		}
	}
	return vals
}

func (s *SeriesPoint) Start() int64 {
	return s.Points()[0].Timestamp()
}

func (s *SeriesPoint) End() int64 {
	return s.Start() + int64(s.Step()*(s.Len()-1))
}

func (s *SeriesPoint) Step() int {
	return s.step
}

func (s *SeriesPoint) Len() int {
	return s.Points().Len()
}

func (s *SeriesPoint) ToSeries() Series {
	return NewSeries(s.Name(), s.Values(), s.Start(), s.Step())
}

package storage

import (
	"math"

	"github.com/yuuki/diamondb/lib/series"
)

type seriesPoint struct {
	name   string
	points datapoints
	step   int
}

func newSeriesPoint(name string, points datapoints, step int) *seriesPoint {
	return &seriesPoint{
		name:   name,
		points: points.Sort(),
		step:   step,
	}
}

func (s *seriesPoint) Name() string {
	return s.name
}

func (s *seriesPoint) Points() datapoints {
	return s.points
}

func (s *seriesPoint) Values() []float64 {
	points := s.Points().Deduplicate()
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

func (s *seriesPoint) Start() int64 {
	return s.Points()[0].Timestamp()
}

func (s *seriesPoint) End() int64 {
	return s.Start() + int64(s.Step()*(s.Len()-1))
}

func (s *seriesPoint) Step() int {
	return s.step
}

func (s *seriesPoint) Len() int {
	return s.Points().Len()
}

func (s *seriesPoint) ToSeries() series.Series {
	return series.NewSeries(s.Name(), s.Values(), s.Start(), s.Step())
}

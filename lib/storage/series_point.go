package storage

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
	vals := make([]float64, s.Len())
	for i, p := range s.Points() {
		vals[i] = p.value
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

package series

type Series interface {
	Name() string
	Values() []float64
	Start() int64
	End() int64
	Step() int
	Len() int
}

type series struct {
	name   string
	values []float64
	start  int64
	step   int
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

package series

func GenerateSeriesSlice() SeriesSlice {
	values1 := make([]float64, 0, 100)
	for i := 0; i < 100; i++ {
		v := float64(i + 1)
		values1 = append(values1, v)
	}
	values2 := make([]float64, 0, 100)
	for i := 0; i < 100; i++ {
		v := float64(i + 1)
		values2 = append(values2, v)
	}
	ss := SeriesSlice{
		NewSeries("server0.loadavg5", values1, int64(0), 1),
		NewSeries("server1.loadavg5", values2, int64(0), 1),
	}
	return ss
}

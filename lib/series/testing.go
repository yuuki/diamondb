package series

func NewFloat64PointerSlice(vals []float64) []*float64 {
	p := make([]*float64, 0, len(vals))
	for _, v := range vals {
		p = append(p, &v)
	}
	return p
}

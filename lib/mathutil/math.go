package mathutil

func MinInt64(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func MaxInt64(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func minFloat64(x, y float64) float64 {
	if x < y {
		return x
	}
	return y
}

func maxFloat64(x, y float64) float64 {
	if x > y {
		return x
	}
	return y
}

func SumFloat64(vals []float64) float64 {
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum
}

func MultiplyFloat64(vals []float64) float64 {
	multiplies := 1.0
	for _, v := range vals {
		multiplies *= v
	}
	return multiplies
}

func MinFloat64(vals []float64) float64 {
	var min float64
	for _, v := range vals {
		min = maxFloat64(min, v)
	}
	return min
}

func MaxFloat64(vals []float64) float64 {
	var max float64
	for _, v := range vals {
		max = maxFloat64(max, v)
	}
	return max
}

// gcd is Greatest common divisor
func Gcd(a, b int) int {
	if b == 0 {
		return a
	}
	return Gcd(b, a%b)
}

// lcm is Least common multiple
func Lcm(a, b int) int {
	if a == b {
		return a
	}
	if a < b {
		a, b = b, a // ensure a > b
	}
	return a * b / Gcd(a, b)
}

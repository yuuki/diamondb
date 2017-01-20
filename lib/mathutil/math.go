package mathutil

import "math"

func notNaNVals(vals []float64) []float64 {
	newVals := make([]float64, 0, len(vals))
	for _, v := range vals {
		if !math.IsNaN(v) {
			newVals = append(newVals, v)
		}
	}
	return newVals
}

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

func SumFloat64(vals []float64) float64 {
	vals = notNaNVals(vals)
	if len(vals) < 1 {
		return math.NaN()
	}

	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum
}

func MultiplyFloat64(vals []float64) float64 {
	vals = notNaNVals(vals)
	if len(vals) < 1 {
		return math.NaN()
	}

	multiplies := float64(1.0)
	for _, v := range vals {
		multiplies *= v
	}
	return multiplies
}

func DivideFloat64(x float64, y float64) float64 {
	if math.IsNaN(x) || math.IsNaN(y) || y == 0.0 {
		return math.NaN()
	}
	return x / y
}

func MinFloat64(vals []float64) float64 {
	vals = notNaNVals(vals)
	if len(vals) < 1 {
		return math.NaN()
	}

	min := math.MaxFloat64
	for _, v := range vals {
		min = math.Min(min, v)
	}
	return min
}

func MaxFloat64(vals []float64) float64 {
	vals = notNaNVals(vals)
	if len(vals) < 1 {
		return math.NaN()
	}

	var max float64
	for _, v := range vals {
		max = math.Max(max, v)
	}
	return max
}

func AvgFloat64(vals []float64) float64 {
	vals = notNaNVals(vals)
	if len(vals) < 1 {
		return math.NaN()
	}
	sum := SumFloat64(vals)
	if math.IsNaN(sum) {
		return math.NaN()
	}
	return sum / float64(len(vals))
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

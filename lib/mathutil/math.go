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

// MinInt64 returns the smaller of x or y.
func MinInt64(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

// MaxInt64 returns the larger of x or y.
func MaxInt64(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

// SumFloat64 returns the sum of vals.
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

// MultiplyFloat64 returns the multiplied value by vals.
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

// DivideFloat64 returns the value obtained by dividing x by y.
func DivideFloat64(x float64, y float64) float64 {
	if math.IsNaN(x) || math.IsNaN(y) || y == 0.0 {
		return math.NaN()
	}
	return x / y
}

// MinFloat64 returns the smallest of vals.
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

// MaxFloat64 returns the largest of vals.
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

// AvgFloat64 returns the average value of vals.
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

// Gcd is Greatest common divisor
func Gcd(a, b int) int {
	if b == 0 {
		return a
	}
	return Gcd(b, a%b)
}

// Lcm is Least common multiple
func Lcm(a, b int) int {
	if a == b {
		return a
	}
	if a < b {
		a, b = b, a // ensure a > b
	}
	return a * b / Gcd(a, b)
}

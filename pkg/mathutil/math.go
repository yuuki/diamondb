package mathutil

import (
	"math"
	"sort"
)

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

// LinearRegressionAnalysis returns factor and offset of linear regression function by least squares method.
// https://github.com/graphite-project/graphite-web/blob/f7634f3e/webapp/graphite/render/functions.py#L2567
// In y = ax + b, a: factor b: offset
func LinearRegressionAnalysis(vals []float64, start int64, step int) (float64, float64) {
	vals = notNaNVals(vals)
	if len(vals) < 1 {
		return math.NaN(), math.NaN()
	}
	var sumI, sumV, sumII, sumIV float64
	for i, v := range vals {
		sumI += float64(i)
		sumV += v
		sumII += float64(i * i)
		sumIV += float64(i) * v
	}
	n := float64(len(vals))
	denominator := float64(n*sumII - sumI*sumI)
	if denominator == 0 {
		return math.NaN(), math.NaN()
	}
	factor := (n*sumIV - sumI*sumV) / denominator / float64(step)
	offset := (sumII*sumV-sumIV*sumI)/denominator - factor*float64(start)
	return factor, offset
}

// Percentile is calculated using the method outlined in the NIST Engineering
// Statistics Handbook:
// http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm
func Percentile(vals []float64, n float64, interpolate bool) float64 {
	vals = notNaNVals(vals)
	if len(vals) < 1 {
		return math.NaN()
	}
	if n > 100 {
		return math.NaN()
	}

	sort.Float64s(vals)
	fractionalRank := (n / 100.0) * float64((len(vals) + 1))
	rank := int(fractionalRank)
	rankFraction := fractionalRank - float64(rank)
	if !interpolate {
		rank += int(math.Ceil(rankFraction))
	}

	var percentile float64
	if rank == 0 {
		percentile = vals[0]
	} else if rank-1 == len(vals) {
		percentile = vals[len(vals)-1]
	} else {
		percentile = vals[rank-1] // Adjust for 0-index
	}

	if interpolate {
		if rank != len(vals) {
			nextValue := vals[rank]
			percentile += rankFraction * (nextValue - percentile)
		}
	}

	return percentile
}

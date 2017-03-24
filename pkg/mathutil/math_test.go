package mathutil

import (
	"math"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestSumFloat64(t *testing.T) {
	tests := []struct {
		desc     string
		vals     []float64
		expected float64
	}{
		{
			"no NaN values",
			[]float64{0.1, 0.2, 0.3},
			float64(0.1) + float64(0.2) + float64(0.3),
		},
		{
			"one NaN value",
			[]float64{0.1, 0.2, math.NaN(), 0.3},
			float64(0.1) + float64(0.2) + float64(0.3),
		},
		{
			"the head value is a NaN",
			[]float64{math.NaN(), math.NaN(), 0.1, 0.2, 0.3},
			float64(0.1) + float64(0.2) + float64(0.3),
		},
		{
			"all values are NaN",
			[]float64{math.NaN(), math.NaN(), math.NaN()},
			math.NaN(),
		},
		{
			"empty slices",
			[]float64{},
			math.NaN(),
		},
	}

	for _, tc := range tests {
		got := SumFloat64(tc.vals)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestMultiplyFloat64(t *testing.T) {
	tests := []struct {
		desc     string
		vals     []float64
		expected float64
	}{
		{
			"no NaN values",
			[]float64{1.0, 2.0, 3.0},
			6.0,
		},
		{
			"one NaN value",
			[]float64{1.0, 2.0, math.NaN(), 3.0},
			6.0,
		},
		{
			"the head value is a NaN",
			[]float64{math.NaN(), math.NaN(), 1.0, 2.0, 3.0},
			6.0,
		},
		{
			"all values are NaN",
			[]float64{math.NaN(), math.NaN(), math.NaN()},
			math.NaN(),
		},
		{
			"empty slices",
			[]float64{},
			math.NaN(),
		},
	}

	for _, tc := range tests {
		got := MultiplyFloat64(tc.vals)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestDivideFloat64(t *testing.T) {
	tests := []struct {
		desc     string
		x        float64
		y        float64
		expected float64
	}{
		{
			"no NaN values", 1.0, 2.0, 0.5,
		},
		{
			"x is NaN value", math.NaN(), 2.0, math.NaN(),
		},
		{
			"y is NaN value", 1.0, math.NaN(), math.NaN(),
		},
		{
			"x and y are NaN", math.NaN(), math.NaN(), math.NaN(),
		},
	}

	for _, tc := range tests {
		got := DivideFloat64(tc.x, tc.y)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestMinFloat64(t *testing.T) {
	tests := []struct {
		desc     string
		vals     []float64
		expected float64
	}{
		{
			"no NaN values",
			[]float64{0.1, 0.2, 0.3},
			float64(0.1),
		},
		{
			"one NaN value",
			[]float64{0.1, 0.2, math.NaN(), 0.3},
			float64(0.1),
		},
		{
			"the head value is a NaN",
			[]float64{math.NaN(), math.NaN(), 0.1, 0.2, 0.3},
			float64(0.1),
		},
		{
			"all values are NaN",
			[]float64{math.NaN(), math.NaN(), math.NaN()},
			math.NaN(),
		},
		{
			"empty slices",
			[]float64{},
			math.NaN(),
		},
	}

	for _, tc := range tests {
		got := MinFloat64(tc.vals)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestMaxFloat64(t *testing.T) {
	tests := []struct {
		desc     string
		vals     []float64
		expected float64
	}{
		{
			"no NaN values",
			[]float64{0.1, 0.2, 0.3},
			float64(0.3),
		},
		{
			"one NaN value",
			[]float64{0.1, 0.2, math.NaN(), 0.3},
			float64(0.3),
		},
		{
			"the head value is a NaN",
			[]float64{math.NaN(), math.NaN(), 0.1, 0.2, 0.3},
			float64(0.3),
		},
		{
			"all values are NaN",
			[]float64{math.NaN(), math.NaN(), math.NaN()},
			math.NaN(),
		},
		{
			"empty slices",
			[]float64{},
			math.NaN(),
		},
	}

	for _, tc := range tests {
		got := MaxFloat64(tc.vals)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestAvgFloat64(t *testing.T) {
	tests := []struct {
		desc     string
		vals     []float64
		expected float64
	}{
		{
			"no NaN values",
			[]float64{0.1, 0.2, 0.3},
			(float64(0.1) + float64(0.2) + float64(0.3)) / float64(3),
		},
		{
			"one NaN value",
			[]float64{0.1, 0.2, math.NaN(), 0.3},
			(float64(0.1) + float64(0.2) + float64(0.3)) / float64(3),
		},
		{
			"the head value is a NaN",
			[]float64{math.NaN(), math.NaN(), 0.1, 0.2, 0.3},
			(float64(0.1) + float64(0.2) + float64(0.3)) / float64(3),
		},
		{
			"all values are NaN",
			[]float64{math.NaN(), math.NaN(), math.NaN()},
			math.NaN(),
		},
		{
			"empty slices",
			[]float64{},
			math.NaN(),
		},
	}

	for _, tc := range tests {
		got := AvgFloat64(tc.vals)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestGcd(t *testing.T) {
	tests := []struct {
		Inputs []int
		Gcd    int
	}{
		{[]int{128, 32}, 32},
		{[]int{237, 9}, 3},
	}
	for _, c := range tests {
		if Gcd(c.Inputs[0], c.Inputs[1]) != c.Gcd {
			t.Fatalf("\nInput: %v\n\nExpected: %#v", c.Inputs, c.Gcd)
		}
	}
}

func TestLcm(t *testing.T) {
	tests := []struct {
		Inputs []int
		Lcm    int
	}{
		{[]int{12, 24}, 24},
		{[]int{27, 28}, 756},
	}
	for _, c := range tests {
		if Lcm(c.Inputs[0], c.Inputs[1]) != c.Lcm {
			t.Fatalf("\nInput: %v\n\nExpected: %#v", c.Inputs, c.Lcm)
		}
	}
}

func TestLinearRegressionAnalysis(t *testing.T) {
	cases := []struct {
		desc           string
		inputVals      []float64
		inputStart     int64
		inputStep      int
		expectedFactor float64
		expectedOffset float64
	}{
		{"y=0.1x+0", []float64{0.1, 0.2, 0.3, 0.4}, 1, 1, 0.1, 0},
		{"y=2x-1", []float64{1.0, 3.0, 5.0, 7.0}, 1, 1, 2.0, -1.0},
		{"y=5.0", []float64{5.0, 5.0, 5.0}, 1, 1, 0.0, 5.0},
		{"input length is zero", []float64{}, 1, 1, math.NaN(), math.NaN()},
		{"input length is one", []float64{1.0}, 1, 1, math.NaN(), math.NaN()},
	}
	for _, c := range cases {
		factor, offset := LinearRegressionAnalysis(c.inputVals, c.inputStart, c.inputStep)
		if factor != c.expectedFactor {
			if !(math.IsNaN(factor) && math.IsNaN(c.expectedFactor)) {
				t.Fatalf("desc: %s, factor should be %g, not %g", c.desc, c.expectedFactor, factor)
			}
		}
		if offset != c.expectedOffset {
			if !(math.IsNaN(factor) && math.IsNaN(c.expectedFactor)) {
				t.Fatalf("desc: %s, offset should be %g, not %g", c.desc, c.expectedOffset, offset)
			}
		}
	}
}

func TestPercentile(t *testing.T) {
	tests := []struct {
		vals        []float64
		n           float64
		interpolate bool
		expected    float64
	}{
		{[]float64{11.0, 12.0, 10.0, 13.0, 14.0}, 50.0, false, 12.0},
		{[]float64{11.0, 12.0, 10.0, 13.0, 14.0}, 51.0, false, 13.0},
		{[]float64{11.0, 12.0, 10.0, 13.0, 14.0}, 50.0, true, 12.0},
		{[]float64{11.0, 12.0, 10.0, 13.0, 14.0}, 51.0, true, 12.06},
		{[]float64{11.0, 12.0, 10.0, 13.0, 14.0}, 78.0, true, 13.68},
	}

	for _, tc := range tests {
		got := Percentile(tc.vals, tc.n, tc.interpolate)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
	}
}

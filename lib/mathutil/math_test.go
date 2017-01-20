package mathutil

import (
	"math"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

var sumFloat64Tests = []struct {
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

func TestSumFloat64(t *testing.T) {
	for _, tc := range sumFloat64Tests {
		got := SumFloat64(tc.vals)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

var multiplyFloat64Tests = []struct {
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

func TestMultiplyFloat64(t *testing.T) {
	for _, tc := range multiplyFloat64Tests {
		got := MultiplyFloat64(tc.vals)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

var minFloat64Tests = []struct {
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

func TestMinFloat64(t *testing.T) {
	for _, tc := range minFloat64Tests {
		got := MinFloat64(tc.vals)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

var maxFloat64Tests = []struct {
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

func TestMaxFloat64(t *testing.T) {
	for _, tc := range maxFloat64Tests {
		got := MaxFloat64(tc.vals)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

var avgFloat64Tests = []struct {
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

func TestAvgFloat64(t *testing.T) {
	for _, tc := range avgFloat64Tests {
		got := AvgFloat64(tc.vals)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("desc: %s diff: (-actual +expected)\n%s", tc.desc, diff)
		}
	}
}

func TestGcd(t *testing.T) {
	cases := []struct {
		Inputs []int
		Gcd    int
	}{
		{[]int{128, 32}, 32},
		{[]int{237, 9}, 3},
	}
	for _, c := range cases {
		if Gcd(c.Inputs[0], c.Inputs[1]) != c.Gcd {
			t.Fatalf("\nInput: %v\n\nExpected: %#v", c.Inputs, c.Gcd)
		}
	}
}

func TestLcm(t *testing.T) {
	cases := []struct {
		Inputs []int
		Lcm    int
	}{
		{[]int{12, 24}, 24},
		{[]int{27, 28}, 756},
	}
	for _, c := range cases {
		if Lcm(c.Inputs[0], c.Inputs[1]) != c.Lcm {
			t.Fatalf("\nInput: %v\n\nExpected: %#v", c.Inputs, c.Lcm)
		}
	}
}

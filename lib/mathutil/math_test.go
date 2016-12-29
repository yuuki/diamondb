package mathutil

import (
	"testing"
)

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

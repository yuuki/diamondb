package mathutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGcd(t *testing.T) {
	assert.Exactly(t, 32, Gcd(128, 32))
	assert.Exactly(t, 3, Gcd(237, 9))
}

func TestLcm(t *testing.T) {
	assert.Exactly(t, 24, Lcm(12, 24))
	assert.Exactly(t, 756, Lcm(27, 28))
}

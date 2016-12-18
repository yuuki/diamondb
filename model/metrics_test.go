package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMetric(t *testing.T) {
	points := []*DataPoint{
		&DataPoint{1465516800, 10.0},
		&DataPoint{1465516860, 15.0},
		&DataPoint{1465516830, 20.0},
	}

	metric := NewMetric("server1.loadavg5", points, 30)

	assert.Equal(t, "server1.loadavg5", metric.Name)
	assert.Equal(t, 30, metric.Step)
	assert.Equal(t, int32(1465516800), metric.Start)
	assert.Equal(t, int32(1465516860), metric.End)

	sortedPoints := []*DataPoint{
		&DataPoint{1465516800, 10.0},
		&DataPoint{1465516830, 20.0},
		&DataPoint{1465516860, 15.0},
	}
	assert.EqualValues(t, sortedPoints, metric.DataPoints)
}

func Test_

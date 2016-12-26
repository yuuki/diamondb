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

	assert.Exactly(t, "server1.loadavg5", metric.Name)
	assert.Exactly(t, 30, metric.Step)
	assert.Exactly(t, uint64(1465516800), metric.Start)
	assert.Exactly(t, uint64(1465516860), metric.End)

	sortedPoints := []*DataPoint{
		&DataPoint{1465516800, 10.0},
		&DataPoint{1465516830, 20.0},
		&DataPoint{1465516860, 15.0},
	}
	assert.Exactly(t, sortedPoints, metric.DataPoints)
}

func TestFilledWithNil(t *testing.T) {
	points := []*DataPoint{
		&DataPoint{1465516800, 10.0},
		&DataPoint{1465516830, 20.0},
		&DataPoint{1465516860, 15.0},
	}
	metric := NewMetric("server1.loadavg5", points, 15)

	metric = metric.FilledWithNil()

	assert.Exactly(t, "server1.loadavg5", metric.Name)
	assert.Exactly(t, 15, metric.Step)
	assert.Exactly(t, uint64(1465516800), metric.Start)
	assert.Exactly(t, uint64(1465516860), metric.End)

	expected := []*DataPoint{
		&DataPoint{1465516800, 10.0},
		nil,
		&DataPoint{1465516830, 20.0},
		nil,
		&DataPoint{1465516860, 15.0},
	}
	assert.Exactly(t, expected, metric.DataPoints)
}

func TestInsertDatapoint(t *testing.T) {
	points := []*DataPoint{
		&DataPoint{1465516800, 10.0},
		&DataPoint{1465516830, 20.0},
		&DataPoint{1465516860, 15.0},
	}
	metric := NewMetric("server1.loadavg5", points, 15)

	metric.insertDatapoint(1, &DataPoint{1465516815, 5.0})

	expected := []*DataPoint{
		&DataPoint{1465516800, 10.0},
		&DataPoint{1465516815, 5.0},
		&DataPoint{1465516830, 20.0},
		&DataPoint{1465516860, 15.0},
	}
	assert.Exactly(t, expected, metric.DataPoints)
}

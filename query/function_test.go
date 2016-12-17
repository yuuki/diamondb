package query

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/yuuki/dynamond/model"
)

func TestGcd(t *testing.T) {
	assert.Equal(t, 32, gcd(128, 32))
	assert.Equal(t, 3, gcd(237, 9))
}

func TestAlias(t *testing.T) {
	seriesList := []*model.Metric{
		model.NewMetric(
			"Sales.widgets.largeBlue",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 10.0},
			},
			60,
		),
		model.NewMetric(
			"Servers.web01.sda1.free_space",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 10.0},
			},
			60,
		),
	}
	metricList := alias(seriesList, "Large Blue Widgets")
	assert.Equal(t, 2, len(metricList))
	assert.Equal(t, metricList[0].Name, "Large Blue Widgets")
	assert.Equal(t, metricList[1].Name, "Large Blue Widgets")
}

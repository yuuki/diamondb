package query

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/yuuki/dynamond/model"
)

func TestAlias(t *testing.T) {
	seriesList := []*model.Metric{
		model.NewMetric(
			"Sales.widgets.largeBlue",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 10.0},
			},
		),
		model.NewMetric(
			"Servers.web01.sda1.free_space",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 10.0},
			},
		),
	}
	metricList := alias(seriesList, "Large Blue Widgets")
	assert.Equal(t, 2, len(metricList))
	assert.Equal(t, metricList[0].Name, "Large Blue Widgets")
	assert.Equal(t, metricList[1].Name, "Large Blue Widgets")
}

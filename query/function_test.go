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

func TestLcm(t *testing.T) {
	assert.Equal(t, 24, lcm(12, 24))
	assert.Equal(t, 756, lcm(27, 28))
}

func TestFormatSeries_Uniq(t *testing.T) {
	seriesList := []*model.Metric{
		&model.Metric{Name: "server1.cpu.system"},
		&model.Metric{Name: "server2.cpu.system"},
		&model.Metric{Name: "server3.cpu.system"},
	}
	format := formatSeries(seriesList)
	assert.Equal(t, "server1.cpu.system,server2.cpu.system,server3.cpu.system", format)
}

func TestFormatSeries_NotUniq(t *testing.T) {
	seriesList := []*model.Metric{
		&model.Metric{Name: "server3.cpu.system"},
		&model.Metric{Name: "server1.cpu.system"},
		&model.Metric{Name: "server2.cpu.system"},
		&model.Metric{Name: "server1.cpu.system"},
	}
	format := formatSeries(seriesList)
	assert.Equal(t, "server1.cpu.system,server2.cpu.system,server3.cpu.system", format)
}

func TestNormalize_Empty(t *testing.T) {
	seriesList, start, end, step := normalize([]*model.Metric{})
	assert.Equal(t, 0, len(seriesList))
	assert.Equal(t, int32(0), start)
	assert.Equal(t, int32(0), end)
	assert.Equal(t, 0, step)
}

func TestNormalize_NonValues(t *testing.T) {
	seriesList, start, end, step := normalize([]*model.Metric{
		&model.Metric{
			Name: "collectd.test-db{0}.load.value",
			Step: 1,
			Start: int32(1),
			End: int32(5),
		},
	})
	assert.Equal(t, "collectd.test-db{0}.load.value", seriesList[0].Name)
	assert.Equal(t, int32(1), start)
	assert.Equal(t, int32(5), end)
	assert.Equal(t, 1, step)
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

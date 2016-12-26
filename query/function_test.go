package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/yuuki/dynamond/model"
)

func TestGcd(t *testing.T) {
	assert.Exactly(t, 32, gcd(128, 32))
	assert.Exactly(t, 3, gcd(237, 9))
}

func TestLcm(t *testing.T) {
	assert.Exactly(t, 24, lcm(12, 24))
	assert.Exactly(t, 756, lcm(27, 28))
}

func generateSeriesList() []*model.Metric {
	step := 1
	datapoints1 := make([]*model.DataPoint, 0, 100)
	for i := 0; i < 100; i++ {
		datapoints1 = append(datapoints1, &model.DataPoint{Timestamp: uint64(step*i), Value: float64(i+1)})
	}
	datapoints2 := make([]*model.DataPoint, 0, 100)
	for i := 0; i < 100; i++ {
		datapoints2 = append(datapoints2, &model.DataPoint{Timestamp: uint64(step*i), Value: float64(i+1)})
	}
	datapoints3 := make([]*model.DataPoint, 0, 1)
	datapoints3 = append(datapoints3, &model.DataPoint{Timestamp: 0, Value: float64(1)})

	seriesList := make([]*model.Metric, 3)
	seriesList[0] = model.NewMetric(fmt.Sprintf("collectd.test-db%d.load.value", 0), datapoints1, 1)
	seriesList[1] = model.NewMetric(fmt.Sprintf("collectd.test-db%d.load.value", 1), datapoints2, 1)
	seriesList[2] = model.NewMetric(fmt.Sprintf("collectd.test-db%d.load.value", 2), datapoints3, 1)

        return seriesList
}

func TestFormatSeries_Uniq(t *testing.T) {
	seriesList := []*model.Metric{
		&model.Metric{Name: "server1.cpu.system"},
		&model.Metric{Name: "server2.cpu.system"},
		&model.Metric{Name: "server3.cpu.system"},
	}
	format := formatSeries(seriesList)
	assert.Exactly(t, "server1.cpu.system,server2.cpu.system,server3.cpu.system", format)
}

func TestFormatSeries_NotUniq(t *testing.T) {
	seriesList := []*model.Metric{
		&model.Metric{Name: "server3.cpu.system"},
		&model.Metric{Name: "server1.cpu.system"},
		&model.Metric{Name: "server2.cpu.system"},
		&model.Metric{Name: "server1.cpu.system"},
	}
	format := formatSeries(seriesList)
	assert.Exactly(t, "server1.cpu.system,server2.cpu.system,server3.cpu.system", format)
}

func TestNormalize_Empty(t *testing.T) {
	seriesList, start, end, step := normalize([]*model.Metric{})
	assert.Exactly(t, 0, len(seriesList))
	assert.Exactly(t, uint64(0), start)
	assert.Exactly(t, uint64(0), end)
	assert.Exactly(t, 0, step)
}

func TestNormalize_NonValues(t *testing.T) {
	seriesList, start, end, step := normalize([]*model.Metric{
		&model.Metric{
			Name: "collectd.test-db{0}.load.value",
			Step: 1,
			Start: uint64(1),
			End: uint64(5),
		},
	})
	assert.Exactly(t, "collectd.test-db{0}.load.value", seriesList[0].Name)
	assert.Exactly(t, uint64(1), start)
	assert.Exactly(t, uint64(5), end)
	assert.Exactly(t, 1, step)
}

func TestNormalize_GenerateSeriesListInput(t *testing.T) {
	_, start, end, step := normalize(generateSeriesList())
	assert.Exactly(t, uint64(0), start)
	assert.Exactly(t, uint64(99), end)
	assert.Exactly(t, 1, step)
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
	assert.Exactly(t, 2, len(metricList))
	assert.Exactly(t, metricList[0].Name, "Large Blue Widgets")
	assert.Exactly(t, metricList[1].Name, "Large Blue Widgets")
}

func TestAverageSeries(t *testing.T) {
	series := averageSeries(generateSeriesList())
	assert.Exactly(t,
		"averageSeries(collectd.test-db0.load.value,collectd.test-db1.load.value,collectd.test-db2.load.value)",
		series.Name,
	)
	expected := make([]*model.DataPoint, 0, 100)
	for i := 0; i < 100; i++ {
		expected = append(expected, model.NewDataPoint(uint64(i), float64(i+1)))
	}
	assert.Exactly(t, expected, series.DataPoints)
}

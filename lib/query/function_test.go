package query

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/yuuki/diamondb/lib/model"
)

func generateSeriesList() []*model.Metric {
	step := 1
	datapoints1 := make([]*model.DataPoint, 0, 100)
	for i := 0; i < 100; i++ {
		datapoints1 = append(datapoints1, &model.DataPoint{Timestamp: int64(step * i), Value: float64(i + 1)})
	}
	datapoints2 := make([]*model.DataPoint, 0, 100)
	for i := 0; i < 100; i++ {
		datapoints2 = append(datapoints2, &model.DataPoint{Timestamp: int64(step * i), Value: float64(i + 1)})
	}
	datapoints3 := make([]*model.DataPoint, 0, 1)
	datapoints3 = append(datapoints3, &model.DataPoint{Timestamp: 0, Value: float64(1)})

	seriesList := make([]*model.Metric, 3)
	seriesList[0] = model.NewMetric(fmt.Sprintf("server%d.loadavg5", 0), datapoints1, 1)
	seriesList[1] = model.NewMetric(fmt.Sprintf("server%d.loadavg5", 1), datapoints2, 1)
	seriesList[2] = model.NewMetric(fmt.Sprintf("server%d.loadavg5", 2), datapoints3, 1)

	return seriesList
}

func TestFormatSeries_Uniq(t *testing.T) {
	seriesList := []*model.Metric{
		{Name: "server1.cpu.system"},
		{Name: "server2.cpu.system"},
		{Name: "server3.cpu.system"},
	}
	format := formatSeries(seriesList)
	expected := "server1.cpu.system,server2.cpu.system,server3.cpu.system"
	if format != expected {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, format)
	}
}

func TestFormatSeries_NotUniq(t *testing.T) {
	seriesList := []*model.Metric{
		{Name: "server3.cpu.system"},
		{Name: "server1.cpu.system"},
		{Name: "server2.cpu.system"},
		{Name: "server1.cpu.system"},
	}
	format := formatSeries(seriesList)
	expected := "server1.cpu.system,server2.cpu.system,server3.cpu.system"
	if format != expected {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, format)
	}
}

func TestNormalize_Empty(t *testing.T) {
	seriesList, start, end, step := normalize([]*model.Metric{})
	if v := len(seriesList); v != 0 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 0, v)
	}
	if start != 0 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 0, start)
	}
	if end != 0 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 0, end)
	}
	if step != 0 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 0, step)
	}
}

func TestNormalize_NonValues(t *testing.T) {
	seriesList, start, end, step := normalize([]*model.Metric{
		{
			Name:  "server1.loadavg5",
			Step:  1,
			Start: int64(1),
			End:   int64(5),
		},
	})
	if v := seriesList[0].Name; v != "server1.loadavg5" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "server1.loadavg5", v)
	}
	if start != 1 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 1, start)
	}
	if end != 5 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 1, end)
	}
	if step != 1 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 1, step)
	}
}

func TestNormalize_GenerateSeriesListInput(t *testing.T) {
	_, start, end, step := normalize(generateSeriesList())
	if start != 0 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 0, start)
	}
	if end != 99 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 99, end)
	}
	if step != 1 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 1, step)
	}
}

func TestAlias(t *testing.T) {
	seriesList := []*model.Metric{
		model.NewMetric(
			"server1.loadavg5",
			[]*model.DataPoint{
				{1000, 10.0},
			},
			60,
		),
		model.NewMetric(
			"server2.loadavg5",
			[]*model.DataPoint{
				{1060, 11.0},
			},
			60,
		),
	}

	metricList := alias(seriesList, "server.loadavg5")

	if v := len(metricList); v != 2 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 2, v)
	}
	for _, m := range metricList {
		if m.Name != "server.loadavg5" {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", 2, m.Name)
		}
	}
}

func TestAverageSeries(t *testing.T) {
	series := averageSeries(generateSeriesList())

	points := make([]*model.DataPoint, 100)
	for i := 0; i < 100; i++ {
		points[i] = model.NewDataPoint(int64(i), float64(i+1))
	}
	expected := &model.Metric{
		Name:       "averageSeries(server0.loadavg5,server1.loadavg5,server2.loadavg5)",
		DataPoints: points,
		Start:      0,
		End:        99,
		Step:       1,
	}
	if !reflect.DeepEqual(series, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, series)
	}
}

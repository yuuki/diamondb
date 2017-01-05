package model

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestNewMetric(t *testing.T) {
	points := []*DataPoint{
		{1030, 10.0},
		{1060, 15.0},
		{1000, 20.0},
	}

	metric := NewMetric("server1.loadavg5", points, 30)

	expected := &Metric{
		Name: "server1.loadavg5",
		DataPoints: []*DataPoint{
			{1000, 20.0},
			{1030, 10.0},
			{1060, 15.0},
		},
		Start: 1000,
		End:   1060,
		Step:  30,
	}
	if diff := pretty.Compare(metric, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestFilledWithNil(t *testing.T) {
	points := []*DataPoint{
		{1030, 10.0},
		{1060, 15.0},
		{1000, 20.0},
	}
	metric := NewMetric("server1.loadavg5", points, 15)

	metric.FilledWithNil()

	expected := &Metric{
		Name: "server1.loadavg5",
		DataPoints: []*DataPoint{
			{1000, 20.0},
			nil,
			{1030, 10.0},
			nil,
			{1060, 15.0},
		},
		Start: 1000,
		End:   1060,
		Step:  15,
	}
	if diff := pretty.Compare(metric, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestInsertDatapoint(t *testing.T) {
	points := []*DataPoint{
		{1030, 10.0},
		{1060, 15.0},
		{1000, 20.0},
	}
	metric := NewMetric("server1.loadavg5", points, 15)

	metric.insertDatapoint(1, &DataPoint{1015, 5.0})

	expected := &Metric{
		Name: "server1.loadavg5",
		DataPoints: []*DataPoint{
			{1000, 20.0},
			{1015, 5.0},
			{1030, 10.0},
			{1060, 15.0},
		},
		Start: 1000,
		End:   1060,
		Step:  15,
	}
	if diff := pretty.Compare(metric, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

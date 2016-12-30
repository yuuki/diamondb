package query

import (
	"reflect"
	"testing"
	"time"

	"github.com/yuuki/diamondb/lib/model"
	"github.com/yuuki/diamondb/lib/tsdb"
)

func TestEvalTarget_Func(t *testing.T) {
	points := []*model.DataPoint{
		{60, 10.0},
		{120, 11.0},
	}
	ctrl := tsdb.SetMockDynamoDB(t, &tsdb.MockDynamoDBParam{
		TableName: tsdb.DynamoDBTableOneHour + "-0",
		ItemEpoch: 0,
		Metrics: []*model.Metric{
			model.NewMetric(
				"server1.loadavg5",
				points,
				60,
			),
		},
	})
	defer ctrl.Finish()

	metrics, err := EvalTarget(
		"alias(server1.loadavg5,\"server01.loadavg5\")",
		time.Unix(0, 0),
		time.Unix(120, 0),
	)

	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := []*model.Metric{
		{Name: "server01.loadavg5", Start: 60, End: 120, Step: 60, DataPoints: points},
	}
	if !reflect.DeepEqual(metrics, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, metrics)
	}
}

func TestEvalTarget_FuncNest(t *testing.T) {
	points := []*model.DataPoint{
		{60, 10.0},
		{120, 11.0},
	}
	ctrl := tsdb.SetMockDynamoDB(t, &tsdb.MockDynamoDBParam{
		TableName: tsdb.DynamoDBTableOneHour + "-0",
		ItemEpoch: 0,
		Metrics: []*model.Metric{
			model.NewMetric(
				"server1.loadavg5",
				points,
				60,
			),
		},
	})
	defer ctrl.Finish()

	metrics, err := EvalTarget(
		"alias(alias(server1.loadavg5,\"server01.loadavg5\"),\"server001.loadavg5\")",
		time.Unix(0, 0),
		time.Unix(120, 0),
	)

	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := []*model.Metric{
		{Name: "server001.loadavg5", Start: 60, End: 120, Step: 60, DataPoints: points},
	}
	if !reflect.DeepEqual(metrics, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, metrics)
	}
}

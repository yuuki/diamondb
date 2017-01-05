package query

import (
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/yuuki/diamondb/lib/model"
	"github.com/yuuki/diamondb/lib/storage/dynamo"
)

func TestEvalTarget_Func(t *testing.T) {
	points := []*model.DataPoint{
		{60, 10.0},
		{120, 11.0},
	}
	ctrl := dynamo.SetMockDynamoDB(t, &dynamo.MockDynamoDBParam{
		TableName: dynamo.DynamoDBTableOneHour + "-0",
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
	if diff := pretty.Compare(metrics, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestEvalTarget_FuncNest(t *testing.T) {
	points := []*model.DataPoint{
		{60, 10.0},
		{120, 11.0},
	}
	ctrl := dynamo.SetMockDynamoDB(t, &dynamo.MockDynamoDBParam{
		TableName: dynamo.DynamoDBTableOneHour + "-0",
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
	if diff := pretty.Compare(metrics, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

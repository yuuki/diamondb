package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/yuuki/dynamond/model"
	"github.com/yuuki/dynamond/tsdb"
)

func TestEvalTarget_Func(t *testing.T) {
	ctrl := tsdb.SetMockDynamoDB(t, &tsdb.MockDynamoDBParam{
		TableName: "SeriesTestRange-1m1h-0",
		ItemEpoch: 0,
		Names: []string{"Sales.widgets.largeBlue"},
		Metrics: []*model.Metric{
			model.NewMetric(
				"Sales.widgets.largeBlue",
				[]*model.DataPoint{
					&model.DataPoint{60, 10.0},
				},
				60,
			),
		},
	})
	defer ctrl.Finish()

	metrics, err := EvalTarget(
		"alias(Sales.widgets.largeBlue,\"Large Blue Widgets\")",
		time.Unix(0, 0),
		time.Unix(120, 0),
	)
	if assert.NoError(t, err) {
		assert.Exactly(t, 1, len(metrics))
		assert.Exactly(t, metrics[0].Name, "Large Blue Widgets")
	}
}

func TestEvalTarget_FuncNest(t *testing.T) {
	ctrl := tsdb.SetMockDynamoDB(t, &tsdb.MockDynamoDBParam{
		TableName: "SeriesTestRange-1m1h-0",
		ItemEpoch: 0,
		Names: []string{"Sales.widgets.largeBlue"},
		Metrics: []*model.Metric{
			model.NewMetric(
				"Sales.widgets.largeBlue",
				[]*model.DataPoint{
					&model.DataPoint{60, 10.0},
				},
				60,
			),
		},
	})
	defer ctrl.Finish()

	metrics, err := EvalTarget(
		"alias(alias(Sales.widgets.largeBlue,\"Large Blue Widgets\"),\"Large Blue Widgets Sales\")",
		time.Unix(0, 0),
		time.Unix(120, 0),
	)
	if assert.NoError(t, err) {
		assert.Exactly(t, 1, len(metrics))
		assert.Exactly(t, metrics[0].Name, "Large Blue Widgets Sales")
	}
}

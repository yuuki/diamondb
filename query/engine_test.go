package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/yuuki/dynamond/model"
	"github.com/yuuki/dynamond/tsdb"
)

func TestEvalTarget_Func(t *testing.T) {
	ctrl := tsdb.SetMockDynamoDB(t, &tsdb.MockDynamoDB{
		TableName: "SeriesTest",
		StartVal: time.Unix(1465516800, 0),
		EndVal: time.Unix(1465526800, 0),
		Metric: model.NewMetric(
			"Sales.widgets.largeBlue",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 10.0},
			},
		),
	})
	defer ctrl.Finish()

	metricList, err := EvalTarget(
		"alias(Sales.widgets.largeBlue,\"Large Blue Widgets\")",
		time.Unix(1465516800, 0),
		time.Unix(1465526800, 0),
	)
	if assert.NoError(t, err) {
		assert.Equal(t, 1, len(metricList))
		assert.Equal(t, metricList[0].Name, "Large Blue Widgets")
	}
}

func TestEvalTarget_FuncNest(t *testing.T) {
	ctrl := tsdb.SetMockDynamoDB(t, &tsdb.MockDynamoDB{
		TableName: "SeriesTest",
		StartVal: time.Unix(1465516800, 0),
		EndVal: time.Unix(1465526800, 0),
		Metric: model.NewMetric(
			"Sales.widgets.largeBlue",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 10.0},
			},
		),
	})
	defer ctrl.Finish()

	metricList, err := EvalTarget(
		"alias(alias(Sales.widgets.largeBlue,\"Large Blue Widgets\"),\"Large Blue Widgets Sales\")",
		time.Unix(1465516800, 0),
		time.Unix(1465526800, 0),
	)
	if assert.NoError(t, err) {
		assert.Equal(t, 1, len(metricList))
		assert.Equal(t, metricList[0].Name, "Large Blue Widgets Sales")
	}
}

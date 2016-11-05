package tsdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/yuuki/dynamond/model"
)

func TestFetchMetric(t *testing.T) {
	ctrl := SetMockDynamoDB(t, &MockDynamoDB{
		TableName: "SeriesTest",
		StartVal: time.Unix(1465516800, 0),
		EndVal: time.Unix(1465526800, 0),
		Metric: model.NewMetric(
			"test",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 10.0},
			},
			60,
		),
	})
	defer ctrl.Finish()

	metricList, err := FetchMetric("test", time.Unix(1465516800, 0), time.Unix(1465526800, 0))
	if assert.NoError(t, err) {
		assert.Equal(t, 1, len(metricList))
		metric := metricList[0]
		assert.Equal(t, "test", metric.Name)
		assert.EqualValues(t, &model.DataPoint{1465516810, 10.0}, metric.DataPoints[0])
	}
}

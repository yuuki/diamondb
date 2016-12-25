package tsdb

import (
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/dynamond/model"
)

func FetchMetric(name string, start, end time.Time) ([]*model.Metric, error) {
	metrics, err := FetchMetricsFromDynamoDB(name, start, end)
	if err != nil {
		return nil, errors.Wrapf(err,
			"Failed to FetchMetricsFromDynamoDB %s %d %d",
			name, start.Unix(), end.Unix(),
		)
	}
	for _, m := range metrics {
		m.FilledWithNil()
	}

	return metrics, nil
}

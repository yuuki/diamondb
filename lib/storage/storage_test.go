package storage

import (
	"testing"

	"github.com/yuuki/diamondb/lib/metric"
)

func TestWriterStoreInsertMetric(t *testing.T) {
	fakeRedisWriter := &FakeRedisWriter{
		FakeInsertDatapoint: func(slot string, name string, p *metric.Datapoint) error {
			return nil
		},
	}
	ws := &WriterStore{Redis: fakeRedisWriter}
	err := ws.InsertMetric(&metric.Metric{
		Name:       "server1.loadavg5",
		Datapoints: []*metric.Datapoint{&metric.Datapoint{100, 0.1}},
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
}

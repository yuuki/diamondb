package storage

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	godynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"

	"github.com/yuuki/diamondb/lib/config"
	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage/dynamodb"
	"github.com/yuuki/diamondb/lib/storage/redis"
)

func TestStorePing(t *testing.T) {
	// mock Redis
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	config.Config.RedisAddrs = []string{s.Addr()}
	r := redis.NewRedis()

	// mock DynamoDB
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := dynamodb.NewMockDynamoDBAPI(ctrl)
	mock.EXPECT().DescribeLimits(gomock.Any()).Return(
		&godynamodb.DescribeLimitsOutput{}, nil,
	)
	d := dynamodb.NewTestDynamoDB(mock)

	store := &Store{
		Redis:    r,
		DynamoDB: d,
	}
	err = store.Ping()
	if err != nil {
		t.Fatalf("should not raise err: %s", err)
	}
}

func TestStoreRead(t *testing.T) {
	store := &Store{
		Redis: &redis.FakeReadWriter{
			FakeRead: func(name string, start, end time.Time) (series.SeriesMap, error) {
				return series.SeriesMap{
					"server1.loadavg5": series.NewSeriesPoint(
						"server1.loadavg5", series.DataPoints{
							series.NewDataPoint(120, 10.0),
							series.NewDataPoint(180, 11.0),
						}, 60,
					),
				}, nil
			},
		},
		DynamoDB: &dynamodb.FakeReadWriter{
			FakeRead: func(name string, start, end time.Time) (series.SeriesMap, error) {
				return series.SeriesMap{
					"server1.loadavg5": series.NewSeriesPoint(
						"server1.loadavg5", series.DataPoints{
							series.NewDataPoint(120, 10.0),
							series.NewDataPoint(180, 11.0),
						}, 60,
					),
				}, nil
			},
		},
	}
	_, err := store.Read("server1.loadavg5", time.Unix(100, 0), time.Unix(1000, 0))
	if err != nil {
		t.Fatalf("should not raise err: %s", err)
	}
}

func TestStoreWrite(t *testing.T) {
	ws := &Store{
		Redis: &redis.FakeReadWriter{
			FakeWrite: func(slot string, name string, p *metric.Datapoint) error {
				return nil
			},
		},
	}
	err := ws.Write(&metric.Metric{
		Name:       "server1.loadavg5",
		Datapoints: []*metric.Datapoint{&metric.Datapoint{100, 0.1}},
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
}

package storage

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"

	"github.com/yuuki/diamondb/lib/config"
	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage/dynamo"
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
	mock := dynamo.NewMockDynamoDBAPI(ctrl)
	mock.EXPECT().DescribeLimits(gomock.Any()).Return(
		&dynamodb.DescribeLimitsOutput{}, nil,
	)
	d := dynamo.NewTestDynamoDB(mock)

	store := &Store{
		Redis:    r,
		DynamoDB: d,
	}
	err = store.Ping()
	if err != nil {
		t.Fatalf("should not raise err: %s", err)
	}
}

func TestStoreFetch(t *testing.T) {
	redisff := &redis.FakeFetcher{
		FakeFetch: func(name string, start, end time.Time) (series.SeriesMap, error) {
			return series.SeriesMap{
				"server1.loadavg5": series.NewSeriesPoint(
					"server1.loadavg5", series.DataPoints{
						series.NewDataPoint(120, 10.0),
						series.NewDataPoint(180, 11.0),
					}, 60,
				),
			}, nil
		},
	}
	dynamodbff := &dynamo.FakeFetcher{
		FakeFetch: func(name string, start, end time.Time) (series.SeriesMap, error) {
			return series.SeriesMap{
				"server1.loadavg5": series.NewSeriesPoint(
					"server1.loadavg5", series.DataPoints{
						series.NewDataPoint(120, 10.0),
						series.NewDataPoint(180, 11.0),
					}, 60,
				),
			}, nil
		},
	}

	store := &Store{
		Redis:    redisff,
		DynamoDB: dynamodbff,
	}
	_, err := store.Fetch("server1.loadavg5", time.Unix(100, 0), time.Unix(1000, 0))
	if err != nil {
		t.Fatalf("should not raise err: %s", err)
	}
}
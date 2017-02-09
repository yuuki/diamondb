package storage

import (
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"

	"github.com/yuuki/diamondb/lib/config"
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

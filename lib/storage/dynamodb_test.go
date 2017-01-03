package storage

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"
	"github.com/yuuki/diamondb/lib/model"
)

func TestFetchMetricsFromDynamoDB(t *testing.T) {
	name := "roleA.r.{1,2}.loadavg"
	expected := []*model.Metric{
		model.NewMetric(
			"roleA.r.1.loadavg",
			[]*model.DataPoint{
				{120, 10.0},
				{180, 11.2},
				{240, 13.1},
			},
			60,
		),
		model.NewMetric(
			"roleA.r.2.loadavg",
			[]*model.DataPoint{
				{120, 1.0},
				{180, 1.2},
				{240, 1.1},
			},
			60,
		),
	}
	ctrl := SetMockDynamoDB(t, &MockDynamoDBParam{
		TableName: DynamoDBTableOneHour + "-0",
		ItemEpoch: 0,
		Metrics:   expected,
	})
	defer ctrl.Finish()

	metrics, err := FetchMetricsFromDynamoDB(name, time.Unix(100, 0), time.Unix(300, 0))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(metrics, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, metrics)
	}
}

func TestFetchMetricsFromDynamoDB_Empty(t *testing.T) {
	tableName := DynamoDBTableOneHour + "-0"

	ctrl := gomock.NewController(t)
	dmock := NewMockDynamoDBAPI(ctrl)
	defer ctrl.Finish()

	responses := make(map[string][]map[string]*dynamodb.AttributeValue)
	responses[tableName] = []map[string]*dynamodb.AttributeValue{}
	reqErr := awserr.NewRequestFailure(
		awserr.New("ResourceNotFoundException", "resource not found", errors.New("dummy")),
		404, "dummyID",
	)
	dmock.EXPECT().BatchGetItem(gomock.Any()).Return(
		&dynamodb.BatchGetItemOutput{Responses: responses}, reqErr,
	)
	SetDynamoDB(dmock)

	name := "roleA.r.{1,2}.loadavg"
	metrics, err := FetchMetricsFromDynamoDB(name, time.Unix(100, 0), time.Unix(300, 0))
	if err != nil {
		t.Fatalf("Should ignore NotFound error: %s", err)
	}
	if len(metrics) != 0 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 0, len(metrics))
	}
}

func TestBatchGet(t *testing.T) {
	expected := []*model.Metric{
		model.NewMetric(
			"server1.loadavg5",
			[]*model.DataPoint{
				{1465516810, 10.0},
			},
			60,
		),
		model.NewMetric(
			"server2.loadavg5",
			[]*model.DataPoint{
				{1465516810, 15.0},
			},
			60,
		),
	}
	ctrl := SetMockDynamoDB(t, &MockDynamoDBParam{
		TableName: DynamoDBTableOneHour + "-0",
		ItemEpoch: 1000,
		Metrics:   expected,
	})
	defer ctrl.Finish()

	metrics, err := batchGet(
		&timeSlot{DynamoDBTableOneHour + "-0", 1000},
		[]string{"server1.loadavg5", "server2.loadavg5"},
		60,
	)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(metrics, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, metrics)
	}
}

func TestConcurrentBatchGet(t *testing.T) {
	expected := []*model.Metric{
		model.NewMetric(
			"server1.loadavg5",
			[]*model.DataPoint{
				{1465516810, 10.0},
			},
			60,
		),
		model.NewMetric(
			"server2.loadavg5",
			[]*model.DataPoint{
				{1465516810, 15.0},
			},
			60,
		),
	}
	ctrl := SetMockDynamoDB(t, &MockDynamoDBParam{
		TableName: DynamoDBTableOneHour + "-0",
		ItemEpoch: 1000,
		Metrics:   expected,
	})
	defer ctrl.Finish()

	c := make(chan interface{})
	concurrentBatchGet(
		&timeSlot{DynamoDBTableOneHour + "-0", 1000},
		[]string{"server1.loadavg5", "server2.loadavg5"},
		60,
		c,
	)
	var metrics []*model.Metric
	ret := <-c
	metrics = append(metrics, ret.([]*model.Metric)...)
	if !reflect.DeepEqual(metrics, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, metrics)
	}
}

var selectTimeSlotsTests = []struct {
	start     time.Time
	end       time.Time
	step      int
	timeSlots []*timeSlot
}{
	{
		time.Unix(100, 0), time.Unix(6000, 0), 60,
		[]*timeSlot{
			{
				tableName: DynamoDBTableOneHour + "-0",
				itemEpoch: 0,
			},
			{
				tableName: DynamoDBTableOneHour + "-0",
				itemEpoch: 3600,
			},
		},
	},
	{
		time.Unix(10000, 0), time.Unix(100000, 0), 300,
		[]*timeSlot{
			{
				tableName: DynamoDBTableOneDay + "-0",
				itemEpoch: 0,
			},
			{
				tableName: DynamoDBTableOneDay + "-86400",
				itemEpoch: 86400,
			},
		},
	},
	{
		time.Unix(100000, 0), time.Unix(1000000, 0), 3600,
		[]*timeSlot{
			{
				tableName: DynamoDBTableOneWeek + "-0",
				itemEpoch: 0,
			},
			{
				tableName: DynamoDBTableOneWeek + "-604800",
				itemEpoch: 604800,
			},
		},
	},
	{
		time.Unix(1000000, 0), time.Unix(100000000, 0), 86400,
		[]*timeSlot{
			{
				tableName: DynamoDBTableOneYear + "-0",
				itemEpoch: 0,
			},
			{
				tableName: DynamoDBTableOneYear + "-31104000",
				itemEpoch: 31104000,
			},
			{
				tableName: DynamoDBTableOneYear + "-62208000",
				itemEpoch: 62208000,
			},
			{
				tableName: DynamoDBTableOneYear + "-93312000",
				itemEpoch: 93312000,
			},
		},
	},
}

func TestSelectTimeSlots(t *testing.T) {
	for _, lc := range selectTimeSlotsTests {
		slots, step := selectTimeSlots(lc.start, lc.end)

		if step != lc.step {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", lc.step, step)
		}
		if !reflect.DeepEqual(slots, lc.timeSlots) {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", lc.timeSlots, slots)
		}
	}
}

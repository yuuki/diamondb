package dynamo

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"
	"github.com/kylelemons/godebug/pretty"
	"github.com/yuuki/diamondb/lib/series"
)

func TestFetchMetricsFromDynamoDB(t *testing.T) {
	name := "roleA.r.{1,2}.loadavg"
	expected := series.SeriesMap{
		"roleA.r.1.loadavg": series.NewSeriesPoint(
			"roleA.r.1.loadavg",
			series.DataPoints{
				series.NewDataPoint(120, 10.0),
				series.NewDataPoint(180, 11.2),
				series.NewDataPoint(240, 13.1),
			},
			60,
		),
		"roleA.r.2.loadavg": series.NewSeriesPoint(
			"roleA.r.2.loadavg",
			series.DataPoints{
				series.NewDataPoint(120, 1.0),
				series.NewDataPoint(180, 1.2),
				series.NewDataPoint(240, 1.1),
			},
			60,
		),
	}
	ctrl := SetMockDynamoDB(t, &MockDynamoDBParam{
		TableName: DynamoDBTableOneHour + "-0",
		ItemEpoch: 0,
		SeriesMap: expected,
	})
	defer ctrl.Finish()

	sm, err := FetchMetricsFromDynamoDB(name, time.Unix(100, 0), time.Unix(300, 0))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
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
	sm, err := FetchMetricsFromDynamoDB(name, time.Unix(100, 0), time.Unix(300, 0))
	if err != nil {
		t.Fatalf("Should ignore NotFound error: %s", err)
	}
	if len(sm) != 0 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 0, len(sm))
	}
}

func TestBatchGet(t *testing.T) {
	expected := series.SeriesMap{
		"server1.loadavg5": series.NewSeriesPoint(
			"server1.loadavg5",
			series.DataPoints{
				series.NewDataPoint(1465516810, 10.0),
			},
			60,
		),
		"server2.loadavg5": series.NewSeriesPoint(
			"server2.loadavg5",
			series.DataPoints{
				series.NewDataPoint(1465516810, 15.0),
			},
			60,
		),
	}
	ctrl := SetMockDynamoDB(t, &MockDynamoDBParam{
		TableName: DynamoDBTableOneHour + "-0",
		ItemEpoch: 1000,
		SeriesMap: expected,
	})
	defer ctrl.Finish()

	sm, err := batchGet(
		&timeSlot{DynamoDBTableOneHour + "-0", 1000},
		[]string{"server1.loadavg5", "server2.loadavg5"},
		60,
	)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestConcurrentBatchGet(t *testing.T) {
	expected := series.SeriesMap{
		"server1.loadavg5": series.NewSeriesPoint(
			"server1.loadavg5",
			series.DataPoints{
				series.NewDataPoint(1465516810, 10.0),
			},
			60,
		),
		"server2.loadavg5": series.NewSeriesPoint(
			"server2.loadavg5",
			series.DataPoints{
				series.NewDataPoint(1465516810, 15.0),
			},
			60,
		),
	}
	ctrl := SetMockDynamoDB(t, &MockDynamoDBParam{
		TableName: DynamoDBTableOneHour + "-0",
		ItemEpoch: 1000,
		SeriesMap: expected,
	})
	defer ctrl.Finish()

	c := make(chan interface{})
	concurrentBatchGet(
		&timeSlot{DynamoDBTableOneHour + "-0", 1000},
		[]string{"server1.loadavg5", "server2.loadavg5"},
		60,
		c,
	)
	ret := <-c
	sm := ret.(series.SeriesMap)
	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
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
		if diff := pretty.Compare(lc.timeSlots, slots); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
	}
}

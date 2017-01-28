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

func TestPing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)
	mock.EXPECT().DescribeLimits(gomock.Any()).Return(
		&dynamodb.DescribeLimitsOutput{}, nil,
	)
	d := newTestDynamoDB(mock)
	err := d.Ping()
	if err != nil {
		t.Fatalf("unexpected error occurs %s", err)
	}
}

func TestFetchSeriesMap(t *testing.T) {
	name := "roleA.r.{1,2}.loadavg"
	expected := series.SeriesMap{
		"roleA.r.1.loadavg": series.NewSeriesPoint(
			"roleA.r.1.loadavg",
			series.DataPoints{
				series.NewDataPoint(120, 10.0),
				series.NewDataPoint(180, 11.2),
				series.NewDataPoint(240, 13.1),
			}, 60,
		),
		"roleA.r.2.loadavg": series.NewSeriesPoint(
			"roleA.r.2.loadavg",
			series.DataPoints{
				series.NewDataPoint(120, 1.0),
				series.NewDataPoint(180, 1.2),
				series.NewDataPoint(240, 1.1),
			}, 60,
		),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)
	param := &mockDynamoDBParam{
		Resolution: "1m1h",
		TableEpoch: 0,
		ItemEpoch:  0,
		SeriesMap:  expected,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param), param)

	d := newTestDynamoDB(mock)
	sm, err := d.Fetch(name, time.Unix(100, 0), time.Unix(300, 0))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestFetchSeriesMap_Concurrent(t *testing.T) {
	tmp := dynamodbBatchLimit
	dynamodbBatchLimit = 1
	defer func() { dynamodbBatchLimit = tmp }()
	name := "roleA.r.{1,2}.loadavg"
	expected1 := series.SeriesMap{
		"roleA.r.1.loadavg": series.NewSeriesPoint(
			"roleA.r.1.loadavg",
			series.DataPoints{
				series.NewDataPoint(120, 10.0),
				series.NewDataPoint(180, 11.2),
				series.NewDataPoint(240, 13.1),
			}, 60,
		),
	}
	expected2 := series.SeriesMap{
		"roleA.r.2.loadavg": series.NewSeriesPoint(
			"roleA.r.2.loadavg",
			series.DataPoints{
				series.NewDataPoint(120, 1.0),
				series.NewDataPoint(180, 1.2),
				series.NewDataPoint(240, 1.1),
			}, 60,
		),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)

	param1 := &mockDynamoDBParam{
		Resolution: "1m1h",
		TableEpoch: 0,
		ItemEpoch:  0,
		SeriesMap:  expected1,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param1), param1)

	param2 := &mockDynamoDBParam{
		Resolution: "1m1h",
		TableEpoch: 0,
		ItemEpoch:  0,
		SeriesMap:  expected2,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param2), param2)

	d := newTestDynamoDB(mock)
	sm, err := d.Fetch(name, time.Unix(100, 0), time.Unix(300, 0))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := series.SeriesMap{
		"roleA.r.1.loadavg": series.NewSeriesPoint(
			"roleA.r.1.loadavg",
			series.DataPoints{
				series.NewDataPoint(120, 10.0),
				series.NewDataPoint(180, 11.2),
				series.NewDataPoint(240, 13.1),
			}, 60,
		),
		"roleA.r.2.loadavg": series.NewSeriesPoint(
			"roleA.r.2.loadavg",
			series.DataPoints{
				series.NewDataPoint(120, 1.0),
				series.NewDataPoint(180, 1.2),
				series.NewDataPoint(240, 1.1),
			}, 60,
		),
	}
	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestFetchSeriesMap_Concurrent_TheSameNameButTheSlotIsDifferent(t *testing.T) {
	tmp := dynamodbBatchLimit
	dynamodbBatchLimit = 1
	defer func() { dynamodbBatchLimit = tmp }()
	expected1 := series.SeriesMap{
		"roleA.r.1.loadavg": series.NewSeriesPoint(
			"roleA.r.1.loadavg",
			series.DataPoints{
				series.NewDataPoint(120, 10.0),
				series.NewDataPoint(180, 11.2),
				series.NewDataPoint(240, 13.1),
			}, 60,
		),
	}
	expected2 := series.SeriesMap{
		"roleA.r.1.loadavg": series.NewSeriesPoint(
			"roleA.r.1.loadavg",
			series.DataPoints{
				series.NewDataPoint(3600, 1.0),
				series.NewDataPoint(3660, 1.2),
				series.NewDataPoint(3720, 1.1),
				series.NewDataPoint(3780, 1.1),
			}, 60,
		),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)

	param1 := &mockDynamoDBParam{
		Resolution: "1m1h",
		TableEpoch: 0,
		ItemEpoch:  0,
		SeriesMap:  expected1,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param1), param1)

	param2 := &mockDynamoDBParam{
		Resolution: "1m1h",
		TableEpoch: 0,
		ItemEpoch:  3600,
		SeriesMap:  expected2,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param2), param2)

	d := newTestDynamoDB(mock)
	sm, err := d.Fetch("roleA.r.1.loadavg", time.Unix(100, 0), time.Unix(4000, 0))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := series.SeriesMap{
		"roleA.r.1.loadavg": series.NewSeriesPoint(
			"roleA.r.1.loadavg",
			series.DataPoints{
				series.NewDataPoint(120, 10.0),
				series.NewDataPoint(180, 11.2),
				series.NewDataPoint(240, 13.1),
				series.NewDataPoint(3600, 1.0),
				series.NewDataPoint(3660, 1.2),
				series.NewDataPoint(3720, 1.1),
				series.NewDataPoint(3780, 1.1),
			}, 60,
		),
	}
	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestFetchSeriesMap_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	dmock := NewMockDynamoDBAPI(ctrl)
	defer ctrl.Finish()

	responses := make(map[string][]map[string]*dynamodb.AttributeValue)
	responses[mockTableName("1m1h", 0)] = []map[string]*dynamodb.AttributeValue{}
	reqErr := awserr.NewRequestFailure(
		awserr.New("ResourceNotFoundException", "resource not found", errors.New("dummy")),
		404, "dummyID",
	)
	dmock.EXPECT().BatchGetItem(gomock.Any()).Return(
		&dynamodb.BatchGetItemOutput{Responses: responses}, reqErr,
	)
	d := newTestDynamoDB(dmock)

	name := "roleA.r.{1,2}.loadavg"
	sm, err := d.Fetch(name, time.Unix(100, 0), time.Unix(300, 0))
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)
	param := &mockDynamoDBParam{
		Resolution: "1m1h",
		TableEpoch: 0,
		ItemEpoch:  1000,
		SeriesMap:  expected,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param), param)
	d := newTestDynamoDB(mock)

	sm, err := d.batchGet(
		&timeSlot{mockTableName("1m1h", 0), 1000},
		[]string{"server1.loadavg5", "server2.loadavg5"},
		60,
	)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	for name, series := range sm {
		if diff := pretty.Compare(series, expected[name]); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)
	param := &mockDynamoDBParam{
		Resolution: "1m1h",
		TableEpoch: 0,
		ItemEpoch:  1000,
		SeriesMap:  expected,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param), param)
	d := newTestDynamoDB(mock)

	c := make(chan interface{})
	d.concurrentBatchGet(
		&timeSlot{mockTableName("1m1h", 0), 1000},
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
				tableName: mockTableName("1m1h", 0),
				itemEpoch: 0,
			},
			{
				tableName: mockTableName("1m1h", 0),
				itemEpoch: 3600,
			},
		},
	},
	{
		time.Unix(10000, 0), time.Unix(100000, 0), 300,
		[]*timeSlot{
			{
				tableName: mockTableName("5m1d", 0),
				itemEpoch: 0,
			},
			{
				tableName: mockTableName("5m1d", 86400),
				itemEpoch: 86400,
			},
		},
	},
	{
		time.Unix(100000, 0), time.Unix(1000000, 0), 3600,
		[]*timeSlot{
			{
				tableName: mockTableName("1h7d", 0),
				itemEpoch: 0,
			},
			{
				tableName: mockTableName("1h7d", 604800),
				itemEpoch: 604800,
			},
		},
	},
	{
		time.Unix(1000000, 0), time.Unix(100000000, 0), 86400,
		[]*timeSlot{
			{
				tableName: mockTableName("1d360d", 0),
				itemEpoch: 0,
			},
			{
				tableName: mockTableName("1d360d", 31104000),
				itemEpoch: 31104000,
			},
			{
				tableName: mockTableName("1d360d", 62208000),
				itemEpoch: 62208000,
			},
			{
				tableName: mockTableName("1d360d", 93312000),
				itemEpoch: 93312000,
			},
		},
	},
}

func TestSelectTimeSlots(t *testing.T) {
	for _, lc := range selectTimeSlotsTests {
		slots, step := selectTimeSlots(lc.start, lc.end, testTableNamePrefix)

		if step != lc.step {
			t.Fatalf("\nExpected: %+v\nActual:   %+v", lc.step, step)
		}
		if diff := pretty.Compare(lc.timeSlots, slots); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
	}
}

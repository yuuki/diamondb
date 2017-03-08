package dynamodb

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	godynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"
	"github.com/kylelemons/godebug/pretty"
	"github.com/yuuki/diamondb/pkg/model"
)

func TestPing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)
	mock.EXPECT().DescribeLimits(gomock.Any()).Return(
		&godynamodb.DescribeLimitsOutput{}, nil,
	)
	d := NewTestDynamoDB(mock)
	err := d.Ping()
	if err != nil {
		t.Fatalf("unexpected error occurs %s", err)
	}
}

func TestFetchSeriesMap(t *testing.T) {
	name := "roleA.r.{1,2}.loadavg"
	expected := model.SeriesMap{
		"roleA.r.1.loadavg": model.NewSeriesPoint(
			"roleA.r.1.loadavg",
			model.DataPoints{
				model.NewDataPoint(120, 10.0),
				model.NewDataPoint(180, 11.2),
				model.NewDataPoint(240, 13.1),
			}, 60,
		),
		"roleA.r.2.loadavg": model.NewSeriesPoint(
			"roleA.r.2.loadavg",
			model.DataPoints{
				model.NewDataPoint(120, 1.0),
				model.NewDataPoint(180, 1.2),
				model.NewDataPoint(240, 1.1),
			}, 60,
		),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)
	param := &mockDynamoDBParam{
		Slot:      &timeSlot{itemEpoch: 0, step: 60},
		SeriesMap: expected,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param), param)

	d := NewTestDynamoDB(mock)
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
	expected1 := model.SeriesMap{
		"roleA.r.1.loadavg": model.NewSeriesPoint(
			"roleA.r.1.loadavg",
			model.DataPoints{
				model.NewDataPoint(120, 10.0),
				model.NewDataPoint(180, 11.2),
				model.NewDataPoint(240, 13.1),
			}, 60,
		),
	}
	expected2 := model.SeriesMap{
		"roleA.r.2.loadavg": model.NewSeriesPoint(
			"roleA.r.2.loadavg",
			model.DataPoints{
				model.NewDataPoint(120, 1.0),
				model.NewDataPoint(180, 1.2),
				model.NewDataPoint(240, 1.1),
			}, 60,
		),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)

	param1 := &mockDynamoDBParam{
		Slot:      &timeSlot{itemEpoch: 0, step: 60},
		SeriesMap: expected1,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param1), param1)

	param2 := &mockDynamoDBParam{
		Slot:      &timeSlot{itemEpoch: 0, step: 60},
		SeriesMap: expected2,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param2), param2)

	d := NewTestDynamoDB(mock)
	sm, err := d.Fetch(name, time.Unix(100, 0), time.Unix(300, 0))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := model.SeriesMap{
		"roleA.r.1.loadavg": model.NewSeriesPoint(
			"roleA.r.1.loadavg",
			model.DataPoints{
				model.NewDataPoint(120, 10.0),
				model.NewDataPoint(180, 11.2),
				model.NewDataPoint(240, 13.1),
			}, 60,
		),
		"roleA.r.2.loadavg": model.NewSeriesPoint(
			"roleA.r.2.loadavg",
			model.DataPoints{
				model.NewDataPoint(120, 1.0),
				model.NewDataPoint(180, 1.2),
				model.NewDataPoint(240, 1.1),
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
	expected1 := model.SeriesMap{
		"roleA.r.1.loadavg": model.NewSeriesPoint(
			"roleA.r.1.loadavg",
			model.DataPoints{
				model.NewDataPoint(120, 10.0),
				model.NewDataPoint(180, 11.2),
				model.NewDataPoint(240, 13.1),
			}, 60,
		),
	}
	expected2 := model.SeriesMap{
		"roleA.r.1.loadavg": model.NewSeriesPoint(
			"roleA.r.1.loadavg",
			model.DataPoints{
				model.NewDataPoint(3600, 1.0),
				model.NewDataPoint(3660, 1.2),
				model.NewDataPoint(3720, 1.1),
				model.NewDataPoint(3780, 1.1),
			}, 60,
		),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)

	param1 := &mockDynamoDBParam{
		Slot:      &timeSlot{itemEpoch: 0, step: 60},
		SeriesMap: expected1,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param1), param1)

	param2 := &mockDynamoDBParam{
		Slot:      &timeSlot{itemEpoch: 3600, step: 60},
		SeriesMap: expected2,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param2), param2)

	d := NewTestDynamoDB(mock)
	sm, err := d.Fetch("roleA.r.1.loadavg", time.Unix(100, 0), time.Unix(4000, 0))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := model.SeriesMap{
		"roleA.r.1.loadavg": model.NewSeriesPoint(
			"roleA.r.1.loadavg",
			model.DataPoints{
				model.NewDataPoint(120, 10.0),
				model.NewDataPoint(180, 11.2),
				model.NewDataPoint(240, 13.1),
				model.NewDataPoint(3600, 1.0),
				model.NewDataPoint(3660, 1.2),
				model.NewDataPoint(3720, 1.1),
				model.NewDataPoint(3780, 1.1),
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

	responses := make(map[string][]map[string]*godynamodb.AttributeValue)
	responses[mockTableName] = []map[string]*godynamodb.AttributeValue{}
	reqErr := awserr.NewRequestFailure(
		awserr.New("ResourceNotFoundException", "resource not found", errors.New("dummy")),
		404, "dummyID",
	)
	dmock.EXPECT().BatchGetItem(gomock.Any()).Return(
		&godynamodb.BatchGetItemOutput{Responses: responses}, reqErr,
	)
	d := NewTestDynamoDB(dmock)

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
	expected := model.SeriesMap{
		"server1.loadavg5": model.NewSeriesPoint(
			"server1.loadavg5",
			model.DataPoints{
				model.NewDataPoint(1100, 10.0),
			},
			60,
		),
		"server2.loadavg5": model.NewSeriesPoint(
			"server2.loadavg5",
			model.DataPoints{
				model.NewDataPoint(1100, 15.0),
			},
			60,
		),
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockDynamoDBAPI(ctrl)
	param := &mockDynamoDBParam{
		Slot:      &timeSlot{itemEpoch: 1000, step: 60},
		SeriesMap: expected,
	}
	mockReturnBatchGetItem(mockExpectBatchGetItem(mock, param), param)
	d := NewTestDynamoDB(mock)

	sm, err := d.batchGet(&query{
		names: []string{"server1.loadavg5", "server2.loadavg5"},
		start: time.Unix(1000, 0),
		end:   time.Unix(2000, 0),
		slot:  &timeSlot{itemEpoch: 1000, step: 60},
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	for name, series := range sm {
		if diff := pretty.Compare(series, expected[name]); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
	}
}

func TestSelectTimeSlots(t *testing.T) {
	tests := []struct {
		start     time.Time
		end       time.Time
		timeSlots []*timeSlot
	}{
		{
			time.Unix(100, 0), time.Unix(6000, 0),
			[]*timeSlot{{itemEpoch: 0, step: 60}, {itemEpoch: 3600, step: 60}},
		},
		{
			time.Unix(10000, 0), time.Unix(100000, 0),
			[]*timeSlot{{itemEpoch: 0, step: 300}, {itemEpoch: 86400, step: 300}},
		},
		{
			time.Unix(100000, 0), time.Unix(1000000, 0),
			[]*timeSlot{{itemEpoch: 0, step: 3600}, {itemEpoch: 604800, step: 3600}},
		},
		{
			time.Unix(1000000, 0), time.Unix(100000000, 0),
			[]*timeSlot{
				{
					itemEpoch: 0,
					step:      86400,
				},
				{
					itemEpoch: 31104000,
					step:      86400,
				},
				{
					itemEpoch: 62208000,
					step:      86400,
				},
				{
					itemEpoch: 93312000,
					step:      86400,
				},
			},
		},
	}

	for _, lc := range tests {
		got := selectTimeSlots(lc.start, lc.end)

		if diff := pretty.Compare(lc.timeSlots, got); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
	}
}

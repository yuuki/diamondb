package tsdb

import (
	"fmt"
	"reflect"
	"testing"
	"time"

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

func TestGroupNames(t *testing.T) {
	var names []string
	for i := 1; i <= 5; i++ {
		names = append(names, fmt.Sprintf("server%d.loadavg5", i))
	}
	nameGroups := groupNames(names, 2)
	expected := [][]string{
		{"server1.loadavg5", "server2.loadavg5"},
		{"server3.loadavg5", "server4.loadavg5"},
		{"server5.loadavg5"},
	}
	if !reflect.DeepEqual(nameGroups, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, nameGroups)
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

func TestSplitName(t *testing.T) {
	name := "roleA.r.{1,2,3,4}.loadavg"
	names := splitName(name)
	expected := []string{
		"roleA.r.1.loadavg",
		"roleA.r.2.loadavg",
		"roleA.r.3.loadavg",
		"roleA.r.4.loadavg",
	}
	if !reflect.DeepEqual(names, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, names)
	}
}

func TestListTablesByRange_1m1h(t *testing.T) {
	slots, step := listTimeSlots(time.Unix(100, 0), time.Unix(6000, 0))

	if step != 60 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 60, step)
	}
	expected := []*timeSlot{
		{
			tableName: DynamoDBTableOneHour + "-0",
			itemEpoch: 0,
		},
		{
			tableName: DynamoDBTableOneHour + "-0",
			itemEpoch: 3600,
		},
	}
	if !reflect.DeepEqual(slots, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, slots)
	}
}

func TestListTablesByRange_5m1d(t *testing.T) {
	slots, step := listTimeSlots(time.Unix(10000, 0), time.Unix(100000, 0))

	if step != 300 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 300, step)
	}
	expected := []*timeSlot{
		{
			tableName: DynamoDBTableOneDay + "-0",
			itemEpoch: 0,
		},
		{
			tableName: DynamoDBTableOneDay + "-86400",
			itemEpoch: 86400,
		},
	}
	if !reflect.DeepEqual(slots, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, slots)
	}
}

func TestListTablesByRange_1h7d(t *testing.T) {
	slots, step := listTimeSlots(time.Unix(100000, 0), time.Unix(1000000, 0))

	if step != 3600 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 3600, step)
	}
	expected := []*timeSlot{
		{
			tableName: DynamoDBTableOneWeek + "-0",
			itemEpoch: 0,
		},
		{
			tableName: DynamoDBTableOneWeek + "-604800",
			itemEpoch: 604800,
		},
	}
	if !reflect.DeepEqual(slots, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, slots)
	}
}

func TestListTablesByRange_1d360d(t *testing.T) {
	slots, step := listTimeSlots(time.Unix(1000000, 0), time.Unix(100000000, 0))

	if step != 86400 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 86400, step)
	}
	expected := []*timeSlot{
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
	}
	if !reflect.DeepEqual(slots, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, slots)
	}
}

package tsdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/yuuki/dynamond/model"
)

func TestFetchMetricsFromDynamoDB(t *testing.T) {
	name := "roleA.r.{1,2}.loadavg"
	expected := []*model.Metric{
		model.NewMetric(
			"roleA.r.1.loadavg",
			[]*model.DataPoint{
				&model.DataPoint{120, 10.0},
				&model.DataPoint{180, 11.2},
				&model.DataPoint{240, 13.1},
			},
			60,
		),
		model.NewMetric(
			"roleA.r.2.loadavg",
			[]*model.DataPoint{
				&model.DataPoint{120, 1.0},
				&model.DataPoint{180, 1.2},
				&model.DataPoint{240, 1.1},
			},
			60,
		),
	}
	ctrl := SetMockDynamoDB(t, &MockDynamoDBParam{
		TableName: "SeriesTestRange-1m1h-0",
		ItemEpoch: 0,
		Metrics: expected,
	})
	defer ctrl.Finish()
	metrics, err := FetchMetricsFromDynamoDB(name, time.Unix(100, 0), time.Unix(300, 0))
	if assert.NoError(t, err) {
		assert.Exactly(t, expected, metrics)
	}
}

func TestGroupNames(t *testing.T) {
	var names []string
	for i := 1; i <= 5; i++ {
		names = append(names, fmt.Sprintf("server%d.loadavg5", i))
	}
	nameGroups := groupNames(names, 2)
	expected := [][]string{
		[]string{"server1.loadavg5", "server2.loadavg5"},
		[]string{"server3.loadavg5", "server4.loadavg5"},
		[]string{"server5.loadavg5"},
	}
	assert.Exactly(t, expected, nameGroups)
}

func TestBatchGet(t *testing.T) {
	expected := []*model.Metric{
		model.NewMetric(
			"server1.loadavg5",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 10.0},
			},
			60,
		),
		model.NewMetric(
			"server2.loadavg5",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 15.0},
			},
			60,
		),
	}
	ctrl := SetMockDynamoDB(t, &MockDynamoDBParam{
		TableName: "SeriesTestRange",
		ItemEpoch: 1000,
		Metrics: expected,
	})
	defer ctrl.Finish()
	metrics, err := batchGet(&timeSlot{"SeriesTestRange", 1000}, []string{"server1.loadavg5", "server2.loadavg5"}, 60)
	assert.NoError(t, err)
	assert.Exactly(t, expected, metrics)
}

func TestConcurrentBatchGet(t *testing.T) {
	expected := []*model.Metric{
		model.NewMetric(
			"server1.loadavg5",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 10.0},
			},
			60,
		),
		model.NewMetric(
			"server2.loadavg5",
			[]*model.DataPoint{
				&model.DataPoint{1465516810, 15.0},
			},
			60,
		),
	}
	ctrl := SetMockDynamoDB(t, &MockDynamoDBParam{
		TableName: "SeriesTestRange",
		ItemEpoch: 1000,
		Metrics: expected,
	})
	defer ctrl.Finish()
	c := make(chan interface{})
	concurrentBatchGet(&timeSlot{"SeriesTestRange", 1000}, []string{"server1.loadavg5", "server2.loadavg5"}, 60, c)
	var metrics []*model.Metric
	ret := <-c
	metrics = append(metrics, ret.([]*model.Metric)...)
	assert.Exactly(t, expected, metrics)
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
	assert.Exactly(t, expected, names)
}

func TestListTablesByRange_1m1h(t *testing.T) {
	s, e := time.Unix(100, 0), time.Unix(6000, 0)
	slots, step := listTimeSlots(s, e)
	assert.Exactly(t, 60, step)
	expected := []*timeSlot{
		&timeSlot{
			tableName: "SeriesTestRange-1m1h-0",
			itemEpoch: 0,
		},
		&timeSlot{
			tableName: "SeriesTestRange-1m1h-0",
			itemEpoch: 3600,
		},
	}
	assert.Exactly(t, expected, slots)
}

func TestListTablesByRange_5m1d(t *testing.T) {
	s, e := time.Unix(10000, 0), time.Unix(100000, 0)
	slots, step := listTimeSlots(s, e)
	assert.Exactly(t, 300, step)
	expected := []*timeSlot{
		&timeSlot{
			tableName: "SeriesTestRange-5m1d-0",
			itemEpoch: 0,
		},
		&timeSlot{
			tableName: "SeriesTestRange-5m1d-86400",
			itemEpoch: 86400,
		},
	}
	assert.Exactly(t, expected, slots)
}

func TestListTablesByRange_1h7d(t *testing.T) {
	s, e := time.Unix(100000, 0), time.Unix(1000000, 0)
	slots, step := listTimeSlots(s, e)
	assert.Exactly(t, 3600, step)
	expected := []*timeSlot{
		&timeSlot{
			tableName: "SeriesTestRange-1h7d-0",
			itemEpoch: 0,
		},
		&timeSlot{
			tableName: "SeriesTestRange-1h7d-604800",
			itemEpoch: 604800,
		},
	}
	assert.Exactly(t, expected, slots)
}

func TestListTablesByRange_1d360d(t *testing.T) {
	s, e := time.Unix(1000000, 0), time.Unix(100000000, 0)
	slots, step := listTimeSlots(s, e)
	assert.Exactly(t, 86400, step)
	expected := []*timeSlot{
		&timeSlot{
			tableName: "SeriesTestRange-1d360d-0",
			itemEpoch: 0,
		},
		&timeSlot{
			tableName: "SeriesTestRange-1d360d-31104000",
			itemEpoch: 31104000,
		},
		&timeSlot{
			tableName: "SeriesTestRange-1d360d-62208000",
			itemEpoch: 62208000,
		},
		&timeSlot{
			tableName: "SeriesTestRange-1d360d-93312000",
			itemEpoch: 93312000,
		},
	}
	assert.Exactly(t, expected, slots)
}

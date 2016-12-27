package tsdb

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"
	"math"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"

	"github.com/yuuki/dynamond/mathutil"
	"github.com/yuuki/dynamond/model"
)

type timeSlot struct {
	tableName string
	itemEpoch int64
}

const (
	dynamoDBTablePrefix string = "SeriesTestRange"
	DynamoDBTableOneYear string = dynamoDBTablePrefix + "-1d360d"
	DynamoDBTableOneWeek string = dynamoDBTablePrefix + "-1h7d"
	DynamoDBTableOneDay  string = dynamoDBTablePrefix + "-5m1d"
	DynamoDBTableOneHour string = dynamoDBTablePrefix + "-1m1h"
	oneYear time.Duration = time.Duration(24 * 360) * time.Hour
	oneWeek time.Duration = time.Duration(24 * 7) * time.Hour
	oneDay  time.Duration = time.Duration(24 * 1) * time.Hour

	dynamodbBatchLimit = 100
)

var (
	oneYearSeconds int = int(oneYear.Seconds())
	oneWeekSeconds int = int(oneWeek.Seconds())
	oneDaySeconds  int = int(oneDay.Seconds())

	dsvc dynamodbiface.DynamoDBAPI = dynamodb.New(session.New(), &aws.Config{Region: aws.String("ap-northeast-1")})
)

// SetClient replace svc to mock dynamodb client
func SetDynamoDB(client dynamodbiface.DynamoDBAPI) {
	dsvc = client
}

func FetchMetricsFromDynamoDB(name string, start, end time.Time) ([]*model.Metric, error) {
	slots, step := listTimeSlots(start, end)
	nameGroups := groupNames(splitName(name), dynamodbBatchLimit)
	c := make(chan interface{})
	for _, slot := range slots {
		for _, names := range nameGroups {
			concurrentBatchGet(slot, names, step, c)
		}
	}
	var metrics []*model.Metric
	for i := 0; i < len(slots) * len(nameGroups); i++ {
		ret := <-c
		switch ret.(type) {
		case []*model.Metric:
			metrics = append(metrics, ret.([]*model.Metric)...)
		case error:
			fmt.Println(ret.(error)) //TODO error handling
		}
	}

	return metrics, nil
}

func groupNames(names []string, count int) [][]string {
	nameGroups := make([][]string, 0, (len(names)+count-1)/count)
	for i, name := range names {
		if i%count == 0 {
			nameGroups = append(nameGroups, []string{})
		}
		nameGroups[len(nameGroups)-1] = append(nameGroups[len(nameGroups)-1], name)
	}
	return nameGroups
}

func batchGetResultToMap(resp *dynamodb.BatchGetItemOutput, step int) []*model.Metric {
	metrics := make([]*model.Metric, 0, 1)
	for _, xs := range resp.Responses {
		for _, x := range xs {
			name := (*x["MetricName"].S)
			points := make([]*model.DataPoint, 0, len(x["Values"].BS))
			for _, y := range x["Values"].BS {
				t := int64(binary.BigEndian.Uint64(y[0:8]))
				v := math.Float64frombits(binary.BigEndian.Uint64(y[8:]))
				points = append(points, model.NewDataPoint(t, v))
			}
			metrics = append(metrics, model.NewMetric(name, points, step))
		}
	}
	return metrics
}

func batchGet(slot *timeSlot, names []string, step int) ([]*model.Metric, error) {
	var keys []map[string]*dynamodb.AttributeValue
	for _, name := range names {
		keys = append(keys, map[string]*dynamodb.AttributeValue{
			"MetricName": &dynamodb.AttributeValue{S: aws.String(name)},
			"Timestamp": &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", slot.itemEpoch))},
		})
	}
	items := make(map[string]*dynamodb.KeysAndAttributes)
	items[slot.tableName] = &dynamodb.KeysAndAttributes{Keys: keys}
	params := &dynamodb.BatchGetItemInput{
		RequestItems:           items,
		ReturnConsumedCapacity: aws.String("NONE"),
	}
	resp, err := dsvc.BatchGetItem(params)
	if err != nil {
		return nil, errors.Wrapf(err,
			"Failed to BatchGetItem %s %d %s",
			slot.tableName, slot.itemEpoch, strings.Join(names, ","),
		)
	}
	return batchGetResultToMap(resp, step), nil
}

func concurrentBatchGet(slot *timeSlot, names []string, step int, c chan<- interface{}) {
	go func() {
		resp, err := batchGet(slot, names, step)
		if err != nil {
			c <- errors.Wrapf(err,
				"Failed to batchGet %s %d %s %d",
				slot.tableName, slot.itemEpoch, strings.Join(names, ","), step,
			)
		} else {
			c <- resp
		}
	}()
}

// roleA.r.{1,2,3,4}.loadavg
func splitName(name string) []string {
	open := strings.IndexRune(name, '{')
	close := strings.IndexRune(name, '}')
	var names []string
	if open >= 0 && close >= 0 {
		prefix := name[0:open]
		indices := name[open+1 : close]
		suffix := name[close+1:]
		for _, i := range strings.Split(indices, ",") {
			names = append(names, prefix+i+suffix)
		}
	} else {
		names = strings.Split(name, ",")
	}
	return names
}

func listTimeSlots(startTime, endTime time.Time) ([]*timeSlot, int) {
	var (
		tableName string
		step int
		tableEpochStep int
		itemEpochStep  int
	)
	diffTime := endTime.Sub(startTime)
	if oneYear <= diffTime {
		tableName = DynamoDBTableOneYear
		tableEpochStep = oneYearSeconds
		itemEpochStep = tableEpochStep
		step = 60 * 60 * 24
	} else if oneWeek <= diffTime {
		tableName = DynamoDBTableOneWeek
		tableEpochStep = 60 * 60 * 24 * 7
		itemEpochStep = tableEpochStep
		step = 60 * 60
	} else if oneDay <= diffTime {
		tableName = DynamoDBTableOneDay
		tableEpochStep = 60 * 60 * 24
		itemEpochStep = tableEpochStep
		step = 5 * 60
	} else {
		tableName = DynamoDBTableOneHour
		tableEpochStep = 60 * 60 * 24
		itemEpochStep = 60 * 60
		step = 60
	}

	slots := make([]*timeSlot, 0, 5)
	startTableEpoch := startTime.Unix() - startTime.Unix() % int64(tableEpochStep)
	endTableEpoch := endTime.Unix()
	for tableEpoch := startTableEpoch; tableEpoch < endTableEpoch; tableEpoch += int64(tableEpochStep) {
		startItemEpoch := mathutil.MaxInt64(tableEpoch, startTime.Unix() - startTime.Unix() % int64(itemEpochStep))
		endItemEpoch := mathutil.MinInt64(tableEpoch + int64(tableEpochStep), endTime.Unix())
		for itemEpoch := startItemEpoch; itemEpoch < endItemEpoch; itemEpoch += int64(itemEpochStep) {
			slot := timeSlot{
				tableName: fmt.Sprintf("%s-%d", tableName, tableEpoch),
				itemEpoch: itemEpoch,
			}
			slots = append(slots, &slot)
		}
	}

	return slots, step
}


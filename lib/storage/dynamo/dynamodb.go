package dynamo

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/config"
	"github.com/yuuki/diamondb/lib/mathutil"
	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/util"
)

// DynamoDB provides a dynamodb client.
type DynamoDB struct {
	svc         dynamodbiface.DynamoDBAPI
	tablePrefix string
}

type timeSlot struct {
	tableName string
	itemEpoch int64
}

type query struct {
	names []string
	start time.Time
	end   time.Time
	slot  *timeSlot
	step  int
	// context
}

const (
	oneYear time.Duration = time.Duration(24*360) * time.Hour
	oneWeek time.Duration = time.Duration(24*7) * time.Hour
	oneDay  time.Duration = time.Duration(24*1) * time.Hour
)

var (
	dynamodbBatchLimit = 100

	oneYearSeconds = int(oneYear.Seconds())
	oneWeekSeconds = int(oneWeek.Seconds())
	oneDaySeconds  = int(oneDay.Seconds())
)

// NewDynamoDB creates a new DynamoDB.
func NewDynamoDB() *DynamoDB {
	return &DynamoDB{
		svc: dynamodb.New(
			session.New(),
			aws.NewConfig().WithRegion(config.Config.DynamoDBRegion),
		),
		tablePrefix: config.Config.DynamoDBTablePrefix,
	}
}

// Ping pings DynamoDB endpoint.
func (d *DynamoDB) Ping() error {
	var params *dynamodb.DescribeLimitsInput
	_, err := d.svc.DescribeLimits(params)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Fetch fetches datapoints by name from start until end.
func (d *DynamoDB) Fetch(name string, start, end time.Time) (series.SeriesMap, error) {
	slots, step := selectTimeSlots(start, end, d.tablePrefix)
	nameGroups := util.GroupNames(util.SplitName(name), dynamodbBatchLimit)
	numQueries := len(slots) * len(nameGroups)
	c := make(chan interface{}, numQueries)
	for _, slot := range slots {
		for _, names := range nameGroups {
			q := &query{
				names: names,
				start: start,
				end:   end,
				slot:  slot,
				step:  step,
			}
			go func() {
				resp, err := d.batchGet(q)
				if err != nil {
					c <- errors.WithStack(err)
				} else {
					c <- resp
				}
			}()
		}
	}
	sm := make(series.SeriesMap, len(nameGroups))
	for i := 0; i < numQueries; i++ {
		ret := <-c
		switch ret.(type) {
		case series.SeriesMap:
			sm.MergePointsToMap(ret.(series.SeriesMap))
		case error:
			return nil, errors.WithStack(ret.(error))
		}
	}
	return sm, nil
}

func batchGetResultToMap(resp *dynamodb.BatchGetItemOutput, q *query) series.SeriesMap {
	sm := make(series.SeriesMap, len(resp.Responses))
	for _, xs := range resp.Responses {
		for _, x := range xs {
			name := (*x["MetricName"].S)
			points := make(series.DataPoints, 0, len(x["Values"].BS))
			for _, y := range x["Values"].BS {
				t := int64(binary.BigEndian.Uint64(y[0:8]))
				v := math.Float64frombits(binary.BigEndian.Uint64(y[8:]))
				points = append(points, series.NewDataPoint(t, v))
			}
			sm[name] = series.NewSeriesPoint(name, points, q.step)
		}
	}
	return sm
}

func (d *DynamoDB) batchGet(q *query) (series.SeriesMap, error) {
	var keys []map[string]*dynamodb.AttributeValue
	for _, name := range q.names {
		keys = append(keys, map[string]*dynamodb.AttributeValue{
			"MetricName": {S: aws.String(name)},
			"Timestamp":  {N: aws.String(fmt.Sprintf("%d", q.slot.itemEpoch))},
		})
	}
	items := make(map[string]*dynamodb.KeysAndAttributes)
	items[q.slot.tableName] = &dynamodb.KeysAndAttributes{Keys: keys}
	params := &dynamodb.BatchGetItemInput{
		RequestItems:           items,
		ReturnConsumedCapacity: aws.String("NONE"),
	}
	resp, err := d.svc.BatchGetItem(params)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceNotFoundException" {
				// Don't handle ResourceNotFoundException as error
				// bacause diamondb web return length 0 series as 200.
				return series.SeriesMap{}, nil
			}
		}
		return nil, errors.Wrapf(err, "Failed to BatchGetItem %s %d %s %d",
			q.slot.tableName, q.slot.itemEpoch, strings.Join(q.names, ","), q.step,
		)
	}
	return batchGetResultToMap(resp, q), nil
}

func selectTimeSlots(startTime, endTime time.Time, tablePrefix string) ([]*timeSlot, int) {
	var (
		tableName      string
		step           int
		tableEpochStep int
		itemEpochStep  int
	)
	diffTime := endTime.Sub(startTime)
	if oneYear <= diffTime {
		tableName = tablePrefix + "-1d360d"
		tableEpochStep = oneYearSeconds
		itemEpochStep = tableEpochStep
		step = 60 * 60 * 24
	} else if oneWeek <= diffTime {
		tableName = tablePrefix + "-1h7d"
		tableEpochStep = 60 * 60 * 24 * 7
		itemEpochStep = tableEpochStep
		step = 60 * 60
	} else if oneDay <= diffTime {
		tableName = tablePrefix + "-5m1d"
		tableEpochStep = 60 * 60 * 24
		itemEpochStep = tableEpochStep
		step = 5 * 60
	} else {
		tableName = tablePrefix + "-1m1h"
		tableEpochStep = 60 * 60 * 24
		itemEpochStep = 60 * 60
		step = 60
	}

	slots := make([]*timeSlot, 0, 5)
	startTableEpoch := startTime.Unix() - startTime.Unix()%int64(tableEpochStep)
	endTableEpoch := endTime.Unix()
	for tableEpoch := startTableEpoch; tableEpoch < endTableEpoch; tableEpoch += int64(tableEpochStep) {
		startItemEpoch := mathutil.MaxInt64(tableEpoch, startTime.Unix()-startTime.Unix()%int64(itemEpochStep))
		endItemEpoch := mathutil.MinInt64(tableEpoch+int64(tableEpochStep), endTime.Unix())
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

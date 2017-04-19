package dynamodb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	godynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	godynamodbiface "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/pkg/config"
	"github.com/yuuki/diamondb/pkg/model"
	"github.com/yuuki/diamondb/pkg/storage/util"
	"github.com/yuuki/diamondb/pkg/timeparser"
)

//go:generate mockgen -source ../../../vendor/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface/interface.go -destination dynamodb_mock.go -package dynamodb

// ReadWriter defines the interface for DynamoDB reader and writer.
type ReadWriter interface {
	Ping() error
	Client() godynamodbiface.DynamoDBAPI
	CreateTable(*CreateTableParam) error
	Fetch(string, time.Time, time.Time) (model.SeriesMap, error)
	batchGet(q *query) (model.SeriesMap, error)
	Put(string, string, string, int64, map[int64]float64) error
}

// DynamoDB provides a dynamodb client.
type DynamoDB struct {
	svc godynamodbiface.DynamoDBAPI
}

type timeSlot struct {
	itemEpoch int64
	step      int
}

type query struct {
	names []string
	start time.Time
	end   time.Time
	slot  *timeSlot
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

var _ ReadWriter = &DynamoDB{}

// New creates a new DynamoDB.
func New() (*DynamoDB, error) {
	awsConf := aws.NewConfig().WithRegion(config.Config.DynamoDBRegion)
	if config.Config.DynamoDBEndpoint != "" {
		// For dynamodb-local configuration
		awsConf.WithEndpoint(config.Config.DynamoDBEndpoint)
		awsConf.WithCredentials(credentials.NewStaticCredentials("dummy", "dummy", "dummy"))
	}
	// the default MaxRetries of DynamoDB is 10, it is too many to take timeout too long.
	// https://github.com/aws/aws-sdk-go/blob/a1f22039/service/dynamodb/customizations.go#L43
	awsConf.WithMaxRetries(3)
	awsConf.WithHTTPClient(&http.Client{
		Timeout:   10 * time.Second,
		Transport: http.DefaultTransport,
	})
	sess, err := session.NewSession(awsConf)
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to create session for dynamodb (%s,%s)",
			config.Config.DynamoDBRegion,
			config.Config.DynamoDBEndpoint,
		)
	}
	return &DynamoDB{
		svc: godynamodb.New(sess),
	}, nil
}

// Client returns the DynamoDB client.
func (d *DynamoDB) Client() godynamodbiface.DynamoDBAPI {
	return d.svc
}

// Ping pings DynamoDB endpoint.
func (d *DynamoDB) Ping() error {
	var params *godynamodb.DescribeLimitsInput
	_, err := d.svc.DescribeLimits(params)
	if err != nil {
		return errors.Wrapf(err, "failed to ping dynamodb")
	}
	return nil
}

// CreateTableParam is parameter set of CreateTable.
type CreateTableParam struct {
	Name string
	RCU  int64 // ReadCapacityUnits
	WCU  int64 // WriteCapacityUnits
}

// CreateTable creates a dynamodb table to store time series data.
// Skip creating table if the table already exists.
func (d *DynamoDB) CreateTable(param *CreateTableParam) error {
	_, err := d.svc.CreateTable(&godynamodb.CreateTableInput{
		TableName: aws.String(param.Name),
		AttributeDefinitions: []*godynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Name"),
				AttributeType: aws.String(godynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("Timestamp"),
				AttributeType: aws.String(godynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*godynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Name"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("Timestamp"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &godynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(param.RCU),
			WriteCapacityUnits: aws.Int64(param.WCU),
		},
		// TODO StreamSpecification to export to s3
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceInUseException" {
				// Skip if the table already exists
				log.Printf("Skip creating DynamoDB table because %s already exists\n", param.Name)
				return nil
			}
		}
		return errors.Wrapf(err, "failed to create dynamodb table (%s,%d,%d)",
			param.Name, param.RCU, param.WCU)
	}

	log.Printf("Creating DynamoDB table (name:%s, rcu:%d, wcu:%d) ...\n",
		param.Name, param.RCU, param.WCU)

	err = d.svc.WaitUntilTableExists(&godynamodb.DescribeTableInput{
		TableName: aws.String(param.Name),
	})
	if err != nil {
		return errors.Wrapf(err, "failed to wait until table exists (%s,%d,%d)",
			param.Name, param.RCU, param.WCU)
	}

	if config.Config.DynamoDBTTL {
		_, err = d.svc.UpdateTimeToLive(&godynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(param.Name),
			TimeToLiveSpecification: &godynamodb.TimeToLiveSpecification{
				AttributeName: aws.String("TTL"),
				Enabled:       aws.Bool(true),
			},
		})
		if err != nil {
			return errors.Wrapf(err, "failed to set TTL to (%s,%d,%d)",
				param.Name, param.RCU, param.WCU)
		}
	}
	return nil
}

// Fetch fetches datapoints by name from start until end.
func (d *DynamoDB) Fetch(name string, start, end time.Time) (model.SeriesMap, error) {
	slots := selectTimeSlots(start, end)
	nameGroups := util.GroupNames(util.SplitName(name), dynamodbBatchLimit)
	numQueries := len(slots) * len(nameGroups)

	type result struct {
		value model.SeriesMap
		err   error
	}
	c := make(chan *result, numQueries)
	for _, slot := range slots {
		for _, names := range nameGroups {
			q := &query{
				names: names,
				start: start,
				end:   end,
				slot:  slot,
			}
			go func(q *query) {
				sm, err := d.batchGet(q)
				c <- &result{value: sm, err: err}
			}(q)
		}
	}
	sm := make(model.SeriesMap, len(nameGroups))
	for i := 0; i < numQueries; i++ {
		ret := <-c
		if ret.err != nil {
			return nil, ret.err
		}
		sm.MergePointsToMap(ret.value)
	}
	return sm, nil
}

func batchGetResultToMap(resp *godynamodb.BatchGetItemOutput, q *query) model.SeriesMap {
	sm := make(model.SeriesMap, len(resp.Responses))
	for _, xs := range resp.Responses {
		for _, x := range xs {
			name := (*x["Name"].S)
			points := make(model.DataPoints, 0, len(x["Values"].BS))
			for _, y := range x["Values"].BS {
				t := int64(binary.BigEndian.Uint64(y[0:8]))
				v := math.Float64frombits(binary.BigEndian.Uint64(y[8:]))
				// Trim datapoints out of [start, end]
				if t < q.start.Unix() || q.end.Unix() < t {
					continue
				}
				points = append(points, model.NewDataPoint(t, v))
			}
			sm[name] = model.NewSeriesPoint(name, points, q.slot.step)
		}
	}
	return sm
}

func (d *DynamoDB) batchGet(q *query) (model.SeriesMap, error) {
	var keys []map[string]*godynamodb.AttributeValue
	for _, name := range q.names {
		keys = append(keys, map[string]*godynamodb.AttributeValue{
			"Name":      {S: aws.String(name)},
			"Timestamp": {S: aws.String(fmt.Sprintf("%d:%d", q.slot.itemEpoch, q.slot.step))},
		})
	}
	items := make(map[string]*godynamodb.KeysAndAttributes)
	items[config.Config.DynamoDBTableName] = &godynamodb.KeysAndAttributes{Keys: keys}
	params := &godynamodb.BatchGetItemInput{
		RequestItems:           items,
		ReturnConsumedCapacity: aws.String("NONE"),
	}
	resp, err := d.svc.BatchGetItem(params)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceNotFoundException" {
				// Don't handle ResourceNotFoundException as error
				// bacause diamondb web return length 0 series as 200.
				return model.SeriesMap{}, nil
			}
		}
		return nil, errors.Wrapf(err,
			"failed to call dynamodb API batchGetItem (%s,%d,%d)",
			config.Config.DynamoDBTableName, q.slot.itemEpoch, q.slot.step,
		)
	}
	return batchGetResultToMap(resp, q), nil
}

// Put writes the datapoints into DynamoDB. It creates item
// if item doesn't exist and updates item if it exists.
func (d *DynamoDB) Put(name, slot, history string, itemEpoch int64, tv map[int64]float64) error {
	stepDuration, err := timeparser.ParseTimeOffset(slot)
	if err != nil {
		return err
	}
	historyDuration, err := timeparser.ParseTimeOffset(history)
	if err != nil {
		return err
	}
	ttl := itemEpoch + int64(historyDuration.Seconds())

	vals := make([][]byte, 0, len(tv))
	for timestamp, value := range tv {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, timestamp)
		binary.Write(buf, binary.BigEndian, math.Float64bits(value))
		vals = append(vals, buf.Bytes())
	}

	params := &godynamodb.UpdateItemInput{
		TableName: aws.String(config.Config.DynamoDBTableName),
		Key: map[string]*godynamodb.AttributeValue{
			"Name":      {S: aws.String(name)},
			"Timestamp": {S: aws.String(fmt.Sprintf("%d:%d", itemEpoch, int64(stepDuration.Seconds())))},
		},
		UpdateExpression: aws.String(`
			SET #ttl = :new_ttl
			ADD #values_set :new_values
		`),
		ExpressionAttributeNames: map[string]*string{
			"#ttl":        aws.String("TTL"),
			"#values_set": aws.String("Values"),
		},
		ExpressionAttributeValues: map[string]*godynamodb.AttributeValue{
			":new_ttl":    {N: aws.String(fmt.Sprintf("%d", ttl))},
			":new_values": {BS: vals},
		},
		ReturnValues: aws.String("NONE"),
	}
	if _, err := d.svc.UpdateItem(params); err != nil {
		return errors.Wrapf(err, "failed to call dynamodb API putItem (%s,%s,%d)",
			config.Config.DynamoDBTableName, name, itemEpoch)
	}
	return nil
}

func selectTimeSlots(startTime, endTime time.Time) []*timeSlot {
	var (
		step          int
		itemEpochStep int
	)
	diff := endTime.Sub(startTime)
	switch {
	case oneYear <= diff:
		itemEpochStep = oneYearSeconds
		step = 60 * 60 * 24
	case oneWeek <= diff:
		itemEpochStep = oneWeekSeconds
		step = 60 * 60
	case oneDay <= diff:
		itemEpochStep = oneDaySeconds
		step = 5 * 60
	default:
		itemEpochStep = 60 * 60
		step = 60
	}

	var slots []*timeSlot
	startItemEpoch := startTime.Unix() - startTime.Unix()%int64(itemEpochStep)
	endItemEpoch := endTime.Unix()
	for epoch := startItemEpoch; epoch < endItemEpoch; epoch += int64(itemEpochStep) {
		slots = append(slots, &timeSlot{itemEpoch: epoch, step: step})
	}

	return slots
}

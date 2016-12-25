package tsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"

	"github.com/yuuki/dynamond/model"
)

type MockDynamoDB struct {
	TableName string
	StartVal  time.Time
	EndVal    time.Time
	Metric    *model.Metric
}

type MockDynamoDB2 struct {
	TableName string
	ItemEpoch int64
	Names     []string
	Metrics   []*model.Metric
}

func SetMockDynamoDB(t *testing.T, m *MockDynamoDB) *gomock.Controller {
	ctrl := gomock.NewController(t)

	dmock := NewMockDynamoDBAPI(ctrl)
	expect := dmock.EXPECT().Query(&dynamodb.QueryInput{
		TableName: aws.String(m.TableName),
		ConsistentRead: aws.Bool(false),
		ExpressionAttributeNames: map[string]*string{
			"#name": aws.String("name"),
			"#timestamp": aws.String("timestamp"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":name_val": &dynamodb.AttributeValue{S: aws.String(m.Metric.Name)},
			":start_val": &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", m.StartVal.Unix()))},
			":end_val": &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", m.EndVal.Unix()))},
		},
		KeyConditionExpression: aws.String("#name = :name_val AND #timestamp BETWEEN :start_val AND :end_val"),
	})

	items := make([]map[string]*dynamodb.AttributeValue, 0, len(m.Metric.DataPoints))
	for _, dp := range m.Metric.DataPoints {
		attribute := map[string]*dynamodb.AttributeValue{
			"name": &dynamodb.AttributeValue{S: aws.String(m.Metric.Name)},
			"timestamp": &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", dp.Timestamp))},
			"value": &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%f", dp.Value))},
		}
		items = append(items, attribute)
	}

	expect.Return(&dynamodb.QueryOutput{
		Count: aws.Int64(int64(len(items))),
		Items: items,
	}, nil)
	SetDynamoDBClient(dmock)

	return ctrl
}

func SetMockDynamoDB2(t *testing.T, m *MockDynamoDB2) *gomock.Controller {
	ctrl := gomock.NewController(t)
	dmock := NewMockDynamoDBAPI(ctrl)

	var keys []map[string]*dynamodb.AttributeValue
	for _, metric := range m.Metrics {
		keys = append(keys, map[string]*dynamodb.AttributeValue{
			"MetricName": &dynamodb.AttributeValue{S: aws.String(metric.Name)},
			"Timestamp": &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", m.ItemEpoch))},
		})
	}
	items := make(map[string]*dynamodb.KeysAndAttributes)
	items[m.TableName] = &dynamodb.KeysAndAttributes{Keys: keys}
	params := &dynamodb.BatchGetItemInput{
		RequestItems:           items,
		ReturnConsumedCapacity: aws.String("NONE"),
	}

	expect := dmock.EXPECT().BatchGetItem(params)

	responses := make(map[string][]map[string]*dynamodb.AttributeValue)
	for _, metric := range m.Metrics {
		var vals [][]byte
		for _, point := range metric.DataPoints {
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, uint64(point.Timestamp))
			binary.Write(buf, binary.BigEndian, math.Float64bits(point.Value))
			vals = append(vals, buf.Bytes())
		}
		attribute := map[string]*dynamodb.AttributeValue{
			"MetricName": &dynamodb.AttributeValue{S: aws.String(metric.Name)},
			"Timestamp": &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", m.ItemEpoch))},
			"Values": &dynamodb.AttributeValue{BS: vals},
		}
		responses[m.TableName] = append(responses[m.TableName], attribute)
	}

	expect.Return(&dynamodb.BatchGetItemOutput{
		Responses: responses,
	}, nil)
	SetDynamoDB(dmock)

	return ctrl
}

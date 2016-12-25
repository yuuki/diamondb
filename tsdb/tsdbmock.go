package tsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"

	"github.com/yuuki/dynamond/model"
)

type MockDynamoDB struct {
	TableName string
	ItemEpoch int64
	Names     []string
	Metrics   []*model.Metric
}

func SetMockDynamoDB(t *testing.T, m *MockDynamoDB) *gomock.Controller {
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

package dynamo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"
	"github.com/yuuki/diamondb/lib/series"
)

type mockDynamoDBParam struct {
	TableName string
	ItemEpoch int64
	SeriesMap series.SeriesMap
}

func mockExpectBatchGetItem(mock *MockDynamoDBAPI, m *mockDynamoDBParam) *gomock.Call {
	var keys []map[string]*dynamodb.AttributeValue
	for name, _ := range m.SeriesMap {
		keys = append(keys, map[string]*dynamodb.AttributeValue{
			"MetricName": {S: aws.String(name)},
			"Timestamp":  {N: aws.String(fmt.Sprintf("%d", m.ItemEpoch))},
		})
	}
	items := make(map[string]*dynamodb.KeysAndAttributes)
	items[m.TableName] = &dynamodb.KeysAndAttributes{Keys: keys}
	params := &dynamodb.BatchGetItemInput{
		RequestItems:           items,
		ReturnConsumedCapacity: aws.String("NONE"),
	}
	return mock.EXPECT().BatchGetItem(params)
}

func mockReturnBatchGetItem(expect *gomock.Call, m *mockDynamoDBParam) *gomock.Call {
	responses := make(map[string][]map[string]*dynamodb.AttributeValue)
	for name, sp := range m.SeriesMap {
		var vals [][]byte
		for _, point := range sp.Points() {
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, int64(point.Timestamp()))
			binary.Write(buf, binary.BigEndian, math.Float64bits(point.Value()))
			vals = append(vals, buf.Bytes())
		}
		attribute := map[string]*dynamodb.AttributeValue{
			"MetricName": {S: aws.String(name)},
			"Timestamp":  {N: aws.String(fmt.Sprintf("%d", m.ItemEpoch))},
			"Values":     {BS: vals},
		}
		responses[m.TableName] = append(responses[m.TableName], attribute)
	}

	expect.Return(&dynamodb.BatchGetItemOutput{
		Responses: responses,
	}, nil)
	return expect
}

func createMockDynamoDB(t *testing.T, m *mockDynamoDBParam) (*gomock.Controller, *DynamoDB) {
	ctrl := gomock.NewController(t)
	dmock := NewMockDynamoDBAPI(ctrl)

	var keys []map[string]*dynamodb.AttributeValue
	for name, _ := range m.SeriesMap {
		keys = append(keys, map[string]*dynamodb.AttributeValue{
			"MetricName": {S: aws.String(name)},
			"Timestamp":  {N: aws.String(fmt.Sprintf("%d", m.ItemEpoch))},
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
	for name, sp := range m.SeriesMap {
		var vals [][]byte
		for _, point := range sp.Points() {
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, int64(point.Timestamp()))
			binary.Write(buf, binary.BigEndian, math.Float64bits(point.Value()))
			vals = append(vals, buf.Bytes())
		}
		attribute := map[string]*dynamodb.AttributeValue{
			"MetricName": {S: aws.String(name)},
			"Timestamp":  {N: aws.String(fmt.Sprintf("%d", m.ItemEpoch))},
			"Values":     {BS: vals},
		}
		responses[m.TableName] = append(responses[m.TableName], attribute)
	}

	expect.Return(&dynamodb.BatchGetItemOutput{
		Responses: responses,
	}, nil)
	return ctrl, &DynamoDB{svc: dmock}
}

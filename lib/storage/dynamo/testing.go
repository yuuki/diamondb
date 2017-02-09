package dynamo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"
	"github.com/yuuki/diamondb/lib/series"
)

type mockDynamoDBParam struct {
	Resolution string
	TableEpoch int64
	ItemEpoch  int64
	SeriesMap  series.SeriesMap
}

var testTableNamePrefix = "diamondb_datapoints_test"

func NewTestDynamoDB(mock *MockDynamoDBAPI) *DynamoDB {
	return &DynamoDB{
		svc:         mock,
		tablePrefix: testTableNamePrefix,
	}
}

func mockTableName(resolution string, tableEpoch int64) string {
	return fmt.Sprintf("%s-%s-%d", testTableNamePrefix, resolution, tableEpoch)
}

func mockExpectBatchGetItem(mock *MockDynamoDBAPI, m *mockDynamoDBParam) *gomock.Call {
	var keys []map[string]*dynamodb.AttributeValue
	for _, name := range m.SeriesMap.SortedNames() {
		keys = append(keys, map[string]*dynamodb.AttributeValue{
			"MetricName": {S: aws.String(name)},
			"Timestamp":  {N: aws.String(fmt.Sprintf("%d", m.ItemEpoch))},
		})
	}
	items := make(map[string]*dynamodb.KeysAndAttributes)
	items[mockTableName(m.Resolution, m.TableEpoch)] = &dynamodb.KeysAndAttributes{Keys: keys}
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
		table := mockTableName(m.Resolution, m.TableEpoch)
		responses[table] = append(responses[table], attribute)
	}

	expect.Return(&dynamodb.BatchGetItemOutput{
		Responses: responses,
	}, nil)
	return expect
}

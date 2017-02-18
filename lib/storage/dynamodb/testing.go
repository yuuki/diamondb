package dynamodb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	godynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"
	"github.com/yuuki/diamondb/lib/series"
)

// FakeReadWriter is for stub testing
type FakeReadWriter struct {
	ReadWriter
	FakeRead func(name string, start, end time.Time) (series.SeriesMap, error)
}

// Read is for stub testing.
func (s *FakeReadWriter) Read(name string, start, end time.Time) (series.SeriesMap, error) {
	return s.FakeRead(name, start, end)
}

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
	var keys []map[string]*godynamodb.AttributeValue
	for _, name := range m.SeriesMap.SortedNames() {
		keys = append(keys, map[string]*godynamodb.AttributeValue{
			"MetricName": {S: aws.String(name)},
			"Timestamp":  {N: aws.String(fmt.Sprintf("%d", m.ItemEpoch))},
		})
	}
	items := make(map[string]*godynamodb.KeysAndAttributes)
	items[mockTableName(m.Resolution, m.TableEpoch)] = &godynamodb.KeysAndAttributes{Keys: keys}
	params := &godynamodb.BatchGetItemInput{
		RequestItems:           items,
		ReturnConsumedCapacity: aws.String("NONE"),
	}
	return mock.EXPECT().BatchGetItem(params)
}

func mockReturnBatchGetItem(expect *gomock.Call, m *mockDynamoDBParam) *gomock.Call {
	responses := make(map[string][]map[string]*godynamodb.AttributeValue)
	for name, sp := range m.SeriesMap {
		var vals [][]byte
		for _, point := range sp.Points() {
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, int64(point.Timestamp()))
			binary.Write(buf, binary.BigEndian, math.Float64bits(point.Value()))
			vals = append(vals, buf.Bytes())
		}
		attribute := map[string]*godynamodb.AttributeValue{
			"MetricName": {S: aws.String(name)},
			"Timestamp":  {N: aws.String(fmt.Sprintf("%d", m.ItemEpoch))},
			"Values":     {BS: vals},
		}
		table := mockTableName(m.Resolution, m.TableEpoch)
		responses[table] = append(responses[table], attribute)
	}

	expect.Return(&godynamodb.BatchGetItemOutput{
		Responses: responses,
	}, nil)
	return expect
}

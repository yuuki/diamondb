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
	"github.com/yuuki/diamondb/pkg/config"
	"github.com/yuuki/diamondb/pkg/model"
)

// FakeReadWriter is for stub testing
type FakeReadWriter struct {
	ReadWriter
	FakeFetch func(name string, start, end time.Time) (model.SeriesMap, error)
}

func (s *FakeReadWriter) Fetch(name string, start, end time.Time) (model.SeriesMap, error) {
	return s.FakeFetch(name, start, end)
}

type mockDynamoDBParam struct {
	Slot      *timeSlot
	SeriesMap model.SeriesMap
}

var mockTableName = "diamondb.testmock.timeseries"

func NewTestDynamoDB(mock *MockDynamoDBAPI) *DynamoDB {
	return &DynamoDB{svc: mock}
}

func mockExpectBatchGetItem(mock *MockDynamoDBAPI, m *mockDynamoDBParam) *gomock.Call {
	config.Config.DynamoDBTableName = mockTableName

	var keys []map[string]*godynamodb.AttributeValue
	for _, name := range m.SeriesMap.SortedNames() {
		keys = append(keys, map[string]*godynamodb.AttributeValue{
			"Name":      {S: aws.String(name)},
			"Timestamp": {S: aws.String(fmt.Sprintf("%d:%d", m.Slot.itemEpoch, m.Slot.step))},
		})
	}
	items := make(map[string]*godynamodb.KeysAndAttributes)
	items[mockTableName] = &godynamodb.KeysAndAttributes{Keys: keys}
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
			"Name":      {S: aws.String(name)},
			"Timestamp": {S: aws.String(fmt.Sprintf("%d:%d", m.Slot.itemEpoch, m.Slot.step))},
			"Values":    {BS: vals},
		}
		responses[mockTableName] = append(responses[mockTableName], attribute)
	}

	expect.Return(&godynamodb.BatchGetItemOutput{
		Responses: responses,
	}, nil)
	return expect
}

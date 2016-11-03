package tsdb

import (
	"fmt"
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

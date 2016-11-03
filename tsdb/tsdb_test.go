package tsdb

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/yuuki/dynamond/model"
)

func TestFetchMetric(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	dmock := NewMockDynamoDBAPI(ctrl)
	dmock.EXPECT().Query(&dynamodb.QueryInput{
		TableName: aws.String("SeriesTest"),
		ConsistentRead: aws.Bool(false),
		ExpressionAttributeNames: map[string]*string{
			"#name": aws.String("name"),
			"#timestamp": aws.String("timestamp"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":name_val": &dynamodb.AttributeValue{S: aws.String("test")},
			":start_val": &dynamodb.AttributeValue{N: aws.String("1465516800")},
			":end_val": &dynamodb.AttributeValue{N: aws.String("1465526800")},
		},
		KeyConditionExpression: aws.String("#name = :name_val AND #timestamp BETWEEN :start_val AND :end_val"),
	}).Return(&dynamodb.QueryOutput{
		Count: aws.Int64(1),
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"name": &dynamodb.AttributeValue{S: aws.String("test")},
				"timestamp": &dynamodb.AttributeValue{N: aws.String("1465516810")},
				"value": &dynamodb.AttributeValue{N: aws.String("10.0")},
			},
		},
	}, nil)
	SetClient(dmock)

	metricList, err := FetchMetric("test", time.Unix(1465516800, 0), time.Unix(1465526800, 0))
	if assert.NoError(t, err) {
		assert.Equal(t, 1, len(metricList))
		metric := metricList[0]
		assert.Equal(t, "test", metric.Name)
		assert.EqualValues(t, []*model.DataPoint{&model.DataPoint{1465516810, 10.0}}, metric.DataPoints)
	}
}

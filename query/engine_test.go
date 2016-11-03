package query

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/yuuki/dynamond/tsdb"
)

func TestEvalTarget_Alias(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	dmock := tsdb.NewMockDynamoDBAPI(ctrl)
	dmock.EXPECT().Query(&dynamodb.QueryInput{
		TableName: aws.String("SeriesTest"),
		ConsistentRead: aws.Bool(false),
		ConditionalOperator: aws.String(dynamodb.ConditionalOperatorAnd),
		KeyConditionExpression: aws.String("name = Sales.widgets.largeBlue AND timestamp BETWEEN 1465516800 AND 1465526800"),
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
	tsdb.SetClient(dmock)

	metricList, err := EvalTarget(
		"alias(Sales.widgets.largeBlue,\"Large Blue Widgets\")",
		time.Unix(1465516800, 0),
		time.Unix(1465526800, 0),
	)
	if assert.NoError(t, err) {
		assert.Equal(t, 1, len(metricList))
		assert.Equal(t, metricList[0].Name, "Large Blue Widgets")
	}
}

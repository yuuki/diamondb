package tsdb

import (
	"fmt"
	"time"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"

	"github.com/yuuki/dynamond/model"
)

var (
	svc dynamodbiface.DynamoDBAPI = dynamodb.New(session.New(), &aws.Config{Region: aws.String("ap-northeast-1")})
)

// SetClient replace svc to mock dynamodb client
func SetClient(client dynamodbiface.DynamoDBAPI) {
	svc = client
}

func FetchMetric(pathExpr string, startTime, endTime time.Time) (*model.Metric, error) {
	expression := fmt.Sprintf(
		"name = %s AND timestamp BETWEEN %d AND %d",
		pathExpr, startTime.Unix(), endTime.Unix(),
	)
	resp, err := svc.Query(&dynamodb.QueryInput{
		TableName: aws.String("SeriesTest"),
		ConsistentRead: aws.Bool(false),
		ConditionalOperator: aws.String(dynamodb.ConditionalOperatorAnd),
		KeyConditionExpression: aws.String(expression),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to dynamodb.Query %s from %d to %d",
			pathExpr, startTime.Unix(), endTime.Unix())
	}

	datapoints := make([]*model.DataPoint, 0, int32(*resp.Count))
	for _, item := range resp.Items {
		ts, _ := strconv.ParseInt(*item["timestamp"].N, 10, 32)
		value, _ := strconv.ParseFloat(*item["value"].N, 64)
		dp := model.NewDataPoint(int32(ts), value)
		datapoints = append(datapoints, dp)
	}
	metric := model.NewMetric(pathExpr, datapoints)

	return metric, nil
}

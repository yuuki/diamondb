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
func SetDynamoDBClient(client dynamodbiface.DynamoDBAPI) {
	svc = client
}

func FetchMetric(pathExpr string, startTime, endTime time.Time) ([]*model.Metric, error) {
	resp, err := svc.Query(&dynamodb.QueryInput{
		TableName: aws.String("SeriesTest"),
		ConsistentRead: aws.Bool(false),
		ExpressionAttributeNames: map[string]*string{
			"#name": aws.String("name"),
			"#timestamp": aws.String("timestamp"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":name_val": &dynamodb.AttributeValue{S: aws.String(pathExpr)},
			":start_val": &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", startTime.Unix()))},
			":end_val": &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", endTime.Unix()))},
		},
		KeyConditionExpression: aws.String("#name = :name_val AND #timestamp BETWEEN :start_val AND :end_val"),
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
	metricList := make([]*model.Metric, 0, 5)
	metricList = append(metricList, model.NewMetric(pathExpr, datapoints))

	return metricList, nil
}

// Package news provides a very simple DynamoDB-backed mailing list for newsletters.
package news

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/expression"
)

// item model.
type item struct {
	Newsletter string    `json:"newsletter"`
	Email      string    `json:"email"`
	CreatedAt  time.Time `json:"created_at"`
}

// New returns a new mailing list store with default AWS credentials.
func New(table string) (store *Store) {
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic(err)
	}
	return &Store{Client: dynamodb.New(cfg), TableName: table}
}

// Store is a DynamoDB mailing list storage implementation.
type Store struct {
	TableName string
	Client    *dynamodb.Client
}

// AddSubscriber adds a subscriber to a newsletter.
func (s *Store) AddSubscriber(newsletter, email string) error {
	i, err := dynamodbattribute.MarshalMap(item{
		Newsletter: newsletter,
		Email:      email,
		CreatedAt:  time.Now(),
	})

	if err != nil {
		return err
	}

	_, err = s.Client.PutItemRequest(&dynamodb.PutItemInput{
		TableName: aws.String(s.TableName),
		Item:      i,
	}).Send(context.TODO())

	return err
}

// RemoveSubscriber removes a subscriber from a newsletter.
func (s *Store) RemoveSubscriber(newsletter, email string) error {
	_, err := s.Client.DeleteItemRequest(&dynamodb.DeleteItemInput{
		TableName: aws.String(s.TableName),
		Key: map[string]dynamodb.AttributeValue{
			"newsletter": {S: aws.String(newsletter)},
			"email":      {S: aws.String(email)},
		},
	}).Send(context.TODO())
	return err
}

// GetSubscribers returns subscriber emails for a newsletter.
func (s *Store) GetSubscribers(newsletter string) (emails []string, err error) {
	expr, err := expression.NewBuilder().
		WithKeyCondition(expression.Key("newsletter").Equal(expression.Value(newsletter))).Build()
	if err != nil {
		return emails, err
	}

	result, err := s.Client.QueryRequest(&dynamodb.QueryInput{
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		KeyConditionExpression:    expr.KeyCondition(),
		TableName:                 aws.String(s.TableName),
	}).Send(context.TODO())
	if err != nil {
		return
	}

	var results []item

	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &results)
	if err != nil {
		return
	}

	for _, v := range results {
		emails = append(emails, v.Email)
	}

	return
}

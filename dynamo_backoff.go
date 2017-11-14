package backoff

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"fmt"
	"time"
)

// If we should retry and backoff or we shouldn't
func shouldRetry(err error) bool {
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case dynamodb.ErrCodeProvisionedThroughputExceededException:
			fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			return true
		case dynamodb.ErrCodeInternalServerError:
			fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			return true
		default:
			fmt.Println(aerr.Error())
			return false
		}
	} else {
		return false
	}
}

type dynamoPutCall func(input dynamodb.PutItemInput) (dynamodb.PutItemOutput, error)
func RetryDynamoPut(call dynamoPutCall, input dynamodb.PutItemInput, b BackOff) (dynamodb.PutItemOutput, error) { return RetryNotifyDynamoPut(call, input, b, nil) }
func RetryNotifyDynamoPut(call dynamoPutCall, input dynamodb.PutItemInput, b BackOff, notify Notify) (result dynamodb.PutItemOutput, err error) {
	var next time.Duration

	cb := ensureContext(b)

	b.Reset()
	for {
		if result, err = call(input); err == nil {
			return result, nil
		}
		if !shouldRetry(err) {
			return result, err
		}

		if next = b.NextBackOff(); next == Stop {
			return
		}

		if notify != nil {
			notify(err, next)
		}

		t := time.NewTimer(next)

		select {
		case <-cb.Context().Done():
			t.Stop()
			return result, err
		case <-t.C:
		}
	}
}

type dynamoUpdateCall func(input dynamodb.UpdateItemInput) (dynamodb.UpdateItemOutput, error)
func RetryDynamoUpdate(call dynamoUpdateCall, input dynamodb.UpdateItemInput, b BackOff) (dynamodb.UpdateItemOutput, error) { return RetryNotifyDynamoUpdate(call, input, b, nil) }
func RetryNotifyDynamoUpdate(call dynamoUpdateCall, input dynamodb.UpdateItemInput, b BackOff, notify Notify) (result dynamodb.UpdateItemOutput, err error) {
	var next time.Duration

	cb := ensureContext(b)

	b.Reset()
	for {
		if result, err = call(input); err == nil {
			return result, nil
		}

		if !shouldRetry(err) {
			return result, err
		}

		if next = b.NextBackOff(); next == Stop {
			return
		}

		if notify != nil {
			notify(err, next)
		}

		t := time.NewTimer(next)

		select {
		case <-cb.Context().Done():
			t.Stop()
			return result, err
		case <-t.C:
		}
	}
}

type dynamoDeleteCall func(input dynamodb.DeleteItemInput) (dynamodb.DeleteItemOutput, error)
func RetryDynamoDelete(call dynamoDeleteCall, input dynamodb.DeleteItemInput, b BackOff) (dynamodb.DeleteItemOutput, error) { return RetryNotifyDynamoDelete(call, input, b, nil) }
func RetryNotifyDynamoDelete(call dynamoDeleteCall, input dynamodb.DeleteItemInput, b BackOff, notify Notify) (result dynamodb.DeleteItemOutput, err error) {
	var next time.Duration

	cb := ensureContext(b)

	b.Reset()
	for {
		if result, err = call(input); err == nil {
			return result, nil
		}

		if !shouldRetry(err) {
			return result, err
		}

		if next = b.NextBackOff(); next == Stop {
			return
		}

		if notify != nil {
			notify(err, next)
		}

		t := time.NewTimer(next)

		select {
		case <-cb.Context().Done():
			t.Stop()
			return result, err
		case <-t.C:
		}
	}
}
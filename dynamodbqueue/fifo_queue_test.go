package dynamodbqueue_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFifoQueue_SingleGroupOrdering verifies strict FIFO ordering within a single message group.
func TestFifoQueue_SingleGroupOrdering(t *testing.T) {
	ctx := context.Background()

	// Create a FIFO queue
	fifoQueue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName("fifoTest-" + dynamodbqueue.RandomString(4)).
		UseClientID("fifoClient")

	// Cast to FifoQueue to access PushMessagesWithGroup
	fq := fifoQueue.(dynamodbqueue.FifoQueue)

	// Clear the queue
	err := fq.Purge(ctx)
	require.NoError(t, err)

	// Push messages to a single group
	messages := []string{"first", "second", "third", "fourth", "fifth"}
	for _, msg := range messages {
		_, err := fq.PushMessagesWithGroup(ctx, 0, "group-A", events.SQSMessage{Body: msg})
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond) // Ensure distinct timestamps
	}

	// Poll messages one at a time
	var polledBodies []string
	for i := 0; i < len(messages); i++ {
		// Poll one message
		polled, err := fq.PollMessages(ctx, 0, time.Second*5, 1, 1)
		require.NoError(t, err)

		if len(polled) == 0 {
			break
		}

		polledBodies = append(polledBodies, polled[0].Body)

		// Delete the message to unblock the group
		_, err = fq.DeleteMessages(ctx, polled[0].ReceiptHandle)
		require.NoError(t, err)
	}

	// Verify strict FIFO ordering
	assert.Equal(t, messages, polledBodies, "Messages should be in exact FIFO order")
}

// TestFifoQueue_OneInFlightPerGroup verifies only one message per group can be in-flight.
func TestFifoQueue_OneInFlightPerGroup(t *testing.T) {
	ctx := context.Background()

	// Create a FIFO queue
	fifoQueue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName("oneInFlight-" + dynamodbqueue.RandomString(4)).
		UseClientID("oifClient")

	fq := fifoQueue.(dynamodbqueue.FifoQueue)

	// Clear the queue
	err := fq.Purge(ctx)
	require.NoError(t, err)

	// Push 3 messages to the same group
	for i := 0; i < 3; i++ {
		_, err := fq.PushMessagesWithGroup(ctx, 0, "group-A", events.SQSMessage{
			Body: fmt.Sprintf("msg-%d", i),
		})
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}

	// Poll with max=10, should only get 1 (one in-flight per group)
	polled, err := fq.PollMessages(ctx, 0, time.Minute, 1, 10)
	require.NoError(t, err)
	assert.Len(t, polled, 1, "Should only get 1 message (one in-flight per group)")
	assert.Equal(t, "msg-0", polled[0].Body)

	// Poll again immediately - should get nothing (group blocked)
	polled2, err := fq.PollMessages(ctx, 0, time.Minute, 1, 10)
	require.NoError(t, err)
	assert.Len(t, polled2, 0, "Group should be blocked, no more messages")

	// Delete first message to unblock
	_, err = fq.DeleteMessages(ctx, polled[0].ReceiptHandle)
	require.NoError(t, err)

	// Now should get the next message
	polled3, err := fq.PollMessages(ctx, 0, time.Minute, 1, 10)
	require.NoError(t, err)
	assert.Len(t, polled3, 1, "Should get next message after delete")
	assert.Equal(t, "msg-1", polled3[0].Body)
}

// TestFifoQueue_ParallelProcessingAcrossGroups verifies messages from different groups can be processed in parallel.
func TestFifoQueue_ParallelProcessingAcrossGroups(t *testing.T) {
	ctx := context.Background()

	// Create a FIFO queue
	fifoQueue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName("parallelGroups-" + dynamodbqueue.RandomString(4)).
		UseClientID("pgClient")

	fq := fifoQueue.(dynamodbqueue.FifoQueue)

	// Clear the queue
	err := fq.Purge(ctx)
	require.NoError(t, err)

	// Push messages to different groups
	groups := []string{"group-A", "group-B", "group-C"}
	for _, group := range groups {
		for i := 0; i < 3; i++ {
			_, err := fq.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s-msg-%d", group, i),
			})
			require.NoError(t, err)
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Poll with max=10, should get 3 messages (one from each group)
	polled, err := fq.PollMessages(ctx, 0, time.Minute, 3, 10)
	require.NoError(t, err)
	assert.Len(t, polled, 3, "Should get 3 messages (one from each group)")

	// Verify each message is the first from its group
	bodiesMap := make(map[string]bool)
	for _, msg := range polled {
		bodiesMap[msg.Body] = true
	}

	// Should have first message from each group
	assert.True(t, bodiesMap["group-A-msg-0"], "Should have first message from group-A")
	assert.True(t, bodiesMap["group-B-msg-0"], "Should have first message from group-B")
	assert.True(t, bodiesMap["group-C-msg-0"], "Should have first message from group-C")
}

// TestFifoQueue_DefaultMessageGroup verifies default group behavior when using PushMessages without group.
func TestFifoQueue_DefaultMessageGroup(t *testing.T) {
	ctx := context.Background()

	// Create a FIFO queue
	fifoQueue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName("defaultGroup-" + dynamodbqueue.RandomString(4)).
		UseClientID("dgClient")

	// Clear the queue
	err := fifoQueue.Purge(ctx)
	require.NoError(t, err)

	// Push messages without explicit group (uses default)
	for i := 0; i < 3; i++ {
		_, err := fifoQueue.PushMessages(ctx, 0, events.SQSMessage{
			Body: fmt.Sprintf("default-msg-%d", i),
		})
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}

	// Poll - should only get 1 (all in default group, one in-flight rule)
	polled, err := fifoQueue.PollMessages(ctx, 0, time.Minute, 1, 10)
	require.NoError(t, err)
	assert.Len(t, polled, 1, "Should only get 1 message (all in default group)")
	assert.Equal(t, "default-msg-0", polled[0].Body)
}

// TestFifoQueue_GroupUnblockedAfterVisibilityExpires verifies visibility timeout expiry unblocks the group.
func TestFifoQueue_GroupUnblockedAfterVisibilityExpires(t *testing.T) {
	ctx := context.Background()

	// Create a FIFO queue
	fifoQueue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName("visExpire-" + dynamodbqueue.RandomString(4)).
		UseClientID("veClient")

	fq := fifoQueue.(dynamodbqueue.FifoQueue)

	// Clear the queue
	err := fq.Purge(ctx)
	require.NoError(t, err)

	// Push messages
	_, err = fq.PushMessagesWithGroup(ctx, 0, "group-A", events.SQSMessage{Body: "msg-1"})
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	_, err = fq.PushMessagesWithGroup(ctx, 0, "group-A", events.SQSMessage{Body: "msg-2"})
	require.NoError(t, err)

	// Poll with short visibility timeout
	shortVisibility := time.Second * 2
	polled1, err := fq.PollMessages(ctx, 0, shortVisibility, 1, 1)
	require.NoError(t, err)
	require.Len(t, polled1, 1, "Should get 1 message from the first poll")
	assert.Equal(t, "msg-1", polled1[0].Body)

	// Poll immediately - group blocked
	polled2, err := fq.PollMessages(ctx, 0, time.Minute, 1, 1)
	require.NoError(t, err)
	assert.Len(t, polled2, 0, "Group should be blocked")

	// Wait for visibility to expire
	time.Sleep(shortVisibility + 500*time.Millisecond)

	// Poll again - should get msg-1 again (FIFO preserved, same message re-visible)
	polled3, err := fq.PollMessages(ctx, 0, time.Minute, 1, 1)
	require.NoError(t, err)
	require.Len(t, polled3, 1, "Should get 1 message after visibility timeout expires")
	assert.Equal(t, "msg-1", polled3[0].Body, "Should get the SAME message again (FIFO preserved)")
}

// TestFifoQueue_QueueTypeIdentification verifies queue type identification.
func TestFifoQueue_QueueTypeIdentification(t *testing.T) {
	// Standard queue
	stdQueue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueStandard)
	assert.Equal(t, dynamodbqueue.QueueStandard, stdQueue.Type())

	// FIFO queue
	fifoQueue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO)
	assert.Equal(t, dynamodbqueue.QueueFIFO, fifoQueue.Type())

	// Default (no type specified) should be Standard
	defaultQueue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0)
	assert.Equal(t, dynamodbqueue.QueueStandard, defaultQueue.Type())
}

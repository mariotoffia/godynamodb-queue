package dynamodbqueue_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNoDuplicateMessagesOnReQuery tests that when the polling loop re-queries
// from the beginning (when lastEvaluatedKey becomes nil), it doesn't return
// duplicate messages that were already locked in previous iterations.
func TestLocalNoDuplicateMessagesOnReQuery(t *testing.T) {
	ctx := context.Background()

	queueName := "dupTest-" + dynamodbqueue.RandomString(4)
	clientID := "dupClient"

	// Clear the queue
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)
	require.NoError(t, err)

	// Push a small number of messages (less than minMessages we'll request)
	// This forces the polling loop to continue and re-query
	numMessages := 3
	for i := 0; i < numMessages; i++ {
		_, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("msg-%d", i)})
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond) // Ensure distinct timestamps
	}

	// Poll with minMessages > numMessages and a short timeout
	// This will force multiple re-queries from the beginning
	polledMessages, err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, time.Second*2, time.Minute, 10, 10)

	require.NoError(t, err)

	// Verify no duplicates - check by MessageId
	seen := make(map[string]bool)
	for _, msg := range polledMessages {
		if seen[msg.MessageId] {
			t.Errorf("Duplicate message ID found: %s", msg.MessageId)
		}
		seen[msg.MessageId] = true
	}

	// Should have exactly numMessages unique messages
	assert.Len(t, polledMessages, numMessages)
	assert.Len(t, seen, numMessages)
}

// TestNoDuplicateMessagesWithConcurrentConsumers tests that with multiple
// concurrent consumers, no message is returned more than once across all consumers.
func TestLocalNoDuplicateMessagesWithConcurrentConsumers(t *testing.T) {
	ctx := context.Background()

	queueName := "concDupTest-" + dynamodbqueue.RandomString(4)
	clientID := "concClient"
	numConsumers := 5
	numMessages := 30
	visibilityTimeout := time.Minute * 5

	// Clear the queue
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)
	require.NoError(t, err)

	// Push messages
	for i := 0; i < numMessages; i++ {
		_, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("msg-%d", i)})
		require.NoError(t, err)
	}

	// Verify all messages were added
	count, err := localQueue.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(numMessages), count)

	// Concurrent consumers
	var wg sync.WaitGroup
	allMessages := &sync.Map{}
	duplicates := &sync.Map{}

	wg.Add(numConsumers)
	for i := 0; i < numConsumers; i++ {
		go func(consumerID int) {
			defer wg.Done()

			msgs, err := localQueue.UseQueueName(queueName).
				UseClientID(clientID).
				PollMessages(ctx, time.Second*3, visibilityTimeout, 10, 10)

			require.NoError(t, err)

			for _, msg := range msgs {
				if _, loaded := allMessages.LoadOrStore(msg.MessageId, consumerID); loaded {
					duplicates.Store(msg.MessageId, true)
				}
			}
		}(i)
	}

	wg.Wait()

	// Check for duplicates
	duplicateCount := 0
	duplicates.Range(func(key, value interface{}) bool {
		t.Errorf("Duplicate message found: %v", key)
		duplicateCount++
		return true
	})

	assert.Equal(t, 0, duplicateCount, "Expected no duplicate messages")

	// Count total unique messages received
	totalReceived := 0
	allMessages.Range(func(key, value interface{}) bool {
		totalReceived++
		return true
	})

	// All messages should be consumed exactly once
	assert.Equal(t, numMessages, totalReceived, "All messages should be consumed exactly once")
}

// TestNoDuplicateMessagesWithLongPolling tests that long polling with timeout
// doesn't produce duplicates when messages become visible during the poll.
func TestLocalNoDuplicateMessagesWithLongPolling(t *testing.T) {
	ctx := context.Background()

	queueName := "longPollTest-" + dynamodbqueue.RandomString(4)
	clientID := "longPollClient"

	// Clear the queue
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)
	require.NoError(t, err)

	// Push initial messages
	initialMessages := 5
	for i := 0; i < initialMessages; i++ {
		_, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("initial-%d", i)})
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}

	// Start polling with a long timeout
	pollDone := make(chan []events.SQSMessage, 1)
	go func() {
		msgs, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PollMessages(ctx, time.Second*5, time.Minute, 20, 20)
		require.NoError(t, err)
		pollDone <- msgs
	}()

	// Add more messages while polling is in progress
	time.Sleep(500 * time.Millisecond)
	additionalMessages := 5
	for i := 0; i < additionalMessages; i++ {
		_, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("additional-%d", i)})
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for poll to complete
	polledMessages := <-pollDone

	// Check for duplicates
	seen := make(map[string]bool)
	for _, msg := range polledMessages {
		if seen[msg.MessageId] {
			t.Errorf("Duplicate message ID found: %s", msg.MessageId)
		}
		seen[msg.MessageId] = true
	}

	// Should have received messages without duplicates
	assert.Len(t, polledMessages, len(seen), "No duplicate messages should exist")
}

// TestNoDuplicatesAfterVisibilityTimeoutExpiry tests that when messages become
// visible again after timeout expiry, they're not counted as duplicates in a
// single poll operation.
func TestLocalNoDuplicatesAfterVisibilityTimeoutExpiry(t *testing.T) {
	ctx := context.Background()

	queueName := "visExpiry-" + dynamodbqueue.RandomString(4)
	clientID := "visClient"

	// Clear the queue
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)
	require.NoError(t, err)

	// Push messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		_, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("msg-%d", i)})
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}

	// First poll - get all messages with short visibility timeout
	shortVisibility := time.Second * 1
	msgs1, err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, time.Second*3, shortVisibility, numMessages, 10)

	require.NoError(t, err)
	assert.Len(t, msgs1, numMessages)

	// Verify no duplicates in first poll
	seen := make(map[string]bool)
	for _, msg := range msgs1 {
		assert.False(t, seen[msg.MessageId], "Duplicate in first poll: %s", msg.MessageId)
		seen[msg.MessageId] = true
	}

	// Wait for visibility to expire
	time.Sleep(shortVisibility + 500*time.Millisecond)

	// Second poll - messages should be visible again
	msgs2, err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, time.Second*3, time.Minute, numMessages, 10)

	require.NoError(t, err)
	assert.Len(t, msgs2, numMessages)

	// Verify no duplicates in second poll
	seen2 := make(map[string]bool)
	for _, msg := range msgs2 {
		assert.False(t, seen2[msg.MessageId], "Duplicate in second poll: %s", msg.MessageId)
		seen2[msg.MessageId] = true
	}
}

// TestMessageBodyUniqueness verifies that message bodies are unique and match
// what was sent, ensuring no corruption or mixing of messages.
func TestLocalMessageBodyUniqueness(t *testing.T) {
	ctx := context.Background()

	queueName := "bodyUniq-" + dynamodbqueue.RandomString(4)
	clientID := "bodyClient"

	// Clear the queue
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)
	require.NoError(t, err)

	// Create messages with unique content
	numMessages := 20
	sentBodies := make(map[string]bool)
	for i := 0; i < numMessages; i++ {
		body := fmt.Sprintf("unique-content-%d-%s", i, dynamodbqueue.RandomString(8))
		sentBodies[body] = true

		_, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: body})
		require.NoError(t, err)
	}

	// Poll all messages
	msgs, err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, time.Second*2, time.Minute, numMessages, numMessages)

	require.NoError(t, err)

	// Verify all received bodies match sent bodies
	receivedBodies := make(map[string]int)
	for _, msg := range msgs {
		receivedBodies[msg.Body]++
	}

	// Check for any duplicate bodies
	for body, count := range receivedBodies {
		assert.Equal(t, 1, count, "Body should appear exactly once: %s", body)
		assert.True(t, sentBodies[body], "Received unexpected body: %s", body)
	}

	// All sent messages should be received
	assert.Len(t, receivedBodies, numMessages)
}

// TestHighVolumeNoDuplicates tests with a high volume of messages to ensure
// no duplicates under stress.
func TestLocalHighVolumeNoDuplicates(t *testing.T) {
	ctx := context.Background()

	queueName := "highVol-" + dynamodbqueue.RandomString(4)
	clientID := "highVolClient"

	// Clear the queue
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)
	require.NoError(t, err)

	// Push many messages in batches
	totalMessages := 100
	batchSize := 25

	for batch := 0; batch < totalMessages/batchSize; batch++ {
		var msgs []events.SQSMessage
		for i := 0; i < batchSize; i++ {
			msgNum := batch*batchSize + i
			msgs = append(msgs, events.SQSMessage{
				Body: fmt.Sprintf("high-vol-msg-%d", msgNum),
			})
		}

		_, err := localQueue.PushMessages(ctx, 0, msgs...)
		require.NoError(t, err)
	}

	// Verify count
	count, err := localQueue.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(totalMessages), count)

	// Poll all messages with multiple concurrent pollers
	numPollers := 4
	var wg sync.WaitGroup
	allReceived := &sync.Map{}
	duplicateCount := int32(0)
	var duplicateMu sync.Mutex

	wg.Add(numPollers)
	for p := 0; p < numPollers; p++ {
		go func() {
			defer wg.Done()

			for {
				msgs, err := localQueue.UseQueueName(queueName).
					UseClientID(clientID).
					PollMessages(ctx, time.Millisecond*500, time.Minute*5, 10, 25)

				require.NoError(t, err)

				if len(msgs) == 0 {
					return
				}

				for _, msg := range msgs {
					if _, loaded := allReceived.LoadOrStore(msg.MessageId, msg.Body); loaded {
						duplicateMu.Lock()
						duplicateCount++
						t.Errorf("Duplicate message: %s", msg.MessageId)
						duplicateMu.Unlock()
					}

					// Delete message
					_, _ = localQueue.DeleteMessages(ctx, msg.ReceiptHandle)
				}
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, int32(0), duplicateCount, "No duplicates should exist")

	// Count total unique received
	totalReceived := 0
	allReceived.Range(func(key, value interface{}) bool {
		totalReceived++
		return true
	})

	assert.Equal(t, totalMessages, totalReceived, "All messages should be consumed")
}

// TestRapidPollCycles tests rapid successive poll operations don't cause duplicates.
func TestLocalRapidPollCycles(t *testing.T) {
	ctx := context.Background()

	queueName := "rapidPoll-" + dynamodbqueue.RandomString(4)
	clientID := "rapidClient"

	// Clear the queue
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)
	require.NoError(t, err)

	// Push some messages
	numMessages := 20
	for i := 0; i < numMessages; i++ {
		_, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("rapid-%d", i)})
		require.NoError(t, err)
	}

	// Rapid poll cycles
	allReceived := make(map[string]int)
	pollCycles := 10

	for cycle := 0; cycle < pollCycles; cycle++ {
		msgs, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PollMessages(ctx, time.Second*2, time.Minute, 5, 5)

		require.NoError(t, err)

		for _, msg := range msgs {
			allReceived[msg.MessageId]++

			// Delete to prevent re-poll
			_, _ = localQueue.DeleteMessages(ctx, msg.ReceiptHandle)
		}
	}

	// Verify no duplicates
	for msgID, count := range allReceived {
		assert.Equal(t, 1, count, "Message should appear exactly once: %s", msgID)
	}
}

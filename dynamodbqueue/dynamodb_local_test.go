package dynamodbqueue_test

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
	"github.com/mariotoffia/godynamodb-queue/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	ddbLocal   *testutil.DynamoDBLocal
	localQueue dynamodbqueue.Queue
	tableName  string
)

// TestMain sets up DynamoDB Local for all tests
func TestMain(m *testing.M) {
	code := runTests(m)
	cleanup()
	os.Exit(code)
}

// cleanup ensures containers are removed regardless of how tests exit.
func cleanup() {
	if ddbLocal != nil {
		_ = ddbLocal.Close()
	}
	_ = testutil.CloseAllInstances(context.Background())
}

// runTests runs the tests and returns the exit code.
func runTests(m *testing.M) int {
	ctx := context.Background()

	// 1. Clean up orphaned containers FIRST
	_ = testutil.CloseAllInstances(ctx)

	suffix := dynamodbqueue.RandomString(6)
	containerName := fmt.Sprintf("ddbq-test-%s", suffix)
	tableName = fmt.Sprintf("test-queue-%s", suffix)

	ddbLocal = testutil.NewLocalDynamoDB(containerName)
	if ddbLocal == nil {
		fmt.Println("Failed to create DynamoDB Local instance")
		return 1
	}

	// Setup signal handling for cleanup on Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt, cleaning up containers...")
		cleanup()
		os.Exit(1)
	}()

	// Start container
	if err := ddbLocal.Start(ctx); err != nil {
		fmt.Printf("Failed to start DynamoDB Local: %v\n", err)
		return 1
	}

	// Wait for DB to be ready (ping with API call)
	if err := ddbLocal.WaitForReady(ctx, 30*time.Second); err != nil {
		fmt.Printf("DynamoDB Local not ready: %v\n", err)
		return 1
	}

	// Create table
	if err := ddbLocal.CreateTable(ctx, tableName); err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
		return 1
	}

	localQueue = dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0).UseTable(tableName)
	return m.Run()
}

func TestLocalCheckIfTableExistSuccess(t *testing.T) {
	exist := localQueue.TableExists(context.Background())
	assert.True(t, exist)
}

func TestLocalCheckIfTableExistFailure(t *testing.T) {
	ctx := context.Background()

	exits := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0).
		UseTable(dynamodbqueue.RandomPostfix("non-existent-table")).
		TableExists(ctx)

	assert.False(t, exits)
}

func TestLocalPutAndPollItemsThenDeleteThem(t *testing.T) {
	ctx := context.Background()

	queueName := "testQueue-" + dynamodbqueue.RandomString(4)
	clientID := "testClient-" + dynamodbqueue.RandomString(4)

	// Clear the queue before testing
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)

	require.NoError(t, err)

	_, err = localQueue.PushMessages(ctx, 0, events.SQSMessage{
		Attributes: map[string]string{
			"test": "test",
		},
		Body: "test body",
	})

	require.NoError(t, err)

	count, err := localQueue.Count(ctx)

	require.NoError(t, err)
	assert.Equal(t, int32(1), count)

	msgs, err := localQueue.PollMessages(
		ctx,
		time.Second*2,
		time.Minute*14,
		10,
		10,
	)

	require.NoError(t, err)
	assert.Len(t, msgs, 1)

	notDeleted, err := localQueue.DeleteMessages(ctx, dynamodbqueue.ToReceiptHandles(msgs)...)

	require.NoError(t, err)
	assert.Len(t, notDeleted, 0)
}

func TestLocalQueueIsolation(t *testing.T) {
	ctx := context.Background()

	suffix := dynamodbqueue.RandomString(4)

	// Clear both queues
	err := localQueue.UseQueueName("queueA-" + suffix).
		UseClientID("client1").
		Purge(ctx)

	require.NoError(t, err)

	err = localQueue.UseQueueName("queueB-" + suffix).
		UseClientID("client1").
		Purge(ctx)

	require.NoError(t, err)

	// Push a message to queueA and queueB
	_, err = localQueue.UseQueueName("queueA-" + suffix).
		UseClientID("client1").
		PushMessages(ctx, 0, events.SQSMessage{Body: "msg for queueA"})

	require.NoError(t, err)

	_, err = localQueue.UseQueueName("queueB-" + suffix).
		UseClientID("client1").
		PushMessages(ctx, 0, events.SQSMessage{Body: "msg for queueB"})

	require.NoError(t, err)

	// Poll messages from queueA
	msgsA, err := localQueue.UseQueueName("queueA-" + suffix).
		UseClientID("client1").
		PollMessages(ctx, time.Second*2, time.Minute, 10, 10)

	require.NoError(t, err)

	// Poll messages from queueB
	msgsB, err := localQueue.UseQueueName("queueB-" + suffix).
		UseClientID("client1").
		PollMessages(ctx, time.Second*2, time.Minute, 10, 10)

	require.NoError(t, err)

	// Ensure the messages were correctly isolated
	assert.Len(t, msgsA, 1)
	assert.Equal(t, "msg for queueA", msgsA[0].Body)
	assert.Len(t, msgsB, 1)
	assert.Equal(t, "msg for queueB", msgsB[0].Body)
}

func TestLocalMessageOrderPreservationFIFO(t *testing.T) {
	ctx := context.Background()

	queueName := "fifoQueue-" + dynamodbqueue.RandomString(4)
	clientID := "testClient"

	// Clear the queue
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)

	require.NoError(t, err)

	// Insert messages into the queue in a specific order
	messages := []string{"first", "second", "third", "fourth"}
	for _, msg := range messages {
		_, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: msg})

		require.NoError(t, err)
		// Small delay to ensure distinct timestamps
		time.Sleep(5 * time.Millisecond)
	}

	// Poll the messages
	polledMessages, err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, time.Second*2, time.Minute, len(messages), len(messages))

	require.NoError(t, err)
	require.Len(t, polledMessages, len(messages))

	// Check the order of the messages
	for i, polledMsg := range polledMessages {
		assert.Equal(t, messages[i], polledMsg.Body)
	}
}

func TestLocalVisibilityTimeoutFunctionality(t *testing.T) {
	ctx := context.Background()

	queueName := "timeoutQueue-" + dynamodbqueue.RandomString(4)
	clientID := "testClient"

	// Clear the queue
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)

	require.NoError(t, err)

	// Push a message into the queue
	messageBody := "visibilityTest"
	_, err = localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		PushMessages(ctx, 0, events.SQSMessage{Body: messageBody})

	require.NoError(t, err)

	// Poll the message with a visibility timeout
	visibilityTimeout := time.Second * 2
	polledMessages, err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, time.Second*2, visibilityTimeout, 1, 1)

	require.NoError(t, err)
	require.Len(t, polledMessages, 1)
	assert.Equal(t, messageBody, polledMessages[0].Body)

	// Try polling immediately, should get no messages because of visibility timeout
	// Use timeout=0 here since we expect empty results (message is hidden)
	polledMessages, err = localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, time.Millisecond*100, visibilityTimeout, 1, 1)

	require.NoError(t, err)
	require.Len(t, polledMessages, 0)

	// Wait for the visibility timeout duration
	time.Sleep(visibilityTimeout + 500*time.Millisecond)

	// Now, try polling again. The message should be visible now.
	polledMessages, err = localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, time.Second*2, visibilityTimeout, 1, 1)

	require.NoError(t, err)
	require.Len(t, polledMessages, 1)
	assert.Equal(t, messageBody, polledMessages[0].Body)
}

func TestLocalPurgeFunctionality(t *testing.T) {
	ctx := context.Background()

	queueName := "purgeQueue-" + dynamodbqueue.RandomString(4)
	clientID := "purgeClient"
	totalMessages := 50

	// Clear the queue before testing
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)

	require.NoError(t, err)

	// Populate the queue with messages
	for i := 0; i < totalMessages; i++ {
		_, err := localQueue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("message%d", i)})

		require.NoError(t, err)
	}

	// Verify messages were added
	count, err := localQueue.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(totalMessages), count)

	// Purge the queue
	err = localQueue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)
	require.NoError(t, err)

	// Verify queue is empty
	count, err = localQueue.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(0), count)
}

func TestLocalConcurrentWritersIntegrity(t *testing.T) {
	ctx := context.Background()

	queueName := "concurrentQueue-" + dynamodbqueue.RandomString(4)
	clientID := "concurrentClient"
	numWriters := 5
	messagesPerWriter := 20

	// Clear the queue before testing
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)

	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Start concurrent writers
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()

			for j := 0; j < messagesPerWriter; j++ {
				_, err := localQueue.UseQueueName(queueName).
					UseClientID(clientID).
					PushMessages(ctx, 0, events.SQSMessage{
						Body: fmt.Sprintf("writer%d-message%d", writerID, j),
					})

				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages were written
	count, err := localQueue.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(numWriters*messagesPerWriter), count)
}

func TestLocalListQueues(t *testing.T) {
	ctx := context.Background()

	suffix := dynamodbqueue.RandomString(4)

	// Clear any existing data
	err := localQueue.PurgeAll(ctx)
	require.NoError(t, err)

	// Push a set of messages and change queue and client id names
	queues := []string{"queue1-" + suffix, "queue2-" + suffix, "queue3-" + suffix}
	clientIDs := []string{"client1", "client2", "client3"}

	for _, q := range queues {
		for _, c := range clientIDs {
			_, err := localQueue.UseQueueName(q).
				UseClientID(c).
				PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("test-%s-%s", q, c)})

			require.NoError(t, err)
		}
	}

	// List all queues
	found, err := localQueue.List(ctx)

	require.NoError(t, err)
	assert.Len(t, found, len(queues)*len(clientIDs))

	find := func(q string, c string) bool {
		for _, f := range found {
			if f.QueueName == q && f.ClientID == c {
				return true
			}
		}
		return false
	}

	for _, q := range queues {
		for _, c := range clientIDs {
			assert.True(t, find(q, c), "Could not find queue: %s and client: %s", q, c)
		}
	}
}

func TestLocalClientIsolationWithinQueue(t *testing.T) {
	ctx := context.Background()

	suffix := dynamodbqueue.RandomString(4)
	queueName := "commonQueue-" + suffix

	// Clear for both client IDs
	err := localQueue.UseQueueName(queueName).
		UseClientID("clientA").
		Purge(ctx)

	require.NoError(t, err)

	err = localQueue.UseQueueName(queueName).
		UseClientID("clientB").
		Purge(ctx)

	require.NoError(t, err)

	// Push a message using clientA
	_, err = localQueue.UseQueueName(queueName).
		UseClientID("clientA").
		PushMessages(ctx, 0, events.SQSMessage{Body: "msg for clientA"})

	require.NoError(t, err)

	// Push another message using clientB
	_, err = localQueue.UseQueueName(queueName).
		UseClientID("clientB").
		PushMessages(ctx, 0, events.SQSMessage{Body: "msg for clientB"})

	require.NoError(t, err)

	// Poll messages using clientA
	msgsA, err := localQueue.UseQueueName(queueName).
		UseClientID("clientA").
		PollMessages(ctx, time.Second*2, time.Minute, 10, 10)

	require.NoError(t, err)

	// Poll messages using clientB
	msgsB, err := localQueue.UseQueueName(queueName).
		UseClientID("clientB").
		PollMessages(ctx, time.Second*2, time.Minute, 10, 10)

	require.NoError(t, err)

	// Ensure clientA only sees its own message
	assert.Len(t, msgsA, 1)
	assert.Equal(t, "msg for clientA", msgsA[0].Body)

	// Ensure clientB only sees its own message
	assert.Len(t, msgsB, 1)
	assert.Equal(t, "msg for clientB", msgsB[0].Body)
}

func TestLocalPushMessageLimits(t *testing.T) {
	ctx := context.Background()

	queueName := "pushLimitQueue-" + dynamodbqueue.RandomString(4)
	clientID := "pushLimitClient"
	messageCountLimit := 25
	smallMessage := "test"

	// Clear the queue before testing
	err := localQueue.UseQueueName(queueName).
		UseClientID(clientID).
		Purge(ctx)

	require.NoError(t, err)

	// Attempt to push more than 25 small messages
	messages := make([]events.SQSMessage, messageCountLimit+1)

	for i := 0; i <= messageCountLimit; i++ {
		messages[i] = events.SQSMessage{Body: smallMessage}
	}

	_, err = localQueue.PushMessages(ctx, 0, messages...)

	assert.Error(t, err, "Pushing more than allowed message count should fail")
}

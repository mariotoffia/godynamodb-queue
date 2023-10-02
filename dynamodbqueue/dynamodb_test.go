package dynamodbqueue_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const LongRunningUnitTest = "Disabled due to no long running unit test enabled"

var queue *dynamodbqueue.DynamoDBQueue

func newCtxAndConfig() (context.Context, aws.Config) {
	ctx := context.Background()

	config, err := awsconfig.LoadDefaultConfig(ctx)

	if err != nil {
		assert.FailNow(nil, err.Error())
	}

	return ctx, config
}

// InLogUnitTest will check if `LONG_UNIT_TEST` is enabled.
func inLogUnitTest() bool {
	s := os.Getenv("LONG_UNIT_TEST")

	return strings.ToLower(s) == "true"
}

// TestMain will create a dynamodb table, run all tests and then drop the table.
func TestMain(m *testing.M) {
	ctx, cfg := newCtxAndConfig()
	queue = dynamodbqueue.NewDynamoDBQueue(ctx, cfg, 0)

	created, err := queue.UseTable(dynamodbqueue.RandomPostfix("test") /*"unit-test-queue-table"*/).
		CreateQueueTable(ctx)

	if err != nil {
		if created {
			queue.DropQueueTable(ctx)
		}

		panic(err)
	}

	// Drop everything before running tests
	queue.PurgeAll(ctx)

	code := m.Run()

	// Make sure to clean up
	queue.PurgeAll(ctx)

	if created {
		queue.DropQueueTable(ctx)
	}

	os.Exit(code)
}

func TestPutAndPollItemsThenDeleteThem(t *testing.T) {
	ctx := context.Background()

	// Clear the queue before testing
	queue.UseQueueName("testQueue").UseClientID("testClientId").Purge(ctx)

	msgs, err := queue.PushMessages(ctx, 0 /*defaultTTL*/, events.SQSMessage{
		Attributes: map[string]string{
			"test": "test",
		},
		MessageAttributes: map[string]events.SQSMessageAttribute{
			"test2": {
				DataType:    "String",
				StringValue: aws.String("test2"),
			},
		},
		Body: "test body",
	})

	require.NoError(t, err)
	require.Len(t, msgs, 0)

	count, err := queue.Count(ctx)

	require.NoError(t, err)
	assert.Equal(t, int32(1), count)

	msgs, err = queue.PollMessages(
		ctx,
		0,              /*noTimeout*/
		time.Minute*14, /*visibilityTimeout*/
		10,             /*maxMessages*/
	)

	require.NoError(t, err)
	assert.Len(t, msgs, 1)

	fmt.Println(msgs[0].ReceiptHandle)

	notDeleted, err := queue.DeleteMessages(ctx, msgs[0].ReceiptHandle)

	require.NoError(t, err)
	assert.Len(t, notDeleted, 0)
}

func TestQueueIsolation(t *testing.T) {
	ctx := context.Background()

	// Clear both queues
	queue.UseQueueName("queueA").UseClientID("client1").Purge(ctx)
	queue.UseQueueName("queueB").UseClientID("client1").Purge(ctx)

	// Push a message to queueA and queueB
	_, err := queue.UseQueueName("queueA").
		UseClientID("client1").
		PushMessages(ctx, 0, events.SQSMessage{Body: "msg for queueA"})

	require.NoError(t, err)

	_, err = queue.UseQueueName("queueB").
		UseClientID("client1").
		PushMessages(ctx, 0, events.SQSMessage{Body: "msg for queueB"})

	require.NoError(t, err)

	// Poll messages from queueA
	msgsA, err := queue.UseQueueName("queueA").
		UseClientID("client1").
		PollMessages(ctx, 0, time.Minute, 10)

	require.NoError(t, err)

	// Poll messages from queueB
	msgsB, err := queue.UseQueueName("queueB").
		UseClientID("client1").
		PollMessages(ctx, 0, time.Minute, 10)

	require.NoError(t, err)

	// Ensure the messages were correctly isolated
	assert.Len(t, msgsA, 1)
	assert.Equal(t, "msg for queueA", msgsA[0].Body)
	assert.Len(t, msgsB, 1)
	assert.Equal(t, "msg for queueB", msgsB[0].Body)
}

func TestClientIsolationWithinQueue(t *testing.T) {
	ctx := context.Background()

	// Clear for both client IDs
	queue.UseQueueName("commonQueue").UseClientID("clientA").Purge(ctx)
	queue.UseQueueName("commonQueue").UseClientID("clientB").Purge(ctx)

	// Push a message using clientA
	_, err := queue.UseQueueName("commonQueue").
		UseClientID("clientA").
		PushMessages(ctx, 0, events.SQSMessage{Body: "msg for clientA"})

	require.NoError(t, err)

	// Push another message using clientB
	_, err = queue.UseQueueName("commonQueue").
		UseClientID("clientB").
		PushMessages(ctx, 0, events.SQSMessage{Body: "msg for clientB"})

	require.NoError(t, err)

	// Poll messages using clientA
	msgsA, err := queue.UseQueueName("commonQueue").
		UseClientID("clientA").
		PollMessages(ctx, 0, time.Minute, 10)

	require.NoError(t, err)

	// Poll messages using clientB
	msgsB, err := queue.UseQueueName("commonQueue").
		UseClientID("clientB").
		PollMessages(ctx, 0, time.Minute, 10)

	require.NoError(t, err)

	// Ensure clientA only sees its own message
	assert.Len(t, msgsA, 1)
	assert.Equal(t, "msg for clientA", msgsA[0].Body)

	// Ensure clientB only sees its own message
	assert.Len(t, msgsB, 1)
	assert.Equal(t, "msg for clientB", msgsB[0].Body)
}

func TestMessageOrderPreservationFIFO(t *testing.T) {
	ctx := context.Background()

	queueName := "fifoQueue"
	clientID := "testClient"

	// Clear the queue
	queue.UseQueueName(queueName).
		UseClientID(clientID).
		AsQueueType(dynamodbqueue.QueueTypeFIFO).
		Purge(ctx)

	// Insert messages into the queue in a specific order
	messages := []string{"first", "second", "third", "fourth"}
	for _, msg := range messages {
		_, err := queue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: msg})

		require.NoError(t, err)
	}

	// Poll the messages
	polledMessages, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, time.Minute, len(messages))

	require.NoError(t, err)
	require.Len(t, polledMessages, len(messages))

	// Check the order of the messages
	for i, polledMsg := range polledMessages {
		assert.Equal(t, messages[i], polledMsg.Body)
	}
}

func TestMessageOrderPreservationLIFO(t *testing.T) {
	ctx := context.Background()

	queueName := "fifoQueue"
	clientID := "testClient"

	// Clear the queue
	queue.UseQueueName(queueName).
		UseClientID(clientID).
		AsQueueType(dynamodbqueue.QueueTypeLIFO).
		Purge(ctx)

	// Insert messages into the queue in a specific order
	messages := []string{"first", "second", "third", "fourth"}

	for _, msg := range messages {
		_, err := queue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: msg})

		require.NoError(t, err)
	}

	// Poll the messages
	polledMessages, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, time.Minute, len(messages))

	require.NoError(t, err)
	require.Len(t, polledMessages, len(messages))

	// Check the order of the messages
	reversed_messages := make([]string, len(messages))
	for i, msg := range messages {
		reversed_messages[len(messages)-i-1] = msg
	}

	for i, polledMsg := range polledMessages {
		assert.Equal(t, reversed_messages[i], polledMsg.Body)
	}
}

func TestVisibilityTimeoutFunctionality(t *testing.T) {
	ctx := context.Background()

	queueName := "timeoutQueue"
	clientID := "testClient"

	// Clear the queue
	queue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)

	// Push a message into the queue
	messageBody := "visibilityTest"
	_, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PushMessages(ctx, 0, events.SQSMessage{Body: messageBody})

	require.NoError(t, err)

	// Poll the message with a visibility timeout
	visibilityTimeout := time.Second * 10
	polledMessages, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 1)

	require.NoError(t, err)
	require.Len(t, polledMessages, 1)
	assert.Equal(t, messageBody, polledMessages[0].Body)

	// Try polling immediately, should get no messages because of visibility timeout
	polledMessages, err = queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 1)

	require.NoError(t, err)
	require.Len(t, polledMessages, 0)

	// Wait for the visibility timeout duration
	time.Sleep(visibilityTimeout + time.Second)

	// Now, try polling again. The message should be visible now.
	polledMessages, err = queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 1)

	require.NoError(t, err)
	require.Len(t, polledMessages, 1)
	assert.Equal(t, messageBody, polledMessages[0].Body)
}

func TestVisibilityTimeoutEffectiveness(t *testing.T) {
	ctx := context.Background()

	queueName := "visibilityQueue"
	clientID := "visibilityClient"
	visibilityTimeout := 5 * time.Second

	// Clear the queue before testing
	queue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)

	// Push a single message
	_, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PushMessages(ctx, 0, events.SQSMessage{Body: "visibilityMessage"})

	require.NoError(t, err)

	// Poll the message with consumer1
	polledMessages, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 1)

	require.NoError(t, err)
	assert.Len(t, polledMessages, 1)

	// Try polling with consumer2 immediately, should not receive the message due to visibility timeout
	polledMessages2, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 1)

	require.NoError(t, err)
	assert.Len(t, polledMessages2, 0)

	// Wait for the visibility timeout to expire
	time.Sleep(visibilityTimeout + 1*time.Second)

	// Now, consumer2 should be able to poll the message
	polledMessages2, err = queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 1)

	require.NoError(t, err)
	assert.Len(t, polledMessages2, 1)
}

func TestVisibilityTimeoutRecovery(t *testing.T) {
	ctx := context.Background()

	queueName := "timeoutQueue"
	clientID := "timeoutClient"
	visibilityTimeout := 5 * time.Second

	// Clear the queue before testing
	queue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)

	// Push a single message into the queue
	_, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PushMessages(ctx, 0, events.SQSMessage{Body: "timeoutTestMessage"})

	require.NoError(t, err)

	// Poll the message but do not delete it
	polledMessages, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 1)

	require.NoError(t, err)
	assert.Len(t, polledMessages, 1)

	// Poll immediately again. This time, no message should be retrieved because of the visibility timeout.
	polledMessages, err = queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 1)

	require.NoError(t, err)
	assert.Len(t, polledMessages, 0)

	// Wait for the visibility timeout to elapse
	time.Sleep(visibilityTimeout + 1*time.Second)

	// Poll again. This time, the previously fetched message should be retrieved.
	polledMessages, err = queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 1)

	require.NoError(t, err)
	assert.Len(t, polledMessages, 1)
	assert.Equal(t, "timeoutTestMessage", polledMessages[0].Body)
}

func TestPurgeFunctionality(t *testing.T) {
	ctx := context.Background()

	queueName := "purgeTestQueue"
	clientID := "purgeTestClient"
	totalMessages := 100
	visibilityTimeout := 5 * time.Minute // Set it high intentionally

	// Clear the queue before testing
	queue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)

	// Populate the queue with messages
	for i := 0; i < totalMessages; i++ {
		_, err := queue.UseQueueName(queueName).
			UseClientID(clientID).
			PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("message%d", i)})

		require.NoError(t, err)
	}

	// Poll a few messages to hide them using visibility timeout
	_, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 10)

	require.NoError(t, err)

	// Purge the queue
	err = queue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)

	require.NoError(t, err)

	// Attempt to poll messages
	messagesAfterPurge, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, 0, totalMessages)

	require.NoError(t, err)

	// Assert that no messages were retrieved after purging
	assert.Empty(t, messagesAfterPurge)
}

func TestPushMessageLimits(t *testing.T) {
	ctx := context.Background()

	queueName := "pushLimitQueue"
	clientID := "pushLimitClient"
	messageSizeLimit := 16 * 1024 * 1024 // 16MB
	messageCountLimit := 25
	bigMessage := strings.Repeat("A", messageSizeLimit)
	smallMessage := "test"

	// Clear the queue before testing
	queue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)

	// Attempt to push a message that exceeds size limit
	_, err := queue.PushMessages(ctx, 0, events.SQSMessage{Body: bigMessage})

	assert.Error(t, err, "Pushing message exceeding size limit should fail")

	// Attempt to push more than 25 small messages
	messages := make([]events.SQSMessage, messageCountLimit+1)

	for i := 0; i <= messageCountLimit; i++ {
		messages[i] = events.SQSMessage{Body: smallMessage}
	}

	_, err = queue.PushMessages(ctx, 0, messages...)

	assert.Error(t, err, "Pushing more than allowed message count should fail")
}

func TestVisibilityTimeoutRespect(t *testing.T) {
	ctx := context.Background()

	queueName := "visibilityTimeoutQueue"
	clientID := "visibilityTimeoutClient"
	visibilityTimeout := 5 * time.Second

	// Clear the queue before testing
	queue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)

	// Push a message to the queue
	_, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PushMessages(ctx, 0, events.SQSMessage{Body: "testVisibility"})

	require.NoError(t, err)

	// Poll the message with a visibility timeout
	polledMsgs, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, visibilityTimeout, 1)

	require.NoError(t, err)
	assert.Len(t, polledMsgs, 1)

	// Try polling immediately; the message should not be visible
	polledMsgs, err = queue.UseQueueName(queueName).UseClientID(clientID).PollMessages(ctx, 0, 0, 1)

	require.NoError(t, err)
	assert.Empty(t, polledMsgs)

	// Sleep for the visibility timeout period + a small buffer
	time.Sleep(visibilityTimeout + 1*time.Second)

	// Try polling after sleeping; the message should now be visible
	polledMsgs, err = queue.UseQueueName(queueName).UseClientID(clientID).PollMessages(ctx, 0, 0, 1)

	require.NoError(t, err)
	assert.Len(t, polledMsgs, 1)
}

func TestConcurrentWritersIntegrity(t *testing.T) {
	if inLogUnitTest() == false {
		t.Skip(LongRunningUnitTest)
	}

	ctx := context.Background()

	queueName := "concurrentQueue"
	clientID := "concurrentClient"
	numWriters := 10
	messagesPerWriter := 100

	// Clear the queue before testing
	queue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)

	var wg sync.WaitGroup

	wg.Add(numWriters)

	// Start concurrent writers
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerWriter; j++ {
				_, err := queue.UseQueueName(queueName).
					UseClientID(clientID).
					PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("writer%d-message%d", writerID, j)})

				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Poll all messages and verify their count
	allMessages, err := queue.UseQueueName(queueName).
		UseClientID(clientID).
		PollMessages(ctx, 0, time.Minute, numWriters*messagesPerWriter)

	require.NoError(t, err)
	assert.Len(t, allMessages, numWriters*messagesPerWriter)
}

func TestVisibilityTimeoutUnderLoad(t *testing.T) {
	if inLogUnitTest() == false {
		t.Skip(LongRunningUnitTest)
	}

	ctx := context.Background()

	queueName := "visibilityQueue"
	clientID := "visibilityClient"
	numConsumers := 50
	totalMessages := 100
	visibilityTimeout := 50 * time.Second

	// Clear the queue before testing
	queue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)

	var messages []events.SQSMessage
	// Populate the queue with messages
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, events.SQSMessage{Body: fmt.Sprintf("message%d", i)})
	}

	// Can only handle 25 messages up to 16MB
	batches := dynamodbqueue.ToBatches(messages, 25)

	for _, batch := range batches {
		_, err := queue.PushMessages(ctx, 0, batch...)

		require.NoError(t, err)
	}

	consumedMessages := &sync.Map{}

	var wg sync.WaitGroup

	wg.Add(numConsumers)

	// Start concurrent consumers
	for i := 0; i < numConsumers; i++ {
		go func() {
			defer wg.Done()

			msg, err := queue.PollMessages(
				ctx,
				time.Second*25, /* give it a little space to get message*/
				visibilityTimeout,
				20,
			)

			require.NoError(t, err)

			if len(msg) > 0 {
				for _, m := range msg {
					_, loaded := consumedMessages.LoadOrStore(m.Body, true)

					require.False(t, loaded, "Duplicate message consumed: %s", m.Body)
				}
			}
		}()
	}

	wg.Wait()

	// Check the total number of consumed messages
	count := 0

	consumedMessages.Range(func(_, _ interface{}) bool {
		count++
		return true
	})

	assert.Equal(t, totalMessages, count)
}

func TestContinuousWriteAndReadWithNoDuplicates(t *testing.T) {
	if inLogUnitTest() == false {
		t.Skip(LongRunningUnitTest)
	}

	ctx := context.Background()

	queueName := "sharedQueue"
	clientID := "commonClient"
	consumerCount := 3
	totalMessages := 100

	// Clear the queue
	queue.UseQueueName(queueName).UseClientID(clientID).Purge(ctx)

	var mu sync.Mutex

	// To keep track of messages consumed to ensure no duplicates
	messagesConsumed := map[string]bool{}

	var wg sync.WaitGroup

	// Writer goroutine that continuously writes messages
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < totalMessages; i++ {
			// Write message
			_, err := queue.UseQueueName(queueName).
				UseClientID(clientID).
				PushMessages(ctx, 0, events.SQSMessage{Body: fmt.Sprintf("message-%d", i)})

			require.NoError(t, err)

			// Small delay to simulate continuous writes
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Consumer goroutines
	for c := 0; c < consumerCount; c++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			emptyCount := 10

			for {
				// Consume
				msgs, err := queue.UseQueueName(queueName).
					UseClientID(clientID).
					PollMessages(ctx, 0, time.Minute*5, 10)

				require.NoError(t, err)

				// Break when no more messages to consume.
				if len(msgs) == 0 {
					if emptyCount <= 0 {
						break
					}

					emptyCount--

					continue
				}

				// Reset empty count
				emptyCount = 10

				mu.Lock()

				for _, m := range msgs {

					_, exists := messagesConsumed[m.Body]

					if exists {
						assert.FailNowf(
							t,
							"Message consumed twice",
							"Message consumed twice: %s",
							m.Body,
						)
					}

					messagesConsumed[m.Body] = true

					// Delete the message after consumption
					_, err = queue.DeleteMessages(ctx, m.ReceiptHandle)

					require.NoError(t, err)
				}

				mu.Unlock()

			}
		}()
	}

	wg.Wait()

	// Ensure all messages written have been consumed
	assert.Equal(t, totalMessages, len(messagesConsumed))
}

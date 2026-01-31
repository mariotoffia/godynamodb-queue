package dynamodbqueue_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ═══════════════════════════════════════════════════════════════════════════════
// FIFO Race Condition Tests - Stress Testing
//
// Long-running tests to catch intermittent race conditions.
// ═══════════════════════════════════════════════════════════════════════════════

// TestFIFORace_StressTest_LongRunning runs extended stress test.
//
// Strategy: Run for longer duration with continuous push/poll to
// catch intermittent race conditions.
func TestFIFORace_StressTest_LongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running stress test")
	}

	ctx := context.Background()

	queueName := fmt.Sprintf("race-stress-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-stress")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numGroups = 3
	const numConsumers = 6
	const testDuration = 10 * time.Second

	groups := make([]string, numGroups)
	groupCounters := make([]int64, numGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("sg%d", i)
	}

	groupMessages := make(map[string][]int)
	var groupMu sync.Mutex
	var totalPushed int64
	var totalReceived int64

	ctx, cancel := context.WithTimeout(ctx, testDuration+5*time.Second)
	defer cancel()

	done := make(chan struct{})

	// Producer goroutines - continuously push
	var producerWg sync.WaitGroup
	for i := 0; i < numGroups; i++ {
		producerWg.Add(1)
		go func(groupIdx int) {
			defer producerWg.Done()
			group := groups[groupIdx]

			for {
				select {
				case <-done:
					return
				case <-ctx.Done():
					return
				default:
				}

				msgNum := atomic.AddInt64(&groupCounters[groupIdx], 1)
				_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
					Body: fmt.Sprintf("%s|%06d", group, msgNum),
				})
				if err != nil {
					return
				}
				atomic.AddInt64(&totalPushed, 1)
				time.Sleep(time.Millisecond * 5)
			}
		}(i)
	}

	// Consumer goroutines
	var consumerWg sync.WaitGroup
	for c := 0; c < numConsumers; c++ {
		consumerWg.Add(1)
		go func(consumerID int) {
			defer consumerWg.Done()

			for {
				select {
				case <-done:
					return
				case <-ctx.Done():
					return
				default:
				}

				msgs, err := fifo.PollMessages(ctx, time.Millisecond*100, time.Minute, 1, numGroups)
				if err != nil {
					return
				}

				for _, msg := range msgs {
					parts := strings.Split(msg.Body, "|")
					if len(parts) != 2 {
						continue
					}
					group := parts[0]
					msgNum, _ := strconv.Atoi(parts[1])

					groupMu.Lock()
					groupMessages[group] = append(groupMessages[group], msgNum)
					groupMu.Unlock()

					_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)
					atomic.AddInt64(&totalReceived, 1)
				}
			}
		}(c)
	}

	// Run for test duration
	time.Sleep(testDuration)
	close(done)

	producerWg.Wait()
	consumerWg.Wait()

	// Check for ordering violations
	violations := 0
	for group, msgs := range groupMessages {
		for i := 1; i < len(msgs); i++ {
			if msgs[i] <= msgs[i-1] {
				violations++
				if violations <= 10 { // Limit error output
					t.Errorf("STRESS RACE in group %s: message %d came after %d",
						group, msgs[i], msgs[i-1])
				}
			}
		}
	}

	t.Logf("Stress test: pushed %d, received %d, violations %d",
		totalPushed, totalReceived, violations)
	assert.Equal(t, 0, violations, "FIFO race condition detected in stress test")
}

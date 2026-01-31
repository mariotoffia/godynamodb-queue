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
// FIFO Race Condition Tests - Basic Concurrency
//
// These tests are designed to expose the TOCTOU (Time-of-Check-Time-of-Use) race
// condition between getInFlightGroups and lockCandidates in the FIFO implementation.
// ═══════════════════════════════════════════════════════════════════════════════

// TestFIFORace_HighConcurrency_ManyConsumers tests with maximum concurrency.
//
// Strategy: Use many consumers (10+) to maximize the chance of hitting
// the race window between getInFlightGroups and lockCandidates.
func TestFIFORace_HighConcurrency_ManyConsumers(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-high-conc-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-test")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numGroups = 3
	const messagesPerGroup = 50
	const numConsumers = 10 // High concurrency to trigger race
	const totalMessages = numGroups * messagesPerGroup

	groups := make([]string, numGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("rg%d", i)
	}

	// Push messages with minimal delay - we want tight timestamps
	t.Logf("Pushing %d messages to %d groups with minimal delay...", totalMessages, numGroups)
	for _, group := range groups {
		for i := 0; i < messagesPerGroup; i++ {
			_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s|%03d", group, i),
			})
			require.NoError(t, err)
			// Minimal delay - just enough for timestamp uniqueness
			time.Sleep(time.Microsecond * 100)
		}
	}

	// Track received messages per group
	groupMessages := make(map[string][]int)
	var groupMu sync.Mutex
	var totalReceived int64

	done := make(chan struct{})
	var doneOnce sync.Once

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	t.Logf("Starting %d consumers to race for messages...", numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func(consumerID int) {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
				}

				// Poll aggressively with no wait timeout
				msgs, err := fifo.PollMessages(ctx, 0, time.Minute*5, 1, numGroups)
				if err != nil {
					return
				}

				for _, msg := range msgs {
					parts := strings.Split(msg.Body, "|")
					group := parts[0]
					msgNum, _ := strconv.Atoi(parts[1])

					groupMu.Lock()
					groupMessages[group] = append(groupMessages[group], msgNum)
					groupMu.Unlock()

					_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)

					received := atomic.AddInt64(&totalReceived, 1)
					if received >= totalMessages {
						doneOnce.Do(func() { close(done) })
						return
					}
				}

				if len(msgs) == 0 {
					// Tiny sleep to avoid CPU spin
					time.Sleep(time.Millisecond)
				}
			}
		}(c)
	}

	// Timeout
	go func() {
		time.Sleep(60 * time.Second)
		doneOnce.Do(func() { close(done) })
	}()

	wg.Wait()

	// Check for ordering violations
	violations := 0
	for group, msgs := range groupMessages {
		for i := 1; i < len(msgs); i++ {
			if msgs[i] <= msgs[i-1] {
				violations++
				t.Errorf("RACE DETECTED in group %s: message %d came after %d (position %d)",
					group, msgs[i], msgs[i-1], i)
			}
		}
	}

	t.Logf("Results: %d messages received, %d ordering violations", totalReceived, violations)
	assert.Equal(t, 0, violations, "FIFO race condition detected - ordering violations found")
	assert.Equal(t, int64(totalMessages), totalReceived, "all messages should be received")
}

// TestFIFORace_SingleGroup_MaxContention tests single group with maximum contention.
//
// Strategy: All consumers fight for messages from a single group, maximizing
// the race window since every consumer wants the same message.
func TestFIFORace_SingleGroup_MaxContention(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-single-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-single")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numMessages = 100
	const numConsumers = 8
	const group = "single-group"

	// Push all messages to single group
	t.Logf("Pushing %d messages to single group...", numMessages)
	for i := 0; i < numMessages; i++ {
		_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
			Body: fmt.Sprintf("%d", i),
		})
		require.NoError(t, err)
		time.Sleep(time.Microsecond * 50) // Minimal delay
	}

	receivedOrder := make([]int, 0, numMessages)
	var mu sync.Mutex
	var totalReceived int64

	done := make(chan struct{})
	var doneOnce sync.Once

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	t.Logf("Starting %d consumers fighting for single group...", numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func(consumerID int) {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
				}

				// All consumers poll the same single group
				msgs, err := fifo.PollMessages(ctx, 0, time.Minute*5, 1, 1)
				if err != nil {
					return
				}

				for _, msg := range msgs {
					msgNum, _ := strconv.Atoi(msg.Body)

					mu.Lock()
					receivedOrder = append(receivedOrder, msgNum)
					mu.Unlock()

					_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)

					received := atomic.AddInt64(&totalReceived, 1)
					if received >= numMessages {
						doneOnce.Do(func() { close(done) })
						return
					}
				}

				if len(msgs) == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}(c)
	}

	go func() {
		time.Sleep(60 * time.Second)
		doneOnce.Do(func() { close(done) })
	}()

	wg.Wait()

	// Check strict FIFO ordering
	violations := 0
	for i := 1; i < len(receivedOrder); i++ {
		if receivedOrder[i] <= receivedOrder[i-1] {
			violations++
			t.Errorf("RACE DETECTED: message %d came after %d (position %d)",
				receivedOrder[i], receivedOrder[i-1], i)
		}
	}

	t.Logf("Results: %d messages received in order, %d violations", len(receivedOrder), violations)
	assert.Equal(t, 0, violations, "FIFO race condition - single group ordering violated")
	assert.Equal(t, int64(numMessages), totalReceived, "all messages should be received")
}

// TestFIFORace_BurstPolling_NoDelay tests with burst polling (no delays).
//
// Strategy: Consumers poll as fast as possible with zero delays,
// maximizing the chance of overlapping getInFlightGroups calls.
func TestFIFORace_BurstPolling_NoDelay(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-burst-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-burst")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numGroups = 5
	const messagesPerGroup = 30
	const numConsumers = 15 // More consumers than groups
	const totalMessages = numGroups * messagesPerGroup

	groups := make([]string, numGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("bg%d", i)
	}

	// Push messages
	for _, group := range groups {
		for i := 0; i < messagesPerGroup; i++ {
			_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s|%03d", group, i),
			})
			require.NoError(t, err)
			time.Sleep(time.Microsecond * 10) // Very minimal delay
		}
	}

	groupMessages := make(map[string][]int)
	var groupMu sync.Mutex
	var totalReceived int64

	done := make(chan struct{})
	var doneOnce sync.Once

	// Start all consumers at exactly the same time
	startBarrier := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	t.Logf("Starting %d consumers with synchronized start...", numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func(consumerID int) {
			defer wg.Done()

			// Wait for barrier
			<-startBarrier

			for {
				select {
				case <-done:
					return
				default:
				}

				// ZERO timeout - poll as fast as possible
				msgs, err := fifo.PollMessages(ctx, 0, time.Minute*5, 1, numGroups)
				if err != nil {
					return
				}

				for _, msg := range msgs {
					parts := strings.Split(msg.Body, "|")
					group := parts[0]
					msgNum, _ := strconv.Atoi(parts[1])

					groupMu.Lock()
					groupMessages[group] = append(groupMessages[group], msgNum)
					groupMu.Unlock()

					_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)

					received := atomic.AddInt64(&totalReceived, 1)
					if received >= totalMessages {
						doneOnce.Do(func() { close(done) })
						return
					}
				}
				// NO SLEEP - burst as fast as possible
			}
		}(c)
	}

	// Release all consumers simultaneously
	close(startBarrier)

	go func() {
		time.Sleep(60 * time.Second)
		doneOnce.Do(func() { close(done) })
	}()

	wg.Wait()

	// Check for violations
	violations := 0
	for group, msgs := range groupMessages {
		for i := 1; i < len(msgs); i++ {
			if msgs[i] <= msgs[i-1] {
				violations++
				t.Errorf("BURST RACE in group %s: message %d came after %d",
					group, msgs[i], msgs[i-1])
			}
		}
	}

	t.Logf("Results: %d messages, %d violations", totalReceived, violations)
	assert.Equal(t, 0, violations, "FIFO race condition detected under burst load")
}

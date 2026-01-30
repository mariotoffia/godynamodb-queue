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
// FIFO Race Condition Tests
//
// These tests are designed to expose the TOCTOU (Time-of-Check-Time-of-Use) race
// condition between getInFlightGroups and lockCandidates in the FIFO implementation.
//
// The race occurs when:
// 1. Consumer A checks in-flight groups (group X not blocked)
// 2. Consumer B checks in-flight groups (group X not blocked)
// 3. Consumer A queries visible messages, selects oldest from group X
// 4. Consumer B queries visible messages, selects oldest from group X
// 5. Consumer A locks message - success
// 6. Consumer B lock fails, but timing can cause ordering violations
// ═══════════════════════════════════════════════════════════════════════════════

// TestFIFORace_HighConcurrency_ManyConsumers tests with maximum concurrency.
//
// Strategy: Use many consumers (10+) to maximize the chance of hitting
// the race window between getInFlightGroups and lockCandidates.
func TestFIFORace_HighConcurrency_ManyConsumers(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-high-conc-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
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
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
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
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
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

// TestFIFORace_RapidDeleteAndPoll tests race between delete and next poll.
//
// Strategy: Two concurrent goroutines process messages from the same group,
// testing that FIFO ordering is maintained despite rapid delete/poll cycles.
func TestFIFORace_RapidDeleteAndPoll(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-delpoll-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-delpoll")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numMessages = 50
	const group = "delpoll-group"

	// Push messages with adequate delay to ensure unique timestamps
	for i := 0; i < numMessages; i++ {
		_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
			Body: fmt.Sprintf("%d", i),
		})
		require.NoError(t, err)
		time.Sleep(time.Millisecond) // Use 1ms for reliable timestamp ordering
	}

	receivedOrder := make([]int, 0, numMessages)
	var mu sync.Mutex
	var totalReceived int64

	done := make(chan struct{})
	var doneOnce sync.Once

	// Two consumers racing to poll/delete from same group
	var wg sync.WaitGroup
	wg.Add(2)

	for c := 0; c < 2; c++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				msgs, err := fifo.PollMessages(ctx, time.Millisecond*50, time.Second*5, 1, 1)
				if err != nil {
					return
				}

				if len(msgs) == 0 {
					continue
				}

				msgNum, _ := strconv.Atoi(msgs[0].Body)

				mu.Lock()
				receivedOrder = append(receivedOrder, msgNum)
				mu.Unlock()

				// Delete immediately
				_, _ = fifo.DeleteMessages(ctx, msgs[0].ReceiptHandle)

				if atomic.AddInt64(&totalReceived, 1) >= numMessages {
					doneOnce.Do(func() { close(done) })
					return
				}
			}
		}()
	}

	// Timeout
	go func() {
		time.Sleep(30 * time.Second)
		doneOnce.Do(func() { close(done) })
	}()

	wg.Wait()

	// Check ordering
	violations := 0
	for i := 1; i < len(receivedOrder); i++ {
		if receivedOrder[i] <= receivedOrder[i-1] {
			violations++
			t.Errorf("DELETE/POLL RACE: message %d came after %d",
				receivedOrder[i], receivedOrder[i-1])
		}
	}

	t.Logf("Results: %d messages, %d violations", len(receivedOrder), violations)
	assert.Equal(t, 0, violations, "Race between delete and poll detected")
	assert.Equal(t, int64(numMessages), totalReceived, "all messages received")
}

// TestFIFORace_VerifyDeliveryOrder directly checks if messages are delivered in SK order.
//
// This test isolates the ordering issue by using sequential consumption
// and checking exact delivery order against push order.
func TestFIFORace_VerifyDeliveryOrder(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-order-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-order")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numMessages = 50
	const group = "order-group"

	// Push messages with MINIMAL delay - maximize timestamp proximity
	t.Log("Pushing messages with 50 microsecond delay...")
	for i := 0; i < numMessages; i++ {
		_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
			Body: fmt.Sprintf("%d", i),
		})
		require.NoError(t, err)
		time.Sleep(50 * time.Microsecond) // Very tight - 50,000 nanoseconds
	}

	// Use MANY concurrent consumers to maximize race potential
	receivedOrder := make([]int, 0, numMessages)
	var mu sync.Mutex
	var received int64

	done := make(chan struct{})
	var doneOnce sync.Once

	const numConsumers = 12 // High number of consumers
	var wg sync.WaitGroup
	wg.Add(numConsumers)

	t.Logf("Starting %d aggressive consumers on single group...", numConsumers)
	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				// Zero timeout, poll aggressively
				msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, 1)
				if err != nil {
					return
				}

				for _, msg := range msgs {
					msgNum, _ := strconv.Atoi(msg.Body)

					mu.Lock()
					receivedOrder = append(receivedOrder, msgNum)
					mu.Unlock()

					_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)

					if atomic.AddInt64(&received, 1) >= numMessages {
						doneOnce.Do(func() { close(done) })
						return
					}
				}
				// No sleep - burst poll
			}
		}()
	}

	go func() {
		time.Sleep(30 * time.Second)
		doneOnce.Do(func() { close(done) })
	}()

	wg.Wait()

	// Check for violations
	violations := 0
	for i := 1; i < len(receivedOrder); i++ {
		if receivedOrder[i] <= receivedOrder[i-1] {
			violations++
			t.Errorf("ORDER VIOLATION: message %d came after %d (positions %d, %d)",
				receivedOrder[i], receivedOrder[i-1], i-1, i)
		}
	}

	t.Logf("Received %d messages, %d violations", len(receivedOrder), violations)
	t.Logf("Order: %v", receivedOrder)
	assert.Equal(t, 0, violations, "FIFO delivery order violated")
}

// TestFIFORace_TimestampCollision tests what happens when timestamps might collide.
//
// This test pushes messages with NO delay to maximize timestamp collision chance.
func TestFIFORace_TimestampCollision(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-ts-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-ts")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numMessages = 20
	const group = "ts-group"

	// Push messages with ZERO delay - maximizes timestamp collision chance
	t.Log("Pushing messages with ZERO delay (timestamp collision test)...")
	for i := 0; i < numMessages; i++ {
		_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
			Body: fmt.Sprintf("%d", i),
		})
		require.NoError(t, err)
		// NO SLEEP - push as fast as possible
	}

	// Single consumer to eliminate race condition - just test timestamp ordering
	receivedOrder := make([]int, 0, numMessages)

	for len(receivedOrder) < numMessages {
		msgs, err := fifo.PollMessages(ctx, time.Second, time.Minute, 1, 1)
		require.NoError(t, err)

		for _, msg := range msgs {
			msgNum, _ := strconv.Atoi(msg.Body)
			receivedOrder = append(receivedOrder, msgNum)
			_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)
		}
	}

	// Check for violations
	violations := 0
	for i := 1; i < len(receivedOrder); i++ {
		if receivedOrder[i] <= receivedOrder[i-1] {
			violations++
			t.Errorf("TIMESTAMP COLLISION: message %d came after %d",
				receivedOrder[i], receivedOrder[i-1])
		}
	}

	t.Logf("Single consumer received: %v", receivedOrder)
	t.Logf("Violations: %d", violations)

	// Note: violations here indicate timestamp collision bug
	// (random suffix causing wrong order when timestamps are equal)
	assert.Equal(t, 0, violations, "Timestamp collision caused wrong ordering")
}

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
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
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

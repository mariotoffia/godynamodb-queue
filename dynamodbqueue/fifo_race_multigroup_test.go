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
// FIFO Multi-Group Race Condition Tests
//
// These tests specifically target the race condition that occurs with multiple
// message groups where consumers can interleave their candidate selection and
// locking, causing messages to be delivered out of FIFO order within a group.
//
// Root Cause: The TOCTOU (Time-of-Check-Time-of-Use) gap between:
//   1. getInFlightGroups() - checking which groups are blocked
//   2. collectFifoCandidates() - selecting oldest message per unblocked group
//   3. lockCandidates() - locking selected messages
//
// During step 3, another consumer can:
//   - Check in-flight groups (sees different state)
//   - Select a DIFFERENT candidate from a group
//   - Lock it BEFORE the first consumer locks their candidate for that group
//
// This causes out-of-order delivery within a group.
// ═══════════════════════════════════════════════════════════════════════════════

// TestFIFORace_MultiGroup_Definitive is the definitive test for the multi-group race.
//
// This test is designed to RELIABLY trigger the race condition by:
//   - Using exactly 2 groups (minimal complexity)
//   - Using many consumers (high contention)
//   - Using minimal push delays (tight timestamps)
//   - Polling for multiple groups at once (triggers batch locking)
func TestFIFORace_MultiGroup_Definitive(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-def-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-def")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numGroups = 2     // Just 2 groups - enough to trigger the bug
	const msgsPerGroup = 30 // 30 messages per group
	const numConsumers = 8  // Many consumers for high contention
	const totalMessages = numGroups * msgsPerGroup

	groups := []string{"alpha", "beta"}

	// Push messages with minimal delay
	t.Log("Pushing messages with 50µs delay...")
	for _, group := range groups {
		for i := 0; i < msgsPerGroup; i++ {
			_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s|%02d", group, i),
			})
			require.NoError(t, err)
			time.Sleep(50 * time.Microsecond)
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

	t.Logf("Starting %d consumers polling for 2 groups at once...", numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func(consumerID int) {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
				}

				// Poll for BOTH groups at once - this is key to triggering the bug
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
			}
		}(c)
	}

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
				t.Errorf("MULTI-GROUP RACE in %s: message %d came after %d (position %d)",
					group, msgs[i], msgs[i-1], i)
			}
		}
		t.Logf("Group %s received %d messages: %v", group, len(msgs), msgs)
	}

	t.Logf("Total: %d messages, %d violations", totalReceived, violations)
	assert.Equal(t, 0, violations, "FIFO multi-group race condition detected")
	assert.Equal(t, int64(totalMessages), totalReceived, "all messages should be received")
}

// TestFIFORace_MultiGroup_Repeated runs the multi-group test multiple times.
//
// Races are often intermittent, so we run multiple iterations to increase
// the chance of catching the bug.
func TestFIFORace_MultiGroup_Repeated(t *testing.T) {
	const iterations = 5
	totalViolations := 0

	for iter := 0; iter < iterations; iter++ {
		violations := runMultiGroupRaceIteration(t, iter)
		totalViolations += violations
	}

	t.Logf("Total violations across %d iterations: %d", iterations, totalViolations)
	assert.Equal(t, 0, totalViolations, "FIFO race detected in repeated test")
}

func runMultiGroupRaceIteration(t *testing.T, iteration int) int {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-rep-%d-%s", iteration, dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-rep")

	fifo := queue.(dynamodbqueue.FifoQueue)
	_ = fifo.Purge(ctx)

	const numGroups = 3
	const msgsPerGroup = 20
	const numConsumers = 6
	const totalMessages = numGroups * msgsPerGroup

	groups := []string{"g0", "g1", "g2"}

	// Push
	for _, group := range groups {
		for i := 0; i < msgsPerGroup; i++ {
			_, _ = fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s|%02d", group, i),
			})
			time.Sleep(30 * time.Microsecond)
		}
	}

	groupMessages := make(map[string][]int)
	var groupMu sync.Mutex
	var totalReceived int64

	done := make(chan struct{})
	var doneOnce sync.Once

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, numGroups)
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

					if atomic.AddInt64(&totalReceived, 1) >= totalMessages {
						doneOnce.Do(func() { close(done) })
						return
					}
				}
			}
		}()
	}

	go func() {
		time.Sleep(30 * time.Second)
		doneOnce.Do(func() { close(done) })
	}()

	wg.Wait()

	violations := 0
	for group, msgs := range groupMessages {
		for i := 1; i < len(msgs); i++ {
			if msgs[i] <= msgs[i-1] {
				violations++
				t.Errorf("Iteration %d: RACE in %s: message %d after %d",
					iteration, group, msgs[i], msgs[i-1])
			}
		}
	}

	return violations
}

// TestFIFORace_MultiGroup_SingleMessagePerPoll tests with single message per poll.
//
// This should NOT trigger the bug because only one message is locked at a time.
func TestFIFORace_MultiGroup_SingleMessagePerPoll(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-single-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-single")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numGroups = 3
	const msgsPerGroup = 30
	const numConsumers = 8
	const totalMessages = numGroups * msgsPerGroup

	groups := []string{"s0", "s1", "s2"}

	for _, group := range groups {
		for i := 0; i < msgsPerGroup; i++ {
			_, _ = fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s|%02d", group, i),
			})
			time.Sleep(30 * time.Microsecond)
		}
	}

	groupMessages := make(map[string][]int)
	var groupMu sync.Mutex
	var totalReceived int64

	done := make(chan struct{})
	var doneOnce sync.Once

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				// Poll for only 1 message at a time - should NOT trigger the bug
				msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, 1)
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

					if atomic.AddInt64(&totalReceived, 1) >= totalMessages {
						doneOnce.Do(func() { close(done) })
						return
					}
				}
			}
		}()
	}

	go func() {
		time.Sleep(30 * time.Second)
		doneOnce.Do(func() { close(done) })
	}()

	wg.Wait()

	violations := 0
	for group, msgs := range groupMessages {
		for i := 1; i < len(msgs); i++ {
			if msgs[i] <= msgs[i-1] {
				violations++
				t.Errorf("SINGLE POLL RACE in %s: message %d after %d", group, msgs[i], msgs[i-1])
			}
		}
	}

	t.Logf("Total: %d messages, %d violations", totalReceived, violations)
	// This test should PASS because we poll one message at a time
	assert.Equal(t, 0, violations, "Single message poll should not have race")
}

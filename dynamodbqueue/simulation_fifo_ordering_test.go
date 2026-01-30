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
// FIFO Strict Ordering Simulations
//
// Verifies FIFO ordering guarantees under various conditions.
// ═══════════════════════════════════════════════════════════════════════════════

// TestSimulation_FIFO_3Groups_1000MessagesEach verifies ordering across 3 groups.
//
// Scenario:
// ───────────────────────────────────────────────────────────────────────────────
//   3 message groups, 1000 messages each (3000 total)
//   Verify: Each group maintains strict FIFO order
//   Verify: Groups can be processed in parallel
// ───────────────────────────────────────────────────────────────────────────────
func TestSimulation_FIFO_3Groups_1000MessagesEach(t *testing.T) {
	runFIFOOrderingSimulation(t, 3, 1000)
}

// TestSimulation_FIFO_5Groups_500MessagesEach verifies ordering across 5 groups.
func TestSimulation_FIFO_5Groups_500MessagesEach(t *testing.T) {
	runFIFOOrderingSimulation(t, 5, 500)
}

// TestSimulation_FIFO_10Groups_300MessagesEach verifies ordering across 10 groups.
func TestSimulation_FIFO_10Groups_300MessagesEach(t *testing.T) {
	runFIFOOrderingSimulation(t, 10, 300)
}

func runFIFOOrderingSimulation(t *testing.T, numGroups, messagesPerGroup int) {
	ctx := context.Background()

	queueName := fmt.Sprintf("sim-fifo-order-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("fifo-sim")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	// Create groups
	groups := make([]string, numGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("group-%d", i)
	}

	// PHASE 1: Push messages to each group in order
	t.Logf("Phase 1: Pushing %d messages to %d groups...", numGroups*messagesPerGroup, numGroups)

	for _, group := range groups {
		for i := 0; i < messagesPerGroup; i++ {
			_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s|%04d", group, i),
			})
			require.NoError(t, err)
			// Small sleep to ensure timestamp ordering
			time.Sleep(time.Millisecond)
		}
		t.Logf("  Group %s: pushed %d messages", group, messagesPerGroup)
	}

	// PHASE 2: Consume and verify ordering
	t.Log("Phase 2: Consuming and verifying FIFO order...")

	// Track next expected message number for each group
	groupNextExpected := make(map[string]int)
	for _, group := range groups {
		groupNextExpected[group] = 0
	}

	totalReceived := 0
	orderViolations := 0
	totalMessages := numGroups * messagesPerGroup

	for totalReceived < totalMessages {
		msgs, err := fifo.PollMessages(ctx, time.Second, time.Minute*5, 1, numGroups)
		require.NoError(t, err)

		if len(msgs) == 0 {
			remaining, _ := fifo.Count(ctx)
			if remaining == 0 {
				break
			}
			continue
		}

		for _, msg := range msgs {
			// Parse body: "group-N|NNNN"
			parts := strings.Split(msg.Body, "|")
			require.Len(t, parts, 2, "message body format incorrect: %s", msg.Body)

			group := parts[0]
			msgNum, err := strconv.Atoi(parts[1])
			require.NoError(t, err)

			expectedNum := groupNextExpected[group]
			if msgNum != expectedNum {
				orderViolations++
				t.Errorf("ORDERING VIOLATION: Group %s expected msg %d, got %d",
					group, expectedNum, msgNum)
			}

			groupNextExpected[group] = msgNum + 1
			totalReceived++

			_, err = fifo.DeleteMessages(ctx, msg.ReceiptHandle)
			require.NoError(t, err)
		}
	}

	t.Logf("Completed: received %d messages, %d order violations", totalReceived, orderViolations)

	// ASSERTIONS
	assert.Equal(t, 0, orderViolations, "should have no FIFO ordering violations")
	assert.Equal(t, totalMessages, totalReceived, "all messages should be consumed")

	for _, group := range groups {
		assert.Equal(t, messagesPerGroup, groupNextExpected[group],
			"group %s should have consumed all %d messages", group, messagesPerGroup)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// FIFO Group Blocking Simulations
//
// Verifies that one slow group doesn't block others.
// ═══════════════════════════════════════════════════════════════════════════════

// TestSimulation_FIFO_SlowGroupNoBlock verifies slow group doesn't block others.
//
// Scenario:
// ───────────────────────────────────────────────────────────────────────────────
//   3 groups: A, B, C
//   Group A: Poll first message but DON'T delete (simulate slow processing)
//   Groups B, C: Should still be able to process their messages
// ───────────────────────────────────────────────────────────────────────────────
func TestSimulation_FIFO_SlowGroupNoBlock(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("sim-fifo-noblock-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("noblock-sim")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	// Push messages to 3 groups
	groups := []string{"group-A", "group-B", "group-C"}
	messagesPerGroup := 10

	for _, group := range groups {
		for i := 0; i < messagesPerGroup; i++ {
			_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s-msg-%d", group, i),
			})
			require.NoError(t, err)
			time.Sleep(time.Millisecond)
		}
	}

	// Poll and HOLD message from group-A (don't delete)
	msgsA, err := fifo.PollMessages(ctx, 0, time.Minute*5, 1, 1)
	require.NoError(t, err)
	require.Len(t, msgsA, 1)
	assert.Contains(t, msgsA[0].Body, "group-A", "first poll should get group-A message")
	t.Logf("Holding message: %s", msgsA[0].Body)

	// Now poll more - should get messages from B and C, NOT more from A
	groupBReceived := 0
	groupCReceived := 0
	groupAReceived := 0

	for i := 0; i < 20; i++ { // Multiple polls
		msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, 3)
		require.NoError(t, err)

		for _, msg := range msgs {
			if strings.Contains(msg.Body, "group-A") {
				groupAReceived++
			} else if strings.Contains(msg.Body, "group-B") {
				groupBReceived++
				_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)
			} else if strings.Contains(msg.Body, "group-C") {
				groupCReceived++
				_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)
			}
		}

		if len(msgs) == 0 {
			break
		}
	}

	t.Logf("Received - A: %d, B: %d, C: %d", groupAReceived, groupBReceived, groupCReceived)

	// Group A should be blocked (only 1 message held, no more received)
	assert.Equal(t, 0, groupAReceived, "group-A should be blocked")

	// Groups B and C should have processed all their messages
	assert.Equal(t, messagesPerGroup, groupBReceived, "group-B should process all messages")
	assert.Equal(t, messagesPerGroup, groupCReceived, "group-C should process all messages")

	// Now delete the held message and verify group-A unblocks
	_, err = fifo.DeleteMessages(ctx, msgsA[0].ReceiptHandle)
	require.NoError(t, err)

	// Poll remaining group-A messages
	for {
		msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, 10)
		require.NoError(t, err)
		if len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			if strings.Contains(msg.Body, "group-A") {
				groupAReceived++
			}
			_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)
		}
	}

	// Group A should now have processed remaining messages
	assert.Equal(t, messagesPerGroup-1, groupAReceived,
		"group-A should process remaining messages after unblock")
}

// ═══════════════════════════════════════════════════════════════════════════════
// FIFO In-Flight Tracking Accuracy Simulations
//
// Verifies in-flight group tracking is accurate at scale.
// ═══════════════════════════════════════════════════════════════════════════════

// TestSimulation_FIFO_InFlightTracking_100Groups verifies in-flight tracking with 100 groups.
//
// Scenario:
// ───────────────────────────────────────────────────────────────────────────────
//   100 groups, 20 messages each (2000 total)
//   Poll all first messages (100 in-flight)
//   Verify: No second message from any group until first is deleted
// ───────────────────────────────────────────────────────────────────────────────
func TestSimulation_FIFO_InFlightTracking_100Groups(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("sim-fifo-inflight-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("inflight-sim")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numGroups = 100
	const messagesPerGroup = 20

	// Create groups and push messages
	groups := make([]string, numGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("g%02d", i)
	}

	for _, group := range groups {
		for i := 0; i < messagesPerGroup; i++ {
			_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s|%d", group, i),
			})
			require.NoError(t, err)
		}
	}

	// Poll first message from all groups (should get 50 messages)
	firstMessages := make(map[string]events.SQSMessage)
	groupsWithInFlight := make(map[string]bool)

	for len(firstMessages) < numGroups {
		msgs, err := fifo.PollMessages(ctx, time.Second, time.Minute*5, 1, numGroups)
		require.NoError(t, err)

		for _, msg := range msgs {
			parts := strings.Split(msg.Body, "|")
			group := parts[0]
			msgNum := parts[1]

			if msgNum == "0" {
				firstMessages[group] = msg
				groupsWithInFlight[group] = true
			} else {
				t.Errorf("Received non-first message %s while first was in-flight", msg.Body)
			}
		}

		if len(msgs) == 0 {
			break
		}
	}

	assert.Equal(t, numGroups, len(firstMessages),
		"should have polled first message from all %d groups", numGroups)

	// Try to poll more - should get NOTHING (all groups blocked)
	extraMsgs, err := fifo.PollMessages(ctx, 0, time.Minute, 0, numGroups)
	require.NoError(t, err)
	assert.Len(t, extraMsgs, 0, "should get no messages while all groups have in-flight")

	// Delete half the first messages
	releasedCount := 0
	for group, msg := range firstMessages {
		if releasedCount >= numGroups/2 {
			break
		}
		_, err := fifo.DeleteMessages(ctx, msg.ReceiptHandle)
		require.NoError(t, err)
		delete(groupsWithInFlight, group)
		releasedCount++
	}

	// Poll again - should now get messages from released groups
	moreMsgs, err := fifo.PollMessages(ctx, time.Second, time.Minute, 1, numGroups)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(moreMsgs), 1,
		"should get at least 1 message after releasing some groups")

	// Verify all received are second messages (index 1) from released groups
	for _, msg := range moreMsgs {
		parts := strings.Split(msg.Body, "|")
		group := parts[0]
		assert.False(t, groupsWithInFlight[group],
			"should not receive from group %s which still has in-flight", group)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// FIFO Concurrent Consumer Simulations
// ═══════════════════════════════════════════════════════════════════════════════

// TestSimulation_FIFO_ConcurrentConsumers_NoOrderViolation verifies concurrent consumers.
//
// Scenario:
// ───────────────────────────────────────────────────────────────────────────────
//   5 groups, 100 messages each (500 total)
//   3 concurrent consumers
//   Verify: Each group still maintains strict FIFO order
//   Note: FIFO guarantees only ONE message in-flight per group at a time,
//         so messages within a group must be processed sequentially.
// ───────────────────────────────────────────────────────────────────────────────
func TestSimulation_FIFO_ConcurrentConsumers_NoOrderViolation(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("sim-fifo-conc-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("fifo-conc")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numGroups = 5
	const messagesPerGroup = 100
	const numConsumers = 3
	const totalMessages = numGroups * messagesPerGroup

	// Create groups
	groups := make([]string, numGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("cg%d", i)
	}

	// Push messages with sufficient delay to ensure timestamp ordering
	// The SK uses nanosecond timestamp, so 10ms delay ensures distinct timestamps
	t.Logf("Pushing %d messages to %d groups...", totalMessages, numGroups)
	for _, group := range groups {
		for i := 0; i < messagesPerGroup; i++ {
			_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s|%03d", group, i),
			})
			require.NoError(t, err)
			// Use 10ms delay to ensure distinct timestamps under any system load
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Track ALL received messages per group to verify order post-hoc
	groupMessages := make(map[string][]int)
	var groupMu sync.Mutex
	var totalReceived int64

	// Done channel to signal consumers to stop
	done := make(chan struct{})
	var doneOnce sync.Once

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	t.Logf("Starting %d consumers...", numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func(consumerID int) {
			defer wg.Done()

			emptyPolls := 0
			for {
				select {
				case <-done:
					return
				default:
				}

				msgs, err := fifo.PollMessages(ctx, time.Millisecond*200, time.Minute*5, 1, numGroups)
				if err != nil {
					return
				}

				if len(msgs) == 0 {
					emptyPolls++
					// With FIFO, all groups might be temporarily blocked
					if emptyPolls > 20 && atomic.LoadInt64(&totalReceived) >= totalMessages {
						return
					}
					time.Sleep(time.Millisecond * 100)
					continue
				}

				emptyPolls = 0

				for _, msg := range msgs {
					parts := strings.Split(msg.Body, "|")
					group := parts[0]
					msgNum, _ := strconv.Atoi(parts[1])

					// Record message for post-hoc analysis
					groupMu.Lock()
					groupMessages[group] = append(groupMessages[group], msgNum)
					groupMu.Unlock()

					// Delete BEFORE incrementing counter to ensure proper ordering
					_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)

					received := atomic.AddInt64(&totalReceived, 1)

					// Signal done when all messages consumed
					if received >= totalMessages {
						doneOnce.Do(func() { close(done) })
						return
					}
				}
			}
		}(c)
	}

	// Timeout safety
	go func() {
		time.Sleep(3 * time.Minute)
		select {
		case <-done:
		default:
			doneOnce.Do(func() { close(done) })
		}
	}()

	wg.Wait()

	// Post-hoc verification: check ordering within each group
	violations := 0
	for group, msgs := range groupMessages {
		for i := 1; i < len(msgs); i++ {
			if msgs[i] <= msgs[i-1] {
				violations++
				t.Errorf("ORDERING VIOLATION in group %s: message %d came after %d (position %d)",
					group, msgs[i], msgs[i-1], i)
			}
		}
	}

	t.Logf("Completed: %d messages received, %d violations", totalReceived, violations)

	assert.Equal(t, 0, violations, "should have no ordering violations")
	assert.Equal(t, int64(totalMessages), totalReceived, "all messages should be consumed")

	// Verify all messages were received for each group
	for _, group := range groups {
		receivedCount := len(groupMessages[group])
		assert.Equal(t, messagesPerGroup, receivedCount,
			"group %s should have received all %d messages", group, messagesPerGroup)
	}
}

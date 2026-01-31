package dynamodbqueue_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
	"github.com/mariotoffia/godynamodb-queue/testutil"
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
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
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
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
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

	// Poll and HOLD one message (from any group - FIFO is per-group, not cross-group)
	heldMsg, err := fifo.PollMessages(ctx, 0, time.Minute*5, 1, 1)
	require.NoError(t, err)
	require.Len(t, heldMsg, 1)

	// Determine which group the held message belongs to using proper parsing
	heldGroup := testutil.ParseGroupFromBody(heldMsg[0].Body)
	require.NotEmpty(t, heldGroup, "held message should belong to a known group")
	t.Logf("Holding message from %s: %s", heldGroup, heldMsg[0].Body)

	// Track received messages per group
	received := make(map[string]int)
	for _, g := range groups {
		received[g] = 0
	}

	// Poll remaining messages - the held group should be blocked
	for i := 0; i < 30; i++ {
		msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, 3)
		require.NoError(t, err)

		if len(msgs) == 0 {
			break
		}

		for _, msg := range msgs {
			g := testutil.ParseGroupFromBody(msg.Body)
			if g != "" {
				received[g]++
			}
			_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)
		}
	}

	t.Logf("Received while holding %s: A=%d, B=%d, C=%d",
		heldGroup, received["group-A"], received["group-B"], received["group-C"])

	// The held group should be blocked (0 additional messages received)
	assert.Equal(t, 0, received[heldGroup],
		"held group %s should be blocked", heldGroup)

	// Other groups should have processed all their messages
	for _, g := range groups {
		if g != heldGroup {
			assert.Equal(t, messagesPerGroup, received[g],
				"group %s should process all messages", g)
		}
	}

	// Now delete the held message and verify the group unblocks
	_, err = fifo.DeleteMessages(ctx, heldMsg[0].ReceiptHandle)
	require.NoError(t, err)

	// Poll remaining messages from the previously held group
	for {
		msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, 10)
		require.NoError(t, err)
		if len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			g := testutil.ParseGroupFromBody(msg.Body)
			if g == heldGroup {
				received[heldGroup]++
			}
			_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)
		}
	}

	// The previously held group should now have processed remaining messages
	assert.Equal(t, messagesPerGroup-1, received[heldGroup],
		"group %s should process remaining messages after unblock", heldGroup)
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
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
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

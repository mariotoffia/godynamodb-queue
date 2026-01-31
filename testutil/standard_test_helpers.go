package testutil

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/events"
)

// StandardTestQueue is an interface that matches the standard queue operations needed for testing.
type StandardTestQueue interface {
	PushMessages(ctx context.Context, ttl time.Duration, messages ...events.SQSMessage) ([]events.SQSMessage, error)
	PollMessages(ctx context.Context, timeout, visibilityTimeout time.Duration, minMessages, maxMessages int) ([]events.SQSMessage, error)
	DeleteMessages(ctx context.Context, receiptHandles ...string) ([]string, error)
	Count(ctx context.Context) (int32, error)
	Purge(ctx context.Context) error
}

// StandardTestHelper provides utilities for testing standard (non-FIFO) queues.
type StandardTestHelper struct {
	Queue             StandardTestQueue
	VisibilityTimeout time.Duration
	PollTimeout       time.Duration
}

// NewStandardTestHelper creates a new standard queue test helper.
func NewStandardTestHelper(queue StandardTestQueue) *StandardTestHelper {
	return &StandardTestHelper{
		Queue:             queue,
		VisibilityTimeout: time.Minute,
		PollTimeout:       100 * time.Millisecond,
	}
}

// WithVisibilityTimeout sets the default visibility timeout for polling.
func (h *StandardTestHelper) WithVisibilityTimeout(d time.Duration) *StandardTestHelper {
	h.VisibilityTimeout = d
	return h
}

// WithPollTimeout sets the default poll timeout.
func (h *StandardTestHelper) WithPollTimeout(d time.Duration) *StandardTestHelper {
	h.PollTimeout = d
	return h
}

// WaitForVisibilityExpiry waits for messages to become visible again using retry polling.
// This replaces hardcoded time.Sleep calls with a more reliable retry-based approach.
// Returns the messages that became visible, or an error if timeout is exceeded.
func (h *StandardTestHelper) WaitForVisibilityExpiry(
	ctx context.Context,
	expectedCount int,
	maxWait time.Duration,
) ([]events.SQSMessage, error) {
	deadline := time.Now().Add(maxWait)
	var allMsgs []events.SQSMessage

	for time.Now().Before(deadline) {
		polled, err := h.Queue.PollMessages(ctx, h.PollTimeout, h.VisibilityTimeout, 1, expectedCount)
		if err != nil {
			return nil, fmt.Errorf("poll error while waiting for visibility expiry: %w", err)
		}

		if len(polled) >= expectedCount {
			return polled, nil
		}

		if len(polled) > 0 {
			allMsgs = append(allMsgs, polled...)
			if len(allMsgs) >= expectedCount {
				return allMsgs, nil
			}
		}

		// Short sleep between retries
		time.Sleep(100 * time.Millisecond)
	}

	return allMsgs, fmt.Errorf("timed out waiting for %d messages to become visible, got %d", expectedCount, len(allMsgs))
}

// WaitForMessages waits for at least minCount messages to be available in the queue.
// This is useful for waiting for visibility timeouts without specifying exact counts.
func (h *StandardTestHelper) WaitForMessages(
	ctx context.Context,
	minCount int,
	maxWait time.Duration,
	visibilityTimeout time.Duration,
) ([]events.SQSMessage, error) {
	deadline := time.Now().Add(maxWait)
	var allMsgs []events.SQSMessage

	for time.Now().Before(deadline) {
		polled, err := h.Queue.PollMessages(ctx, h.PollTimeout, visibilityTimeout, 1, minCount*2)
		if err != nil {
			return nil, fmt.Errorf("poll error while waiting for messages: %w", err)
		}

		if len(polled) > 0 {
			allMsgs = append(allMsgs, polled...)
			if len(allMsgs) >= minCount {
				return allMsgs, nil
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	return allMsgs, fmt.Errorf("timed out waiting for %d messages, got %d", minCount, len(allMsgs))
}

// ConsumeAll consumes all available messages from the queue.
// Returns the messages consumed and any error encountered.
func (h *StandardTestHelper) ConsumeAll(ctx context.Context, maxMessages int) ([]events.SQSMessage, error) {
	var allMsgs []events.SQSMessage
	emptyPolls := 0
	const maxEmptyPolls = 10

	for len(allMsgs) < maxMessages && emptyPolls < maxEmptyPolls {
		msgs, err := h.Queue.PollMessages(ctx, h.PollTimeout, h.VisibilityTimeout, 1, 10)
		if err != nil {
			return allMsgs, err
		}

		if len(msgs) == 0 {
			emptyPolls++
			continue
		}
		emptyPolls = 0

		for i := range msgs {
			_, err := h.Queue.DeleteMessages(ctx, msgs[i].ReceiptHandle)
			if err != nil {
				return allMsgs, err
			}
			allMsgs = append(allMsgs, msgs[i])
		}
	}

	return allMsgs, nil
}

// ProcessWithRetry processes messages, simulating failures that trigger redelivery.
// failFunc determines whether a message should "fail" (not be deleted).
// Returns successfully processed messages, failed messages that need redelivery, and any error.
func (h *StandardTestHelper) ProcessWithRetry(
	ctx context.Context,
	maxMessages int,
	visibilityTimeout time.Duration,
	failFunc func(msg events.SQSMessage) bool,
) (processed, failed []events.SQSMessage, err error) {
	emptyPolls := 0
	const maxEmptyPolls = 5

	totalProcessed := 0
	for totalProcessed < maxMessages && emptyPolls < maxEmptyPolls {
		msgs, pollErr := h.Queue.PollMessages(ctx, h.PollTimeout, visibilityTimeout, 1, 10)
		if pollErr != nil {
			return processed, failed, pollErr
		}

		if len(msgs) == 0 {
			emptyPolls++
			continue
		}
		emptyPolls = 0

		for i := range msgs {
			totalProcessed++
			if failFunc(msgs[i]) {
				// Simulate failure - don't delete
				failed = append(failed, msgs[i])
			} else {
				// Success - delete
				_, delErr := h.Queue.DeleteMessages(ctx, msgs[i].ReceiptHandle)
				if delErr != nil {
					return processed, failed, delErr
				}
				processed = append(processed, msgs[i])
			}
		}
	}

	return processed, failed, nil
}

// AssertQueueEmpty verifies the queue has no messages.
func (h *StandardTestHelper) AssertQueueEmpty(ctx context.Context) error {
	count, err := h.Queue.Count(ctx)
	if err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("expected queue to be empty, but has %d messages", count)
	}
	return nil
}

// AssertQueueCount verifies the queue has the expected number of messages.
func (h *StandardTestHelper) AssertQueueCount(ctx context.Context, expected int32) error {
	count, err := h.Queue.Count(ctx)
	if err != nil {
		return err
	}
	if count != expected {
		return fmt.Errorf("expected queue to have %d messages, but has %d", expected, count)
	}
	return nil
}

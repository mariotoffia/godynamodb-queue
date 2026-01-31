package testutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-lambda-go/events"
)

// FIFOTestQueue is an interface that matches the FIFO queue operations needed for testing.
type FIFOTestQueue interface {
	PushMessagesWithGroup(ctx context.Context, ttl time.Duration, messageGroup string, messages ...events.SQSMessage) ([]events.SQSMessage, error)
	PollMessages(ctx context.Context, timeout, visibilityTimeout time.Duration, minMessages, maxMessages int) ([]events.SQSMessage, error)
	DeleteMessages(ctx context.Context, receiptHandles ...string) ([]string, error)
	Count(ctx context.Context) (int32, error)
	Purge(ctx context.Context) error
}

// MessageBodyFormat defines how message bodies are formatted for testing.
type MessageBodyFormat int

const (
	// FormatGroupMsgNum formats as "group-name|msgNum" (e.g., "group-A|0")
	FormatGroupMsgNum MessageBodyFormat = iota
	// FormatGroupDashMsgNum formats as "group-name-msg-msgNum" (e.g., "group-A-msg-0")
	FormatGroupDashMsgNum
)

// FIFOTestHelper provides utilities for testing FIFO queues.
type FIFOTestHelper struct {
	Queue             FIFOTestQueue
	groupMessages     map[string][]int  // group -> ordered list of message numbers received
	duplicates        map[string]int    // messageID -> count
	processedBodies   map[string]bool
	VisibilityTimeout time.Duration
	PollTimeout       time.Duration
	mu                sync.Mutex
	totalReceived     int64
	violations        int
	Format            MessageBodyFormat
}

// NewFIFOTestHelper creates a new FIFO test helper.
func NewFIFOTestHelper(queue FIFOTestQueue) *FIFOTestHelper {
	return &FIFOTestHelper{
		Queue:             queue,
		Format:            FormatGroupMsgNum,
		VisibilityTimeout: time.Minute,
		PollTimeout:       100 * time.Millisecond,
		groupMessages:     make(map[string][]int),
		duplicates:        make(map[string]int),
		processedBodies:   make(map[string]bool),
	}
}

// WithFormat sets the message body format.
func (h *FIFOTestHelper) WithFormat(format MessageBodyFormat) *FIFOTestHelper {
	h.Format = format
	return h
}

// WithVisibilityTimeout sets the visibility timeout for polling.
func (h *FIFOTestHelper) WithVisibilityTimeout(d time.Duration) *FIFOTestHelper {
	h.VisibilityTimeout = d
	return h
}

// WithPollTimeout sets the poll timeout.
func (h *FIFOTestHelper) WithPollTimeout(d time.Duration) *FIFOTestHelper {
	h.PollTimeout = d
	return h
}

// Reset clears all tracking state.
func (h *FIFOTestHelper) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.groupMessages = make(map[string][]int)
	h.violations = 0
	h.totalReceived = 0
	h.duplicates = make(map[string]int)
	h.processedBodies = make(map[string]bool)
}

// PushMessages pushes messages to a group with the configured format.
func (h *FIFOTestHelper) PushMessages(ctx context.Context, group string, count int) error {
	return h.PushMessagesWithDelay(ctx, group, count, 0)
}

// PushMessagesWithDelay pushes messages to a group with a delay between each.
func (h *FIFOTestHelper) PushMessagesWithDelay(ctx context.Context, group string, count int, delay time.Duration) error {
	for i := 0; i < count; i++ {
		body := h.formatBody(group, i)
		_, err := h.Queue.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{Body: body})
		if err != nil {
			return fmt.Errorf("failed to push message %d to group %s: %w", i, group, err)
		}
		if delay > 0 && i < count-1 {
			time.Sleep(delay)
		}
	}
	return nil
}

// PushToGroups pushes messages to multiple groups.
func (h *FIFOTestHelper) PushToGroups(ctx context.Context, groups []string, messagesPerGroup int, delay time.Duration) error {
	for _, group := range groups {
		if err := h.PushMessagesWithDelay(ctx, group, messagesPerGroup, delay); err != nil {
			return err
		}
	}
	return nil
}

// ConsumeAll consumes all messages from the queue and tracks ordering.
// Returns the total number of messages consumed.
func (h *FIFOTestHelper) ConsumeAll(ctx context.Context, maxMessages int) (int, error) {
	return h.ConsumeWithCallback(ctx, maxMessages, nil)
}

// ConsumeWithCallback consumes messages and calls the callback for each.
// If callback returns false, the message is NOT deleted (simulating processing failure).
func (h *FIFOTestHelper) ConsumeWithCallback(ctx context.Context, maxMessages int, callback func(msg events.SQSMessage) bool) (int, error) {
	consumed := 0
	emptyPolls := 0
	const maxEmptyPolls = 10

	for consumed < maxMessages && emptyPolls < maxEmptyPolls {
		msgs, err := h.Queue.PollMessages(ctx, h.PollTimeout, h.VisibilityTimeout, 1, 10)
		if err != nil {
			return consumed, err
		}

		if len(msgs) == 0 {
			emptyPolls++
			continue
		}
		emptyPolls = 0

		for i := range msgs {
			shouldDelete := true
			if callback != nil {
				shouldDelete = callback(msgs[i])
			}

			h.trackMessage(&msgs[i])

			if shouldDelete {
				_, err := h.Queue.DeleteMessages(ctx, msgs[i].ReceiptHandle)
				if err != nil {
					return consumed, err
				}
			}
			consumed++
		}
	}

	return consumed, nil
}

// ConsumeAllConcurrently consumes messages using multiple concurrent consumers.
func (h *FIFOTestHelper) ConsumeAllConcurrently(ctx context.Context, numConsumers, totalMessages int, timeout time.Duration) (int, error) {
	// Use context.WithTimeout to avoid goroutine leaks
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	errChan := make(chan error, numConsumers)

	for range numConsumers {
		go h.concurrentConsumerWorker(ctx, &wg, errChan, totalMessages)
	}

	wg.Wait()

	select {
	case err := <-errChan:
		return int(atomic.LoadInt64(&h.totalReceived)), err
	default:
		return int(atomic.LoadInt64(&h.totalReceived)), nil
	}
}

// concurrentConsumerWorker is a worker goroutine for concurrent consumption.
func (h *FIFOTestHelper) concurrentConsumerWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	errChan chan<- error,
	totalMessages int,
) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if atomic.LoadInt64(&h.totalReceived) >= int64(totalMessages) {
			return
		}

		msgs, err := h.Queue.PollMessages(ctx, h.PollTimeout, h.VisibilityTimeout, 1, 10)
		if err != nil {
			// Context cancellation errors are expected, don't report them
			if ctx.Err() != nil {
				return
			}
			select {
			case errChan <- err:
			default:
			}
			return
		}

		if len(msgs) == 0 {
			continue
		}

		h.processMessages(ctx, msgs)
	}
}

// processMessages processes a batch of messages (track and delete).
func (h *FIFOTestHelper) processMessages(ctx context.Context, msgs []events.SQSMessage) {
	for i := range msgs {
		h.trackMessage(&msgs[i])
		//nolint:errcheck // Ignore delete errors in concurrent consumer - best effort cleanup
		_, _ = h.Queue.DeleteMessages(ctx, msgs[i].ReceiptHandle)
	}
}

// WaitForVisibilityExpiry waits for messages to become visible again using retry polling.
func (h *FIFOTestHelper) WaitForVisibilityExpiry(ctx context.Context, expectedCount int, maxWait time.Duration) ([]events.SQSMessage, error) {
	deadline := time.Now().Add(maxWait)
	var msgs []events.SQSMessage

	for time.Now().Before(deadline) {
		polled, err := h.Queue.PollMessages(ctx, h.PollTimeout, h.VisibilityTimeout, 1, expectedCount)
		if err != nil {
			return nil, err
		}
		if len(polled) >= expectedCount {
			return polled, nil
		}
		if len(polled) > 0 {
			msgs = append(msgs, polled...)
			if len(msgs) >= expectedCount {
				return msgs, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return msgs, fmt.Errorf("timed out waiting for %d messages, got %d", expectedCount, len(msgs))
}

// trackMessage records a message and checks for ordering violations.
func (h *FIFOTestHelper) trackMessage(msg *events.SQSMessage) {
	group, msgNum, err := h.parseBody(msg.Body)
	if err != nil {
		return // Skip unparseable messages
	}

	// Atomic increment outside mutex to avoid mixed synchronization
	atomic.AddInt64(&h.totalReceived, 1)

	h.mu.Lock()
	defer h.mu.Unlock()

	// Track duplicates by message ID
	h.duplicates[msg.MessageId]++

	// Track body duplicates
	if h.processedBodies[msg.Body] {
		// Already processed this body - duplicate
		return
	}
	h.processedBodies[msg.Body] = true

	// Track per-group ordering
	msgs := h.groupMessages[group]
	if len(msgs) > 0 {
		lastNum := msgs[len(msgs)-1]
		if msgNum <= lastNum {
			h.violations++
		}
	}
	h.groupMessages[group] = append(msgs, msgNum)
}

// Violations returns the number of ordering violations detected.
func (h *FIFOTestHelper) Violations() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.violations
}

// TotalReceived returns the total number of messages received.
func (h *FIFOTestHelper) TotalReceived() int {
	return int(atomic.LoadInt64(&h.totalReceived))
}

// GroupMessages returns the messages received per group (in order).
func (h *FIFOTestHelper) GroupMessages() map[string][]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make(map[string][]int)
	for k, v := range h.groupMessages {
		cp := make([]int, len(v))
		copy(cp, v)
		result[k] = cp
	}
	return result
}

// Duplicates returns message IDs that were received more than once.
func (h *FIFOTestHelper) Duplicates() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make(map[string]int)
	for k, v := range h.duplicates {
		if v > 1 {
			result[k] = v
		}
	}
	return result
}

// VerifyComplete checks that all expected messages were received in order.
func (h *FIFOTestHelper) VerifyComplete(groups []string, messagesPerGroup int) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, group := range groups {
		msgs := h.groupMessages[group]
		if len(msgs) != messagesPerGroup {
			return fmt.Errorf("group %s: expected %d messages, got %d", group, messagesPerGroup, len(msgs))
		}

		for i, msgNum := range msgs {
			if msgNum != i {
				return fmt.Errorf("group %s: expected message %d at position %d, got %d", group, i, i, msgNum)
			}
		}
	}

	if h.violations > 0 {
		return fmt.Errorf("detected %d ordering violations", h.violations)
	}

	return nil
}

// VerifyOrdering checks that messages within each group are in order.
func (h *FIFOTestHelper) VerifyOrdering() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.violations > 0 {
		return fmt.Errorf("detected %d ordering violations", h.violations)
	}
	return nil
}

// VerifyNoDuplicates checks that no messages were received more than once.
func (h *FIFOTestHelper) VerifyNoDuplicates() error {
	dups := h.Duplicates()
	if len(dups) > 0 {
		return fmt.Errorf("detected %d duplicate messages", len(dups))
	}
	return nil
}

// formatBody creates a message body in the configured format.
func (h *FIFOTestHelper) formatBody(group string, msgNum int) string {
	switch h.Format {
	case FormatGroupDashMsgNum:
		return fmt.Sprintf("%s-msg-%d", group, msgNum)
	default:
		return fmt.Sprintf("%s|%d", group, msgNum)
	}
}

// ParseGroupFromBody extracts the group name from a message body.
// Supports two formats:
//   - "group-name-msg-N" -> returns "group-name"
//   - "group-name|N" -> returns "group-name"
func ParseGroupFromBody(body string) string {
	// Try "-msg-" format first (e.g., "group-A-msg-0")
	if idx := strings.LastIndex(body, "-msg-"); idx != -1 {
		return body[:idx]
	}
	// Try "|" format (e.g., "group|0")
	if group, _, found := strings.Cut(body, "|"); found {
		return group
	}
	return ""
}

// parseBody extracts group and message number from a body.
func (h *FIFOTestHelper) parseBody(body string) (group string, msgNum int, err error) {
	switch h.Format {
	case FormatGroupDashMsgNum:
		// Format: "group-name-msg-N"
		idx := strings.LastIndex(body, "-msg-")
		if idx == -1 {
			return "", 0, fmt.Errorf("invalid body format: %s", body)
		}
		group = body[:idx]
		msgNum, err = strconv.Atoi(body[idx+5:])
		if err != nil {
			return "", 0, fmt.Errorf("invalid message number in body: %s", body)
		}
		return group, msgNum, nil
	default:
		// Format: "group|N"
		parts := strings.Split(body, "|")
		if len(parts) != 2 {
			return "", 0, fmt.Errorf("invalid body format: %s", body)
		}
		group = parts[0]
		msgNum, err = strconv.Atoi(parts[1])
		if err != nil {
			return "", 0, fmt.Errorf("invalid message number in body: %s", body)
		}
		return group, msgNum, nil
	}
}

// AssertQueueEmpty verifies the queue has no messages.
func (h *FIFOTestHelper) AssertQueueEmpty(ctx context.Context) error {
	count, err := h.Queue.Count(ctx)
	if err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("expected queue to be empty, but has %d messages", count)
	}
	return nil
}

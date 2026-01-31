package dynamodbqueue_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Push Throughput Benchmarks
//
// Measures message push performance at various batch sizes.
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkPush_SingleMessage measures single message push latency.
func BenchmarkPush_SingleMessage(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	msg := events.SQSMessage{Body: "benchmark-single"}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := queue.PushMessages(ctx, 0, msg)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	cleanupBenchmarkQueue(b, queue)
}

// BenchmarkPush_BatchSize5 measures push throughput with 5-message batches.
func BenchmarkPush_BatchSize5(b *testing.B) {
	benchmarkPushBatch(b, 5)
}

// BenchmarkPush_BatchSize10 measures push throughput with 10-message batches.
func BenchmarkPush_BatchSize10(b *testing.B) {
	benchmarkPushBatch(b, 10)
}

// BenchmarkPush_BatchSize25 measures push throughput at max batch size.
func BenchmarkPush_BatchSize25(b *testing.B) {
	benchmarkPushBatch(b, 25)
}

func benchmarkPushBatch(b *testing.B, batchSize int) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	// Pre-create messages
	messages := make([]events.SQSMessage, batchSize)
	for i := range messages {
		messages[i] = events.SQSMessage{Body: fmt.Sprintf("bench-%d", i)}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := queue.PushMessages(ctx, 0, messages...)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()

	totalMessages := b.N * batchSize
	b.ReportMetric(float64(totalMessages)/b.Elapsed().Seconds(), "msgs/sec")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Poll Throughput Benchmarks
//
// Measures message poll performance at various queue depths.
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkPoll_EmptyQueue measures poll latency when queue is empty.
func BenchmarkPoll_EmptyQueue(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := queue.PollMessages(ctx, 0, time.Minute, 0, 10)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	cleanupBenchmarkQueue(b, queue)
}

// BenchmarkPoll_QueueDepth500 measures poll latency with 500 queued messages.
func BenchmarkPoll_QueueDepth500(b *testing.B) {
	benchmarkPollAtDepth(b, 500, 25)
}

// BenchmarkPoll_QueueDepth2000 measures poll latency with 2000 queued messages.
func BenchmarkPoll_QueueDepth2000(b *testing.B) {
	benchmarkPollAtDepth(b, 2000, 25)
}

// BenchmarkPoll_QueueDepth10000 measures poll latency with 10000 queued messages.
func BenchmarkPoll_QueueDepth10000(b *testing.B) {
	benchmarkPollAtDepth(b, 10000, 25)
}

func benchmarkPollAtDepth(b *testing.B, depth, pollSize int) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	// Pre-populate queue
	populateQueue(b, queue, depth)

	b.ResetTimer()
	b.ReportAllocs()

	totalPolled := 0
	for range b.N {
		msgs, err := queue.PollMessages(ctx, 0, time.Minute, pollSize, pollSize)
		if err != nil {
			b.Fatal(err)
		}
		totalPolled += len(msgs)

		// Delete to allow re-poll
		if len(msgs) > 0 {
			handles := dynamodbqueue.ToReceiptHandles(msgs)
			_, _ = queue.DeleteMessages(ctx, handles...)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(totalPolled)/b.Elapsed().Seconds(), "msgs/sec")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Delete Throughput Benchmarks
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkDelete_SingleMessage measures single message delete latency.
func BenchmarkDelete_SingleMessage(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	b.ResetTimer()

	for range b.N {
		b.StopTimer()
		// Push a message
		_, err := queue.PushMessages(ctx, 0, events.SQSMessage{Body: "delete-test"})
		if err != nil {
			b.Fatal(err)
		}
		// Poll to get receipt handle
		msgs, err := queue.PollMessages(ctx, 0, time.Minute, 1, 1)
		if err != nil || len(msgs) == 0 {
			b.Fatal("failed to poll message")
		}
		b.StartTimer()

		// Measure delete
		_, err = queue.DeleteMessages(ctx, msgs[0].ReceiptHandle)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	cleanupBenchmarkQueue(b, queue)
}

// BenchmarkDelete_Batch25 measures batch delete throughput.
func BenchmarkDelete_Batch25(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	b.ResetTimer()

	for range b.N {
		b.StopTimer()
		// Push 25 messages
		messages := make([]events.SQSMessage, 25)
		for j := range messages {
			messages[j] = events.SQSMessage{Body: fmt.Sprintf("delete-batch-%d", j)}
		}
		_, err := queue.PushMessages(ctx, 0, messages...)
		if err != nil {
			b.Fatal(err)
		}
		// Poll all
		msgs, err := queue.PollMessages(ctx, time.Second, time.Minute, 25, 25)
		if err != nil {
			b.Fatal(err)
		}
		handles := dynamodbqueue.ToReceiptHandles(msgs)
		b.StartTimer()

		// Measure delete
		_, err = queue.DeleteMessages(ctx, handles...)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N*25)/b.Elapsed().Seconds(), "msgs/sec")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// End-to-End Cycle Benchmarks
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkEndToEnd_SingleCycle measures complete push→poll→delete cycle.
func BenchmarkEndToEnd_SingleCycle(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	msg := events.SQSMessage{Body: "e2e-cycle"}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		// Push
		_, err := queue.PushMessages(ctx, 0, msg)
		if err != nil {
			b.Fatal(err)
		}

		// Poll
		msgs, err := queue.PollMessages(ctx, 0, time.Minute, 1, 1)
		if err != nil || len(msgs) == 0 {
			b.Fatal("poll failed")
		}

		// Delete
		_, err = queue.DeleteMessages(ctx, msgs[0].ReceiptHandle)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "cycles/sec")

	cleanupBenchmarkQueue(b, queue)
}

// BenchmarkEndToEnd_BatchCycle measures batch push→poll→delete cycle.
func BenchmarkEndToEnd_BatchCycle(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	const batchSize = 25
	messages := make([]events.SQSMessage, batchSize)
	for i := range messages {
		messages[i] = events.SQSMessage{Body: fmt.Sprintf("e2e-batch-%d", i)}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		// Push batch
		_, err := queue.PushMessages(ctx, 0, messages...)
		if err != nil {
			b.Fatal(err)
		}

		// Poll batch
		msgs, err := queue.PollMessages(ctx, time.Second, time.Minute, batchSize, batchSize)
		if err != nil {
			b.Fatal(err)
		}

		// Delete batch
		if len(msgs) > 0 {
			handles := dynamodbqueue.ToReceiptHandles(msgs)
			_, err = queue.DeleteMessages(ctx, handles...)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "msgs/sec")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// FIFO Queue Benchmarks
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkFIFO_PushSingleGroup measures FIFO push to single group.
func BenchmarkFIFO_PushSingleGroup(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueFIFO)
	fifo := queue.(dynamodbqueue.FifoQueue)

	msg := events.SQSMessage{Body: "fifo-single-group"}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := fifo.PushMessagesWithGroup(ctx, 0, "group-A", msg)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	cleanupBenchmarkQueue(b, queue)
}

// BenchmarkFIFO_PushMultipleGroups measures FIFO push to different groups.
func BenchmarkFIFO_PushMultipleGroups(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueFIFO)
	fifo := queue.(dynamodbqueue.FifoQueue)

	msg := events.SQSMessage{Body: "fifo-multi-group"}
	groups := []string{"group-A", "group-B", "group-C", "group-D", "group-E"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		group := groups[i%len(groups)]
		_, err := fifo.PushMessagesWithGroup(ctx, 0, group, msg)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helper Functions
// ═══════════════════════════════════════════════════════════════════════════════

func setupBenchmarkQueue(b *testing.B, queueType dynamodbqueue.QueueType) dynamodbqueue.Queue {
	b.Helper()

	suffix := dynamodbqueue.RandomString(6)
	queueName := fmt.Sprintf("bench-%s", suffix)

	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, queueType).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("bench-client")

	// Clear any existing data
	_ = queue.Purge(context.Background())

	return queue
}

func cleanupBenchmarkQueue(b *testing.B, queue dynamodbqueue.Queue) {
	b.Helper()
	_ = queue.Purge(context.Background())
}

func populateQueue(b *testing.B, queue dynamodbqueue.Queue, count int) {
	b.Helper()
	ctx := context.Background()

	batchSize := 25
	for i := 0; i < count; i += batchSize {
		remaining := count - i
		if remaining > batchSize {
			remaining = batchSize
		}

		messages := make([]events.SQSMessage, remaining)
		for j := range messages {
			messages[j] = events.SQSMessage{Body: fmt.Sprintf("populate-%d", i+j)}
		}

		_, err := queue.PushMessages(ctx, 0, messages...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

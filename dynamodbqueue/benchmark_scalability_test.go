package dynamodbqueue_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Consumer Count Scalability Benchmarks
//
// Measures throughput as consumer count increases.
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkScalability_ConsumerCount_1 measures throughput with 1 consumer.
func BenchmarkScalability_ConsumerCount_1(b *testing.B) {
	benchmarkConsumerScalability(b, 1, 1000)
}

// BenchmarkScalability_ConsumerCount_3 measures throughput with 3 consumers.
func BenchmarkScalability_ConsumerCount_3(b *testing.B) {
	benchmarkConsumerScalability(b, 3, 1500)
}

// BenchmarkScalability_ConsumerCount_5 measures throughput with 5 consumers.
func BenchmarkScalability_ConsumerCount_5(b *testing.B) {
	benchmarkConsumerScalability(b, 5, 2000)
}

// BenchmarkScalability_ConsumerCount_10 measures throughput with 10 consumers.
func BenchmarkScalability_ConsumerCount_10(b *testing.B) {
	benchmarkConsumerScalability(b, 10, 3000)
}

// BenchmarkScalability_ConsumerCount_20 measures throughput with 20 consumers.
func BenchmarkScalability_ConsumerCount_20(b *testing.B) {
	benchmarkConsumerScalability(b, 20, 5000)
}

func benchmarkConsumerScalability(b *testing.B, numConsumers, numMessages int) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	// Pre-populate queue
	populateQueue(b, queue, numMessages)

	var totalConsumed int64

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	start := time.Now()

	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()

			for {
				msgs, err := queue.PollMessages(ctx, time.Millisecond*100, time.Minute, 1, 10)
				if err != nil {
					return
				}

				if len(msgs) == 0 {
					return
				}

				atomic.AddInt64(&totalConsumed, int64(len(msgs)))

				// Delete messages
				handles := dynamodbqueue.ToReceiptHandles(msgs)
				_, _ = queue.DeleteMessages(ctx, handles...)
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	b.StopTimer()

	throughput := float64(totalConsumed) / elapsed.Seconds()
	b.ReportMetric(throughput, "msgs/sec")
	b.ReportMetric(float64(numConsumers), "consumers")
	b.ReportMetric(float64(totalConsumed), "total_consumed")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// FIFO Group Count Scalability Benchmarks
//
// Measures FIFO throughput as group count increases.
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkScalability_FIFO_GroupCount_1 measures FIFO throughput with 1 group.
func BenchmarkScalability_FIFO_GroupCount_1(b *testing.B) {
	benchmarkFIFOGroupScalability(b, 1, 500)
}

// BenchmarkScalability_FIFO_GroupCount_5 measures FIFO throughput with 5 groups.
func BenchmarkScalability_FIFO_GroupCount_5(b *testing.B) {
	benchmarkFIFOGroupScalability(b, 5, 500)
}

// BenchmarkScalability_FIFO_GroupCount_10 measures FIFO throughput with 10 groups.
func BenchmarkScalability_FIFO_GroupCount_10(b *testing.B) {
	benchmarkFIFOGroupScalability(b, 10, 1000)
}

// BenchmarkScalability_FIFO_GroupCount_25 measures FIFO throughput with 25 groups.
func BenchmarkScalability_FIFO_GroupCount_25(b *testing.B) {
	benchmarkFIFOGroupScalability(b, 25, 1000)
}

// BenchmarkScalability_FIFO_GroupCount_50 measures FIFO throughput with 50 groups.
func BenchmarkScalability_FIFO_GroupCount_50(b *testing.B) {
	benchmarkFIFOGroupScalability(b, 50, 1000)
}

// BenchmarkScalability_FIFO_GroupCount_100 measures FIFO throughput with 100 groups.
func BenchmarkScalability_FIFO_GroupCount_100(b *testing.B) {
	benchmarkFIFOGroupScalability(b, 100, 500)
}

func benchmarkFIFOGroupScalability(b *testing.B, numGroups, messagesPerGroup int) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueFIFO)
	fifo := queue.(dynamodbqueue.FifoQueue)

	// Create groups and push messages
	groups := make([]string, numGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("group-%d", i)
	}

	// Push messages to all groups
	for _, group := range groups {
		for i := 0; i < messagesPerGroup; i++ {
			_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("fifo-scale-%s-%d", group, i),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	totalMessages := numGroups * messagesPerGroup
	var totalConsumed int64

	b.ResetTimer()

	start := time.Now()

	// Poll until empty
	for totalConsumed < int64(totalMessages) {
		msgs, err := fifo.PollMessages(ctx, time.Millisecond*100, time.Minute, 1, numGroups)
		if err != nil {
			b.Fatal(err)
		}

		if len(msgs) == 0 {
			continue
		}

		atomic.AddInt64(&totalConsumed, int64(len(msgs)))

		// Delete messages
		handles := dynamodbqueue.ToReceiptHandles(msgs)
		_, _ = fifo.DeleteMessages(ctx, handles...)
	}

	elapsed := time.Since(start)

	b.StopTimer()

	throughput := float64(totalConsumed) / elapsed.Seconds()
	b.ReportMetric(throughput, "msgs/sec")
	b.ReportMetric(float64(numGroups), "groups")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Queue Depth Impact Benchmarks
//
// Measures how queue depth affects poll performance.
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkScalability_QueueDepth_500 measures poll performance at depth 500.
func BenchmarkScalability_QueueDepth_500(b *testing.B) {
	benchmarkQueueDepthImpact(b, 500)
}

// BenchmarkScalability_QueueDepth_2000 measures poll performance at depth 2000.
func BenchmarkScalability_QueueDepth_2000(b *testing.B) {
	benchmarkQueueDepthImpact(b, 2000)
}

// BenchmarkScalability_QueueDepth_5000 measures poll performance at depth 5000.
func BenchmarkScalability_QueueDepth_5000(b *testing.B) {
	benchmarkQueueDepthImpact(b, 5000)
}

// BenchmarkScalability_QueueDepth_10000 measures poll performance at depth 10000.
func BenchmarkScalability_QueueDepth_10000(b *testing.B) {
	benchmarkQueueDepthImpact(b, 10000)
}

func benchmarkQueueDepthImpact(b *testing.B, depth int) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	// Pre-populate to target depth
	populateQueue(b, queue, depth)

	b.ResetTimer()

	// Measure poll latency without consuming (just query time)
	for i := 0; i < b.N; i++ {
		msgs, err := queue.PollMessages(ctx, 0, time.Minute, 10, 10)
		if err != nil {
			b.Fatal(err)
		}

		// Delete to prevent queue from emptying
		if len(msgs) > 0 {
			handles := dynamodbqueue.ToReceiptHandles(msgs)
			_, _ = queue.DeleteMessages(ctx, handles...)

			// Repopulate
			messages := make([]events.SQSMessage, len(msgs))
			for j := range messages {
				messages[j] = events.SQSMessage{Body: fmt.Sprintf("repop-%d-%d", i, j)}
			}
			_, _ = queue.PushMessages(ctx, 0, messages...)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(depth), "queue_depth")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Concurrent Push Scalability Benchmarks
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkScalability_ConcurrentPush_1 measures push throughput with 1 pusher.
func BenchmarkScalability_ConcurrentPush_1(b *testing.B) {
	benchmarkConcurrentPush(b, 1, 500)
}

// BenchmarkScalability_ConcurrentPush_5 measures push throughput with 5 pushers.
func BenchmarkScalability_ConcurrentPush_5(b *testing.B) {
	benchmarkConcurrentPush(b, 5, 500)
}

// BenchmarkScalability_ConcurrentPush_10 measures push throughput with 10 pushers.
func BenchmarkScalability_ConcurrentPush_10(b *testing.B) {
	benchmarkConcurrentPush(b, 10, 500)
}

// BenchmarkScalability_ConcurrentPush_20 measures push throughput with 20 pushers.
func BenchmarkScalability_ConcurrentPush_20(b *testing.B) {
	benchmarkConcurrentPush(b, 20, 500)
}

func benchmarkConcurrentPush(b *testing.B, numPushers, messagesPerPusher int) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	var totalPushed int64

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(numPushers)

	start := time.Now()

	for p := 0; p < numPushers; p++ {
		go func(pusherID int) {
			defer wg.Done()

			for i := 0; i < messagesPerPusher; i++ {
				_, err := queue.PushMessages(ctx, 0, events.SQSMessage{
					Body: fmt.Sprintf("conc-push-%d-%d", pusherID, i),
				})
				if err != nil {
					return
				}
				atomic.AddInt64(&totalPushed, 1)
			}
		}(p)
	}

	wg.Wait()
	elapsed := time.Since(start)

	b.StopTimer()

	throughput := float64(totalPushed) / elapsed.Seconds()
	b.ReportMetric(throughput, "msgs/sec")
	b.ReportMetric(float64(numPushers), "pushers")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Batch Size Impact Benchmarks
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkScalability_BatchSize_1 measures throughput with batch size 1.
func BenchmarkScalability_BatchSize_1(b *testing.B) {
	benchmarkBatchSizeImpact(b, 1)
}

// BenchmarkScalability_BatchSize_5 measures throughput with batch size 5.
func BenchmarkScalability_BatchSize_5(b *testing.B) {
	benchmarkBatchSizeImpact(b, 5)
}

// BenchmarkScalability_BatchSize_15 measures throughput with batch size 15.
func BenchmarkScalability_BatchSize_15(b *testing.B) {
	benchmarkBatchSizeImpact(b, 15)
}

// BenchmarkScalability_BatchSize_25 measures throughput with batch size 25.
func BenchmarkScalability_BatchSize_25(b *testing.B) {
	benchmarkBatchSizeImpact(b, 25)
}

func benchmarkBatchSizeImpact(b *testing.B, batchSize int) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	// Create batch of messages
	messages := make([]events.SQSMessage, batchSize)
	for i := range messages {
		messages[i] = events.SQSMessage{Body: fmt.Sprintf("batch-impact-%d", i)}
	}

	totalMessages := 0

	b.ResetTimer()

	start := time.Now()

	for i := 0; i < b.N; i++ {
		_, err := queue.PushMessages(ctx, 0, messages...)
		if err != nil {
			b.Fatal(err)
		}
		totalMessages += batchSize
	}

	elapsed := time.Since(start)

	b.StopTimer()

	throughput := float64(totalMessages) / elapsed.Seconds()
	b.ReportMetric(throughput, "msgs/sec")
	b.ReportMetric(float64(batchSize), "batch_size")

	cleanupBenchmarkQueue(b, queue)
}

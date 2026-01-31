package dynamodbqueue_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Memory Allocation Benchmarks
//
// Measures allocations per operation for various message operations.
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkMemory_PushSingleMessage measures allocations for single push.
func BenchmarkMemory_PushSingleMessage(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	msg := events.SQSMessage{Body: "memory-test"}

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

// BenchmarkMemory_PushBatch25 measures allocations for batch push.
func BenchmarkMemory_PushBatch25(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	messages := make([]events.SQSMessage, 25)
	for i := range messages {
		messages[i] = events.SQSMessage{Body: fmt.Sprintf("mem-batch-%d", i)}
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
	cleanupBenchmarkQueue(b, queue)
}

// BenchmarkMemory_PollAndLock measures poll operation allocations.
func BenchmarkMemory_PollAndLock(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	// Pre-populate
	populateQueue(b, queue, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		msgs, err := queue.PollMessages(ctx, 0, time.Minute, 10, 10)
		if err != nil {
			b.Fatal(err)
		}

		// Delete to allow re-poll
		if len(msgs) > 0 {
			handles := dynamodbqueue.ToReceiptHandles(msgs)
			_, _ = queue.DeleteMessages(ctx, handles...)
		}
	}

	b.StopTimer()
	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Sustained Load Memory Benchmarks
//
// Tests memory stability under sustained operations.
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkMemory_SustainedLoad_2k measures memory under 2k message sustained load.
func BenchmarkMemory_SustainedLoad_2k(b *testing.B) {
	benchmarkSustainedLoad(b, 2000)
}

// BenchmarkMemory_SustainedLoad_10k measures memory under 10k message sustained load.
func BenchmarkMemory_SustainedLoad_10k(b *testing.B) {
	benchmarkSustainedLoad(b, 10000)
}

// BenchmarkMemory_SustainedLoad_25k measures memory under 25k message sustained load.
func BenchmarkMemory_SustainedLoad_25k(b *testing.B) {
	benchmarkSustainedLoad(b, 25000)
}

func benchmarkSustainedLoad(b *testing.B, messageCount int) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	// Force GC before starting
	runtime.GC()

	var initialAlloc uint64
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	initialAlloc = m.Alloc

	// Push all messages
	batchSize := 25
	for i := 0; i < messageCount; i += batchSize {
		remaining := messageCount - i
		if remaining > batchSize {
			remaining = batchSize
		}

		messages := make([]events.SQSMessage, remaining)
		for j := range messages {
			messages[j] = events.SQSMessage{Body: fmt.Sprintf("sustained-%d", i+j)}
		}

		_, err := queue.PushMessages(ctx, 0, messages...)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	// Continuous poll/delete cycles
	totalProcessed := 0
	for i := range b.N {
		msgs, err := queue.PollMessages(ctx, 0, time.Minute, 10, 25)
		if err != nil {
			b.Fatal(err)
		}

		if len(msgs) == 0 {
			// Repopulate if empty
			for j := range 100 {
				_, _ = queue.PushMessages(ctx, 0, events.SQSMessage{
					Body: fmt.Sprintf("repop-%d-%d", i, j),
				})
			}
			continue
		}

		totalProcessed += len(msgs)
		handles := dynamodbqueue.ToReceiptHandles(msgs)
		_, _ = queue.DeleteMessages(ctx, handles...)
	}

	b.StopTimer()

	// Force GC and measure
	runtime.GC()
	runtime.ReadMemStats(&m)

	peakAlloc := float64(m.Alloc-initialAlloc) / (1024 * 1024)
	b.ReportMetric(peakAlloc, "peak_MB")
	b.ReportMetric(float64(m.NumGC), "gc_cycles")
	b.ReportMetric(float64(totalProcessed), "msgs_processed")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Message Size Memory Benchmarks
//
// Tests memory impact of different message payload sizes.
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkMemory_MessageSize_1KB measures allocations with 1KB message bodies.
func BenchmarkMemory_MessageSize_1KB(b *testing.B) {
	benchmarkMessageSize(b, 1024)
}

// BenchmarkMemory_MessageSize_10KB measures allocations with 10KB message bodies.
func BenchmarkMemory_MessageSize_10KB(b *testing.B) {
	benchmarkMessageSize(b, 10*1024)
}

// BenchmarkMemory_MessageSize_64KB measures allocations with 64KB message bodies.
func BenchmarkMemory_MessageSize_64KB(b *testing.B) {
	benchmarkMessageSize(b, 64*1024)
}

func benchmarkMessageSize(b *testing.B, size int) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	// Create message with specified body size
	body := strings.Repeat("x", size)
	msg := events.SQSMessage{Body: body}

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
		if err != nil {
			b.Fatal(err)
		}

		// Delete
		if len(msgs) > 0 {
			_, _ = queue.DeleteMessages(ctx, msgs[0].ReceiptHandle)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(size), "body_bytes")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Utility Function Memory Benchmarks
//
// Measures local-only utility function allocations.
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkMemory_ToBatches measures ToBatches allocation overhead.
func BenchmarkMemory_ToBatches(b *testing.B) {
	items := make([]int, 1000)
	for i := range items {
		items[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = dynamodbqueue.ToBatches(items, 25)
	}
}

// BenchmarkMemory_ToReceiptHandles measures ToReceiptHandles allocation.
func BenchmarkMemory_ToReceiptHandles(b *testing.B) {
	msgs := make([]events.SQSMessage, 100)
	for i := range msgs {
		msgs[i] = events.SQSMessage{ReceiptHandle: fmt.Sprintf("handle-%d", i)}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = dynamodbqueue.ToReceiptHandles(msgs)
	}
}

// BenchmarkMemory_RandomString measures RandomString allocation.
func BenchmarkMemory_RandomString(b *testing.B) {
	b.ReportAllocs()

	for range b.N {
		_ = dynamodbqueue.RandomString(10)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// FIFO Memory Benchmarks
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkMemory_FIFO_InFlightTracking measures FIFO in-flight group tracking.
func BenchmarkMemory_FIFO_InFlightTracking(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueFIFO)
	fifo := queue.(dynamodbqueue.FifoQueue)

	// Create 50 groups with messages
	groups := make([]string, 50)
	for i := range groups {
		groups[i] = fmt.Sprintf("group-%d", i)
		_, err := fifo.PushMessagesWithGroup(ctx, 0, groups[i], events.SQSMessage{
			Body: fmt.Sprintf("fifo-mem-%d", i),
		})
		if err != nil {
			b.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, 10)
		if err != nil {
			b.Fatal(err)
		}

		if len(msgs) > 0 {
			handles := dynamodbqueue.ToReceiptHandles(msgs)
			_, _ = fifo.DeleteMessages(ctx, handles...)
		}

		// Repopulate some groups
		if i%10 == 0 {
			for j := range 5 {
				_, _ = fifo.PushMessagesWithGroup(ctx, 0, groups[j], events.SQSMessage{
					Body: fmt.Sprintf("repop-%d", i),
				})
			}
		}
	}

	b.StopTimer()
	cleanupBenchmarkQueue(b, queue)
}

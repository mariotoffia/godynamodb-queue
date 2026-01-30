package dynamodbqueue_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Latency Distribution Benchmarks
//
// Measures push/poll latency percentiles (p50, p95, p99).
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkLatency_PushDistribution measures push latency distribution.
func BenchmarkLatency_PushDistribution(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	const iterations = 100
	latencies := make([]time.Duration, 0, iterations)
	msg := events.SQSMessage{Body: "latency-test"}

	b.ResetTimer()

	for i := 0; i < iterations; i++ {
		start := time.Now()
		_, err := queue.PushMessages(ctx, 0, msg)
		latency := time.Since(start)

		if err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, latency)
	}

	b.StopTimer()

	// Calculate percentiles
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	b.ReportMetric(float64(p50.Microseconds()), "p50_us")
	b.ReportMetric(float64(p95.Microseconds()), "p95_us")
	b.ReportMetric(float64(p99.Microseconds()), "p99_us")

	cleanupBenchmarkQueue(b, queue)
}

// BenchmarkLatency_PollDistribution measures poll latency distribution.
func BenchmarkLatency_PollDistribution(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	// Pre-populate
	populateQueue(b, queue, 200)

	const iterations = 100
	latencies := make([]time.Duration, 0, iterations)

	b.ResetTimer()

	for i := 0; i < iterations; i++ {
		start := time.Now()
		msgs, err := queue.PollMessages(ctx, 0, time.Minute, 1, 1)
		latency := time.Since(start)

		if err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, latency)

		// Delete to allow repoll
		if len(msgs) > 0 {
			_, _ = queue.DeleteMessages(ctx, msgs[0].ReceiptHandle)
		}
	}

	b.StopTimer()

	// Calculate percentiles
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	b.ReportMetric(float64(p50.Microseconds()), "p50_us")
	b.ReportMetric(float64(p95.Microseconds()), "p95_us")
	b.ReportMetric(float64(p99.Microseconds()), "p99_us")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Contention Benchmarks
//
// Measures lock acquisition latency under concurrent consumer contention.
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkLatency_PollWithContention_2Consumers measures with 2 concurrent pollers.
func BenchmarkLatency_PollWithContention_2Consumers(b *testing.B) {
	benchmarkPollContention(b, 2, 200)
}

// BenchmarkLatency_PollWithContention_5Consumers measures with 5 concurrent pollers.
func BenchmarkLatency_PollWithContention_5Consumers(b *testing.B) {
	benchmarkPollContention(b, 5, 500)
}

// BenchmarkLatency_PollWithContention_10Consumers measures with 10 concurrent pollers.
func BenchmarkLatency_PollWithContention_10Consumers(b *testing.B) {
	benchmarkPollContention(b, 10, 1000)
}

// BenchmarkLatency_PollWithContention_20Consumers measures with 20 concurrent pollers.
func BenchmarkLatency_PollWithContention_20Consumers(b *testing.B) {
	benchmarkPollContention(b, 20, 2000)
}

func benchmarkPollContention(b *testing.B, numConsumers, numMessages int) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	// Pre-populate
	populateQueue(b, queue, numMessages)

	var totalLatency int64
	var totalPolled int64
	var contentionCount int64

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()

			for {
				start := time.Now()
				msgs, err := queue.PollMessages(ctx, 0, time.Minute, 1, 5)
				latency := time.Since(start)

				if err != nil {
					return
				}

				if len(msgs) == 0 {
					return
				}

				atomic.AddInt64(&totalLatency, latency.Nanoseconds())
				atomic.AddInt64(&totalPolled, int64(len(msgs)))

				// Track contention (poll returned fewer than requested)
				if len(msgs) < 5 {
					atomic.AddInt64(&contentionCount, 1)
				}

				// Delete messages
				handles := dynamodbqueue.ToReceiptHandles(msgs)
				_, _ = queue.DeleteMessages(ctx, handles...)
			}
		}()
	}

	wg.Wait()
	b.StopTimer()

	if totalPolled == 0 {
		totalPolled = 1
	}
	avgLatency := time.Duration(totalLatency / totalPolled)
	b.ReportMetric(float64(avgLatency.Microseconds()), "avg_latency_us")
	b.ReportMetric(float64(contentionCount), "contention_events")
	b.ReportMetric(float64(totalPolled), "total_polled")

	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Delete Race Benchmarks
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkLatency_DeleteWithRaces measures delete latency with race conditions.
func BenchmarkLatency_DeleteWithRaces(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueStandard)

	const numMessages = 20
	const numDeleters = 3

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Push messages
		for j := 0; j < numMessages; j++ {
			_, err := queue.PushMessages(ctx, 0, events.SQSMessage{
				Body: fmt.Sprintf("race-delete-%d-%d", i, j),
			})
			if err != nil {
				b.Fatal(err)
			}
		}

		// Poll all messages
		msgs, err := queue.PollMessages(ctx, time.Second, time.Minute, numMessages, numMessages)
		if err != nil {
			b.Fatal(err)
		}

		handles := dynamodbqueue.ToReceiptHandles(msgs)
		b.StartTimer()

		// Race to delete
		var wg sync.WaitGroup
		wg.Add(numDeleters)

		for d := 0; d < numDeleters; d++ {
			go func() {
				defer wg.Done()
				for _, handle := range handles {
					_, _ = queue.DeleteMessages(ctx, handle)
				}
			}()
		}

		wg.Wait()
	}

	b.StopTimer()
	cleanupBenchmarkQueue(b, queue)
}

// ═══════════════════════════════════════════════════════════════════════════════
// FIFO Latency Benchmarks
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkLatency_FIFO_SingleGroupPoll measures FIFO poll latency single group.
func BenchmarkLatency_FIFO_SingleGroupPoll(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueFIFO)
	fifo := queue.(dynamodbqueue.FifoQueue)

	const numMessages = 200

	// Push messages to single group
	for i := 0; i < numMessages; i++ {
		_, err := fifo.PushMessagesWithGroup(ctx, 0, "group-A", events.SQSMessage{
			Body: fmt.Sprintf("fifo-latency-%d", i),
		})
		if err != nil {
			b.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}

	latencies := make([]time.Duration, 0, numMessages)

	b.ResetTimer()

	for i := 0; i < numMessages; i++ {
		start := time.Now()
		msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, 1)
		latency := time.Since(start)

		if err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, latency)

		if len(msgs) > 0 {
			_, _ = fifo.DeleteMessages(ctx, msgs[0].ReceiptHandle)
		}
	}

	b.StopTimer()

	// Calculate percentiles
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]

	b.ReportMetric(float64(p50.Microseconds()), "p50_us")
	b.ReportMetric(float64(p95.Microseconds()), "p95_us")

	cleanupBenchmarkQueue(b, queue)
}

// BenchmarkLatency_FIFO_MultiGroupPoll measures FIFO poll latency multiple groups.
func BenchmarkLatency_FIFO_MultiGroupPoll(b *testing.B) {
	ctx := context.Background()
	queue := setupBenchmarkQueue(b, dynamodbqueue.QueueFIFO)
	fifo := queue.(dynamodbqueue.FifoQueue)

	groups := []string{"group-A", "group-B", "group-C", "group-D", "group-E",
		"group-F", "group-G", "group-H", "group-I", "group-J"}
	messagesPerGroup := 50
	totalMessages := len(groups) * messagesPerGroup

	// Push messages to multiple groups
	for _, group := range groups {
		for i := 0; i < messagesPerGroup; i++ {
			_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("fifo-multi-%s-%d", group, i),
			})
			if err != nil {
				b.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
	}

	latencies := make([]time.Duration, 0, totalMessages)

	b.ResetTimer()

	for i := 0; i < totalMessages; i++ {
		start := time.Now()
		msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, len(groups))
		latency := time.Since(start)

		if err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, latency)

		if len(msgs) > 0 {
			handles := dynamodbqueue.ToReceiptHandles(msgs)
			_, _ = fifo.DeleteMessages(ctx, handles...)
		}
	}

	b.StopTimer()

	// Calculate percentiles
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]

	b.ReportMetric(float64(p50.Microseconds()), "p50_us")
	b.ReportMetric(float64(p95.Microseconds()), "p95_us")

	cleanupBenchmarkQueue(b, queue)
}

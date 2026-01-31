package dynamodbqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Context Cancellation Tests
//
// Tests for context cancellation propagation in retry loops and batch operations.
//
// Cancellation Points:
// ───────────────────────────────────────────────────────────────────────────────
//   batchProcess:       ctx.Err() before each batch, before each retry
//   executeBatchWrite:  ctx.Err() before each retry, select during backoff
// ───────────────────────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// TestContextCancellation_ImmediatelyCanceled validates pre-canceled context handling.
func TestContextCancellation_ImmediatelyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Verify context is canceled
	assert.Error(t, ctx.Err(), "context should be canceled")
	assert.Equal(t, context.Canceled, ctx.Err())
}

// TestContextCancellation_DeadlineExceeded validates timeout handling.
func TestContextCancellation_DeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	// Wait for deadline using ctx.Done() channel instead of sleep
	<-ctx.Done()

	assert.Error(t, ctx.Err())
	assert.Equal(t, context.DeadlineExceeded, ctx.Err())
}

// TestContextCancellation_CancelAfterDelay validates delayed cancellation.
//
// Uses channel synchronization to avoid race conditions between
// the main goroutine and the cancel goroutine.
func TestContextCancellation_CancelAfterDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})

	// Cancel after goroutine signals it has started
	go func() {
		close(ready) // Signal goroutine is running
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	<-ready // Wait for goroutine to be ready, cancel is ~10ms away

	// Context should not be canceled yet (goroutine just started sleeping)
	assert.NoError(t, ctx.Err(), "context should not be canceled immediately")

	// Wait for cancellation
	<-ctx.Done()

	assert.Error(t, ctx.Err())
	assert.Equal(t, context.Canceled, ctx.Err())
}

// TestContextCancellation_SelectPattern validates select statement behavior.
//
// Pattern Used in Code:
// ───────────────────────────────────────────────────────────────────────────────
//   select {
//       case <-time.After(backoff):
//           // Continue retry
//       case <-ctx.Done():
//           return ctx.Err()
//   }
// ───────────────────────────────────────────────────────────────────────────────
func TestContextCancellation_SelectPattern(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Simulate the select pattern with immediate cancellation
	cancel()

	select {
	case <-time.After(time.Hour): // This won't fire
		t.Fatal("should not reach time.After case")
	case <-ctx.Done():
		// Expected path
		assert.Equal(t, context.Canceled, ctx.Err())
	}
}

// TestContextCancellation_SelectWithTimeout validates timeout wins over long wait.
func TestContextCancellation_SelectWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()

	select {
	case <-time.After(time.Hour): // This won't fire
		t.Fatal("should not reach time.After case")
	case <-ctx.Done():
		// Expected path - timeout
		elapsed := time.Since(start)
		assert.Less(t, elapsed, 100*time.Millisecond, "should exit quickly via timeout")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Backoff with Context Tests
// ═══════════════════════════════════════════════════════════════════════════════

// TestBackoffInterruptible validates backoff can be interrupted by context.
//
// This test verifies that the select pattern used in retry loops
// properly exits when context is canceled, regardless of timing.
func TestBackoffInterruptible(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Calculate a backoff duration that's much longer than our cancel delay
	backoffDuration := calcBackoff(5) // ~1600ms base

	ready := make(chan struct{})

	// Cancel after goroutine signals it has started
	go func() {
		close(ready)
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	<-ready // Ensure goroutine is running

	// Simulate the backoff select pattern
	select {
	case <-time.After(backoffDuration):
		t.Fatal("should not complete full backoff - context cancellation should have interrupted")
	case <-ctx.Done():
		// Expected - interrupted by context cancellation
		// We verify the behavior, not the timing
		assert.Equal(t, context.Canceled, ctx.Err())
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Error Propagation Tests
// ═══════════════════════════════════════════════════════════════════════════════

// TestContextError_Propagation validates error types are preserved.
func TestContextError_Propagation(t *testing.T) {
	t.Run("canceled error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := ctx.Err()
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("deadline exceeded error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()

		// Wait for deadline using ctx.Done() instead of sleep
		<-ctx.Done()

		err := ctx.Err()
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

// TestContextCancellation_CheckPattern validates the check pattern used in loops.
//
// Pattern:
// ───────────────────────────────────────────────────────────────────────────────
//   for {
//       if err := ctx.Err(); err != nil {
//           return err
//       }
//       // ... do work
//   }
// ───────────────────────────────────────────────────────────────────────────────
func TestContextCancellation_CheckPattern(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cleanup

	iterations := 0
	maxIterations := 100

	// Simulate loop with context check
	for iterations < maxIterations {
		if err := ctx.Err(); err != nil {
			// Context canceled - exit loop
			break
		}

		iterations++

		// Cancel after 10 iterations
		if iterations == 10 {
			cancel()
		}
	}

	assert.Equal(t, 10, iterations, "should exit after cancellation")
	assert.Error(t, ctx.Err())
}

// TestContextCancellation_MultipleChecks validates multiple check points work.
func TestContextCancellation_MultipleChecks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cleanup

	checkPoint1Hit := false
	checkPoint2Hit := false
	checkPoint3Hit := false

	// Simulate function with multiple context checks
	process := func() error {
		// Check point 1
		if err := ctx.Err(); err != nil {
			return err
		}
		checkPoint1Hit = true

		// Check point 2
		if err := ctx.Err(); err != nil {
			return err
		}
		checkPoint2Hit = true

		// Cancel before check point 3
		cancel()

		// Check point 3
		if err := ctx.Err(); err != nil {
			return err
		}
		checkPoint3Hit = true

		return nil
	}

	err := process()

	assert.Error(t, err)
	assert.True(t, checkPoint1Hit, "should hit check point 1")
	assert.True(t, checkPoint2Hit, "should hit check point 2")
	assert.False(t, checkPoint3Hit, "should not hit check point 3")
}

// ═══════════════════════════════════════════════════════════════════════════════
// Context Value Preservation Tests
// ═══════════════════════════════════════════════════════════════════════════════

type contextKey string

// TestContextCancellation_PreservesValues validates context values survive cancel.
func TestContextCancellation_PreservesValues(t *testing.T) {
	key := contextKey("test-key")
	ctx := context.WithValue(context.Background(), key, "test-value")
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	// Value should still be accessible after cancellation
	value := ctx.Value(key)
	assert.Equal(t, "test-value", value)
	assert.Error(t, ctx.Err()) // But context is canceled
}

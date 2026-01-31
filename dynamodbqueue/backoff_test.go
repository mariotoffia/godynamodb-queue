package dynamodbqueue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Exponential Backoff Tests
//
// Tests for calcBackoff which calculates exponential backoff with jitter.
//
// Backoff Formula:
// ───────────────────────────────────────────────────────────────────────────────
//   base = 50ms * 2^attempt (capped at 5s)
//   jitter = ±25% of base
//   result = base + jitter
// ───────────────────────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// TestCalcBackoff_Attempt0_ReturnsBaseValue validates first attempt backoff.
func TestCalcBackoff_Attempt0_ReturnsBaseValue(t *testing.T) {
	// Attempt 0: 50ms * 2^0 = 50ms (±25% jitter)
	// Expected range: 37.5ms to 62.5ms

	for i := 0; i < 50; i++ {
		backoff := calcBackoff(0)

		assert.GreaterOrEqual(t, backoff, 37*time.Millisecond,
			"attempt 0 backoff should be >= 37ms (50ms - 25%% jitter)")
		assert.LessOrEqual(t, backoff, 63*time.Millisecond,
			"attempt 0 backoff should be <= 63ms (50ms + 25%% jitter)")
	}
}

// TestCalcBackoff_ExponentialGrowth validates doubling behavior.
//
// Expected Values (before jitter):
// ───────────────────────────────────────────────────────────────────────────────
//   Attempt 0: 50ms    Attempt 3: 400ms
//   Attempt 1: 100ms   Attempt 4: 800ms
//   Attempt 2: 200ms   Attempt 5: 1600ms
//   Attempt 6+: capped at 5000ms
// ───────────────────────────────────────────────────────────────────────────────
func TestCalcBackoff_ExponentialGrowth(t *testing.T) {
	tests := []struct {
		attempt int
		minMs   int64 // base * 0.75 (minus 25% jitter)
		maxMs   int64 // base * 1.25 (plus 25% jitter)
	}{
		{0, 37, 63},      // 50ms ± 25%
		{1, 75, 125},     // 100ms ± 25%
		{2, 150, 250},    // 200ms ± 25%
		{3, 300, 500},    // 400ms ± 25%
		{4, 600, 1000},   // 800ms ± 25%
		{5, 1200, 2000},  // 1600ms ± 25%
		{6, 2400, 4000},  // 3200ms ± 25%
	}

	for _, tt := range tests {
		t.Run("attempt_"+string(rune('0'+tt.attempt)), func(t *testing.T) {
			// Sample multiple times due to jitter
			for i := 0; i < 20; i++ {
				backoff := calcBackoff(tt.attempt)
				backoffMs := backoff.Milliseconds()

				assert.GreaterOrEqual(t, backoffMs, tt.minMs,
					"attempt %d backoff should be >= %dms", tt.attempt, tt.minMs)
				assert.LessOrEqual(t, backoffMs, tt.maxMs,
					"attempt %d backoff should be <= %dms", tt.attempt, tt.maxMs)
			}
		})
	}
}

// TestCalcBackoff_MaximumCap validates exponent cap at 6.
//
// Implementation Detail:
// ───────────────────────────────────────────────────────────────────────────────
//   base = 50ms * 2^min(attempt, 6)
//   For attempt >= 6: 50ms * 64 = 3200ms
//   With ±25% jitter: 2400ms to 4000ms
// ───────────────────────────────────────────────────────────────────────────────
func TestCalcBackoff_MaximumCap(t *testing.T) {
	attempts := []int{7, 8, 9, 10, 20, 50, 100}

	for _, attempt := range attempts {
		t.Run("attempt_high", func(t *testing.T) {
			// Sample multiple times
			for i := 0; i < 20; i++ {
				backoff := calcBackoff(attempt)

				// Exponent capped at 6: 50ms * 64 = 3200ms
				// With ±25% jitter: 2400ms to 4000ms
				assert.LessOrEqual(t, backoff, 4000*time.Millisecond,
					"backoff should never exceed 4s (3200ms + 25%% jitter)")

				// Minimum should be at least 2.4s (3200ms - 25%)
				assert.GreaterOrEqual(t, backoff, 2400*time.Millisecond,
					"capped backoff should be >= 2.4s (3200ms - 25%% jitter)")
			}
		})
	}
}

// TestCalcBackoff_Jitter_WithinExpectedRange validates jitter bounds.
func TestCalcBackoff_Jitter_WithinExpectedRange(t *testing.T) {
	// Test attempt 2 (base = 200ms) for clear jitter analysis
	// Expected range: 150ms to 250ms (±25%)

	minSeen := time.Hour
	maxSeen := time.Duration(0)

	for i := 0; i < 100; i++ {
		backoff := calcBackoff(2)

		if backoff < minSeen {
			minSeen = backoff
		}
		if backoff > maxSeen {
			maxSeen = backoff
		}

		// Each individual call should be within bounds
		assert.GreaterOrEqual(t, backoff, 150*time.Millisecond)
		assert.LessOrEqual(t, backoff, 250*time.Millisecond)
	}

	// With 100 samples, we should see variation (not always same value)
	assert.NotEqual(t, minSeen, maxSeen, "jitter should produce variation")
}

// TestCalcBackoff_Jitter_ProducesVariation validates jitter is not zero.
func TestCalcBackoff_Jitter_ProducesVariation(t *testing.T) {
	// Collect samples and verify we see different values
	seen := make(map[int64]bool)

	for i := 0; i < 50; i++ {
		backoff := calcBackoff(3) // 400ms base
		seen[backoff.Milliseconds()] = true
	}

	// With good jitter, we should see multiple different values
	assert.Greater(t, len(seen), 1,
		"jitter should produce multiple different backoff values (got %d unique)", len(seen))
}

// TestCalcBackoff_NegativeAttempt_HandlesGracefully validates negative input handling.
//
// Behavior:
// ───────────────────────────────────────────────────────────────────────────────
//   Negative attempts are treated as attempt 0 (50ms base ± jitter)
// ───────────────────────────────────────────────────────────────────────────────
func TestCalcBackoff_NegativeAttempt_HandlesGracefully(t *testing.T) {
	// Negative attempts should be treated as attempt 0
	attempts := []int{-1, -10, -100}

	for _, attempt := range attempts {
		t.Run("negative_attempt", func(t *testing.T) {
			// Should not panic
			backoff := calcBackoff(attempt)

			// Should return attempt 0 value (~50ms ± 25% jitter = 37-63ms)
			assert.GreaterOrEqual(t, backoff, 37*time.Millisecond,
				"negative attempt should return attempt 0 range (min)")
			assert.LessOrEqual(t, backoff, 63*time.Millisecond,
				"negative attempt should return attempt 0 range (max)")
		})
	}
}

// TestCalcBackoff_LargeAttempt_DoesNotOverflow validates large input handling.
func TestCalcBackoff_LargeAttempt_DoesNotOverflow(t *testing.T) {
	largeAttempts := []int{1000, 10000, 1000000}

	for _, attempt := range largeAttempts {
		t.Run("large_attempt", func(t *testing.T) {
			// Should not panic or overflow
			backoff := calcBackoff(attempt)

			// Exponent capped at 6: 3200ms + 25% jitter = max 4000ms
			assert.LessOrEqual(t, backoff, 4000*time.Millisecond,
				"large attempt should be capped at max backoff")

			// Should be positive and reasonable (min ~2400ms)
			assert.GreaterOrEqual(t, backoff, 2400*time.Millisecond)
		})
	}
}

// TestCalcBackoff_ConsecutiveAttempts_NonDecreasing validates monotonic growth until cap.
func TestCalcBackoff_ConsecutiveAttempts_NonDecreasing(t *testing.T) {
	// Average backoff should increase with attempt number (until cap)
	// Test without jitter influence by sampling many times

	getAverageBackoff := func(attempt int) time.Duration {
		var total time.Duration
		samples := 50
		for i := 0; i < samples; i++ {
			total += calcBackoff(attempt)
		}
		return total / time.Duration(samples)
	}

	prev := getAverageBackoff(0)
	for attempt := 1; attempt <= 6; attempt++ {
		current := getAverageBackoff(attempt)
		assert.Greater(t, current, prev,
			"average backoff for attempt %d should be greater than attempt %d", attempt, attempt-1)
		prev = current
	}
}

// Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package buffered

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/AccelByte/extend-challenge-common/pkg/cache"
	"github.com/AccelByte/extend-challenge-common/pkg/common"
	"github.com/AccelByte/extend-challenge-common/pkg/domain"
	"github.com/AccelByte/extend-challenge-common/pkg/repository"

	"github.com/rs/zerolog"
)

// Repository defines the interface for buffered repository operations.
type Repository interface {
	// UpdateProgress buffers a progress update (for absolute/daily goals)
	UpdateProgress(ctx context.Context, progress *domain.UserGoalProgress) error

	// IncrementProgress buffers an increment update (for increment goals)
	IncrementProgress(ctx context.Context, userID, goalID, challengeID, namespace string,
		delta, targetValue int, isDailyIncrement bool) error

	// GetFromBuffer retrieves a progress record from the buffer
	GetFromBuffer(userID, goalID string) *domain.UserGoalProgress

	// Flush writes all buffered updates to database
	Flush(ctx context.Context) error

	// Close stops the flusher and performs final flush
	Close() error

	// GetBufferSize returns current buffer size
	GetBufferSize() int
}

// BufferedRepository implements a write-buffering layer over GoalRepository.
//
// Key Features:
// - Map-based deduplication: Only stores latest update per user-goal pair
// - Dual-flush mechanism: Time-based (periodic) + Size-based (threshold)
// - Batch UPSERT: Single DB query for entire batch (1,000,000x query reduction)
// - Thread-safe: Mutex protects buffer map
// - Goroutine flood prevention: Only one async flush runs at a time
//
// Performance:
// - 1,000 events/sec → 1 DB query/sec (vs 1,000 queries/sec without buffering)
// - Flush time: ~10-20ms for 1,000 updates (vs ~1s for one-by-one)
// - Memory: ~200KB for 1,000 buffered updates
type BufferedRepository struct {
	// buffer stores pending absolute/daily updates, key: "{user_id}:{goal_id}"
	buffer map[string]*domain.UserGoalProgress

	// bufferIncrement stores pending increment deltas, key: "{user_id}:{goal_id}"
	// Accumulates deltas for regular increment goals (e.g., 1+1+1 = 3 before flush)
	bufferIncrement map[string]int

	// bufferIncrementDaily tracks last event time for daily increment goals
	// Prevents same-day duplicate increments via client-side date checking
	// Capped at 200K entries with graceful degradation (relies on SQL DATE() check when full)
	bufferIncrementDaily map[string]time.Time

	// mu protects all buffer maps from concurrent access
	mu sync.RWMutex

	// repo is the underlying GoalRepository for database operations
	repo repository.GoalRepository

	// goalCache is used to look up goal metadata during flush
	goalCache cache.GoalCache

	// namespace is the current deployment namespace
	namespace string

	// ticker triggers time-based flushes
	ticker *time.Ticker

	// cleanupTicker triggers periodic cleanup of bufferIncrementDaily
	cleanupTicker *time.Ticker

	// maxBufferSize is the threshold for size-based flushes
	maxBufferSize int

	// flushInterval is the duration between time-based flushes
	flushInterval time.Duration

	// logger for structured logging
	logger zerolog.Logger

	// stopCh signals the flusher goroutine to stop
	stopCh chan struct{}

	// cleanupStopCh signals the cleanup goroutine to stop
	cleanupStopCh chan struct{}

	// immediateFlushCh signals the flusher to perform immediate flush
	immediateFlushCh chan struct{}

	// flushCompleteCh signals waiting goroutines that flush completed
	flushCompleteCh chan struct{}

	// backpressureThreshold defines when to start blocking (e.g., 1.5x maxBufferSize)
	backpressureThreshold int

	// circuitBreakerThreshold defines absolute hard limit (e.g., 2.5x maxBufferSize)
	circuitBreakerThreshold int

	// wg waits for all goroutines to finish
	wg sync.WaitGroup
}

// NewBufferedRepository creates a new BufferedRepository with the specified configuration.
//
// Parameters:
// - repo: Underlying GoalRepository for database operations
// - goalCache: Goal cache for looking up goal metadata during flush
// - namespace: Current deployment namespace
// - flushInterval: How often to flush buffer (e.g., 1*time.Second)
// - maxBufferSize: Maximum buffer entries before forcing flush (e.g., 1000)
// - logger: Structured logger
//
// The returned repository automatically starts the background flusher and cleanup goroutines.
// Call Close() to stop all goroutines and perform final flush.
func NewBufferedRepository(
	repo repository.GoalRepository,
	goalCache cache.GoalCache,
	namespace string,
	flushInterval time.Duration,
	maxBufferSize int,
	logger zerolog.Logger,
) *BufferedRepository {
	r := &BufferedRepository{
		buffer:                  make(map[string]*domain.UserGoalProgress),
		bufferIncrement:         make(map[string]int),
		bufferIncrementDaily:    make(map[string]time.Time),
		repo:                    repo,
		goalCache:               goalCache,
		namespace:               namespace,
		ticker:                  time.NewTicker(flushInterval),
		cleanupTicker:           time.NewTicker(1 * time.Hour), // Cleanup every hour
		maxBufferSize:           maxBufferSize,
		flushInterval:           flushInterval,
		logger:                  logger,
		stopCh:                  make(chan struct{}),
		cleanupStopCh:           make(chan struct{}),
		immediateFlushCh:        make(chan struct{}, 1),            // Buffered channel (size 1)
		flushCompleteCh:         make(chan struct{}, 1),            // Buffered to prevent signal loss
		backpressureThreshold:   int(float64(maxBufferSize) * 1.5), // 1.5x threshold
		circuitBreakerThreshold: int(float64(maxBufferSize) * 2.5), // 2.5x threshold
	}

	// Start background flusher
	r.wg.Add(1)
	go r.startFlusher()

	// Start background cleanup for daily buffer
	r.wg.Add(1)
	go r.startDailyBufferCleanup()

	r.logger.Info().
		Dur("flush_interval", flushInterval).
		Int("max_buffer_size", maxBufferSize).
		Int("backpressure_threshold", r.backpressureThreshold).
		Int("circuit_breaker_threshold", r.circuitBreakerThreshold).
		Str("namespace", namespace).
		Msg("BufferedRepository initialized with backpressure")

	return r
}

// UpdateProgress buffers a progress update without immediately writing to database.
//
// Behavior:
// - Overwrites any previous buffered update for same user-goal pair (deduplication)
// - Applies backpressure (blocks) if buffer exceeds 1.5x threshold
// - Activates circuit breaker (rejects) if buffer exceeds 2.5x threshold
// - Triggers size-based flush if buffer exceeds maxBufferSize
//
// This is the primary method called by event processors.
func (r *BufferedRepository) UpdateProgress(ctx context.Context, progress *domain.UserGoalProgress) error {
	// Input validation
	if progress == nil {
		return fmt.Errorf("progress cannot be nil")
	}

	if progress.UserID == "" {
		return fmt.Errorf("userID cannot be empty")
	}

	if progress.GoalID == "" {
		return fmt.Errorf("goalID cannot be empty")
	}

	// Fast path: Check buffer size under read lock (no contention)
	r.mu.RLock()
	currentSize := len(r.buffer)
	r.mu.RUnlock()

	// Layer 1: Backpressure (1.5x threshold)
	// Block and wait for flush instead of immediately dropping
	if currentSize >= r.backpressureThreshold {
		r.logger.Warn().
			Int("buffer_size", currentSize).
			Int("threshold", r.backpressureThreshold).
			Str("user_id", progress.UserID).
			Str("goal_id", progress.GoalID).
			Msg("Backpressure activated: waiting for flush")

		// Wait for next flush completion with timeout
		select {
		case <-r.flushCompleteCh:
			r.logger.Debug().
				Str("user_id", progress.UserID).
				Str("goal_id", progress.GoalID).
				Msg("Flush completed, proceeding with buffering")

		case <-time.After(5 * time.Second):
			// Timeout: Database likely down
			r.logger.Error().
				Int("buffer_size", currentSize).
				Str("user_id", progress.UserID).
				Str("goal_id", progress.GoalID).
				Msg("Backpressure timeout: database may be down")
			return fmt.Errorf("backpressure timeout after 5s: database unavailable")

		case <-ctx.Done():
			return fmt.Errorf("context canceled during backpressure: %w", ctx.Err())
		}
	}

	// Layer 2: Circuit Breaker (2.5x threshold)
	// Hard limit to prevent unbounded memory growth
	r.mu.Lock()

	if len(r.buffer) >= r.circuitBreakerThreshold {
		currentSize := len(r.buffer)
		r.mu.Unlock()

		r.logger.Error().
			Int("buffer_size", currentSize).
			Int("threshold", r.circuitBreakerThreshold).
			Str("user_id", progress.UserID).
			Str("goal_id", progress.GoalID).
			Msg("Circuit breaker activated: database is down, rejecting event")

		return fmt.Errorf("circuit breaker: buffer size %d exceeds max %d (database unavailable)",
			currentSize, r.circuitBreakerThreshold)
	}

	// Normal path: Buffer the update
	key := fmt.Sprintf("%s:%s", progress.UserID, progress.GoalID)
	r.buffer[key] = progress

	bufferSize := len(r.buffer)
	r.mu.Unlock()

	r.logger.Debug().
		Str("user_id", progress.UserID).
		Str("goal_id", progress.GoalID).
		Int("progress", progress.Progress).
		Str("status", string(progress.Status)).
		Int("buffer_size", bufferSize).
		Msg("Progress buffered")

	// Signal immediate flush if maxBufferSize reached
	if bufferSize >= r.maxBufferSize {
		r.logger.Warn().
			Int("buffer_size", bufferSize).
			Int("threshold", r.maxBufferSize).
			Msg("Buffer size threshold reached, signaling immediate flush")

		select {
		case r.immediateFlushCh <- struct{}{}:
			r.logger.Debug().Msg("Immediate flush signal sent")
		default:
			r.logger.Debug().Msg("Immediate flush already signaled, skipping")
		}
	}

	return nil
}

// IncrementProgress buffers an increment update for increment goals.
//
// Behavior:
// - Accumulates deltas for regular increment goals (e.g., 3 events → delta=3)
// - For daily increments: Only buffers once per day (client-side date checking)
// - Graceful degradation: If bufferIncrementDaily is full (200K entries), falls back to SQL date check
//
// This method is called by EventProcessor for increment-type goals.
func (r *BufferedRepository) IncrementProgress(ctx context.Context, userID, goalID, challengeID, namespace string,
	delta, targetValue int, isDailyIncrement bool) error {
	// Input validation
	if userID == "" {
		return fmt.Errorf("userID cannot be empty")
	}

	if goalID == "" {
		return fmt.Errorf("goalID cannot be empty")
	}

	if delta < 0 {
		return fmt.Errorf("delta cannot be negative: %d", delta)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := fmt.Sprintf("%s:%s", userID, goalID)

	if !isDailyIncrement {
		// Regular increment - accumulate deltas
		r.bufferIncrement[key] += delta
		return nil
	}

	// Daily increment - check if same day
	if r.shouldSkipDailyIncrement(key, userID, goalID) {
		return nil
	}

	// Store timestamp if buffer has space
	r.storeDailyTimestamp(key, userID, goalID)

	// Accumulate delta in buffer (not replace)
	// Important: If yesterday's flush failed and today's event arrives, we accumulate
	// to prevent data loss (yesterday's 1 + today's 1 = 2)
	r.bufferIncrement[key] += delta
	return nil
}

// shouldSkipDailyIncrement checks if a daily increment should be skipped (same day).
// Returns true if the last event was today, false otherwise.
func (r *BufferedRepository) shouldSkipDailyIncrement(key, userID, goalID string) bool {
	now := time.Now().UTC() // Always use UTC for consistency across timezones
	today := common.TruncateToDateUTC(now)

	lastEventTime, exists := r.bufferIncrementDaily[key]
	if !exists {
		return false
	}

	lastEventDate := common.TruncateToDateUTC(lastEventTime)
	if !lastEventDate.Equal(today) {
		return false
	}

	// Same day - skip buffering
	r.logger.Debug().
		Str("user_id", userID).
		Str("goal_id", goalID).
		Time("last_event", lastEventTime).
		Time("now", now).
		Msg("Skipping daily increment: same day")
	return true
}

// storeDailyTimestamp stores the timestamp for a daily increment with graceful degradation.
func (r *BufferedRepository) storeDailyTimestamp(key, userID, goalID string) {
	_, exists := r.bufferIncrementDaily[key]

	// Graceful degradation: Check if bufferIncrementDaily is at capacity
	if len(r.bufferIncrementDaily) >= 200000 && !exists {
		r.logger.Warn().
			Int("size", len(r.bufferIncrementDaily)).
			Str("user_id", userID).
			Str("goal_id", goalID).
			Msg("bufferIncrementDaily at capacity, relying on SQL date check")
		return
	}

	r.bufferIncrementDaily[key] = time.Now().UTC() // Always use UTC for consistency across timezones
}

// Flush writes all buffered updates to database using swap pattern and separate transactions.
//
// Behavior:
// - Swap pattern: Copies buffers and releases lock before processing (Decision Q2, Phase 5.2.2c)
// - Separate transactions: Absolute and increment buffers flushed independently (Decision Q12)
// - Partial success allowed: One buffer can succeed while other retries
// - On failure: Restores failed updates for retry on next flush
// - Signals completion: Wakes up goroutines waiting in backpressure
//
// This method is thread-safe and can be called from multiple goroutines.
func (r *BufferedRepository) Flush(ctx context.Context) error {
	// Signal waiting goroutines after flush completes
	defer func() {
		// Non-blocking signal to all waiters
		select {
		case r.flushCompleteCh <- struct{}{}:
			r.logger.Debug().Msg("Flush completion signal sent")
		default:
			// Channel full, signal already pending
		}
	}()

	r.mu.Lock()

	// Swap pattern: Copy all buffer references and create new empty buffers
	// This releases the lock immediately, allowing event processing to continue
	absoluteToFlush := r.buffer
	r.buffer = make(map[string]*domain.UserGoalProgress)

	incrementToFlush := r.bufferIncrement
	r.bufferIncrement = make(map[string]int)

	dailyToFlush := r.bufferIncrementDaily
	r.bufferIncrementDaily = make(map[string]time.Time)

	r.mu.Unlock() // ← Release lock BEFORE processing (Decision Q2, Phase 5.2.2c)

	// Early return if nothing to flush
	if len(absoluteToFlush) == 0 && len(incrementToFlush) == 0 {
		return nil
	}

	var flushErrors []error

	// 1. Flush absolute/daily goals (INDEPENDENT TRANSACTION)
	// Uses PostgreSQL COPY protocol for 5-10x faster performance
	if len(absoluteToFlush) > 0 {
		startTime := time.Now()
		absoluteUpdates := make([]*domain.UserGoalProgress, 0, len(absoluteToFlush))
		for _, progress := range absoluteToFlush {
			absoluteUpdates = append(absoluteUpdates, progress)
		}

		// Phase 2: Use COPY protocol for 5-10x faster flush (10-20ms vs 62-105ms)
		err := r.repo.BatchUpsertProgressWithCOPY(ctx, absoluteUpdates)
		if err != nil {
			r.logger.Error().
				Err(err).
				Int("count", len(absoluteUpdates)).
				Dur("duration", time.Since(startTime)).
				Msg("Failed to flush absolute updates with COPY, will retry")
			flushErrors = append(flushErrors, fmt.Errorf("absolute flush: %w", err))

			// Re-acquire lock to restore failed updates for retry
			r.mu.Lock()
			for key, progress := range absoluteToFlush {
				if _, exists := r.buffer[key]; !exists {
					r.buffer[key] = progress
				}
			}
			r.mu.Unlock()
		} else {
			duration := time.Since(startTime)
			r.logger.Info().
				Int("count", len(absoluteUpdates)).
				Int64("duration_ms", duration.Milliseconds()).
				Msg("Successfully flushed absolute updates with COPY")
			// Buffer already cleared via swap pattern (no action needed)
		}
	}

	// 2. Flush increment goals (INDEPENDENT TRANSACTION)
	if len(incrementToFlush) > 0 {
		err := r.flushIncrementBuffer(ctx, incrementToFlush, dailyToFlush)
		if err != nil {
			flushErrors = append(flushErrors, err)
		}
	}

	// Return combined errors (if any), but don't fail the flush entirely
	// This allows partial success: one buffer type can succeed while the other retries
	if len(flushErrors) > 0 {
		return fmt.Errorf("flush partial failure: %v", flushErrors)
	}

	return nil
}

// flushIncrementBuffer handles flushing of increment goals to the database.
// Extracted to reduce complexity of Flush method.
func (r *BufferedRepository) flushIncrementBuffer(ctx context.Context, incrementToFlush map[string]int, dailyToFlush map[string]time.Time) error {
	startTime := time.Now()

	// Collect all increments into batch array
	increments := make([]repository.ProgressIncrement, 0, len(incrementToFlush))

	for key, delta := range incrementToFlush {
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			r.logger.Warn().Str("key", key).Msg("Invalid buffer key format, skipping")
			continue
		}
		userID, goalID := parts[0], parts[1]

		// Look up goal metadata from cache
		goal := r.goalCache.GetGoalByID(goalID)
		if goal == nil {
			r.logger.Warn().Str("goalID", goalID).Msg("Goal not found for increment, skipping")
			continue
		}

		// Check if this is a daily increment goal
		isDailyIncrement := goal.Type == domain.GoalTypeIncrement && goal.Daily

		// Add to batch
		increments = append(increments, repository.ProgressIncrement{
			UserID:           userID,
			GoalID:           goalID,
			ChallengeID:      goal.ChallengeID,
			Namespace:        r.namespace,
			Delta:            delta,
			TargetValue:      goal.Requirement.TargetValue,
			IsDailyIncrement: isDailyIncrement,
		})
	}

	// Early return if no valid increments
	if len(increments) == 0 {
		return nil
	}

	// Batch increment all goals in single database query
	err := r.repo.BatchIncrementProgress(ctx, increments)
	if err != nil {
		r.logger.Error().
			Err(err).
			Int("count", len(increments)).
			Dur("duration", time.Since(startTime)).
			Msg("Failed to flush increment updates, will retry")

		// Re-acquire lock to restore failed updates for retry
		r.mu.Lock()
		for key, delta := range incrementToFlush {
			// Accumulate with any new deltas that arrived during flush
			r.bufferIncrement[key] += delta
		}
		// Restore daily tracking map (timestamp preservation - Decision Q5, Phase 5.2.2c)
		for key, timestamp := range dailyToFlush {
			if _, exists := r.bufferIncrementDaily[key]; !exists {
				r.bufferIncrementDaily[key] = timestamp // Keep original timestamp
			}
		}
		r.mu.Unlock()

		return fmt.Errorf("increment flush: %w", err)
	}

	duration := time.Since(startTime)
	r.logger.Info().
		Int("count", len(increments)).
		Int64("duration_ms", duration.Milliseconds()).
		Msg("Successfully flushed increment updates")
	return nil
}

// startFlusher runs the periodic flush loop in a background goroutine.
//
// This goroutine:
// - Waits for ticker events (time-based flush)
// - Waits for immediate flush signals (size-based flush)
// - Calls Flush() periodically or on-demand
// - Stops when stopCh is closed
//
// This design prevents goroutine proliferation by using a single long-running
// flusher instead of creating goroutines for each size-based flush.
func (r *BufferedRepository) startFlusher() {
	defer r.wg.Done()

	r.logger.Info().Msg("Background flusher started")

	for {
		select {
		case <-r.ticker.C:
			// Time-based flush (periodic)
			if err := r.Flush(context.Background()); err != nil {
				r.logger.Error().Err(err).Msg("Periodic flush failed")
			}

		case <-r.immediateFlushCh:
			// Size-based flush (immediate)
			r.logger.Debug().Msg("Immediate flush triggered by buffer size threshold")
			if err := r.Flush(context.Background()); err != nil {
				r.logger.Error().Err(err).Msg("Immediate flush failed")
			}

		case <-r.stopCh:
			r.logger.Info().Msg("Background flusher stopping")
			return
		}
	}
}

// startDailyBufferCleanup runs the periodic cleanup loop for bufferIncrementDaily.
//
// This goroutine:
// - Runs every hour
// - Removes entries older than 48 hours
// - Prevents unbounded memory growth
// - Stops when cleanupStopCh is closed
func (r *BufferedRepository) startDailyBufferCleanup() {
	defer r.wg.Done()

	r.logger.Info().Msg("Background daily buffer cleanup started")

	for {
		select {
		case <-r.cleanupTicker.C:
			r.cleanupOldDailyEntries()

		case <-r.cleanupStopCh:
			r.logger.Info().Msg("Background daily buffer cleanup stopping")
			return
		}
	}
}

// cleanupOldDailyEntries removes entries from bufferIncrementDaily older than 48 hours.
//
// This prevents unbounded memory growth while keeping recent entries for date checking.
// Cleanup runs every hour with 48-hour retention (keeps today + yesterday).
func (r *BufferedRepository) cleanupOldDailyEntries() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC() // Always use UTC for consistency across timezones
	cutoff := now.Add(-48 * time.Hour) // Keep last 2 days
	cleaned := 0

	for key, lastEventTime := range r.bufferIncrementDaily {
		if lastEventTime.Before(cutoff) {
			delete(r.bufferIncrementDaily, key)
			cleaned++
		}
	}

	if cleaned > 0 {
		r.logger.Info().
			Int("cleaned", cleaned).
			Int("remaining", len(r.bufferIncrementDaily)).
			Msg("Cleaned up old daily increment entries")
	}
}

// Close stops all background goroutines and performs a final flush.
//
// This should be called during application shutdown to ensure all buffered
// updates are written to the database.
func (r *BufferedRepository) Close() error {
	r.logger.Info().Msg("Closing BufferedRepository")

	// Stop the tickers
	r.ticker.Stop()
	r.cleanupTicker.Stop()

	// Signal goroutines to stop
	close(r.stopCh)
	close(r.cleanupStopCh)

	// Wait for all goroutines to finish
	r.wg.Wait()

	// Perform final flush
	if err := r.Flush(context.Background()); err != nil {
		r.logger.Error().Err(err).Msg("Final flush failed")
		return err
	}

	r.logger.Info().Msg("BufferedRepository closed")
	return nil
}

// GetBufferSize returns the current number of buffered updates.
// This is useful for monitoring and metrics.
func (r *BufferedRepository) GetBufferSize() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.buffer)
}

// GetFromBuffer retrieves a progress record from the buffer (if exists).
//
// Returns:
// - The buffered progress if exists
// - nil if not in buffer
//
// This is useful for EventProcessor to check latest buffered state
// when validating prerequisites.
func (r *BufferedRepository) GetFromBuffer(userID, goalID string) *domain.UserGoalProgress {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", userID, goalID)
	return r.buffer[key]
}

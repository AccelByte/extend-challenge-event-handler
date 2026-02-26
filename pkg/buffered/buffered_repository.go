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
	"github.com/AccelByte/extend-challenge-common/pkg/domain"
	"github.com/AccelByte/extend-challenge-common/pkg/repository"

	"github.com/rs/zerolog"
)

// Repository defines the interface for buffered repository operations.
// M5 Phase 2: Unified single-buffer interface using BufferedEvent.
type Repository interface {
	// UpdateProgress buffers a progress event (both absolute and increment events)
	UpdateProgress(ctx context.Context, event *domain.BufferedEvent) error

	// GetFromBuffer retrieves a buffered event from the buffer
	GetFromBuffer(userID, goalID string) *domain.BufferedEvent

	// Flush writes all buffered updates to database
	Flush(ctx context.Context) error

	// Close stops the flusher and performs final flush
	Close() error

	// GetBufferSize returns current buffer size
	GetBufferSize() int
}

// BufferedRepository implements a write-buffering layer over GoalRepository.
// M5 Phase 2: Unified single buffer carrying BufferedEvent through a single COPY flush path.
//
// Key Features:
// - Single buffer for all event types (absolute + increment)
// - IncValue accumulation for nil-Progress events (login increments)
// - SQL-side status computation (no Go-side status pre-computation)
// - Map-based deduplication: Only stores latest update per user-goal pair
// - Dual-flush mechanism: Time-based (periodic) + Size-based (threshold)
// - Thread-safe: Mutex protects buffer map
type BufferedRepository struct {
	// buffer stores pending events, key: "{user_id}:{goal_id}"
	buffer map[string]*domain.BufferedEvent

	// mu protects the buffer map from concurrent access
	mu sync.RWMutex

	// repo is the underlying GoalRepository for database operations
	repo repository.GoalRepository

	// goalCache is used to look up goal metadata during flush (enrichment)
	goalCache cache.GoalCache

	// namespace is the current deployment namespace
	namespace string

	// ticker triggers time-based flushes
	ticker *time.Ticker

	// maxBufferSize is the threshold for size-based flushes
	maxBufferSize int

	// flushInterval is the duration between time-based flushes
	flushInterval time.Duration

	// logger for structured logging
	logger zerolog.Logger

	// stopCh signals the flusher goroutine to stop
	stopCh chan struct{}

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
// M5 Phase 2: Single buffer, single flusher goroutine (no cleanup goroutine needed).
func NewBufferedRepository(
	repo repository.GoalRepository,
	goalCache cache.GoalCache,
	namespace string,
	flushInterval time.Duration,
	maxBufferSize int,
	logger zerolog.Logger,
) *BufferedRepository {
	r := &BufferedRepository{
		buffer:                  make(map[string]*domain.BufferedEvent),
		repo:                    repo,
		goalCache:               goalCache,
		namespace:               namespace,
		ticker:                  time.NewTicker(flushInterval),
		maxBufferSize:           maxBufferSize,
		flushInterval:           flushInterval,
		logger:                  logger,
		stopCh:                  make(chan struct{}),
		immediateFlushCh:        make(chan struct{}, 1),            // Buffered channel (size 1)
		flushCompleteCh:         make(chan struct{}, 1),            // Buffered to prevent signal loss
		backpressureThreshold:   int(float64(maxBufferSize) * 1.5), // 1.5x threshold
		circuitBreakerThreshold: int(float64(maxBufferSize) * 2.5), // 2.5x threshold
	}

	// Start background flusher
	r.wg.Add(1)
	go r.startFlusher()

	r.logger.Info().
		Dur("flush_interval", flushInterval).
		Int("max_buffer_size", maxBufferSize).
		Int("backpressure_threshold", r.backpressureThreshold).
		Int("circuit_breaker_threshold", r.circuitBreakerThreshold).
		Str("namespace", namespace).
		Msg("BufferedRepository initialized with unified COPY path")

	return r
}

// UpdateProgress buffers a progress event without immediately writing to database.
// M5 Phase 2: Accepts BufferedEvent. For nil-Progress events (login), accumulates IncValue.
// For non-nil Progress events (absolute), replaces previous entry (latest value wins).
func (r *BufferedRepository) UpdateProgress(ctx context.Context, event *domain.BufferedEvent) error {
	if event == nil {
		return fmt.Errorf("event cannot be nil")
	}

	if event.UserID == "" {
		return fmt.Errorf("userID cannot be empty")
	}

	if event.GoalID == "" {
		return fmt.Errorf("goalID cannot be empty")
	}

	// Fast path: Check buffer size under read lock (no contention)
	r.mu.RLock()
	currentSize := len(r.buffer)
	r.mu.RUnlock()

	// Layer 1: Backpressure (1.5x threshold)
	if currentSize >= r.backpressureThreshold {
		r.logger.Warn().
			Int("buffer_size", currentSize).
			Int("threshold", r.backpressureThreshold).
			Str("user_id", event.UserID).
			Str("goal_id", event.GoalID).
			Msg("Backpressure activated: waiting for flush")

		select {
		case <-r.flushCompleteCh:
			r.logger.Debug().
				Str("user_id", event.UserID).
				Str("goal_id", event.GoalID).
				Msg("Flush completed, proceeding with buffering")

		case <-time.After(5 * time.Second):
			r.logger.Error().
				Int("buffer_size", currentSize).
				Str("user_id", event.UserID).
				Str("goal_id", event.GoalID).
				Msg("Backpressure timeout: database may be down")
			return fmt.Errorf("backpressure timeout after 5s: database unavailable")

		case <-ctx.Done():
			return fmt.Errorf("context canceled during backpressure: %w", ctx.Err())
		}
	}

	// Layer 2: Circuit Breaker (2.5x threshold)
	r.mu.Lock()

	if len(r.buffer) >= r.circuitBreakerThreshold {
		currentSize := len(r.buffer)
		r.mu.Unlock()

		r.logger.Error().
			Int("buffer_size", currentSize).
			Int("threshold", r.circuitBreakerThreshold).
			Str("user_id", event.UserID).
			Str("goal_id", event.GoalID).
			Msg("Circuit breaker activated: database is down, rejecting event")

		return fmt.Errorf("circuit breaker: buffer size %d exceeds max %d (database unavailable)",
			currentSize, r.circuitBreakerThreshold)
	}

	// Normal path: Buffer the event
	key := fmt.Sprintf("%s:%s", event.UserID, event.GoalID)

	// IncValue accumulation for nil-Progress events (login increments)
	// When two login events arrive for the same user-goal between flushes,
	// accumulate IncValue instead of replacing (since latest value doesn't exist).
	if existing, ok := r.buffer[key]; ok && event.Progress == nil {
		existing.IncValue += event.IncValue
		r.mu.Unlock()

		r.logger.Debug().
			Str("user_id", event.UserID).
			Str("goal_id", event.GoalID).
			Int("accumulated_inc", existing.IncValue).
			Msg("Login event accumulated")

		return nil
	}

	r.buffer[key] = event

	bufferSize := len(r.buffer)
	r.mu.Unlock()

	r.logger.Debug().
		Str("user_id", event.UserID).
		Str("goal_id", event.GoalID).
		Int("buffer_size", bufferSize).
		Msg("Event buffered")

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

// Flush writes all buffered updates to database using swap pattern and unified COPY path.
// M5 Phase 2: Single flush path. Enriches events with goal cache metadata, builds CopyRows.
func (r *BufferedRepository) Flush(ctx context.Context) error {
	// Signal waiting goroutines after flush completes
	defer func() {
		select {
		case r.flushCompleteCh <- struct{}{}:
			r.logger.Debug().Msg("Flush completion signal sent")
		default:
		}
	}()

	r.mu.Lock()

	// Swap pattern: Copy buffer reference and create new empty buffer
	toFlush := r.buffer
	r.buffer = make(map[string]*domain.BufferedEvent)

	r.mu.Unlock()

	if len(toFlush) == 0 {
		return nil
	}

	startTime := time.Now()

	// Enrich events with goal cache metadata and build CopyRows
	now := time.Now().UTC()
	copyRows := make([]repository.CopyRow, 0, len(toFlush))
	for key, event := range toFlush {
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			r.logger.Warn().Str("key", key).Msg("Invalid buffer key format, skipping")
			continue
		}

		// Look up goal metadata from cache for enrichment
		goal := r.goalCache.GetGoalByID(event.GoalID)
		if goal == nil {
			r.logger.Warn().
				Str("goal_id", event.GoalID).
				Str("user_id", event.UserID).
				Msg("Goal not found in cache during flush enrichment, skipping")
			continue
		}

		copyRows = append(copyRows, EnrichCopyRow(event, goal, now))
	}

	if len(copyRows) == 0 {
		return nil
	}

	// Single COPY flush path for all event types
	err := r.repo.BatchUpsertProgressWithCOPY(ctx, copyRows)
	if err != nil {
		r.logger.Error().
			Err(err).
			Int("count", len(copyRows)).
			Dur("duration", time.Since(startTime)).
			Msg("Failed to flush updates with COPY, will retry")

		// Re-acquire lock to restore failed updates for retry
		r.mu.Lock()
		for key, event := range toFlush {
			if _, exists := r.buffer[key]; !exists {
				r.buffer[key] = event
			}
		}
		r.mu.Unlock()

		return fmt.Errorf("flush: %w", err)
	}

	duration := time.Since(startTime)
	r.logger.Info().
		Int("count", len(copyRows)).
		Int64("duration_ms", duration.Milliseconds()).
		Msg("Successfully flushed updates with unified COPY path")

	return nil
}

// startFlusher runs the periodic flush loop in a background goroutine.
func (r *BufferedRepository) startFlusher() {
	defer r.wg.Done()

	r.logger.Info().Msg("Background flusher started")

	for {
		select {
		case <-r.ticker.C:
			if err := r.Flush(context.Background()); err != nil {
				r.logger.Error().Err(err).Msg("Periodic flush failed")
			}

		case <-r.immediateFlushCh:
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

// Close stops the background flusher and performs a final flush.
func (r *BufferedRepository) Close() error {
	r.logger.Info().Msg("Closing BufferedRepository")

	// Stop the ticker
	r.ticker.Stop()

	// Signal goroutine to stop
	close(r.stopCh)

	// Wait for goroutine to finish
	r.wg.Wait()

	// Perform final flush
	if err := r.Flush(context.Background()); err != nil {
		r.logger.Error().Err(err).Msg("Final flush failed")
		return err
	}

	r.logger.Info().Msg("BufferedRepository closed")
	return nil
}

// GetBufferSize returns the current number of buffered events.
func (r *BufferedRepository) GetBufferSize() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.buffer)
}

// GetFromBuffer retrieves a buffered event from the buffer (if exists).
func (r *BufferedRepository) GetFromBuffer(userID, goalID string) *domain.BufferedEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", userID, goalID)
	return r.buffer[key]
}

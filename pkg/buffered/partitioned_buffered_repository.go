// Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package buffered

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/AccelByte/extend-challenge-common/pkg/cache"
	"github.com/AccelByte/extend-challenge-common/pkg/domain"
	"github.com/AccelByte/extend-challenge-common/pkg/repository"

	"github.com/rs/zerolog"
)

// PartitionedBufferedRepository implements parallel flush workers using hash-based partitioning.
//
// Architecture:
// - N independent BufferedRepository instances (partitions)
// - Each partition has its own flush goroutine (N parallel workers)
// - Updates routed by hash(user_id) % N for deterministic distribution
// - Each partition has bufferSize/N capacity
//
// Performance:
// - 6 workers = 6x flush throughput (6 x 2,700 updates/sec = 16,200 updates/sec)
// - 8 workers = 8x flush throughput (8 x 2,700 updates/sec = 21,600 updates/sec)
// - Supports 500 EPS (50,000 updates/sec with 100 goals per event)
//
// Consistency:
// - Same user always routed to same partition (hash-based)
// - Per-partition mutex prevents race conditions within partition
// - No cross-partition coordination needed
type PartitionedBufferedRepository struct {
	// partitions is the array of BufferedRepository instances (one per worker)
	partitions []*BufferedRepository

	// numWorkers is the number of parallel flush workers
	numWorkers int

	// logger for structured logging
	logger zerolog.Logger
}

// NewPartitionedBufferedRepository creates a new partitioned repository with N parallel flush workers.
//
// Parameters:
// - repo: Underlying GoalRepository for database operations
// - goalCache: Goal cache for looking up goal metadata during flush
// - namespace: Current deployment namespace
// - flushInterval: How often each partition flushes (e.g., 100*time.Millisecond)
// - totalBufferSize: Total buffer capacity across all partitions
// - numWorkers: Number of parallel flush workers (recommended: 6-8 for 500 EPS)
// - logger: Structured logger
//
// Design:
// - Each partition gets totalBufferSize/numWorkers capacity
// - Each partition has independent flush goroutine running at flushInterval
// - Updates routed by hash(user_id) % numWorkers
//
// Example:
//
//	repo := NewPartitionedBufferedRepository(
//	    postgresRepo, goalCache, "demo",
//	    100*time.Millisecond, 3000, 8, logger,
//	)
//	// Creates 8 workers, each with 375 buffer capacity
//	// Each worker flushes independently every 100ms
//	// Total throughput: 8 x 2,700 = 21,600 updates/sec
func NewPartitionedBufferedRepository(
	repo repository.GoalRepository,
	goalCache cache.GoalCache,
	namespace string,
	flushInterval time.Duration,
	totalBufferSize int,
	numWorkers int,
	logger zerolog.Logger,
) (*PartitionedBufferedRepository, error) {
	// Validation
	if numWorkers < 1 {
		return nil, fmt.Errorf("numWorkers must be >= 1, got: %d", numWorkers)
	}

	if numWorkers > 32 {
		return nil, fmt.Errorf("numWorkers must be <= 32, got: %d (too many workers)", numWorkers)
	}

	if totalBufferSize < numWorkers {
		return nil, fmt.Errorf("totalBufferSize (%d) must be >= numWorkers (%d)", totalBufferSize, numWorkers)
	}

	// Calculate buffer size per partition
	bufferSizePerPartition := totalBufferSize / numWorkers

	// Create partitions
	partitions := make([]*BufferedRepository, numWorkers)
	for i := 0; i < numWorkers; i++ {
		partitionLogger := logger.With().
			Int("partition_id", i).
			Int("partition_buffer_size", bufferSizePerPartition).
			Logger()

		partitions[i] = NewBufferedRepository(
			repo,
			goalCache,
			namespace,
			flushInterval,
			bufferSizePerPartition,
			partitionLogger,
		)
	}

	r := &PartitionedBufferedRepository{
		partitions: partitions,
		numWorkers: numWorkers,
		logger:     logger,
	}

	logger.Info().
		Int("num_workers", numWorkers).
		Int("buffer_size_per_partition", bufferSizePerPartition).
		Int("total_buffer_size", totalBufferSize).
		Dur("flush_interval", flushInterval).
		Str("namespace", namespace).
		Msg("PartitionedBufferedRepository initialized with parallel flush workers")

	return r, nil
}

// getPartition returns the partition index for a given user_id using consistent hashing.
//
// Algorithm: FNV-1a hash (fast, good distribution)
// Formula: hash(user_id) % numWorkers
//
// Properties:
// - Deterministic: Same user_id always routes to same partition
// - Uniform distribution: Users evenly distributed across partitions
// - Fast: ~100ns per hash
func (r *PartitionedBufferedRepository) getPartition(userID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID)) // fnv.Write never returns error
	// #nosec G115 -- numWorkers is validated to be 1-32, no overflow possible
	return int(h.Sum32() % uint32(r.numWorkers))
}

// UpdateProgress routes the update to the appropriate partition based on user_id hash.
//
// Behavior:
// - Routes by hash(user_id) % numWorkers
// - Same user always goes to same partition
// - Each partition handles update independently with its own backpressure/circuit breaker
func (r *PartitionedBufferedRepository) UpdateProgress(ctx context.Context, progress *domain.UserGoalProgress) error {
	if progress == nil {
		return fmt.Errorf("progress cannot be nil")
	}

	if progress.UserID == "" {
		return fmt.Errorf("userID cannot be empty")
	}

	partitionIdx := r.getPartition(progress.UserID)
	return r.partitions[partitionIdx].UpdateProgress(ctx, progress)
}

// IncrementProgress routes the increment update to the appropriate partition based on user_id hash.
func (r *PartitionedBufferedRepository) IncrementProgress(
	ctx context.Context,
	userID, goalID, challengeID, namespace string,
	delta, targetValue int,
	isDailyIncrement bool,
) error {
	if userID == "" {
		return fmt.Errorf("userID cannot be empty")
	}

	partitionIdx := r.getPartition(userID)
	return r.partitions[partitionIdx].IncrementProgress(ctx, userID, goalID, challengeID, namespace, delta, targetValue, isDailyIncrement)
}

// GetFromBuffer retrieves a progress record from the appropriate partition's buffer.
func (r *PartitionedBufferedRepository) GetFromBuffer(userID, goalID string) *domain.UserGoalProgress {
	if userID == "" {
		return nil
	}

	partitionIdx := r.getPartition(userID)
	return r.partitions[partitionIdx].GetFromBuffer(userID, goalID)
}

// Flush flushes all partitions.
//
// Note: Each partition already has its own independent flush goroutine running at flushInterval.
// This method is provided for manual flush (e.g., during shutdown via Close()).
//
// Behavior:
// - Calls Flush() on all partitions sequentially
// - Returns first error encountered (but continues flushing other partitions)
func (r *PartitionedBufferedRepository) Flush(ctx context.Context) error {
	var firstError error

	for i, partition := range r.partitions {
		if err := partition.Flush(ctx); err != nil {
			r.logger.Error().
				Err(err).
				Int("partition_id", i).
				Msg("Partition flush failed")

			if firstError == nil {
				firstError = err
			}
		}
	}

	return firstError
}

// Close stops all partition flush workers and performs final flush on all partitions.
//
// This ensures graceful shutdown with no data loss.
func (r *PartitionedBufferedRepository) Close() error {
	r.logger.Info().Msg("Closing PartitionedBufferedRepository")

	var firstError error

	for i, partition := range r.partitions {
		if err := partition.Close(); err != nil {
			r.logger.Error().
				Err(err).
				Int("partition_id", i).
				Msg("Failed to close partition")

			if firstError == nil {
				firstError = err
			}
		}
	}

	if firstError != nil {
		return firstError
	}

	r.logger.Info().Msg("PartitionedBufferedRepository closed successfully")
	return nil
}

// GetBufferSize returns the total number of buffered updates across all partitions.
func (r *PartitionedBufferedRepository) GetBufferSize() int {
	totalSize := 0
	for _, partition := range r.partitions {
		totalSize += partition.GetBufferSize()
	}
	return totalSize
}

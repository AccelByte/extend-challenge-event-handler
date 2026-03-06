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
// M5 Phase 2: Uses unified BufferedEvent and single COPY flush path per partition.
//
// Architecture:
// - N independent BufferedRepository instances (partitions)
// - Each partition has its own flush goroutine (N parallel workers)
// - Updates routed by hash(user_id) % N for deterministic distribution
// - Each partition has bufferSize/N capacity
type PartitionedBufferedRepository struct {
	// partitions is the array of BufferedRepository instances (one per worker)
	partitions []*BufferedRepository

	// numWorkers is the number of parallel flush workers
	numWorkers int

	// logger for structured logging
	logger zerolog.Logger
}

// NewPartitionedBufferedRepository creates a new partitioned repository with N parallel flush workers.
func NewPartitionedBufferedRepository(
	repo repository.GoalRepository,
	goalCache cache.GoalCache,
	namespace string,
	flushInterval time.Duration,
	totalBufferSize int,
	numWorkers int,
	logger zerolog.Logger,
) (*PartitionedBufferedRepository, error) {
	if numWorkers < 1 {
		return nil, fmt.Errorf("numWorkers must be >= 1, got: %d", numWorkers)
	}

	if numWorkers > 32 {
		return nil, fmt.Errorf("numWorkers must be <= 32, got: %d (too many workers)", numWorkers)
	}

	if totalBufferSize < numWorkers {
		return nil, fmt.Errorf("totalBufferSize (%d) must be >= numWorkers (%d)", totalBufferSize, numWorkers)
	}

	bufferSizePerPartition := totalBufferSize / numWorkers

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
func (r *PartitionedBufferedRepository) getPartition(userID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	// #nosec G115 -- numWorkers is validated to be 1-32, no overflow possible
	return int(h.Sum32() % uint32(r.numWorkers))
}

// UpdateProgress routes the event to the appropriate partition based on user_id hash.
func (r *PartitionedBufferedRepository) UpdateProgress(ctx context.Context, event *domain.BufferedEvent) error {
	if event == nil {
		return fmt.Errorf("event cannot be nil")
	}

	if event.UserID == "" {
		return fmt.Errorf("userID cannot be empty")
	}

	partitionIdx := r.getPartition(event.UserID)
	return r.partitions[partitionIdx].UpdateProgress(ctx, event)
}

// GetFromBuffer retrieves a buffered event from the appropriate partition's buffer.
func (r *PartitionedBufferedRepository) GetFromBuffer(userID, goalID string) *domain.BufferedEvent {
	if userID == "" {
		return nil
	}

	partitionIdx := r.getPartition(userID)
	return r.partitions[partitionIdx].GetFromBuffer(userID, goalID)
}

// Flush flushes all partitions.
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

// GetBufferSize returns the total number of buffered events across all partitions.
func (r *PartitionedBufferedRepository) GetBufferSize() int {
	totalSize := 0
	for _, partition := range r.partitions {
		totalSize += partition.GetBufferSize()
	}
	return totalSize
}

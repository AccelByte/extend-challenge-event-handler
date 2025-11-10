// Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package buffered

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/AccelByte/extend-challenge-common/pkg/domain"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPartitioned_NewPartitionedBufferedRepository(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Mock for Close
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, err := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	assert.NoError(t, err)
	assert.NotNil(t, repo)
	assert.Equal(t, 4, repo.numWorkers)
	assert.Equal(t, 4, len(repo.partitions))

	_ = repo.Close()
}

func TestPartitioned_ValidationErrors(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	tests := []struct {
		name      string
		workers   int
		bufSize   int
		wantError string
	}{
		{"numWorkers < 1", 0, 1000, "numWorkers must be >= 1"},
		{"numWorkers > 32", 33, 1000, "numWorkers must be <= 32"},
		{"buffer < workers", 10, 5, "totalBufferSize"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, err := NewPartitionedBufferedRepository(
				mockRepo, mockCache, "test-namespace",
				100*time.Millisecond, tt.bufSize, tt.workers, logger,
			)
			assert.Error(t, err)
			assert.Nil(t, repo)
			assert.Contains(t, err.Error(), tt.wantError)
		})
	}
}

func TestPartitioned_GetPartitionConsistency(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	// Same user always goes to same partition
	userID := "user123"
	p1 := repo.getPartition(userID)
	p2 := repo.getPartition(userID)
	p3 := repo.getPartition(userID)

	assert.Equal(t, p1, p2)
	assert.Equal(t, p1, p3)
	assert.GreaterOrEqual(t, p1, 0)
	assert.Less(t, p1, 4)

	_ = repo.Close()
}

func TestPartitioned_UpdateProgressRouting(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	progress := &domain.UserGoalProgress{
		UserID:      "user123",
		GoalID:      "goal1",
		ChallengeID: "challenge1",
		Namespace:   "test-namespace",
		Progress:    50,
		Status:      domain.GoalStatusInProgress,
	}

	err := repo.UpdateProgress(context.Background(), progress)
	assert.NoError(t, err)

	// Verify routing
	partition := repo.getPartition("user123")
	retrieved := repo.partitions[partition].GetFromBuffer("user123", "goal1")
	assert.NotNil(t, retrieved)
	assert.Equal(t, 50, retrieved.Progress)

	_ = repo.Close()
}

func TestPartitioned_IncrementProgressRouting(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	err := repo.IncrementProgress(
		context.Background(),
		"user456", "goal2", "challenge1", "test-namespace",
		5, 10, false,
	)
	assert.NoError(t, err)

	// Verify routing by checking partition
	partition := repo.getPartition("user456")
	assert.GreaterOrEqual(t, partition, 0)
	assert.Less(t, partition, 4)

	_ = repo.Close()
}

func TestPartitioned_GetBufferSize(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	assert.Equal(t, 0, repo.GetBufferSize())

	// Add updates
	for i := 0; i < 10; i++ {
		progress := &domain.UserGoalProgress{
			UserID:      "user" + string(rune(i)),
			GoalID:      "goal1",
			ChallengeID: "challenge1",
			Namespace:   "test-namespace",
			Progress:    i,
			Status:      domain.GoalStatusInProgress,
		}
		_ = repo.UpdateProgress(context.Background(), progress)
	}

	assert.Equal(t, 10, repo.GetBufferSize())
	_ = repo.Close()
}

func TestPartitioned_GetFromBuffer(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	// Add a progress entry
	progress := &domain.UserGoalProgress{
		UserID:      "user123",
		GoalID:      "goal1",
		ChallengeID: "challenge1",
		Namespace:   "test-namespace",
		Progress:    75,
		Status:      domain.GoalStatusInProgress,
	}
	_ = repo.UpdateProgress(context.Background(), progress)

	// Retrieve from buffer
	retrieved := repo.GetFromBuffer("user123", "goal1")
	assert.NotNil(t, retrieved)
	assert.Equal(t, 75, retrieved.Progress)
	assert.Equal(t, "goal1", retrieved.GoalID)

	// Test with non-existent entry
	notFound := repo.GetFromBuffer("user123", "nonexistent")
	assert.Nil(t, notFound)

	// Test with empty userID (error path)
	nilResult := repo.GetFromBuffer("", "goal1")
	assert.Nil(t, nilResult)

	_ = repo.Close()
}

func TestPartitioned_Flush(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Second, 1000, 4, logger,
	)

	// Add some progress entries
	for i := 0; i < 5; i++ {
		progress := &domain.UserGoalProgress{
			UserID:      "user" + string(rune(i)),
			GoalID:      "goal1",
			ChallengeID: "challenge1",
			Namespace:   "test-namespace",
			Progress:    i * 10,
			Status:      domain.GoalStatusInProgress,
		}
		_ = repo.UpdateProgress(context.Background(), progress)
	}

	// Verify buffer has entries
	assert.Greater(t, repo.GetBufferSize(), 0)

	// Manual flush
	err := repo.Flush(context.Background())
	assert.NoError(t, err)

	// Buffer should be empty after flush
	assert.Equal(t, 0, repo.GetBufferSize())

	_ = repo.Close()
}

func TestPartitioned_FlushError(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Mock flush to fail
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).
		Return(assert.AnError).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Second, 1000, 4, logger,
	)

	// Add progress entry
	progress := &domain.UserGoalProgress{
		UserID:      "user1",
		GoalID:      "goal1",
		ChallengeID: "challenge1",
		Namespace:   "test-namespace",
		Progress:    50,
		Status:      domain.GoalStatusInProgress,
	}
	_ = repo.UpdateProgress(context.Background(), progress)

	// Flush should return error
	err := repo.Flush(context.Background())
	assert.Error(t, err)

	_ = repo.Close()
}

func TestPartitioned_CloseError(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Mock to cause close error
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).
		Return(assert.AnError).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	// Add progress entry so flush has something to fail on
	progress := &domain.UserGoalProgress{
		UserID:      "user1",
		GoalID:      "goal1",
		ChallengeID: "challenge1",
		Namespace:   "test-namespace",
		Progress:    50,
		Status:      domain.GoalStatusInProgress,
	}
	_ = repo.UpdateProgress(context.Background(), progress)

	// Close should return error from partition close
	err := repo.Close()
	assert.Error(t, err)
}

func TestPartitioned_UpdateProgressError(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)
	defer func() { _ = repo.Close() }()

	// Test with nil progress (error path)
	err := repo.UpdateProgress(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "progress cannot be nil")
}

func TestPartitioned_IncrementProgressError(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockRepo.On("BatchIncrementProgress", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)
	defer func() { _ = repo.Close() }()

	// Test with empty userID (error path)
	err := repo.IncrementProgress(
		context.Background(),
		"", "goal1", "challenge1", "test-namespace",
		5, 10, false,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "userID cannot be empty")
}

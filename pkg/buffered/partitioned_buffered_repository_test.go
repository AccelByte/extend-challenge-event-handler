// Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package buffered

import (
	"context"
	"fmt"
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
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	event := &domain.BufferedEvent{
		UserID:       "user123",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test-namespace",
		Progress:     intPtr(50),
		IncValue:     1,
		ProgressMode: domain.ProgressModeAbsolute,
	}

	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)

	// Verify routing
	partition := repo.getPartition("user123")
	retrieved := repo.partitions[partition].GetFromBuffer("user123", "goal1")
	assert.NotNil(t, retrieved)
	assert.NotNil(t, retrieved.Progress)
	assert.Equal(t, 50, *retrieved.Progress)

	_ = repo.Close()
}

func TestPartitioned_UpdateProgressRouting_LoginEvent(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	// Login event: Progress is nil, IncValue >= 1
	event := &domain.BufferedEvent{
		UserID:       "user456",
		GoalID:       "goal2",
		ChallengeID:  "challenge1",
		Namespace:    "test-namespace",
		Progress:     nil,
		IncValue:     1,
		ProgressMode: domain.ProgressModeRelative,
	}

	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)

	// Verify routing by checking partition
	partition := repo.getPartition("user456")
	retrieved := repo.partitions[partition].GetFromBuffer("user456", "goal2")
	assert.NotNil(t, retrieved)
	assert.Nil(t, retrieved.Progress)
	assert.Equal(t, 1, retrieved.IncValue)

	_ = repo.Close()
}

func TestPartitioned_GetBufferSize(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	assert.Equal(t, 0, repo.GetBufferSize())

	// Add updates
	for i := 0; i < 10; i++ {
		event := &domain.BufferedEvent{
			UserID:       fmt.Sprintf("user%d", i),
			GoalID:       "goal1",
			ChallengeID:  "challenge1",
			Namespace:    "test-namespace",
			Progress:     intPtr(i),
			IncValue:     1,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		_ = repo.UpdateProgress(context.Background(), event)
	}

	assert.Equal(t, 10, repo.GetBufferSize())
	_ = repo.Close()
}

func TestPartitioned_GetFromBuffer(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	// Add a progress entry
	event := &domain.BufferedEvent{
		UserID:       "user123",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test-namespace",
		Progress:     intPtr(75),
		IncValue:     1,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	_ = repo.UpdateProgress(context.Background(), event)

	// Retrieve from buffer
	retrieved := repo.GetFromBuffer("user123", "goal1")
	assert.NotNil(t, retrieved)
	assert.NotNil(t, retrieved.Progress)
	assert.Equal(t, 75, *retrieved.Progress)
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
	mockCache.On("GetGoalByID", mock.Anything).Return(&domain.Goal{
		ID:          "goal1",
		Requirement: domain.Requirement{TargetValue: 100, ProgressMode: domain.ProgressModeAbsolute},
	}).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Second, 1000, 4, logger,
	)

	// Add some progress entries
	for i := 0; i < 5; i++ {
		event := &domain.BufferedEvent{
			UserID:       fmt.Sprintf("user%d", i),
			GoalID:       "goal1",
			ChallengeID:  "challenge1",
			Namespace:    "test-namespace",
			Progress:     intPtr(i * 10),
			IncValue:     1,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		_ = repo.UpdateProgress(context.Background(), event)
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
	mockCache.On("GetGoalByID", mock.Anything).Return(&domain.Goal{
		ID:          "goal1",
		Requirement: domain.Requirement{TargetValue: 100, ProgressMode: domain.ProgressModeAbsolute},
	}).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Second, 1000, 4, logger,
	)

	// Add progress entry
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test-namespace",
		Progress:     intPtr(50),
		IncValue:     1,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	_ = repo.UpdateProgress(context.Background(), event)

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
	mockCache.On("GetGoalByID", mock.Anything).Return(&domain.Goal{
		ID:          "goal1",
		Requirement: domain.Requirement{TargetValue: 100, ProgressMode: domain.ProgressModeAbsolute},
	}).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)

	// Add progress entry so flush has something to fail on
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test-namespace",
		Progress:     intPtr(50),
		IncValue:     1,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	_ = repo.UpdateProgress(context.Background(), event)

	// Close should return error from partition close
	err := repo.Close()
	assert.Error(t, err)
}

func TestPartitioned_UpdateProgressError(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)
	defer func() { _ = repo.Close() }()

	// Test with nil event (error path)
	err := repo.UpdateProgress(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "event cannot be nil")
}

func TestPartitioned_UpdateProgressEmptyUserID(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return((*domain.Goal)(nil)).Maybe()

	repo, _ := NewPartitionedBufferedRepository(
		mockRepo, mockCache, "test-namespace",
		100*time.Millisecond, 1000, 4, logger,
	)
	defer func() { _ = repo.Close() }()

	// Test with empty userID (error path)
	err := repo.UpdateProgress(context.Background(), &domain.BufferedEvent{
		UserID:       "",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test-namespace",
		Progress:     intPtr(5),
		IncValue:     1,
		ProgressMode: domain.ProgressModeAbsolute,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "userID cannot be empty")
}

// intPtr is a helper that returns a pointer to the given int value.
// This is defined here in case it is not already provided in the same package.
// If buffered_repository_test.go also defines it, one of them should be removed.
func intPtr(v int) *int { return &v }

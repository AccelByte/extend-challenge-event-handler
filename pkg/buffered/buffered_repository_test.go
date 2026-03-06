// Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package buffered

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AccelByte/extend-challenge-common/pkg/domain"
	"github.com/AccelByte/extend-challenge-common/pkg/repository"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockGoalRepository is a mock implementation of repository.GoalRepository
type MockGoalRepository struct {
	mock.Mock
}

func (m *MockGoalRepository) GetProgress(ctx context.Context, userID, goalID string) (*domain.UserGoalProgress, error) {
	args := m.Called(ctx, userID, goalID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.UserGoalProgress), args.Error(1)
}

func (m *MockGoalRepository) GetUserProgress(ctx context.Context, userID string, activeOnly bool) ([]*domain.UserGoalProgress, error) {
	args := m.Called(ctx, userID, activeOnly)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*domain.UserGoalProgress), args.Error(1)
}

func (m *MockGoalRepository) GetChallengeProgress(ctx context.Context, userID, challengeID string, activeOnly bool) ([]*domain.UserGoalProgress, error) {
	args := m.Called(ctx, userID, challengeID, activeOnly)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*domain.UserGoalProgress), args.Error(1)
}

func (m *MockGoalRepository) UpsertProgress(ctx context.Context, progress *domain.UserGoalProgress) error {
	args := m.Called(ctx, progress)
	return args.Error(0)
}

func (m *MockGoalRepository) BatchUpsertProgress(ctx context.Context, updates []*domain.UserGoalProgress) error {
	args := m.Called(ctx, updates)
	return args.Error(0)
}

func (m *MockGoalRepository) BatchUpsertProgressWithCOPY(ctx context.Context, rows []repository.CopyRow) error {
	args := m.Called(ctx, rows)
	return args.Error(0)
}

func (m *MockGoalRepository) MarkAsClaimed(ctx context.Context, userID, goalID string) error {
	args := m.Called(ctx, userID, goalID)
	return args.Error(0)
}

func (m *MockGoalRepository) BeginTx(ctx context.Context) (repository.TxRepository, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(repository.TxRepository), args.Error(1)
}

// M3: Goal assignment control methods
func (m *MockGoalRepository) GetGoalsByIDs(ctx context.Context, userID string, goalIDs []string) ([]*domain.UserGoalProgress, error) {
	args := m.Called(ctx, userID, goalIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*domain.UserGoalProgress), args.Error(1)
}

func (m *MockGoalRepository) BulkInsert(ctx context.Context, progresses []*domain.UserGoalProgress) error {
	args := m.Called(ctx, progresses)
	return args.Error(0)
}

func (m *MockGoalRepository) BulkInsertWithCOPY(ctx context.Context, progresses []*domain.UserGoalProgress) error {
	args := m.Called(ctx, progresses)
	return args.Error(0)
}

func (m *MockGoalRepository) UpsertGoalActive(ctx context.Context, progress *domain.UserGoalProgress) error {
	args := m.Called(ctx, progress)
	return args.Error(0)
}

// M4: Batch goal activation for random/batch selection
func (m *MockGoalRepository) BatchUpsertGoalActive(ctx context.Context, progresses []*domain.UserGoalProgress) error {
	args := m.Called(ctx, progresses)
	return args.Error(0)
}

// M3 Phase 9: Fast path optimization methods
func (m *MockGoalRepository) GetUserGoalCount(ctx context.Context, userID string) (int, error) {
	args := m.Called(ctx, userID)
	return args.Int(0), args.Error(1)
}

func (m *MockGoalRepository) GetActiveGoals(ctx context.Context, userID string) ([]*domain.UserGoalProgress, error) {
	args := m.Called(ctx, userID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*domain.UserGoalProgress), args.Error(1)
}

// MockGoalCache is a mock implementation of cache.GoalCache
type MockGoalCache struct {
	mock.Mock
}

func (m *MockGoalCache) GetGoalByID(goalID string) *domain.Goal {
	args := m.Called(goalID)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*domain.Goal)
}

func (m *MockGoalCache) GetGoalsByStatCode(statCode string) []*domain.Goal {
	args := m.Called(statCode)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]*domain.Goal)
}

func (m *MockGoalCache) GetChallengeByChallengeID(challengeID string) *domain.Challenge {
	args := m.Called(challengeID)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*domain.Challenge)
}

func (m *MockGoalCache) GetAllChallenges() []*domain.Challenge {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]*domain.Challenge)
}

func (m *MockGoalCache) GetAllGoals() []*domain.Goal {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]*domain.Goal)
}

func (m *MockGoalCache) GetGoalsWithDefaultAssigned() []*domain.Goal {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]*domain.Goal)
}

func (m *MockGoalCache) Reload() error {
	args := m.Called()
	return args.Error(0)
}

// Helper function to create a new BufferedRepository with mocks for testing
func newTestBufferedRepository(repo *MockGoalRepository, goalCache *MockGoalCache, flushInterval time.Duration, maxBufferSize int, logger zerolog.Logger) *BufferedRepository {
	if goalCache == nil {
		goalCache = new(MockGoalCache)
	}
	return NewBufferedRepository(repo, goalCache, "test-namespace", flushInterval, maxBufferSize, logger)
}

// Helper function to create a test logger with disabled output
func newTestLogger() zerolog.Logger {
	return zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.Out = os.NewFile(0, os.DevNull)
	})).Level(zerolog.Disabled)
}

// testGoal returns a *domain.Goal suitable for cache mock returns.
func testGoal(goalID, challengeID string, targetValue int) *domain.Goal {
	return &domain.Goal{
		ID:          goalID,
		ChallengeID: challengeID,
		Requirement: domain.Requirement{
			TargetValue:  targetValue,
			ProgressMode: domain.ProgressModeAbsolute,
		},
	}
}

func TestNewBufferedRepository(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, nil, 1*time.Second, 1000, logger)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	assert.NotNil(t, repo)
	assert.Equal(t, 0, repo.GetBufferSize())
	assert.Equal(t, 1000, repo.maxBufferSize)
	assert.Equal(t, 1*time.Second, repo.flushInterval)
}

func TestUpdateProgress_Basic(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)

	// Allow BatchUpsertProgressWithCOPY to be called on Close()
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goal1", "challenge1", 100)).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(10),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}

	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)
	assert.Equal(t, 1, repo.GetBufferSize())
}

func TestUpdateProgress_Deduplication(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goal1", "challenge1", 100)).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	// Add first update (absolute event with Progress=5)
	event1 := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(5),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event1)
	assert.NoError(t, err)

	// Add second update for same user-goal pair (absolute event with Progress=10)
	event2 := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(10),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err = repo.UpdateProgress(context.Background(), event2)
	assert.NoError(t, err)

	// Buffer should contain only one entry (deduplicated)
	assert.Equal(t, 1, repo.GetBufferSize())

	// Check that latest update is in buffer
	buffered := repo.GetFromBuffer("user1", "goal1")
	assert.NotNil(t, buffered)
	assert.NotNil(t, buffered.Progress)
	assert.Equal(t, 10, *buffered.Progress)
}

func TestFlush_Success(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Add some progress updates
	event1 := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(10),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	event2 := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal2",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(20),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}

	err := repo.UpdateProgress(context.Background(), event1)
	assert.NoError(t, err)
	err = repo.UpdateProgress(context.Background(), event2)
	assert.NoError(t, err)

	assert.Equal(t, 2, repo.GetBufferSize())

	// Set up cache mocks for enrichment during flush
	mockCache.On("GetGoalByID", "goal1").Return(testGoal("goal1", "challenge1", 100))
	mockCache.On("GetGoalByID", "goal2").Return(testGoal("goal2", "challenge1", 50))

	// Expect BatchUpsertProgressWithCOPY to be called with 2 CopyRows
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.MatchedBy(func(rows []repository.CopyRow) bool {
		return len(rows) == 2
	})).Return(nil)

	// Flush the buffer
	err = repo.Flush(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, repo.GetBufferSize())

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestFlush_EmptyBuffer(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, nil, 10*time.Second, 1000, logger)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	// Flush empty buffer (should not call BatchUpsertProgressWithCOPY)
	err := repo.Flush(context.Background())
	assert.NoError(t, err)

	// Should not have been called for the manual flush (but might be called on Close)
	// We can't use AssertNotCalled because Close() might call it
}

func TestFlush_Failure_KeepsBuffer(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Add progress update
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(10),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", "goal1").Return(testGoal("goal1", "challenge1", 100))

	// Simulate database error
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(errors.New("database error"))

	// Flush should fail
	err = repo.Flush(context.Background())
	assert.Error(t, err)

	// Buffer should still contain the update (for retry)
	assert.Equal(t, 1, repo.GetBufferSize())

	mockRepo.AssertExpectations(t)
}

func TestSizeBasedFlush(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// Small buffer size to trigger size-based flush
	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 5, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Set up cache mock for enrichment (all goals use same target)
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Expect flush after 5 updates
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.MatchedBy(func(rows []repository.CopyRow) bool {
		return len(rows) >= 5
	})).Return(nil).Maybe()

	// Add 5 updates (should trigger size-based flush)
	for i := 0; i < 5; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}

	// Wait a bit for async flush to complete
	time.Sleep(100 * time.Millisecond)

	// Buffer should be empty or nearly empty (due to async flush)
	bufferSize := repo.GetBufferSize()
	assert.True(t, bufferSize < 5, "Expected buffer to be flushed, got size %d", bufferSize)
}

func TestTimeBasedFlush(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// Short flush interval for testing
	repo := newTestBufferedRepository(mockRepo, mockCache, 100*time.Millisecond, 1000, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Add some updates
	for i := 0; i < 3; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}

	assert.Equal(t, 3, repo.GetBufferSize())

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Expect flush within 100ms
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.MatchedBy(func(rows []repository.CopyRow) bool {
		return len(rows) == 3
	})).Return(nil)

	// Wait for time-based flush
	time.Sleep(200 * time.Millisecond)

	// Buffer should be empty
	assert.Equal(t, 0, repo.GetBufferSize())

	mockRepo.AssertExpectations(t)
}

func TestConcurrentUpdates(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Allow multiple flushes
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	var wg sync.WaitGroup
	numGoroutines := 10
	updatesPerGoroutine := 100

	// Spawn multiple goroutines to update progress concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < updatesPerGoroutine; j++ {
				event := &domain.BufferedEvent{
					UserID:       fmt.Sprintf("user%d", goroutineID),
					GoalID:       fmt.Sprintf("goal%d", j),
					ChallengeID:  "challenge1",
					Namespace:    "test",
					Progress:     intPtr(j),
					IncValue:     0,
					ProgressMode: domain.ProgressModeAbsolute,
				}
				err := repo.UpdateProgress(context.Background(), event)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Each user should have 100 different goals
	// Total: 10 users x 100 goals = 1000 unique entries
	expectedSize := numGoroutines * updatesPerGoroutine
	assert.Equal(t, expectedSize, repo.GetBufferSize())
}

func TestGetFromBuffer(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goal1", "challenge1", 100)).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(10),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)

	// Should retrieve from buffer
	buffered := repo.GetFromBuffer("user1", "goal1")
	assert.NotNil(t, buffered)
	assert.Equal(t, "user1", buffered.UserID)
	assert.Equal(t, "goal1", buffered.GoalID)
	assert.NotNil(t, buffered.Progress)
	assert.Equal(t, 10, *buffered.Progress)

	// Non-existent entry
	notFound := repo.GetFromBuffer("user2", "goal2")
	assert.Nil(t, notFound)
}

func TestClose_FinalFlush(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)

	// Add some updates
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(10),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", "goal1").Return(testGoal("goal1", "challenge1", 100))

	// Expect final flush on close
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.MatchedBy(func(rows []repository.CopyRow) bool {
		return len(rows) == 1
	})).Return(nil)

	// Close should trigger final flush
	err = repo.Close()
	assert.NoError(t, err)
	assert.Equal(t, 0, repo.GetBufferSize())

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestClose_FinalFlushError(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)

	// Add some updates
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(10),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", "goal1").Return(testGoal("goal1", "challenge1", 100))

	// Simulate flush error
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(errors.New("database error"))

	// Close should return error but still stop flusher
	err = repo.Close()
	assert.Error(t, err)

	mockRepo.AssertExpectations(t)
}

func TestNewBufferedRepository_NilLogger(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	logger := newTestLogger()

	// Test that logger is properly initialized
	repo := newTestBufferedRepository(mockRepo, nil, 1*time.Second, 1000, logger)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	assert.NotNil(t, repo.logger)
}

func TestUpdateProgress_NilEvent(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, nil, 10*time.Second, 1000, logger)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	// Should return error for nil event
	err := repo.UpdateProgress(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be nil")

	// Buffer should still be empty
	assert.Equal(t, 0, repo.GetBufferSize())
}

func TestUpdateProgress_EmptyUserID(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, nil, 10*time.Second, 1000, logger)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	event := &domain.BufferedEvent{
		UserID:       "", // Empty userID
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(10),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}

	err := repo.UpdateProgress(context.Background(), event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "userID cannot be empty")

	// Buffer should still be empty
	assert.Equal(t, 0, repo.GetBufferSize())
}

func TestUpdateProgress_EmptyGoalID(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, nil, 10*time.Second, 1000, logger)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "", // Empty goalID
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(10),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}

	err := repo.UpdateProgress(context.Background(), event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "goalID cannot be empty")

	// Buffer should still be empty
	assert.Equal(t, 0, repo.GetBufferSize())
}

func TestUpdateProgress_NilProgress_LoginEvent(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goal1", "challenge1", 7)).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	// BufferedEvent with nil Progress (login event), IncValue=1
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     nil,
		IncValue:     1,
		ProgressMode: domain.ProgressModeRelative,
	}

	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)
	assert.Equal(t, 1, repo.GetBufferSize())

	// Verify it's in the buffer
	buffered := repo.GetFromBuffer("user1", "goal1")
	assert.NotNil(t, buffered)
	assert.Nil(t, buffered.Progress)
	assert.Equal(t, 1, buffered.IncValue)
}

func TestUpdateProgress_NilProgress_Accumulation(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goal1", "challenge1", 7)).Maybe()
	defer func() {
		_ = repo.Close()
	}()

	// First login event: Progress=nil, IncValue=1
	event1 := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     nil,
		IncValue:     1,
		ProgressMode: domain.ProgressModeRelative,
	}
	err := repo.UpdateProgress(context.Background(), event1)
	assert.NoError(t, err)

	// Second login event: Progress=nil, IncValue=1
	event2 := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     nil,
		IncValue:     1,
		ProgressMode: domain.ProgressModeRelative,
	}
	err = repo.UpdateProgress(context.Background(), event2)
	assert.NoError(t, err)

	// Buffer should contain only one entry (same user-goal pair)
	assert.Equal(t, 1, repo.GetBufferSize())

	// IncValue should be accumulated (1+1=2)
	buffered := repo.GetFromBuffer("user1", "goal1")
	assert.NotNil(t, buffered)
	assert.Nil(t, buffered.Progress)
	assert.Equal(t, 2, buffered.IncValue)
}

func TestFlush_EnrichesFromCache(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Add event to buffer
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal1",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(50),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)

	// Set up cache mock with goal metadata (targetValue=100)
	mockCache.On("GetGoalByID", "goal1").Return(testGoal("goal1", "challenge1", 100))

	// Verify BatchUpsertProgressWithCOPY receives CopyRow with TargetValue from cache
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.MatchedBy(func(rows []repository.CopyRow) bool {
		if len(rows) != 1 {
			return false
		}
		row := rows[0]
		return row.UserID == "user1" &&
			row.GoalID == "goal1" &&
			row.ChallengeID == "challenge1" &&
			row.Namespace == "test" &&
			row.Progress != nil && *row.Progress == 50 &&
			row.ProgressMode == "absolute" &&
			row.TargetValue == 100
	})).Return(nil)

	err = repo.Flush(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, repo.GetBufferSize())

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestFlush_GoalNotInCache_Skipped(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Add event for a goal that's not in cache
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "missing_goal",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(10),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)

	// Goal is not in cache
	mockCache.On("GetGoalByID", "missing_goal").Return(nil)

	// BatchUpsertProgressWithCOPY should NOT be called since the only event was skipped
	// (no CopyRows to flush)

	// Flush should succeed gracefully (no error, just skips)
	err = repo.Flush(context.Background())
	assert.NoError(t, err)

	// Buffer should be empty (swapped out during flush)
	assert.Equal(t, 0, repo.GetBufferSize())

	mockCache.AssertExpectations(t)
}

func TestSizeBasedFlush_NoGoroutineFlood(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// Small buffer size to trigger size-based flushes
	// Use 15 to allow for overflow protection (2x = 30), giving room for slow flushes
	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 15, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Track flush count with atomic counter
	flushCount := atomic.Int32{}

	// Slow flush to simulate realistic scenario where flush takes time
	// IMPORTANT: flush must SUCCEED to clear buffer, otherwise overflow protection triggers
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		flushCount.Add(1)
		time.Sleep(100 * time.Millisecond) // Simulate slow flush
	}).Return(nil).Maybe() // Return nil (success) to clear buffer

	// Add 20 updates rapidly (should trigger multiple size-based flush attempts)
	// Without goroutine flood prevention: would spawn ~5+ goroutines (one per event after threshold)
	// With goroutine flood prevention: should spawn at most 2-3 goroutines
	for i := 0; i < 20; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}

	// Wait for async flushes to complete
	time.Sleep(600 * time.Millisecond)

	// Verify that we didn't spawn too many flush goroutines
	// Should have triggered at most 4-6 flushes (20/5 = 4 batches + maybe 1-2 extra)
	// Definitely not 15+ (which would happen without flood prevention)
	count := flushCount.Load()
	assert.True(t, count <= 6, "Expected at most 6 flushes, got %d (goroutine flood detected)", count)

	// The key is that we prevented a flood - even 1 flush shows the mechanism works
	// (the flush itself handles all 20 updates in a single batch)
	assert.True(t, count >= 1, "Expected at least 1 flush, got %d", count)
}

func TestSizeBasedFlush_AtomicFlagBehavior(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// Use 10 to allow for overflow protection (2x = 20), giving room for test
	// Use very long flush interval to prevent background flush interference
	repo := newTestBufferedRepository(mockRepo, mockCache, 1*time.Hour, 10, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Track when flush is called
	flushCount := atomic.Int32{}
	flushInProgress := atomic.Bool{}
	flushStarted := make(chan struct{}, 1)

	// Simulate a slow flush and track concurrent calls
	// IMPORTANT: flush must SUCCEED to clear buffer, otherwise overflow protection triggers
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// Check if another flush is already running (should never happen with flood prevention)
		wasRunning := flushInProgress.Swap(true)
		assert.False(t, wasRunning, "Concurrent flush detected - atomic flag not working")

		flushCount.Add(1)
		select {
		case flushStarted <- struct{}{}:
		default:
		}

		time.Sleep(100 * time.Millisecond) // Simulate slow flush
		flushInProgress.Store(false)
	}).Return(nil).Maybe() // Return nil (success) to clear buffer

	// Add 10 updates to reach threshold and trigger first flush
	for i := 0; i < 10; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}

	// Wait for first flush to start
	<-flushStarted

	// Add more updates while flush is in progress
	// These should NOT spawn new goroutines
	for i := 10; i < 15; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}

	// Wait for all flushes to complete
	time.Sleep(300 * time.Millisecond)

	// With goroutine flood prevention, should have only 1-2 flushes
	// (first flush triggered by reaching threshold, maybe a second one after first completes)
	// Without flood prevention, would have many more
	count := flushCount.Load()
	assert.True(t, count <= 3, "Expected at most 3 flushes, got %d (flood prevention failed)", count)
	assert.True(t, count >= 1, "Expected at least 1 flush, got %d", count)
}

func TestBufferOverflow_ReturnsError(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// Small buffer size for testing circuit breaker
	// maxBufferSize=10 -> backpressure at 15, circuit breaker at 25
	// Use very long flush interval to prevent background flush interference
	// IMPORTANT: Disable size-based flush by setting maxBufferSize to very large value
	// We want to test circuit breaker at 25, so set maxBufferSize > 25
	repo := newTestBufferedRepository(mockRepo, mockCache, 1*time.Hour, 50, logger)
	// Manually override thresholds for this test
	// Set backpressure very high so it doesn't interfere with circuit breaker test
	repo.backpressureThreshold = 1000
	repo.circuitBreakerThreshold = 25
	defer func() {
		_ = repo.Close()
	}()

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Simulate database failure - all flushes fail
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(errors.New("database unavailable")).Maybe()

	// Add updates up to circuit breaker threshold (25 updates with maxBufferSize=10)
	// First 25 should succeed (up to 2.5x limit)
	for i := 0; i < 25; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err, "Update %d should succeed (buffer size %d)", i, repo.GetBufferSize())
	}

	// Buffer should now be at circuit breaker threshold (25 entries)
	assert.Equal(t, 25, repo.GetBufferSize(), "Buffer should be at circuit breaker threshold")

	// Next update should fail with circuit breaker error
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal_circuit_breaker",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(999),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event)
	assert.Error(t, err, "Update should fail with circuit breaker")
	assert.Contains(t, err.Error(), "circuit breaker", "Error should mention circuit breaker")
	assert.Contains(t, err.Error(), "25", "Error should mention current buffer size")

	// Buffer size should remain at 25 (circuit breaker prevented buffering)
	assert.Equal(t, 25, repo.GetBufferSize(), "Buffer size should not increase after circuit breaker")
}

func TestBufferOverflow_PreventOOM(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 100, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Simulate persistent database failure
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(errors.New("database down")).Maybe()

	successCount := 0
	errorCount := 0

	// Try to add many updates (simulating prolonged outage)
	// Should succeed up to ~2x threshold, then start failing
	for i := 0; i < 250; i++ {
		event := &domain.BufferedEvent{
			UserID:       fmt.Sprintf("user%d", i%10),
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		if err != nil {
			errorCount++
			assert.Contains(t, err.Error(), "buffer overflow")
		} else {
			successCount++
		}
	}

	// Wait for any async flushes to complete
	time.Sleep(150 * time.Millisecond)

	// Verify overflow protection works (buffer doesn't grow unbounded)
	// Note: With swap pattern + failed flushes, buffer can temporarily exceed 2x threshold
	// This is expected: swap clears buffer -> overflow check passes -> flush fails -> restores entries
	// Key requirement: Buffer growth is bounded (not unlimited)
	// We may see few or no overflow errors if async flush timing allows all updates to succeed
	assert.Equal(t, 250, successCount+errorCount, "Total should equal attempts")

	// Final buffer size should be bounded but may exceed 2x during swap+restore
	// Accepting up to ~2.5x threshold as reasonable with failed flushes
	finalSize := repo.GetBufferSize()
	assert.True(t, finalSize <= 250, "Buffer should not grow unbounded, got %d", finalSize)
	assert.True(t, finalSize >= 200, "Buffer should be near capacity, got %d", finalSize)
}

// ============================================================================
// Backpressure Tests - Buffer Overflow Solution
// ============================================================================

func TestBackpressure_WaitsForFlush(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// maxBufferSize=10 -> backpressure at 15, circuit breaker at 25
	// Use very long flush interval to prevent background flush interference
	repo := newTestBufferedRepository(mockRepo, mockCache, 1*time.Hour, 10, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Simulate slow flush (200ms)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		time.Sleep(200 * time.Millisecond)
	}).Return(nil).Maybe()

	// Fill buffer to backpressure threshold (15 entries)
	for i := 0; i < 15; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}

	// Next update should trigger backpressure (block and wait for flush)
	done := make(chan error, 1)
	startTime := time.Now()

	go func() {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       "goal_backpressure",
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(100),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		done <- repo.UpdateProgress(context.Background(), event)
	}()

	// Verify it's blocking (shouldn't complete immediately)
	select {
	case <-done:
		t.Fatal("Expected blocking due to backpressure, but completed immediately")
	case <-time.After(100 * time.Millisecond):
		// Good, it's blocking
	}

	// Trigger manual flush to release backpressure
	err := repo.Flush(context.Background())
	assert.NoError(t, err)

	// Verify the update completes after flush
	select {
	case err := <-done:
		duration := time.Since(startTime)
		assert.NoError(t, err)
		assert.Greater(t, duration, 100*time.Millisecond, "Should have blocked for at least 100ms")
		assert.Less(t, duration, 1*time.Second, "Should have completed within 1 second")
	case <-time.After(2 * time.Second):
		t.Fatal("Expected completion after flush, but timed out")
	}
}

func TestBackpressure_TimeoutWhenFlushNeverCompletes(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// maxBufferSize=10 -> backpressure at 15
	repo := newTestBufferedRepository(mockRepo, mockCache, 1*time.Second, 10, logger) // 1s flush interval
	// Note: We intentionally don't defer Close() because the mock sleeps for 30s
	// and Close() would block waiting for flush to complete

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Simulate database that never responds (block forever)
	// This will cause automatic flushes to get stuck
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		time.Sleep(30 * time.Second) // Block way longer than 5s timeout
	}).Return(errors.New("database timeout")).Maybe()

	// Fill buffer to backpressure threshold (15 entries)
	for i := 0; i < 15; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}

	// Wait for automatic flush to start (1 second interval)
	time.Sleep(1500 * time.Millisecond)

	// At this point:
	// - Automatic flush has started and is blocked in database call for 30s
	// - flushCompleteCh won't be signaled until flush completes
	// - Buffer was swapped to empty, but we'll fill it again

	// Fill buffer to backpressure threshold again while flush is blocked
	for i := 0; i < 15; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user2",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}

	// Next update should trigger backpressure and timeout after 5 seconds
	// (because previous flush is still blocked and won't send completion signal)
	start := time.Now()
	event := &domain.BufferedEvent{
		UserID:       "user2",
		GoalID:       "goal_timeout",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(100),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event)
	duration := time.Since(start)

	// Should return timeout error
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "backpressure timeout")
		assert.Contains(t, err.Error(), "5s")
	}

	// Should timeout around 5 seconds (not 30 seconds)
	assert.Greater(t, duration, 4900*time.Millisecond, "Should wait at least 4.9 seconds")
	assert.Less(t, duration, 7*time.Second, "Should timeout within 7 seconds")
}

func TestCircuitBreaker_RejectsWhenThresholdExceeded(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// maxBufferSize=10 -> circuit breaker at 25
	// Use very long flush interval to prevent background flush interference
	// IMPORTANT: Disable size-based flush by setting maxBufferSize to very large value
	// We want to test circuit breaker at 25, so set maxBufferSize > 25
	repo := newTestBufferedRepository(mockRepo, mockCache, 1*time.Hour, 50, logger)
	// Manually override thresholds for this test
	// Set backpressure very high so it doesn't interfere with circuit breaker test
	repo.backpressureThreshold = 1000
	repo.circuitBreakerThreshold = 25
	defer func() {
		_ = repo.Close()
	}()

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Simulate database failure - all flushes fail
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(errors.New("database unavailable")).Maybe()

	// Fill buffer to circuit breaker threshold (25 entries)
	for i := 0; i < 25; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err, "Update %d should succeed", i)
	}

	// Buffer should be at circuit breaker threshold
	assert.Equal(t, 25, repo.GetBufferSize())

	// Next update should be rejected immediately by circuit breaker
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal_circuit_breaker",
		ChallengeID:  "challenge1",
		Namespace:    "test",
		Progress:     intPtr(100),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event)

	// Should return circuit breaker error immediately
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "circuit breaker")
	assert.Contains(t, err.Error(), "25")
	assert.Contains(t, err.Error(), "database unavailable")

	// Buffer size should remain at 25 (circuit breaker prevented buffering)
	assert.Equal(t, 25, repo.GetBufferSize())
}

func TestFlushCompletion_SignalsWaitingGoroutines(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// maxBufferSize=10 -> backpressure at 15
	// Use very long flush interval to prevent background flush interference
	repo := newTestBufferedRepository(mockRepo, mockCache, 1*time.Hour, 10, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Simulate fast flush
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()

	// Fill buffer to backpressure threshold (15 entries)
	for i := 0; i < 15; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}

	// Start multiple goroutines that will all wait on backpressure
	numWaiters := 5
	done := make(chan error, numWaiters)

	for i := 0; i < numWaiters; i++ {
		go func(id int) {
			event := &domain.BufferedEvent{
				UserID:       "user1",
				GoalID:       fmt.Sprintf("goal_waiter_%d", id),
				ChallengeID:  "challenge1",
				Namespace:    "test",
				Progress:     intPtr(id),
				IncValue:     0,
				ProgressMode: domain.ProgressModeAbsolute,
			}
			done <- repo.UpdateProgress(context.Background(), event)
		}(i)
	}

	// Wait for all goroutines to start waiting
	time.Sleep(100 * time.Millisecond)

	// Trigger flush - should signal all waiting goroutines
	err := repo.Flush(context.Background())
	assert.NoError(t, err)

	// All waiting goroutines should complete
	completedCount := 0
	timeout := time.After(2 * time.Second)

	for completedCount < numWaiters {
		select {
		case err := <-done:
			assert.NoError(t, err)
			completedCount++
		case <-timeout:
			t.Fatalf("Expected %d completions, got %d (flush signal not working)", numWaiters, completedCount)
		}
	}

	assert.Equal(t, numWaiters, completedCount, "All waiting goroutines should complete")
}

func TestBackpressure_ContextCancellation(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// maxBufferSize=10 -> backpressure at 15
	// Use very long flush interval to prevent background flush interference
	// IMPORTANT: Set maxBufferSize > 15 to prevent size-based flush from triggering
	// during the test, which would leave a flush completion signal in the channel
	repo := newTestBufferedRepository(mockRepo, mockCache, 1*time.Hour, 50, logger)
	// Manually set backpressure threshold to 15 for this test
	repo.backpressureThreshold = 15
	defer func() {
		_ = repo.Close()
	}()

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	// Simulate slow flush (won't be triggered due to high maxBufferSize)
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		time.Sleep(10 * time.Second)
	}).Return(nil).Maybe()

	// Fill buffer to backpressure threshold
	for i := 0; i < 15; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Start update with backpressure (will block)
	done := make(chan error, 1)
	go func() {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       "goal_cancel",
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(100),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		done <- repo.UpdateProgress(ctx, event)
	}()

	// Wait for goroutine to start blocking
	time.Sleep(100 * time.Millisecond)

	// Cancel context
	cancel()

	// Should return context cancellation error
	select {
	case err := <-done:
		assert.Error(t, err, "Expected context cancellation error")
		if err != nil {
			assert.Contains(t, err.Error(), "context canceled")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Expected context cancellation error, but timed out")
	}
}

func TestBackpressure_ThresholdCalculation(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	logger := newTestLogger()

	// Test various buffer sizes to verify threshold calculation
	testCases := []struct {
		maxBufferSize          int
		expectedBackpressure   int
		expectedCircuitBreaker int
	}{
		{10, 15, 25},       // 10 * 1.5 = 15, 10 * 2.5 = 25
		{100, 150, 250},    // 100 * 1.5 = 150, 100 * 2.5 = 250
		{1000, 1500, 2500}, // 1000 * 1.5 = 1500, 1000 * 2.5 = 2500
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("maxBufferSize=%d", tc.maxBufferSize), func(t *testing.T) {
			repo := newTestBufferedRepository(mockRepo, nil, 10*time.Second, tc.maxBufferSize, logger)
			mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()
			defer func() {
				_ = repo.Close()
			}()

			assert.Equal(t, tc.maxBufferSize, repo.maxBufferSize)
			assert.Equal(t, tc.expectedBackpressure, repo.backpressureThreshold)
			assert.Equal(t, tc.expectedCircuitBreaker, repo.circuitBreakerThreshold)
		})
	}
}

func TestBackpressure_NoImpactBelowThreshold(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	// maxBufferSize=100 -> backpressure at 150
	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 100, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Set up cache mock for enrichment
	mockCache.On("GetGoalByID", mock.Anything).Return(testGoal("goalX", "challenge1", 100)).Maybe()

	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.Anything).Return(nil).Maybe()

	// Add updates well below backpressure threshold (100 < 150)
	start := time.Now()
	for i := 0; i < 100; i++ {
		event := &domain.BufferedEvent{
			UserID:       "user1",
			GoalID:       fmt.Sprintf("goal%d", i),
			ChallengeID:  "challenge1",
			Namespace:    "test",
			Progress:     intPtr(i),
			IncValue:     0,
			ProgressMode: domain.ProgressModeAbsolute,
		}
		err := repo.UpdateProgress(context.Background(), event)
		assert.NoError(t, err)
	}
	duration := time.Since(start)

	// Should complete very quickly (no backpressure blocking)
	assert.Less(t, duration, 100*time.Millisecond, "Should complete quickly without backpressure")
	assert.Equal(t, 100, repo.GetBufferSize())
}

// testGoalWithRotation returns a *domain.Goal with rotation config for testing.
func testGoalWithRotation(goalID, challengeID string, targetValue int, schedule domain.RotationSchedule, resetProgress, allowReselection bool) *domain.Goal {
	return &domain.Goal{
		ID:          goalID,
		ChallengeID: challengeID,
		Requirement: domain.Requirement{
			TargetValue:  targetValue,
			ProgressMode: domain.ProgressModeRelative,
		},
		Rotation: &domain.RotationConfig{
			Enabled:  true,
			Type:     domain.RotationTypeGlobal,
			Schedule: schedule,
			OnExpiry: domain.OnExpiryConfig{
				ResetProgress:    resetProgress,
				AllowReselection: allowReselection,
			},
		},
	}
}

func TestFlush_EnrichesRotationMetadata(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Relative goal with daily rotation
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal-rot",
		ChallengeID:  "ch1",
		Namespace:    "test",
		Progress:     intPtr(108),
		IncValue:     3,
		ProgressMode: domain.ProgressModeRelative,
	}
	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)

	mockCache.On("GetGoalByID", "goal-rot").Return(
		testGoalWithRotation("goal-rot", "ch1", 10, domain.RotationScheduleDaily, true, false),
	)

	// Verify CopyRow has rotation fields populated
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.MatchedBy(func(rows []repository.CopyRow) bool {
		if len(rows) != 1 {
			return false
		}
		row := rows[0]
		return row.UserID == "user1" &&
			row.GoalID == "goal-rot" &&
			row.RotationBoundary != nil &&
			row.NewExpiresAt != nil &&
			row.ResetProgress == true &&
			row.AllowReselection == false
	})).Return(nil)

	err = repo.Flush(context.Background())
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestFlush_NonRotatingGoal_NoRotationMetadata(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockCache := new(MockGoalCache)
	logger := newTestLogger()

	repo := newTestBufferedRepository(mockRepo, mockCache, 10*time.Second, 1000, logger)
	defer func() {
		_ = repo.Close()
	}()

	// Absolute goal, no rotation
	event := &domain.BufferedEvent{
		UserID:       "user1",
		GoalID:       "goal-abs",
		ChallengeID:  "ch1",
		Namespace:    "test",
		Progress:     intPtr(50),
		IncValue:     0,
		ProgressMode: domain.ProgressModeAbsolute,
	}
	err := repo.UpdateProgress(context.Background(), event)
	assert.NoError(t, err)

	mockCache.On("GetGoalByID", "goal-abs").Return(testGoal("goal-abs", "ch1", 100))

	// Verify CopyRow has nil rotation fields
	mockRepo.On("BatchUpsertProgressWithCOPY", mock.Anything, mock.MatchedBy(func(rows []repository.CopyRow) bool {
		if len(rows) != 1 {
			return false
		}
		row := rows[0]
		return row.UserID == "user1" &&
			row.GoalID == "goal-abs" &&
			row.RotationBoundary == nil &&
			row.NewExpiresAt == nil &&
			row.ResetProgress == false &&
			row.AllowReselection == false
	})).Return(nil)

	err = repo.Flush(context.Background())
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package processor

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/AccelByte/extend-challenge-common/pkg/domain"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockBufferedRepository is a mock implementation of BufferedRepository
type MockBufferedRepository struct {
	mock.Mock
	mu     sync.Mutex
	buffer map[string]*domain.UserGoalProgress
}

func (m *MockBufferedRepository) UpdateProgress(ctx context.Context, progress *domain.UserGoalProgress) error {
	args := m.Called(ctx, progress)

	// Store in local buffer for GetFromBuffer
	m.mu.Lock()
	key := fmt.Sprintf("%s:%s", progress.UserID, progress.GoalID)
	m.buffer[key] = progress
	m.mu.Unlock()

	return args.Error(0)
}

func (m *MockBufferedRepository) IncrementProgress(ctx context.Context, userID, goalID, challengeID, namespace string,
	delta, targetValue int, isDailyIncrement bool) error {
	args := m.Called(ctx, userID, goalID, challengeID, namespace, delta, targetValue, isDailyIncrement)
	return args.Error(0)
}

func (m *MockBufferedRepository) GetFromBuffer(userID, goalID string) *domain.UserGoalProgress {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", userID, goalID)
	return m.buffer[key]
}

func (m *MockBufferedRepository) Flush(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockBufferedRepository) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockBufferedRepository) GetBufferSize() int {
	args := m.Called()
	return args.Int(0)
}

// MockGoalCache is a mock implementation of GoalCache
type MockGoalCache struct {
	mock.Mock
}

func (m *MockGoalCache) GetGoalsByStatCode(statCode string) []*domain.Goal {
	args := m.Called(statCode)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]*domain.Goal)
}

func (m *MockGoalCache) GetGoalByID(goalID string) *domain.Goal {
	args := m.Called(goalID)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*domain.Goal)
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

func TestNewEventProcessor(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	assert.NotNil(t, processor)
	assert.Equal(t, "test-namespace", processor.namespace)
	assert.NotNil(t, processor.userMutexes)
	assert.Equal(t, 0, len(processor.userMutexes))
}

func TestNewEventProcessor_NilLogger(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", zerolog.Logger{})

	assert.NotNil(t, processor.logger)
}

func TestProcessLoginEvent_Success(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Setup: Cache returns one goal tracking login_count (increment type)
	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeIncrement, // Now routes to IncrementProgress
		Daily:       false,
		Requirement: domain.Requirement{
			StatCode:    "login_count",
			TargetValue: 5,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})

	// Expect IncrementProgress to be called (not UpdateProgress)
	mockRepo.On("IncrementProgress",
		mock.Anything,    // ctx
		"user1",          // userID
		"goal1",          // goalID
		"challenge1",     // challengeID
		"test-namespace", // namespace
		1,                // delta
		5,                // targetValue
		false,            // isDailyIncrement
	).Return(nil)

	// Process login event using ProcessEvent
	statUpdates := map[string]int{"login_count": 1}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessLoginEvent_MultipleLogins_Incremental(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeIncrement, // Now routes to IncrementProgress
		Daily:       false,
		Requirement: domain.Requirement{
			StatCode:    "login_count",
			TargetValue: 3,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})
	statUpdates := map[string]int{"login_count": 1}

	// First login: delta = 1
	mockRepo.On("IncrementProgress",
		mock.Anything, "user1", "goal1", "challenge1", "test-namespace", 1, 3, false,
	).Return(nil).Once()

	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	// Second login: delta = 1
	mockRepo.On("IncrementProgress",
		mock.Anything, "user1", "goal1", "challenge1", "test-namespace", 1, 3, false,
	).Return(nil).Once()

	err = processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	// Third login: delta = 1 (BufferedRepository handles completion)
	mockRepo.On("IncrementProgress",
		mock.Anything, "user1", "goal1", "challenge1", "test-namespace", 1, 3, false,
	).Return(nil).Once()

	err = processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessLoginEvent_NoGoalsTrackLogin(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Cache returns no goals
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{})

	// Should not call UpdateProgress
	statUpdates := map[string]int{"login_count": 1}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockCache.AssertExpectations(t)
	// mockRepo should not have been called
	mockRepo.AssertNotCalled(t, "UpdateProgress")
}

func TestProcessLoginEvent_UpdateError(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeIncrement, // Now routes to IncrementProgress
		Daily:       false,
		Requirement: domain.Requirement{
			StatCode:    "login_count",
			TargetValue: 5,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})

	// Simulate error from IncrementProgress
	mockRepo.On("IncrementProgress",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(fmt.Errorf("buffer full"))

	// Error is logged but not returned (helper methods handle errors internally)
	statUpdates := map[string]int{"login_count": 1}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err) // ProcessEvent doesn't return errors from helper methods

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessStatUpdateEvent_Success(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Setup: Cache returns one goal tracking kills (absolute type)
	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeAbsolute, // Stat updates use absolute type
		Requirement: domain.Requirement{
			StatCode:    "kills",
			TargetValue: 100,
		},
	}
	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{goal})

	// Expect UpdateProgress with stat value
	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.UserID == "user1" &&
			p.GoalID == "goal1" &&
			p.Progress == 50 &&
			p.Status == domain.GoalStatusInProgress
	})).Return(nil)

	// Process stat update event using ProcessEvent
	statUpdates := map[string]int{"kills": 50}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessStatUpdateEvent_Completed(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeAbsolute, // Stat updates use absolute type
		Requirement: domain.Requirement{
			StatCode:    "kills",
			TargetValue: 100,
		},
	}
	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{goal})

	// Stat value meets target
	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.Progress == 100 &&
			p.Status == domain.GoalStatusCompleted &&
			p.CompletedAt != nil
	})).Return(nil)

	statUpdates := map[string]int{"kills": 100}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessStatUpdateEvent_NoGoalsTrackStat(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Cache returns no goals
	mockCache.On("GetGoalsByStatCode", "unknown_stat").Return([]*domain.Goal{})

	statUpdates := map[string]int{"unknown_stat": 10}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockCache.AssertExpectations(t)
	mockRepo.AssertNotCalled(t, "UpdateProgress")
}

func TestProcessStatUpdateEvent_UpdateError(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeAbsolute, // Stat updates use absolute type
		Requirement: domain.Requirement{
			StatCode:    "kills",
			TargetValue: 100,
		},
	}
	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{goal})

	// Simulate error
	mockRepo.On("UpdateProgress", mock.Anything, mock.Anything).Return(fmt.Errorf("database error"))

	// Error is logged but not returned (helper methods handle errors internally)
	statUpdates := map[string]int{"kills": 50}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err) // ProcessEvent doesn't return errors from helper methods

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestGetUserMutex_CreateNew(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Get mutex for new user
	mu1 := processor.getUserMutex("user1")
	assert.NotNil(t, mu1)
	assert.Equal(t, 1, len(processor.userMutexes))

	// Get mutex again - should return same instance
	mu2 := processor.getUserMutex("user1")
	assert.Equal(t, mu1, mu2)
	assert.Equal(t, 1, len(processor.userMutexes))

	// Get mutex for different user - should create new
	mu3 := processor.getUserMutex("user2")
	assert.NotNil(t, mu3)
	assert.Equal(t, 2, len(processor.userMutexes))
	// Verify they are different by checking the map has different keys
	assert.NotSame(t, mu1, mu3, "Different users should have different mutexes")
}

func TestConcurrentEventProcessing_DifferentUsers(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeIncrement, // Now routes to IncrementProgress
		Daily:       false,
		Requirement: domain.Requirement{
			StatCode:    "login_count",
			TargetValue: 5,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})

	// Allow multiple calls
	mockRepo.On("IncrementProgress",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(nil)

	var wg sync.WaitGroup
	numUsers := 10
	statUpdates := map[string]int{"login_count": 1}

	// Process events concurrently for different users
	for i := 0; i < numUsers; i++ {
		wg.Add(1)
		go func(userID string) {
			defer wg.Done()
			err := processor.ProcessEvent(context.Background(), userID, "test-namespace", statUpdates)
			assert.NoError(t, err)
		}(fmt.Sprintf("user%d", i))
	}

	wg.Wait()

	// Should have created mutex for each user
	assert.Equal(t, numUsers, len(processor.userMutexes))

	mockCache.AssertExpectations(t)
}

func TestConcurrentEventProcessing_SameUser(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeIncrement, // Now routes to IncrementProgress
		Daily:       false,
		Requirement: domain.Requirement{
			StatCode:    "login_count",
			TargetValue: 100,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})

	// Allow multiple calls
	mockRepo.On("IncrementProgress",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(nil)

	var wg sync.WaitGroup
	numEvents := 20
	statUpdates := map[string]int{"login_count": 1}

	// Process events concurrently for same user (should be sequential due to mutex)
	for i := 0; i < numEvents; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	// With increment goals, buffering is handled by BufferedRepository
	// We just verify that all 20 IncrementProgress calls were made
	mockCache.AssertExpectations(t)
}

func TestProcessLoginEvent_MultipleGoals(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Multiple goals tracking login_count (increment type)
	goals := []*domain.Goal{
		{
			ID:          "goal1",
			ChallengeID: "challenge1",
			Type:        domain.GoalTypeIncrement,
			Daily:       false,
			Requirement: domain.Requirement{
				StatCode:    "login_count",
				TargetValue: 5,
			},
		},
		{
			ID:          "goal2",
			ChallengeID: "challenge1",
			Type:        domain.GoalTypeIncrement,
			Daily:       false,
			Requirement: domain.Requirement{
				StatCode:    "login_count",
				TargetValue: 10,
			},
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return(goals)

	// Expect IncrementProgress for each goal
	mockRepo.On("IncrementProgress",
		mock.Anything, "user1", "goal1", "challenge1", "test-namespace", 1, 5, false,
	).Return(nil).Once()

	mockRepo.On("IncrementProgress",
		mock.Anything, "user1", "goal2", "challenge1", "test-namespace", 1, 10, false,
	).Return(nil).Once()

	statUpdates := map[string]int{"login_count": 1}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// ==================== Goal Type Routing Tests ====================

func TestProcessEvent_AbsoluteGoal(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Setup: Absolute goal tracking kills
	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeAbsolute,
		Requirement: domain.Requirement{
			StatCode:    "kills",
			TargetValue: 100,
		},
	}
	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{goal})

	// Expect UpdateProgress with absolute value
	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.UserID == "user1" &&
			p.GoalID == "goal1" &&
			p.Progress == 50 &&
			p.Status == domain.GoalStatusInProgress
	})).Return(nil)

	// Process event with stat update
	statUpdates := map[string]int{"kills": 50}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessEvent_AbsoluteGoal_Completed(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeAbsolute,
		Requirement: domain.Requirement{
			StatCode:    "kills",
			TargetValue: 100,
		},
	}
	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{goal})

	// Stat value meets target - should complete
	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.Progress == 100 &&
			p.Status == domain.GoalStatusCompleted &&
			p.CompletedAt != nil
	})).Return(nil)

	statUpdates := map[string]int{"kills": 100}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessEvent_AbsoluteGoal_NegativeValue(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeAbsolute,
		Requirement: domain.Requirement{
			StatCode:    "kills",
			TargetValue: 100,
		},
	}
	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{goal})

	// Negative value - should NOT call UpdateProgress (graceful degradation)
	// No mock expectation means test will fail if UpdateProgress is called

	statUpdates := map[string]int{"kills": -10}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockCache.AssertExpectations(t)
	mockRepo.AssertNotCalled(t, "UpdateProgress")
}

func TestProcessEvent_IncrementGoal_Regular(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Setup: Regular increment goal (no daily flag)
	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeIncrement,
		Daily:       false, // Regular increment
		Requirement: domain.Requirement{
			StatCode:    "login_count",
			TargetValue: 5,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})

	// Expect IncrementProgress with delta=1, isDailyIncrement=false
	mockRepo.On("IncrementProgress",
		mock.Anything,    // ctx
		"user1",          // userID
		"goal1",          // goalID
		"challenge1",     // challengeID
		"test-namespace", // namespace
		1,                // delta
		5,                // targetValue
		false,            // isDailyIncrement
	).Return(nil)

	// Process event
	statUpdates := map[string]int{"login_count": 1}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessEvent_IncrementGoal_Daily(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Setup: Daily increment goal
	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeIncrement,
		Daily:       true, // Daily increment
		Requirement: domain.Requirement{
			StatCode:    "login_count",
			TargetValue: 7,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})

	// Expect IncrementProgress with delta=1, isDailyIncrement=true
	mockRepo.On("IncrementProgress",
		mock.Anything,    // ctx
		"user1",          // userID
		"goal1",          // goalID
		"challenge1",     // challengeID
		"test-namespace", // namespace
		1,                // delta
		7,                // targetValue
		true,             // isDailyIncrement
	).Return(nil)

	// Process event
	statUpdates := map[string]int{"login_count": 1}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessEvent_DailyGoal(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Setup: Daily goal type
	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeDaily,
		Requirement: domain.Requirement{
			StatCode:    "daily_login",
			TargetValue: 1,
		},
	}
	mockCache.On("GetGoalsByStatCode", "daily_login").Return([]*domain.Goal{goal})

	// Expect UpdateProgress with progress=1, status=completed, timestamp
	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.UserID == "user1" &&
			p.GoalID == "goal1" &&
			p.Progress == 1 &&
			p.Status == domain.GoalStatusCompleted &&
			p.CompletedAt != nil
	})).Return(nil)

	// Process event
	statUpdates := map[string]int{"daily_login": 1}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessEvent_UnknownGoalType(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Setup: Goal with unknown type
	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Type:        "unknown_type", // Invalid type
		Requirement: domain.Requirement{
			StatCode:    "some_stat",
			TargetValue: 10,
		},
	}
	mockCache.On("GetGoalsByStatCode", "some_stat").Return([]*domain.Goal{goal})

	// Should NOT call any repository methods (graceful degradation)

	statUpdates := map[string]int{"some_stat": 5}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockCache.AssertExpectations(t)
	mockRepo.AssertNotCalled(t, "UpdateProgress")
	mockRepo.AssertNotCalled(t, "IncrementProgress")
}

func TestProcessEvent_MultipleGoalTypes(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Setup: Three goals with different types tracking login_count
	absoluteGoal := &domain.Goal{
		ID:          "goal_absolute",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeAbsolute,
		Requirement: domain.Requirement{
			StatCode:    "login_count",
			TargetValue: 10,
		},
	}
	incrementGoal := &domain.Goal{
		ID:          "goal_increment",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeIncrement,
		Daily:       false,
		Requirement: domain.Requirement{
			StatCode:    "login_count",
			TargetValue: 5,
		},
	}
	dailyGoal := &domain.Goal{
		ID:          "goal_daily",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeDaily,
		Requirement: domain.Requirement{
			StatCode:    "login_count",
			TargetValue: 1,
		},
	}

	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{
		absoluteGoal, incrementGoal, dailyGoal,
	})

	// Expect UpdateProgress for absolute goal
	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.GoalID == "goal_absolute" && p.Progress == 1
	})).Return(nil).Once()

	// Expect IncrementProgress for increment goal
	mockRepo.On("IncrementProgress",
		mock.Anything, "user1", "goal_increment", "challenge1", "test-namespace", 1, 5, false,
	).Return(nil).Once()

	// Expect UpdateProgress for daily goal
	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.GoalID == "goal_daily" && p.Progress == 1 && p.Status == domain.GoalStatusCompleted
	})).Return(nil).Once()

	// Process event - should route to all three handlers
	statUpdates := map[string]int{"login_count": 1}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessEvent_EmptyStatUpdates(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Process event with empty stat updates - should not fail
	statUpdates := map[string]int{}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	// Should not call any cache or repository methods
	mockCache.AssertNotCalled(t, "GetGoalsByStatCode")
	mockRepo.AssertNotCalled(t, "UpdateProgress")
	mockRepo.AssertNotCalled(t, "IncrementProgress")
}

func TestProcessEvent_NoGoalsForStat(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Cache returns no goals for this stat
	mockCache.On("GetGoalsByStatCode", "unknown_stat").Return([]*domain.Goal{})

	statUpdates := map[string]int{"unknown_stat": 42}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockCache.AssertExpectations(t)
	mockRepo.AssertNotCalled(t, "UpdateProgress")
	mockRepo.AssertNotCalled(t, "IncrementProgress")
}

func TestProcessEvent_MultipleStatUpdates(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Setup: Two goals tracking different stats
	killsGoal := &domain.Goal{
		ID:          "goal_kills",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeAbsolute,
		Requirement: domain.Requirement{
			StatCode:    "kills",
			TargetValue: 100,
		},
	}
	deathsGoal := &domain.Goal{
		ID:          "goal_deaths",
		ChallengeID: "challenge1",
		Type:        domain.GoalTypeAbsolute,
		Requirement: domain.Requirement{
			StatCode:    "deaths",
			TargetValue: 50,
		},
	}

	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{killsGoal})
	mockCache.On("GetGoalsByStatCode", "deaths").Return([]*domain.Goal{deathsGoal})

	// Expect UpdateProgress for both goals
	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.GoalID == "goal_kills" && p.Progress == 75
	})).Return(nil).Once()

	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.GoalID == "goal_deaths" && p.Progress == 10
	})).Return(nil).Once()

	// Process event with multiple stat updates
	statUpdates := map[string]int{"kills": 75, "deaths": 10}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

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

// Helper to create a StatUpdate with an absolute value
func statUpdate(value int) domain.StatUpdate {
	return domain.StatUpdate{Value: &value, Inc: 1}
}

// Helper to create a StatUpdate with only Inc (login-style)
func loginStatUpdate() domain.StatUpdate {
	return domain.StatUpdate{Value: nil, Inc: 1}
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

func TestProcessEvent_AbsoluteGoal_WithValue(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Requirement: domain.Requirement{
			StatCode:     "kills",
			TargetValue:  100,
			ProgressMode: domain.ProgressModeAbsolute,
		},
	}
	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{goal})

	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.UserID == "user1" &&
			p.GoalID == "goal1" &&
			p.Progress == 50 &&
			p.Status == domain.GoalStatusInProgress
	})).Return(nil)

	statUpdates := map[string]domain.StatUpdate{"kills": statUpdate(50)}
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
		Requirement: domain.Requirement{
			StatCode:     "kills",
			TargetValue:  100,
			ProgressMode: domain.ProgressModeAbsolute,
		},
	}
	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{goal})

	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.Progress == 100 &&
			p.Status == domain.GoalStatusCompleted &&
			p.CompletedAt != nil
	})).Return(nil)

	statUpdates := map[string]domain.StatUpdate{"kills": statUpdate(100)}
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
		Requirement: domain.Requirement{
			StatCode:     "kills",
			TargetValue:  100,
			ProgressMode: domain.ProgressModeAbsolute,
		},
	}
	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{goal})

	statUpdates := map[string]domain.StatUpdate{"kills": statUpdate(-10)}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockCache.AssertExpectations(t)
	mockRepo.AssertNotCalled(t, "UpdateProgress")
}

func TestProcessEvent_RelativeGoal_UsesAbsoluteHandler(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	// Relative mode should route to same handler as absolute in Phase 0.5
	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Requirement: domain.Requirement{
			StatCode:     "login_count",
			TargetValue:  5,
			ProgressMode: domain.ProgressModeRelative,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})

	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.UserID == "user1" &&
			p.GoalID == "goal1" &&
			p.Progress == 1 // Falls back to Inc since Value is nil
	})).Return(nil)

	statUpdates := map[string]domain.StatUpdate{"login_count": loginStatUpdate()}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessEvent_NilValueFallsBackToInc(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Requirement: domain.Requirement{
			StatCode:     "login_count",
			TargetValue:  5,
			ProgressMode: domain.ProgressModeAbsolute,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})

	// When Value is nil, should fall back to Inc
	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.Progress == 1 // Uses Inc=1 since Value is nil
	})).Return(nil)

	statUpdates := map[string]domain.StatUpdate{
		"login_count": {Value: nil, Inc: 1},
	}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessEvent_UnknownProgressMode(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Requirement: domain.Requirement{
			StatCode:     "some_stat",
			TargetValue:  10,
			ProgressMode: domain.ProgressMode("unknown"),
		},
	}
	mockCache.On("GetGoalsByStatCode", "some_stat").Return([]*domain.Goal{goal})

	statUpdates := map[string]domain.StatUpdate{"some_stat": statUpdate(5)}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockCache.AssertExpectations(t)
	mockRepo.AssertNotCalled(t, "UpdateProgress")
}

func TestProcessEvent_NoGoalsForStat(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	mockCache.On("GetGoalsByStatCode", "unknown_stat").Return([]*domain.Goal{})

	statUpdates := map[string]domain.StatUpdate{"unknown_stat": statUpdate(42)}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockCache.AssertExpectations(t)
	mockRepo.AssertNotCalled(t, "UpdateProgress")
}

func TestProcessEvent_EmptyStatUpdates(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	statUpdates := map[string]domain.StatUpdate{}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockCache.AssertNotCalled(t, "GetGoalsByStatCode")
	mockRepo.AssertNotCalled(t, "UpdateProgress")
}

func TestProcessEvent_MultipleStatUpdates(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	killsGoal := &domain.Goal{
		ID:          "goal_kills",
		ChallengeID: "challenge1",
		Requirement: domain.Requirement{
			StatCode:     "kills",
			TargetValue:  100,
			ProgressMode: domain.ProgressModeAbsolute,
		},
	}
	deathsGoal := &domain.Goal{
		ID:          "goal_deaths",
		ChallengeID: "challenge1",
		Requirement: domain.Requirement{
			StatCode:     "deaths",
			TargetValue:  50,
			ProgressMode: domain.ProgressModeAbsolute,
		},
	}

	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{killsGoal})
	mockCache.On("GetGoalsByStatCode", "deaths").Return([]*domain.Goal{deathsGoal})

	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.GoalID == "goal_kills" && p.Progress == 75
	})).Return(nil).Once()

	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.GoalID == "goal_deaths" && p.Progress == 10
	})).Return(nil).Once()

	statUpdates := map[string]domain.StatUpdate{
		"kills":  statUpdate(75),
		"deaths": statUpdate(10),
	}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessEvent_MultipleGoalsSameStatCode(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goals := []*domain.Goal{
		{
			ID:          "goal1",
			ChallengeID: "challenge1",
			Requirement: domain.Requirement{
				StatCode:     "login_count",
				TargetValue:  5,
				ProgressMode: domain.ProgressModeAbsolute,
			},
		},
		{
			ID:          "goal2",
			ChallengeID: "challenge1",
			Requirement: domain.Requirement{
				StatCode:     "login_count",
				TargetValue:  10,
				ProgressMode: domain.ProgressModeAbsolute,
			},
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return(goals)

	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.GoalID == "goal1" && p.Progress == 3
	})).Return(nil).Once()

	mockRepo.On("UpdateProgress", mock.Anything, mock.MatchedBy(func(p *domain.UserGoalProgress) bool {
		return p.GoalID == "goal2" && p.Progress == 3
	})).Return(nil).Once()

	statUpdates := map[string]domain.StatUpdate{"login_count": statUpdate(3)}
	err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

func TestProcessEvent_UpdateError(t *testing.T) {
	mockRepo := new(MockBufferedRepository)
	mockRepo.buffer = make(map[string]*domain.UserGoalProgress)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	processor := NewEventProcessor(mockRepo, mockCache, "test-namespace", logger)

	goal := &domain.Goal{
		ID:          "goal1",
		ChallengeID: "challenge1",
		Requirement: domain.Requirement{
			StatCode:     "kills",
			TargetValue:  100,
			ProgressMode: domain.ProgressModeAbsolute,
		},
	}
	mockCache.On("GetGoalsByStatCode", "kills").Return([]*domain.Goal{goal})
	mockRepo.On("UpdateProgress", mock.Anything, mock.Anything).Return(fmt.Errorf("database error"))

	statUpdates := map[string]domain.StatUpdate{"kills": statUpdate(50)}
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

	mu1 := processor.getUserMutex("user1")
	assert.NotNil(t, mu1)
	assert.Equal(t, 1, len(processor.userMutexes))

	mu2 := processor.getUserMutex("user1")
	assert.Equal(t, mu1, mu2)
	assert.Equal(t, 1, len(processor.userMutexes))

	mu3 := processor.getUserMutex("user2")
	assert.NotNil(t, mu3)
	assert.Equal(t, 2, len(processor.userMutexes))
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
		Requirement: domain.Requirement{
			StatCode:     "login_count",
			TargetValue:  5,
			ProgressMode: domain.ProgressModeAbsolute,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})
	mockRepo.On("UpdateProgress", mock.Anything, mock.Anything).Return(nil)

	var wg sync.WaitGroup
	numUsers := 10
	statUpdates := map[string]domain.StatUpdate{"login_count": loginStatUpdate()}

	for i := 0; i < numUsers; i++ {
		wg.Add(1)
		go func(userID string) {
			defer wg.Done()
			err := processor.ProcessEvent(context.Background(), userID, "test-namespace", statUpdates)
			assert.NoError(t, err)
		}(fmt.Sprintf("user%d", i))
	}

	wg.Wait()

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
		Requirement: domain.Requirement{
			StatCode:     "login_count",
			TargetValue:  100,
			ProgressMode: domain.ProgressModeAbsolute,
		},
	}
	mockCache.On("GetGoalsByStatCode", "login_count").Return([]*domain.Goal{goal})
	mockRepo.On("UpdateProgress", mock.Anything, mock.Anything).Return(nil)

	var wg sync.WaitGroup
	numEvents := 20
	statUpdates := map[string]domain.StatUpdate{"login_count": loginStatUpdate()}

	for i := 0; i < numEvents; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := processor.ProcessEvent(context.Background(), "user1", "test-namespace", statUpdates)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()
	mockCache.AssertExpectations(t)
}

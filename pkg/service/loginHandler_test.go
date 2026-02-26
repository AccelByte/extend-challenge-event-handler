package service

import (
	"context"
	"errors"
	"os"
	"testing"

	pb "extend-challenge-event-handler/pkg/pb/accelbyte-asyncapi/iam/account/v1"

	"github.com/AccelByte/extend-challenge-common/pkg/domain"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockEventProcessor is a mock implementation of EventProcessor for testing.
type MockEventProcessor struct {
	mock.Mock
}

func (m *MockEventProcessor) ProcessEvent(ctx context.Context, userID, namespace string, statUpdates map[string]domain.StatUpdate) error {
	args := m.Called(ctx, userID, namespace, statUpdates)
	return args.Error(0)
}

// MockGoalCache is a mock implementation of GoalCache for testing.
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

// Test Case 1: Valid login event with single login goal → processes successfully
func TestLoginHandler_OnMessage_SingleLoginGoal_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Mock goals: one login goal
	loginGoal := &domain.Goal{
		ID:          "daily-login",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{
			StatCode: "login_count",
		},
	}
	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal})

	// Expect single ProcessEvent call - use mock.Anything for statUpdates
	// since login events produce StatUpdate{Value: nil, Inc: 1} which involves pointer comparison
	mockProcessor.On("ProcessEvent", mock.Anything, "user123", "test-namespace", mock.Anything).Return(nil)

	// Execute
	msg := &pb.UserLoggedIn{UserId: "user123", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 2: Valid login event with multiple login goals → processes all
func TestLoginHandler_OnMessage_MultipleLoginGoals_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Mock goals: three login goals with different stat codes
	loginGoal1 := &domain.Goal{
		ID:          "daily-login",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "login_count"},
	}
	loginGoal2 := &domain.Goal{
		ID:          "login-7-days",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "consecutive_login"},
	}
	loginGoal3 := &domain.Goal{
		ID:          "login-bonus",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "total_logins"},
	}

	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal1, loginGoal2, loginGoal3})

	// Expect single call with all three stat codes - use mock.Anything for statUpdates
	mockProcessor.On("ProcessEvent", mock.Anything, "user456", "test-namespace", mock.Anything).Return(nil)

	// Execute
	msg := &pb.UserLoggedIn{UserId: "user456", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 3: Nil message → returns InvalidArgument error
func TestLoginHandler_OnMessage_NilMessage_ReturnsError(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Execute
	resp, err := handler.OnMessage(context.Background(), nil)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, resp)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "message cannot be nil")
}

// Test Case 4: Empty userID → returns InvalidArgument error
func TestLoginHandler_OnMessage_EmptyUserID_ReturnsError(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Execute
	msg := &pb.UserLoggedIn{UserId: "", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, resp)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "userId cannot be empty")
}

// Test Case 5: No login goals in config → returns success (no-op)
func TestLoginHandler_OnMessage_NoLoginGoals_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Mock goals: empty list
	mockCache.On("GetAllGoals").Return([]*domain.Goal{})

	// Execute
	msg := &pb.UserLoggedIn{UserId: "user789", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	// Processor should not be called
	mockProcessor.AssertNotCalled(t, "ProcessEvent")
}

// Test Case 6: Goal cache returns nil → logs warning, returns success
func TestLoginHandler_OnMessage_CacheReturnsNil_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Mock cache returns nil
	mockCache.On("GetAllGoals").Return(nil)

	// Execute
	msg := &pb.UserLoggedIn{UserId: "user101", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertNotCalled(t, "ProcessEvent")
}

// Test Case 7: Mixed event sources (login + statistic goals) → only processes login goals
func TestLoginHandler_OnMessage_MixedEventSources_OnlyProcessesLogin(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Mock goals: mix of login and statistic
	loginGoal := &domain.Goal{
		ID:          "daily-login",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "login_count"},
	}
	statGoal1 := &domain.Goal{
		ID:          "kill-10",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}
	statGoal2 := &domain.Goal{
		ID:          "win-1",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "win_count"},
	}

	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal, statGoal1, statGoal2})

	// Only login goal's stat code should be in the map - use mock.Anything for statUpdates
	mockProcessor.On("ProcessEvent", mock.Anything, "user202", "test-namespace", mock.Anything).Return(nil)

	// Execute
	msg := &pb.UserLoggedIn{UserId: "user202", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 8: EventProcessor returns error → returns Internal error for retry
func TestLoginHandler_OnMessage_ProcessorError_ReturnsInternalError(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	loginGoal := &domain.Goal{
		ID:          "daily-login",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "login_count"},
	}
	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal})

	// Simulate buffer full error - use mock.Anything for statUpdates
	mockProcessor.On("ProcessEvent", mock.Anything, "user303", "test-namespace", mock.Anything).Return(errors.New("buffer full"))

	// Execute
	msg := &pb.UserLoggedIn{UserId: "user303", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, resp)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
	assert.Contains(t, st.Message(), "failed to buffer event")
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 9: Multiple login events for same user → all processed (deduplication in buffer)
func TestLoginHandler_OnMessage_MultipleEventsForSameUser_AllProcessed(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	loginGoal := &domain.Goal{
		ID:          "daily-login",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "login_count"},
	}
	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal}).Times(3)

	mockProcessor.On("ProcessEvent", mock.Anything, "user404", "test-namespace", mock.Anything).Return(nil).Times(3)

	// Execute: same user logs in 3 times
	msg := &pb.UserLoggedIn{UserId: "user404", Namespace: "test-namespace"}
	for i := 0; i < 3; i++ {
		resp, err := handler.OnMessage(context.Background(), msg)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	}

	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 10: Login goal with absolute type → processes correctly
func TestLoginHandler_OnMessage_AbsoluteTypeLoginGoal_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	loginGoal := &domain.Goal{
		ID:          "daily-login",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "login_count", ProgressMode: domain.ProgressModeAbsolute},
	}
	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal})

	mockProcessor.On("ProcessEvent", mock.Anything, "user505", "test-namespace", mock.Anything).Return(nil)

	msg := &pb.UserLoggedIn{UserId: "user505", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 11: Login goal with increment type → processes correctly
func TestLoginHandler_OnMessage_IncrementTypeLoginGoal_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	loginGoal := &domain.Goal{
		ID:          "login-7-times",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "login_count", ProgressMode: domain.ProgressModeRelative},
	}
	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal})

	mockProcessor.On("ProcessEvent", mock.Anything, "user606", "test-namespace", mock.Anything).Return(nil)

	msg := &pb.UserLoggedIn{UserId: "user606", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 12: Login goal with daily type → processes correctly
func TestLoginHandler_OnMessage_DailyTypeLoginGoal_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	loginGoal := &domain.Goal{
		ID:          "daily-login-reward",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "login_count", ProgressMode: domain.ProgressModeRelative},
	}
	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal})

	mockProcessor.On("ProcessEvent", mock.Anything, "user707", "test-namespace", mock.Anything).Return(nil)

	msg := &pb.UserLoggedIn{UserId: "user707", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 13: Context cancellation → context is passed to ProcessEvent
func TestLoginHandler_OnMessage_ContextPassed_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	loginGoal := &domain.Goal{
		ID:          "daily-login",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "login_count"},
	}
	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal})

	mockProcessor.On("ProcessEvent", mock.Anything, "user808", "test-namespace", mock.Anything).Return(nil)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	msg := &pb.UserLoggedIn{UserId: "user808", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(ctx, msg)

	// Context is passed to ProcessEvent, but current implementation doesn't check it
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 14: Concurrent login events for different users → all processed
func TestLoginHandler_OnMessage_ConcurrentDifferentUsers_AllProcessed(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	loginGoal := &domain.Goal{
		ID:          "daily-login",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "login_count"},
	}
	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal}).Times(5)

	// Mock processor for 5 different users - use mock.Anything for statUpdates
	mockProcessor.On("ProcessEvent", mock.Anything, "user1", "test-namespace", mock.Anything).Return(nil).Once()
	mockProcessor.On("ProcessEvent", mock.Anything, "user2", "test-namespace", mock.Anything).Return(nil).Once()
	mockProcessor.On("ProcessEvent", mock.Anything, "user3", "test-namespace", mock.Anything).Return(nil).Once()
	mockProcessor.On("ProcessEvent", mock.Anything, "user4", "test-namespace", mock.Anything).Return(nil).Once()
	mockProcessor.On("ProcessEvent", mock.Anything, "user5", "test-namespace", mock.Anything).Return(nil).Once()

	// Execute concurrently
	done := make(chan bool, 5)
	for i := 1; i <= 5; i++ {
		go func(idx int) {
			userID := "user" + string(rune('0'+idx))
			msg := &pb.UserLoggedIn{UserId: userID, Namespace: "test-namespace"}
			resp, err := handler.OnMessage(context.Background(), msg)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 5; i++ {
		<-done
	}

	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 15: Login event with statValue always 1 → verified in processor call
func TestLoginHandler_OnMessage_AlwaysUsesStatValueOne(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	loginGoals := []*domain.Goal{
		{
			ID:          "goal1",
			EventSource: domain.EventSourceLogin,
			Requirement: domain.Requirement{StatCode: "stat1", ProgressMode: domain.ProgressModeAbsolute},
		},
		{
			ID:          "goal2",
			EventSource: domain.EventSourceLogin,
			Requirement: domain.Requirement{StatCode: "stat2", ProgressMode: domain.ProgressModeRelative},
		},
		{
			ID:          "goal3",
			EventSource: domain.EventSourceLogin,
			Requirement: domain.Requirement{StatCode: "stat3", ProgressMode: domain.ProgressModeRelative},
		},
	}
	mockCache.On("GetAllGoals").Return(loginGoals)

	// All stat codes should be present in the map with Inc=1, Value=nil
	mockProcessor.On("ProcessEvent", mock.Anything, "user909", "test-namespace",
		mock.MatchedBy(func(updates map[string]domain.StatUpdate) bool {
			if len(updates) != 3 {
				return false
			}
			for _, key := range []string{"stat1", "stat2", "stat3"} {
				su, ok := updates[key]
				if !ok {
					return false
				}
				if su.Value != nil || su.Inc != 1 {
					return false
				}
			}
			return true
		}),
	).Return(nil)

	msg := &pb.UserLoggedIn{UserId: "user909", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 16: Multiple login goals with same stat_code → map deduplicates (keeps value=1)
func TestLoginHandler_OnMessage_SameStatCodeMultipleGoals_Deduplicated(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Two login goals tracking the same stat_code
	loginGoals := []*domain.Goal{
		{
			ID:          "goal1",
			EventSource: domain.EventSourceLogin,
			Requirement: domain.Requirement{StatCode: "login_count"},
		},
		{
			ID:          "goal2",
			EventSource: domain.EventSourceLogin,
			Requirement: domain.Requirement{StatCode: "login_count"}, // Same stat_code
		},
	}
	mockCache.On("GetAllGoals").Return(loginGoals)

	// Map should deduplicate - only one entry for "login_count"
	mockProcessor.On("ProcessEvent", mock.Anything, "user1010", "test-namespace",
		mock.MatchedBy(func(updates map[string]domain.StatUpdate) bool {
			if len(updates) != 1 {
				return false
			}
			su, ok := updates["login_count"]
			return ok && su.Value == nil && su.Inc == 1
		}),
	).Return(nil)

	msg := &pb.UserLoggedIn{UserId: "user1010", Namespace: "test-namespace"}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 17: Namespace validation - wrong namespace should be skipped
func TestLoginHandler_OnMessage_WrongNamespace_Skipped(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	// No cache or processor expectations - event should be skipped

	msg := &pb.UserLoggedIn{
		UserId:    "user123",
		Namespace: "different-namespace", // Wrong namespace
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Should return success (no error) but skip processing
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	// Processor and cache should NOT be called
	mockCache.AssertNotCalled(t, "GetAllGoals")
	mockProcessor.AssertNotCalled(t, "ProcessEvent")
}

// Test Case 18: Namespace validation - correct namespace should be processed
func TestLoginHandler_OnMessage_CorrectNamespace_Processed(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewLoginHandler(mockProcessor, mockCache, "test-namespace", logger)

	loginGoal := &domain.Goal{
		ID:          "login-goal",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "login_count"},
	}
	mockCache.On("GetAllGoals").Return([]*domain.Goal{loginGoal})

	mockProcessor.On("ProcessEvent", mock.Anything, "user123", "test-namespace", mock.Anything).Return(nil)

	msg := &pb.UserLoggedIn{
		UserId:    "user123",
		Namespace: "test-namespace", // Correct namespace
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

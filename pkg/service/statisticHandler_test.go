package service

import (
	"context"
	"errors"
	"os"
	"testing"

	statpb "extend-challenge-event-handler/pkg/pb/accelbyte-asyncapi/social/statistic/v1"

	"github.com/AccelByte/extend-challenge-common/pkg/domain"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Test Case 1: Valid statistic event with single goal → processes successfully
func TestStatisticHandler_OnMessage_SingleStatGoal_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Mock goals: one statistic goal
	statGoal := &domain.Goal{
		ID:          "kill-10",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{
			StatCode: "kill_count",
		},
	}
	mockCache.On("GetGoalsByStatCode", "kill_count").Return([]*domain.Goal{statGoal})

	// Expect single ProcessEvent call with statUpdates map
	expectedStatUpdates := map[string]int{"kill_count": 50}
	mockProcessor.On("ProcessEvent", mock.Anything, "user123", "test-namespace", expectedStatUpdates).Return(nil)

	// Execute
	msg := &statpb.StatItemUpdated{
		UserId:    "user123",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "kill_count",
			LatestValue: 50.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 2: Valid statistic event with multiple goals (same stat_code) → processes all
func TestStatisticHandler_OnMessage_MultipleStatGoals_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Mock goals: three statistic goals tracking same stat_code
	statGoal1 := &domain.Goal{
		ID:          "kill-10",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}
	statGoal2 := &domain.Goal{
		ID:          "kill-50",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}
	statGoal3 := &domain.Goal{
		ID:          "kill-100",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}

	mockCache.On("GetGoalsByStatCode", "kill_count").Return([]*domain.Goal{statGoal1, statGoal2, statGoal3})

	// Expect single call with stat code and value
	expectedStatUpdates := map[string]int{"kill_count": 75}
	mockProcessor.On("ProcessEvent", mock.Anything, "user456", "test-namespace", expectedStatUpdates).Return(nil)

	// Execute
	msg := &statpb.StatItemUpdated{
		UserId:    "user456",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "kill_count",
			LatestValue: 75.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 3: Nil message → returns InvalidArgument error
func TestStatisticHandler_OnMessage_NilMessage_ReturnsError(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

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
func TestStatisticHandler_OnMessage_EmptyUserID_ReturnsError(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Execute
	msg := &statpb.StatItemUpdated{
		UserId: "",
		Payload: &statpb.StatItem{
			StatCode:    "kill_count",
			LatestValue: 50.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, resp)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "userId cannot be empty")
}

// Test Case 5: Nil payload → returns InvalidArgument error
func TestStatisticHandler_OnMessage_NilPayload_ReturnsError(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Execute
	msg := &statpb.StatItemUpdated{
		UserId:  "user123",
		Payload: nil,
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, resp)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "payload cannot be nil")
}

// Test Case 6: Empty statCode → returns InvalidArgument error (Q7 decision)
func TestStatisticHandler_OnMessage_EmptyStatCode_ReturnsError(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Execute
	msg := &statpb.StatItemUpdated{
		UserId:    "user123",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "",
			LatestValue: 50.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, resp)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "statCode cannot be empty")
}

// Test Case 7: No statistic goals for stat_code → returns success (no-op)
func TestStatisticHandler_OnMessage_NoStatGoals_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Mock cache returns empty list
	mockCache.On("GetGoalsByStatCode", "unknown_stat").Return([]*domain.Goal{})

	// Execute
	msg := &statpb.StatItemUpdated{
		UserId:    "user789",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "unknown_stat",
			LatestValue: 100.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	// Processor should not be called
	mockProcessor.AssertNotCalled(t, "ProcessEvent")
}

// Test Case 8: Goal cache returns nil → logs warning, returns success
func TestStatisticHandler_OnMessage_CacheReturnsNil_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Mock cache returns nil
	mockCache.On("GetGoalsByStatCode", "kill_count").Return(nil)

	// Execute
	msg := &statpb.StatItemUpdated{
		UserId:    "user101",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "kill_count",
			LatestValue: 50.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertNotCalled(t, "ProcessEvent")
}

// Test Case 9: Mixed event sources (statistic + login goals) → only processes statistic goals (Q6 decision)
func TestStatisticHandler_OnMessage_MixedEventSources_OnlyProcessesStatistic(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	// Mock goals: mix of statistic and login
	statGoal := &domain.Goal{
		ID:          "kill-10",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}
	loginGoal1 := &domain.Goal{
		ID:          "daily-login",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "kill_count"}, // Same stat code but wrong source
	}
	loginGoal2 := &domain.Goal{
		ID:          "login-streak",
		EventSource: domain.EventSourceLogin,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}

	mockCache.On("GetGoalsByStatCode", "kill_count").Return([]*domain.Goal{statGoal, loginGoal1, loginGoal2})

	// Only statistic goal should be processed
	expectedStatUpdates := map[string]int{"kill_count": 25}
	mockProcessor.On("ProcessEvent", mock.Anything, "user202", "test-namespace", expectedStatUpdates).Return(nil)

	// Execute
	msg := &statpb.StatItemUpdated{
		UserId:    "user202",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "kill_count",
			LatestValue: 25.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 10: EventProcessor returns error → returns Internal error for retry
func TestStatisticHandler_OnMessage_ProcessorError_ReturnsInternalError(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "kill-10",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}
	mockCache.On("GetGoalsByStatCode", "kill_count").Return([]*domain.Goal{statGoal})

	// Simulate buffer full error
	expectedStatUpdates := map[string]int{"kill_count": 50}
	mockProcessor.On("ProcessEvent", mock.Anything, "user303", "test-namespace", expectedStatUpdates).Return(errors.New("buffer full"))

	// Execute
	msg := &statpb.StatItemUpdated{
		UserId:    "user303",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "kill_count",
			LatestValue: 50.0,
		},
	}
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

// Test Case 11: Multiple events for same user → all processed (deduplication in buffer)
func TestStatisticHandler_OnMessage_MultipleEventsForSameUser_AllProcessed(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "kill-10",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}
	mockCache.On("GetGoalsByStatCode", "kill_count").Return([]*domain.Goal{statGoal}).Times(3)

	// Expect 3 calls with different values
	mockProcessor.On("ProcessEvent", mock.Anything, "user404", "test-namespace", map[string]int{"kill_count": 10}).Return(nil).Once()
	mockProcessor.On("ProcessEvent", mock.Anything, "user404", "test-namespace", map[string]int{"kill_count": 20}).Return(nil).Once()
	mockProcessor.On("ProcessEvent", mock.Anything, "user404", "test-namespace", map[string]int{"kill_count": 30}).Return(nil).Once()

	// Execute: same user, 3 stat updates
	values := []float64{10.0, 20.0, 30.0}
	for _, val := range values {
		msg := &statpb.StatItemUpdated{
			UserId:    "user404",
			Namespace: "test-namespace",
			Payload: &statpb.StatItem{
				StatCode:    "kill_count",
				LatestValue: val,
			},
		}
		resp, err := handler.OnMessage(context.Background(), msg)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	}

	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 12: Statistic goal with absolute type → processes correctly
func TestStatisticHandler_OnMessage_AbsoluteTypeStatGoal_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "reach-level-5",
		EventSource: domain.EventSourceStatistic,
		Type:        domain.GoalTypeAbsolute,
		Requirement: domain.Requirement{StatCode: "player_level"},
	}
	mockCache.On("GetGoalsByStatCode", "player_level").Return([]*domain.Goal{statGoal})

	expectedStatUpdates := map[string]int{"player_level": 5}
	mockProcessor.On("ProcessEvent", mock.Anything, "user505", "test-namespace", expectedStatUpdates).Return(nil)

	msg := &statpb.StatItemUpdated{
		UserId:    "user505",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "player_level",
			LatestValue: 5.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 13: Statistic goal with increment type → processes correctly
func TestStatisticHandler_OnMessage_IncrementTypeStatGoal_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "kill-100",
		EventSource: domain.EventSourceStatistic,
		Type:        domain.GoalTypeIncrement,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}
	mockCache.On("GetGoalsByStatCode", "kill_count").Return([]*domain.Goal{statGoal})

	expectedStatUpdates := map[string]int{"kill_count": 75}
	mockProcessor.On("ProcessEvent", mock.Anything, "user606", "test-namespace", expectedStatUpdates).Return(nil)

	msg := &statpb.StatItemUpdated{
		UserId:    "user606",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "kill_count",
			LatestValue: 75.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 14: Statistic goal with daily type → processes correctly
func TestStatisticHandler_OnMessage_DailyTypeStatGoal_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "daily-match",
		EventSource: domain.EventSourceStatistic,
		Type:        domain.GoalTypeDaily,
		Requirement: domain.Requirement{StatCode: "matches_played"},
	}
	mockCache.On("GetGoalsByStatCode", "matches_played").Return([]*domain.Goal{statGoal})

	expectedStatUpdates := map[string]int{"matches_played": 3}
	mockProcessor.On("ProcessEvent", mock.Anything, "user707", "test-namespace", expectedStatUpdates).Return(nil)

	msg := &statpb.StatItemUpdated{
		UserId:    "user707",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "matches_played",
			LatestValue: 3.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 15: Context cancellation → context is passed to ProcessEvent
func TestStatisticHandler_OnMessage_ContextPassed_Success(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "kill-10",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}
	mockCache.On("GetGoalsByStatCode", "kill_count").Return([]*domain.Goal{statGoal})

	expectedStatUpdates := map[string]int{"kill_count": 50}
	mockProcessor.On("ProcessEvent", mock.Anything, "user808", "test-namespace", expectedStatUpdates).Return(nil)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	msg := &statpb.StatItemUpdated{
		UserId:    "user808",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "kill_count",
			LatestValue: 50.0,
		},
	}
	resp, err := handler.OnMessage(ctx, msg)

	// Context is passed to ProcessEvent, but current implementation doesn't check it
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 16: Concurrent statistic events for different users → all processed
func TestStatisticHandler_OnMessage_ConcurrentDifferentUsers_AllProcessed(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "kill-10",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "kill_count"},
	}
	mockCache.On("GetGoalsByStatCode", "kill_count").Return([]*domain.Goal{statGoal}).Times(5)

	expectedStatUpdates := map[string]int{"kill_count": 10}

	// Mock processor for 5 different users
	mockProcessor.On("ProcessEvent", mock.Anything, "user1", "test-namespace", expectedStatUpdates).Return(nil).Once()
	mockProcessor.On("ProcessEvent", mock.Anything, "user2", "test-namespace", expectedStatUpdates).Return(nil).Once()
	mockProcessor.On("ProcessEvent", mock.Anything, "user3", "test-namespace", expectedStatUpdates).Return(nil).Once()
	mockProcessor.On("ProcessEvent", mock.Anything, "user4", "test-namespace", expectedStatUpdates).Return(nil).Once()
	mockProcessor.On("ProcessEvent", mock.Anything, "user5", "test-namespace", expectedStatUpdates).Return(nil).Once()

	// Execute concurrently
	done := make(chan bool, 5)
	for i := 1; i <= 5; i++ {
		go func(idx int) {
			userID := "user" + string(rune('0'+idx))
			msg := &statpb.StatItemUpdated{
				UserId:    userID,
				Namespace: "test-namespace",
				Payload: &statpb.StatItem{
					StatCode:    "kill_count",
					LatestValue: 10.0,
				},
			}
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

// Test Case 17: Double value truncation → int(42.7) = 42 (Q1 decision)
func TestStatisticHandler_OnMessage_DoubleToIntTruncation(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "stat-goal",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "test_stat"},
	}
	mockCache.On("GetGoalsByStatCode", "test_stat").Return([]*domain.Goal{statGoal})

	// Expect truncated value: int(42.7) = 42
	expectedStatUpdates := map[string]int{"test_stat": 42}
	mockProcessor.On("ProcessEvent", mock.Anything, "user909", "test-namespace", expectedStatUpdates).Return(nil)

	msg := &statpb.StatItemUpdated{
		UserId:    "user909",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "test_stat",
			LatestValue: 42.7, // Double value with decimal
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 18: Negative stat value → passed to EventProcessor (Q2 decision: no rejection at handler level)
func TestStatisticHandler_OnMessage_NegativeValue_PassedToProcessor(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "health-stat",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "health"},
	}
	mockCache.On("GetGoalsByStatCode", "health").Return([]*domain.Goal{statGoal})

	// Negative value should be passed to EventProcessor without rejection
	expectedStatUpdates := map[string]int{"health": -10}
	mockProcessor.On("ProcessEvent", mock.Anything, "user1010", "test-namespace", expectedStatUpdates).Return(nil)

	msg := &statpb.StatItemUpdated{
		UserId:    "user1010",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "health",
			LatestValue: -10.0, // Negative value
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 19: INT_MAX value handling
func TestStatisticHandler_OnMessage_IntMaxValue(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "max-stat",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "max_value"},
	}
	mockCache.On("GetGoalsByStatCode", "max_value").Return([]*domain.Goal{statGoal})

	// INT_MAX = 2147483647
	expectedStatUpdates := map[string]int{"max_value": 2147483647}
	mockProcessor.On("ProcessEvent", mock.Anything, "user-max", "test-namespace", expectedStatUpdates).Return(nil)

	msg := &statpb.StatItemUpdated{
		UserId:    "user-max",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "max_value",
			LatestValue: 2147483647.0, // INT_MAX as double
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 20: Truncation edge cases (values close to 0 and 1)
func TestStatisticHandler_OnMessage_TruncationEdgeCases(t *testing.T) {
	testCases := []struct {
		name          string
		inputValue    float64
		expectedValue int
	}{
		{"zero point nine", 0.9, 0},
		{"one point one", 1.1, 1},
		{"zero point zero one", 0.01, 0},
		{"nine nine nine point nine nine", 999.99, 999},
		{"exact zero", 0.0, 0},
		{"exact one", 1.0, 1},
		{"negative zero point nine", -0.9, 0},
		{"negative one point one", -1.1, -1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockProcessor := new(MockEventProcessor)
			mockCache := new(MockGoalCache)
			logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

			handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

			statGoal := &domain.Goal{
				ID:          "trunc-goal",
				EventSource: domain.EventSourceStatistic,
				Requirement: domain.Requirement{StatCode: "trunc_stat"},
			}
			mockCache.On("GetGoalsByStatCode", "trunc_stat").Return([]*domain.Goal{statGoal})

			expectedStatUpdates := map[string]int{"trunc_stat": tc.expectedValue}
			mockProcessor.On("ProcessEvent", mock.Anything, "user-trunc", "test-namespace", expectedStatUpdates).Return(nil)

			msg := &statpb.StatItemUpdated{
				UserId:    "user-trunc",
				Namespace: "test-namespace",
				Payload: &statpb.StatItem{
					StatCode:    "trunc_stat",
					LatestValue: tc.inputValue,
				},
			}
			resp, err := handler.OnMessage(context.Background(), msg)

			assert.NoError(t, err)
			assert.NotNil(t, resp)
			mockCache.AssertExpectations(t)
			mockProcessor.AssertExpectations(t)
		})
	}
}

// Test Case 21: Very large double value
func TestStatisticHandler_OnMessage_VeryLargeValue(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "large-stat",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "large_value"},
	}
	mockCache.On("GetGoalsByStatCode", "large_value").Return([]*domain.Goal{statGoal})

	// Test with a very large value
	largeValue := 999999999999.99 // Just under 1 trillion
	expectedStatUpdates := map[string]int{"large_value": int(largeValue)}
	mockProcessor.On("ProcessEvent", mock.Anything, "user-large", "test-namespace", expectedStatUpdates).Return(nil)

	msg := &statpb.StatItemUpdated{
		UserId:    "user-large",
		Namespace: "test-namespace",
		Payload: &statpb.StatItem{
			StatCode:    "large_value",
			LatestValue: largeValue,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// Test Case 22: Namespace validation - wrong namespace should be skipped
func TestStatisticHandler_OnMessage_WrongNamespace_Skipped(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	// No cache or processor expectations - event should be skipped

	msg := &statpb.StatItemUpdated{
		UserId:    "user123",
		Namespace: "different-namespace", // Wrong namespace
		Payload: &statpb.StatItem{
			StatCode:    "test_stat",
			LatestValue: 100.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	// Should return success (no error) but skip processing
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	// Processor and cache should NOT be called
	mockCache.AssertNotCalled(t, "GetGoalsByStatCode")
	mockProcessor.AssertNotCalled(t, "ProcessEvent")
}

// Test Case 23: Namespace validation - correct namespace should be processed
func TestStatisticHandler_OnMessage_CorrectNamespace_Processed(t *testing.T) {
	mockProcessor := new(MockEventProcessor)
	mockCache := new(MockGoalCache)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	handler := NewStatisticHandler(mockProcessor, mockCache, "test-namespace", logger)

	statGoal := &domain.Goal{
		ID:          "stat-goal",
		EventSource: domain.EventSourceStatistic,
		Requirement: domain.Requirement{StatCode: "test_stat"},
	}
	mockCache.On("GetGoalsByStatCode", "test_stat").Return([]*domain.Goal{statGoal})

	expectedStatUpdates := map[string]int{"test_stat": 100}
	mockProcessor.On("ProcessEvent", mock.Anything, "user123", "test-namespace", expectedStatUpdates).Return(nil)

	msg := &statpb.StatItemUpdated{
		UserId:    "user123",
		Namespace: "test-namespace", // Correct namespace
		Payload: &statpb.StatItem{
			StatCode:    "test_stat",
			LatestValue: 100.0,
		},
	}
	resp, err := handler.OnMessage(context.Background(), msg)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockCache.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

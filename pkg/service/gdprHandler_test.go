package service

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	pb "extend-challenge-event-handler/pkg/pb/accelbyte-asyncapi/iam/account/v1"

	"github.com/AccelByte/extend-challenge-common/pkg/domain"
	"github.com/AccelByte/extend-challenge-common/pkg/repository"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockGoalRepository is a mock implementation of GoalRepository for GDPR handler tests.
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

func (m *MockGoalRepository) BatchUpsertGoalActive(ctx context.Context, progresses []*domain.UserGoalProgress) error {
	args := m.Called(ctx, progresses)
	return args.Error(0)
}

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

func (m *MockGoalRepository) DeleteExpiredRows(ctx context.Context, namespace string, cutoff time.Time, batchSize int) (int64, error) {
	args := m.Called(ctx, namespace, cutoff, batchSize)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockGoalRepository) DeleteUserData(ctx context.Context, namespace string, userID string) (int64, error) {
	args := m.Called(ctx, namespace, userID)
	return args.Get(0).(int64), args.Error(1)
}

func testGDPRLogger() zerolog.Logger {
	return zerolog.New(os.Stdout).Level(zerolog.DebugLevel)
}

func TestGDPRDeletionHandler_Success(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockRepo.On("DeleteUserData", mock.Anything, "test-ns", "user-123").Return(int64(5), nil)

	handler := NewGDPRDeletionHandler(mockRepo, "test-ns", testGDPRLogger())

	resp, err := handler.OnMessage(context.Background(), &pb.GdprRequestDataDeletionResponse{
		Payload: &pb.GdprRequestDataDeletionResponsePayloadData{
			DeletionGdpr: &pb.DeletionGDPR{
				UserId:    "user-123",
				Namespace: "test-ns",
			},
		},
		Namespace: "test-ns",
		UserId:    "user-123",
	})

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockRepo.AssertExpectations(t)
}

func TestGDPRDeletionHandler_NilMessage(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	handler := NewGDPRDeletionHandler(mockRepo, "test-ns", testGDPRLogger())

	resp, err := handler.OnMessage(context.Background(), nil)

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGDPRDeletionHandler_EmptyUserID(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	handler := NewGDPRDeletionHandler(mockRepo, "test-ns", testGDPRLogger())

	resp, err := handler.OnMessage(context.Background(), &pb.GdprRequestDataDeletionResponse{
		Namespace: "test-ns",
	})

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGDPRDeletionHandler_WrongNamespace(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	handler := NewGDPRDeletionHandler(mockRepo, "test-ns", testGDPRLogger())

	resp, err := handler.OnMessage(context.Background(), &pb.GdprRequestDataDeletionResponse{
		Namespace: "other-ns",
		UserId:    "user-123",
	})

	// Should skip (return empty, no error)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockRepo.AssertNotCalled(t, "DeleteUserData")
}

func TestGDPRDeletionHandler_DBError(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockRepo.On("DeleteUserData", mock.Anything, "test-ns", "user-123").Return(int64(0), errors.New("db error"))

	handler := NewGDPRDeletionHandler(mockRepo, "test-ns", testGDPRLogger())

	resp, err := handler.OnMessage(context.Background(), &pb.GdprRequestDataDeletionResponse{
		Namespace: "test-ns",
		UserId:    "user-123",
	})

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.Internal, status.Code(err))
	mockRepo.AssertExpectations(t)
}

func TestGDPRDeletionHandler_ZeroRows(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockRepo.On("DeleteUserData", mock.Anything, "test-ns", "user-new").Return(int64(0), nil)

	handler := NewGDPRDeletionHandler(mockRepo, "test-ns", testGDPRLogger())

	resp, err := handler.OnMessage(context.Background(), &pb.GdprRequestDataDeletionResponse{
		Namespace: "test-ns",
		UserId:    "user-new",
	})

	// Zero rows is success (user had no data)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockRepo.AssertExpectations(t)
}

func TestGDPRDeletionHandler_FallbackToTopLevelUserID(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockRepo.On("DeleteUserData", mock.Anything, "test-ns", "user-456").Return(int64(1), nil)

	handler := NewGDPRDeletionHandler(mockRepo, "test-ns", testGDPRLogger())

	// No payload, only top-level UserId
	resp, err := handler.OnMessage(context.Background(), &pb.GdprRequestDataDeletionResponse{
		Namespace: "test-ns",
		UserId:    "user-456",
	})

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockRepo.AssertExpectations(t)
}

func TestGDPRDeletionHandler_PayloadWithEmptyGdprUserID(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockRepo.On("DeleteUserData", mock.Anything, "test-ns", "user-top").Return(int64(3), nil)

	handler := NewGDPRDeletionHandler(mockRepo, "test-ns", testGDPRLogger())

	// Payload with DeletionGdpr that has empty UserId — should fall back to top-level
	resp, err := handler.OnMessage(context.Background(), &pb.GdprRequestDataDeletionResponse{
		Payload: &pb.GdprRequestDataDeletionResponsePayloadData{
			DeletionGdpr: &pb.DeletionGDPR{
				UserId:    "",
				Namespace: "test-ns",
			},
		},
		Namespace: "test-ns",
		UserId:    "user-top",
	})

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockRepo.AssertExpectations(t)
}

func TestGDPRDeletionHandler_PayloadWithNilDeletionGdpr(t *testing.T) {
	mockRepo := new(MockGoalRepository)
	mockRepo.On("DeleteUserData", mock.Anything, "test-ns", "user-fallback").Return(int64(1), nil)

	handler := NewGDPRDeletionHandler(mockRepo, "test-ns", testGDPRLogger())

	// Payload exists but DeletionGdpr is nil — should fall back to top-level UserId
	resp, err := handler.OnMessage(context.Background(), &pb.GdprRequestDataDeletionResponse{
		Payload: &pb.GdprRequestDataDeletionResponsePayloadData{
			DeletionGdpr: nil,
		},
		Namespace: "test-ns",
		UserId:    "user-fallback",
	})

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockRepo.AssertExpectations(t)
}
